from __future__ import annotations

SCHEMA_VERSION = 43
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

SUPERVISION_SCHEMA = """
ALTER TABLE runs ADD COLUMN supervision_policy_json TEXT NOT NULL DEFAULT
    '{"attempt_idle_escalation_seconds":[],"attempt_lease_expired_escalation_seconds":[],"auto_execute":false,"interaction_escalation_seconds":[],"other_attention_escalation_seconds":[],"retry_backoff_seconds":[5,30,300],"schema_version":1}';

CREATE TABLE supervisor_actions (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN ('run.execute', 'attempt.reconcile', 'run.reconcile')
    ),
    subject_id TEXT NOT NULL,
    subject_version INTEGER NOT NULL CHECK (subject_version >= 1),
    run_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'claimed', 'succeeded', 'exhausted')
    ),
    version INTEGER NOT NULL CHECK (version >= 1),
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 9),
    retry_backoff_json TEXT NOT NULL,
    available_at TEXT NOT NULL,
    owner_id TEXT,
    lease_expires_at TEXT,
    last_error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE (kind, subject_id, subject_version),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    CHECK (
        (
            state = 'pending' AND owner_id IS NULL
            AND lease_expires_at IS NULL AND finished_at IS NULL
        ) OR (
            state = 'claimed' AND owner_id IS NOT NULL
            AND lease_expires_at IS NOT NULL AND finished_at IS NULL
        ) OR (
            state IN ('succeeded', 'exhausted') AND owner_id IS NULL
            AND lease_expires_at IS NULL AND finished_at IS NOT NULL
        )
    )
) STRICT;

CREATE TABLE supervisor_escalations (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (
        kind IN (
            'interaction.pending', 'attempt.idle',
            'attempt.lease_expired', 'run.missing_active_attempt',
            'run.waiting_without_interaction', 'authority.unavailable'
        )
    ),
    subject_type TEXT NOT NULL CHECK (
        subject_type IN ('interaction', 'attempt', 'run', 'provider_grant')
    ),
    subject_id TEXT NOT NULL,
    threshold_seconds INTEGER NOT NULL CHECK (
        threshold_seconds BETWEEN 0 AND 2592000
    ),
    age_started_at TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel IN ('control_room', 'krab_reminder')),
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'claimed', 'delivered', 'dismissed', 'exhausted')
    ),
    version INTEGER NOT NULL CHECK (version >= 1),
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 9),
    retry_backoff_json TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    available_at TEXT NOT NULL,
    owner_id TEXT,
    lease_expires_at TEXT,
    last_error_code TEXT,
    krab_event_seq INTEGER CHECK (krab_event_seq IS NULL OR krab_event_seq >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    delivered_at TEXT,
    UNIQUE (kind, subject_id, threshold_seconds),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    CHECK (
        (
            state = 'pending' AND owner_id IS NULL
            AND lease_expires_at IS NULL AND delivered_at IS NULL
        ) OR (
            state = 'claimed' AND owner_id IS NOT NULL
            AND lease_expires_at IS NOT NULL AND delivered_at IS NULL
        ) OR (
            state IN ('delivered', 'dismissed', 'exhausted')
            AND owner_id IS NULL AND lease_expires_at IS NULL
            AND delivered_at IS NOT NULL
        )
    ),
    CHECK (channel != 'control_room' OR state = 'delivered')
) STRICT;

CREATE INDEX supervisor_action_due
    ON supervisor_actions(state, available_at, lease_expires_at);
CREATE INDEX supervisor_action_run
    ON supervisor_actions(run_id, created_at DESC);
CREATE INDEX supervisor_escalation_due
    ON supervisor_escalations(state, available_at, lease_expires_at);
CREATE INDEX supervisor_escalation_run
    ON supervisor_escalations(run_id, created_at DESC);

CREATE TRIGGER run_supervision_policy_immutable
BEFORE UPDATE OF supervision_policy_json ON runs
WHEN NEW.supervision_policy_json IS NOT OLD.supervision_policy_json
BEGIN
    SELECT RAISE(ABORT, 'run supervision policy is immutable');
END;
CREATE TRIGGER supervisor_action_identity_immutable
BEFORE UPDATE OF
    id, kind, subject_id, subject_version, run_id, max_attempts,
    retry_backoff_json, created_at
ON supervisor_actions
BEGIN
    SELECT RAISE(ABORT, 'supervisor action identity is immutable');
END;
CREATE TRIGGER supervisor_action_immutable_delete
BEFORE DELETE ON supervisor_actions
BEGIN
    SELECT RAISE(ABORT, 'supervisor actions cannot be deleted');
END;
CREATE TRIGGER supervisor_escalation_identity_immutable
BEFORE UPDATE OF
    id, run_id, kind, subject_type, subject_id, threshold_seconds,
    age_started_at, channel, max_attempts, retry_backoff_json,
    idempotency_key, created_at
ON supervisor_escalations
BEGIN
    SELECT RAISE(ABORT, 'supervisor escalation identity is immutable');
END;
CREATE TRIGGER supervisor_escalation_immutable_delete
BEFORE DELETE ON supervisor_escalations
BEGIN
    SELECT RAISE(ABORT, 'supervisor escalations cannot be deleted');
END;
"""

CANDIDATE_SCHEMA = """
CREATE TABLE integration_candidates (
    id TEXT PRIMARY KEY,
    attempt_result_id TEXT NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    manifest_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    project_id TEXT NOT NULL,
    context_version_id TEXT NOT NULL,
    work_thread_id TEXT NOT NULL,
    producer_run_id TEXT NOT NULL,
    producer_attempt_id TEXT NOT NULL,
    base_revision TEXT NOT NULL,
    head_revision TEXT NOT NULL,
    tree_revision TEXT NOT NULL,
    object_format TEXT NOT NULL CHECK (object_format IN ('sha1', 'sha256')),
    result_content_sha256 TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (attempt_result_id)
        REFERENCES attempt_results(id) ON DELETE RESTRICT,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (context_version_id)
        REFERENCES project_context_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_thread_id) REFERENCES work_threads(id) ON DELETE RESTRICT,
    FOREIGN KEY (producer_run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (producer_attempt_id)
        REFERENCES run_attempts(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (object_format = 'sha1'
            AND length(base_revision) = 40
            AND length(head_revision) = 40
            AND length(tree_revision) = 40)
        OR
        (object_format = 'sha256'
            AND length(base_revision) = 64
            AND length(head_revision) = 64
            AND length(tree_revision) = 64)
    )
) STRICT;

CREATE INDEX integration_candidate_thread_created
    ON integration_candidates(work_thread_id, created_at DESC, id DESC);
CREATE INDEX integration_candidate_project_created
    ON integration_candidates(project_id, created_at DESC, id DESC);
CREATE INDEX integration_candidate_run_created
    ON integration_candidates(producer_run_id, created_at DESC, id DESC);
CREATE INDEX integration_candidate_created
    ON integration_candidates(created_at DESC, id DESC);

CREATE TRIGGER integration_candidate_immutable_update
BEFORE UPDATE ON integration_candidates
BEGIN
    SELECT RAISE(ABORT, 'integration candidates are immutable');
END;
CREATE TRIGGER integration_candidate_immutable_delete
BEFORE DELETE ON integration_candidates
BEGIN
    SELECT RAISE(ABORT, 'integration candidates cannot be deleted');
END;
"""

PROVIDER_DISPATCH_SCHEMA = """
CREATE TABLE provider_dispatch_reservations (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    provider_grant_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    reserved_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (provider_grant_id)
        REFERENCES provider_grants(id) ON DELETE RESTRICT,
    FOREIGN KEY (attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT,
    CHECK (expires_at > reserved_at)
) STRICT;

CREATE INDEX provider_dispatch_grant_expiry
    ON provider_dispatch_reservations(provider_grant_id, expires_at);
"""

GATE_SCHEMA = """
CREATE TABLE integration_gate_policies (
    id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    manifest_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    repository_identity TEXT NOT NULL,
    project_id TEXT NOT NULL,
    context_version_id TEXT NOT NULL,
    work_thread_id TEXT NOT NULL,
    integration_ref TEXT NOT NULL,
    expected_base_revision TEXT NOT NULL,
    head_revision TEXT NOT NULL,
    tree_revision TEXT NOT NULL,
    object_format TEXT NOT NULL CHECK (object_format IN ('sha1', 'sha256')),
    policy_revision TEXT NOT NULL,
    review_separation TEXT CHECK (
        review_separation IS NULL OR review_separation IN (
            'different_profile', 'different_identity',
            'different_profile_and_identity'
        )
    ),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (context_version_id)
        REFERENCES project_context_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_thread_id) REFERENCES work_threads(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (object_format = 'sha1'
            AND length(expected_base_revision) = 40
            AND length(head_revision) = 40
            AND length(tree_revision) = 40)
        OR
        (object_format = 'sha256'
            AND length(expected_base_revision) = 64
            AND length(head_revision) = 64
            AND length(tree_revision) = 64)
    )
) STRICT;

CREATE INDEX integration_gate_policy_candidate_created
    ON integration_gate_policies(candidate_id, created_at DESC, id DESC);
CREATE INDEX integration_gate_policy_project_created
    ON integration_gate_policies(project_id, created_at DESC, id DESC);
CREATE INDEX integration_gate_policy_thread_created
    ON integration_gate_policies(work_thread_id, created_at DESC, id DESC);

CREATE TABLE integration_gate_review_runs (
    run_id TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE integration_gate_evidence (
    id TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    kind TEXT NOT NULL CHECK (
        kind IN ('independent_review', 'local_validation')
    ),
    gate_name TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('passed', 'failed')),
    evidence_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    repository_identity TEXT NOT NULL,
    integration_ref TEXT NOT NULL,
    expected_base_revision TEXT NOT NULL,
    head_revision TEXT NOT NULL,
    tree_revision TEXT NOT NULL,
    object_format TEXT NOT NULL CHECK (object_format IN ('sha1', 'sha256')),
    policy_revision TEXT NOT NULL,
    reviewer_run_id TEXT,
    reviewer_attempt_id TEXT,
    recorded_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (policy_id, sequence),
    UNIQUE (policy_id, content_sha256),
    FOREIGN KEY (policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (reviewer_run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (reviewer_attempt_id)
        REFERENCES run_attempts(id) ON DELETE RESTRICT,
    FOREIGN KEY (recorded_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (kind = 'independent_review'
            AND reviewer_run_id IS NOT NULL
            AND reviewer_attempt_id IS NOT NULL)
        OR
        (kind = 'local_validation'
            AND reviewer_run_id IS NULL
            AND reviewer_attempt_id IS NULL)
    ),
    CHECK (
        (object_format = 'sha1'
            AND length(expected_base_revision) = 40
            AND length(head_revision) = 40
            AND length(tree_revision) = 40)
        OR
        (object_format = 'sha256'
            AND length(expected_base_revision) = 64
            AND length(head_revision) = 64
            AND length(tree_revision) = 64)
    )
) STRICT;

CREATE INDEX integration_gate_evidence_policy_sequence
    ON integration_gate_evidence(policy_id, sequence DESC);
CREATE INDEX integration_gate_evidence_policy_gate_sequence
    ON integration_gate_evidence(policy_id, gate_name, sequence DESC);
CREATE INDEX integration_gate_evidence_candidate_created
    ON integration_gate_evidence(candidate_id, created_at DESC, id DESC);

CREATE TRIGGER integration_gate_policy_immutable_update
BEFORE UPDATE ON integration_gate_policies
BEGIN
    SELECT RAISE(ABORT, 'integration gate policies are immutable');
END;
CREATE TRIGGER integration_gate_policy_immutable_delete
BEFORE DELETE ON integration_gate_policies
BEGIN
    SELECT RAISE(ABORT, 'integration gate policies cannot be deleted');
END;
CREATE TRIGGER integration_gate_review_run_immutable_update
BEFORE UPDATE ON integration_gate_review_runs
BEGIN
    SELECT RAISE(ABORT, 'integration gate review runs are immutable');
END;
CREATE TRIGGER integration_gate_review_run_immutable_delete
BEFORE DELETE ON integration_gate_review_runs
BEGIN
    SELECT RAISE(ABORT, 'integration gate review runs cannot be deleted');
END;
CREATE TRIGGER integration_gate_evidence_immutable_update
BEFORE UPDATE ON integration_gate_evidence
BEGIN
    SELECT RAISE(ABORT, 'integration gate evidence is immutable');
END;
CREATE TRIGGER integration_gate_evidence_immutable_delete
BEFORE DELETE ON integration_gate_evidence
BEGIN
    SELECT RAISE(ABORT, 'integration gate evidence cannot be deleted');
END;
"""

GATE_CHANGE_SCHEMA = """
CREATE TABLE integration_gate_change_runs (
    run_id TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    review_evidence_id TEXT NOT NULL,
    review_entry_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (review_evidence_id)
        REFERENCES integration_gate_evidence(id) ON DELETE RESTRICT,
    FOREIGN KEY (review_entry_id)
        REFERENCES work_log_entries(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX integration_gate_change_evidence
    ON integration_gate_change_runs(review_evidence_id, created_at DESC);

CREATE TRIGGER integration_gate_change_run_immutable_update
BEFORE UPDATE ON integration_gate_change_runs
BEGIN
    SELECT RAISE(ABORT, 'integration gate change runs are immutable');
END;
CREATE TRIGGER integration_gate_change_run_immutable_delete
BEFORE DELETE ON integration_gate_change_runs
BEGIN
    SELECT RAISE(ABORT, 'integration gate change runs cannot be deleted');
END;
"""

MAINTAINER_REVIEW_SCHEMA = """
CREATE TABLE integration_maintainer_reviews (
    id TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    review_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('approved', 'changes_requested')),
    candidate_id TEXT NOT NULL,
    repository_identity TEXT NOT NULL,
    integration_ref TEXT NOT NULL,
    expected_base_revision TEXT NOT NULL,
    head_revision TEXT NOT NULL,
    tree_revision TEXT NOT NULL,
    object_format TEXT NOT NULL CHECK (object_format IN ('sha1', 'sha256')),
    policy_revision TEXT NOT NULL,
    recorded_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (policy_id, sequence),
    FOREIGN KEY (policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (recorded_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (object_format = 'sha1'
            AND length(expected_base_revision) = 40
            AND length(head_revision) = 40
            AND length(tree_revision) = 40)
        OR
        (object_format = 'sha256'
            AND length(expected_base_revision) = 64
            AND length(head_revision) = 64
            AND length(tree_revision) = 64)
    )
) STRICT;

CREATE INDEX integration_maintainer_review_policy_sequence
    ON integration_maintainer_reviews(policy_id, sequence DESC);
CREATE INDEX integration_maintainer_review_candidate_created
    ON integration_maintainer_reviews(candidate_id, created_at DESC, id DESC);

CREATE TABLE integration_gate_maintainer_change_runs (
    run_id TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    maintainer_review_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (maintainer_review_id)
        REFERENCES integration_maintainer_reviews(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX integration_gate_maintainer_change_review
    ON integration_gate_maintainer_change_runs(
        maintainer_review_id, created_at DESC
    );

CREATE TRIGGER integration_maintainer_review_immutable_update
BEFORE UPDATE ON integration_maintainer_reviews
BEGIN
    SELECT RAISE(ABORT, 'maintainer reviews are immutable');
END;
CREATE TRIGGER integration_maintainer_review_immutable_delete
BEFORE DELETE ON integration_maintainer_reviews
BEGIN
    SELECT RAISE(ABORT, 'maintainer reviews cannot be deleted');
END;
CREATE TRIGGER integration_gate_maintainer_change_run_immutable_update
BEFORE UPDATE ON integration_gate_maintainer_change_runs
BEGIN
    SELECT RAISE(ABORT, 'maintainer change-run bindings are immutable');
END;
CREATE TRIGGER integration_gate_maintainer_change_run_immutable_delete
BEFORE DELETE ON integration_gate_maintainer_change_runs
BEGIN
    SELECT RAISE(ABORT, 'maintainer change-run bindings cannot be deleted');
END;
"""

INTEGRATION_SCHEMA = """
CREATE TABLE work_item_landing_dependencies (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    dependent_work_item_id TEXT NOT NULL,
    prerequisite_work_item_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'removed')),
    version INTEGER NOT NULL CHECK (version >= 1),
    created_by TEXT NOT NULL,
    removed_by TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    removed_at TEXT,
    UNIQUE (dependent_work_item_id, prerequisite_work_item_id),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (dependent_work_item_id)
        REFERENCES work_items(id) ON DELETE RESTRICT,
    FOREIGN KEY (prerequisite_work_item_id)
        REFERENCES work_items(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (removed_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (dependent_work_item_id != prerequisite_work_item_id),
    CHECK (
        (state = 'active' AND removed_by IS NULL AND removed_at IS NULL)
        OR
        (state = 'removed' AND removed_by IS NOT NULL AND removed_at IS NOT NULL)
    )
) STRICT;

CREATE TABLE work_item_landing_dependency_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dependency_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (kind IN ('dependency.added', 'dependency.removed')),
    actor_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (dependency_id, sequence),
    FOREIGN KEY (dependency_id)
        REFERENCES work_item_landing_dependencies(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor_id) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX work_item_landing_dependency_active
    ON work_item_landing_dependencies(
        project_id, dependent_work_item_id, prerequisite_work_item_id
    ) WHERE state = 'active';
CREATE INDEX work_item_landing_dependency_event_stream
    ON work_item_landing_dependency_events(dependency_id, sequence);

CREATE TABLE integration_refs (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    repository_identity TEXT NOT NULL,
    integration_ref TEXT NOT NULL,
    object_format TEXT NOT NULL CHECK (object_format IN ('sha1', 'sha256')),
    current_revision TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (repository_identity = project_id),
    CHECK (
        (object_format = 'sha1' AND length(current_revision) = 40)
        OR (object_format = 'sha256' AND length(current_revision) = 64)
    )
) STRICT;

CREATE UNIQUE INDEX integration_ref_identity
    ON integration_refs(project_id, integration_ref);

CREATE TABLE integration_queue_items (
    id TEXT PRIMARY KEY,
    integration_ref_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL,
    work_thread_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    current_policy_id TEXT NOT NULL,
    current_fold_id TEXT,
    state TEXT NOT NULL CHECK (
        state IN (
            'queued', 'claimed', 'awaiting_receipt',
            'refold_required', 'landed', 'cancelled'
        )
    ),
    version INTEGER NOT NULL CHECK (version >= 1),
    event_sequence INTEGER NOT NULL CHECK (event_sequence >= 1),
    owner_id TEXT,
    lease_expires_at TEXT,
    landed_receipt_id TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    landed_at TEXT,
    cancelled_at TEXT,
    UNIQUE (integration_ref_id, candidate_id),
    FOREIGN KEY (integration_ref_id) REFERENCES integration_refs(id) ON DELETE RESTRICT,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_item_id) REFERENCES work_items(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_thread_id) REFERENCES work_threads(id) ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (current_policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
    FOREIGN KEY (current_fold_id) REFERENCES integration_folds(id) ON DELETE RESTRICT,
    FOREIGN KEY (landed_receipt_id)
        REFERENCES integration_landing_receipts(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (
            state IN ('queued', 'refold_required')
            AND owner_id IS NULL AND lease_expires_at IS NULL
            AND landed_receipt_id IS NULL
            AND landed_at IS NULL AND cancelled_at IS NULL
        ) OR (
            state = 'claimed'
            AND owner_id IS NOT NULL AND lease_expires_at IS NOT NULL
            AND landed_receipt_id IS NULL
            AND landed_at IS NULL AND cancelled_at IS NULL
        ) OR (
            state = 'awaiting_receipt'
            AND owner_id IS NULL AND lease_expires_at IS NULL
            AND landed_receipt_id IS NULL
            AND landed_at IS NULL AND cancelled_at IS NULL
        ) OR (
            state = 'landed'
            AND owner_id IS NULL AND lease_expires_at IS NULL
            AND landed_receipt_id IS NOT NULL
            AND landed_at IS NOT NULL AND cancelled_at IS NULL
        ) OR (
            state = 'cancelled'
            AND owner_id IS NULL AND lease_expires_at IS NULL
            AND landed_receipt_id IS NULL
            AND landed_at IS NULL AND cancelled_at IS NOT NULL
        )
    )
) STRICT;

CREATE UNIQUE INDEX integration_queue_one_active_thread
    ON integration_queue_items(integration_ref_id, work_thread_id)
    WHERE state NOT IN ('landed', 'cancelled');
CREATE UNIQUE INDEX integration_queue_one_lane_owner
    ON integration_queue_items(integration_ref_id)
    WHERE state IN ('claimed', 'awaiting_receipt');
CREATE INDEX integration_queue_state_created
    ON integration_queue_items(state, created_at, id);
CREATE INDEX integration_queue_ref_created
    ON integration_queue_items(integration_ref_id, created_at, id);
CREATE INDEX integration_queue_work_item
    ON integration_queue_items(work_item_id, state, created_at);

CREATE TABLE integration_folds (
    id TEXT PRIMARY KEY,
    queue_item_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    manifest_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    project_id TEXT NOT NULL,
    context_version_id TEXT NOT NULL,
    work_thread_id TEXT NOT NULL,
    integration_ref TEXT NOT NULL,
    base_revision TEXT NOT NULL,
    head_revision TEXT NOT NULL,
    tree_revision TEXT NOT NULL,
    object_format TEXT NOT NULL CHECK (object_format IN ('sha1', 'sha256')),
    result_content_sha256 TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (queue_item_id, generation),
    UNIQUE (candidate_id, integration_ref, base_revision, head_revision),
    FOREIGN KEY (queue_item_id)
        REFERENCES integration_queue_items(id) ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (context_version_id)
        REFERENCES project_context_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_thread_id) REFERENCES work_threads(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (object_format = 'sha1'
            AND length(base_revision) = 40
            AND length(head_revision) = 40
            AND length(tree_revision) = 40)
        OR
        (object_format = 'sha256'
            AND length(base_revision) = 64
            AND length(head_revision) = 64
            AND length(tree_revision) = 64)
    )
) STRICT;

CREATE INDEX integration_fold_item_generation
    ON integration_folds(queue_item_id, generation DESC);
CREATE INDEX integration_fold_exact_target
    ON integration_folds(candidate_id, integration_ref, base_revision, head_revision);

CREATE TABLE integration_queue_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_item_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'queue.created', 'queue.claimed', 'queue.claim_expired',
            'queue.claim_released', 'queue.plan_issued',
            'queue.not_landed', 'queue.refolded',
            'queue.refold_required', 'queue.landed', 'queue.cancelled'
        )
    ),
    from_state TEXT,
    to_state TEXT NOT NULL,
    actor TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (queue_item_id, sequence),
    FOREIGN KEY (queue_item_id)
        REFERENCES integration_queue_items(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX integration_queue_event_stream
    ON integration_queue_events(queue_item_id, sequence);

CREATE TABLE integration_landing_plans (
    id TEXT PRIMARY KEY,
    queue_item_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    plan_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    nonce_sha256 TEXT NOT NULL,
    evidence_snapshot_json TEXT NOT NULL,
    evidence_snapshot_sha256 TEXT NOT NULL,
    actuator TEXT NOT NULL CHECK (actuator = 'git-force-with-lease-v1'),
    assurance TEXT NOT NULL CHECK (assurance = 'base_and_head_enforced'),
    state TEXT NOT NULL CHECK (state IN ('pending', 'landed', 'not_landed')),
    issued_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    UNIQUE (queue_item_id, sequence),
    FOREIGN KEY (queue_item_id)
        REFERENCES integration_queue_items(id) ON DELETE RESTRICT,
    FOREIGN KEY (issued_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'pending' AND resolved_at IS NULL)
        OR (state IN ('landed', 'not_landed') AND resolved_at IS NOT NULL)
    )
) STRICT;

CREATE UNIQUE INDEX integration_plan_one_pending
    ON integration_landing_plans(queue_item_id) WHERE state = 'pending';
CREATE INDEX integration_plan_created
    ON integration_landing_plans(created_at DESC, id DESC);

CREATE TABLE integration_landing_receipts (
    id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    receipt_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    outcome TEXT NOT NULL CHECK (outcome IN ('landed', 'not_landed')),
    observed_revision TEXT NOT NULL,
    assurance TEXT NOT NULL CHECK (
        assurance IN ('base_and_head_enforced', 'operator_attested')
    ),
    recorded_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (plan_id)
        REFERENCES integration_landing_plans(id) ON DELETE RESTRICT,
    FOREIGN KEY (recorded_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX integration_receipt_created
    ON integration_landing_receipts(created_at DESC, id DESC);

CREATE TRIGGER work_item_landing_dependency_identity_immutable
BEFORE UPDATE OF
    id, project_id, dependent_work_item_id, prerequisite_work_item_id,
    created_by, created_at
ON work_item_landing_dependencies
BEGIN
    SELECT RAISE(ABORT, 'landing dependency identity is immutable');
END;
CREATE TRIGGER work_item_landing_dependency_immutable_delete
BEFORE DELETE ON work_item_landing_dependencies
BEGIN
    SELECT RAISE(ABORT, 'landing dependencies cannot be deleted');
END;
CREATE TRIGGER work_item_landing_dependency_event_immutable_update
BEFORE UPDATE ON work_item_landing_dependency_events
BEGIN
    SELECT RAISE(ABORT, 'landing dependency events are immutable');
END;
CREATE TRIGGER work_item_landing_dependency_event_immutable_delete
BEFORE DELETE ON work_item_landing_dependency_events
BEGIN
    SELECT RAISE(ABORT, 'landing dependency events cannot be deleted');
END;
CREATE TRIGGER integration_ref_identity_immutable
BEFORE UPDATE OF
    id, project_id, repository_identity, integration_ref, object_format,
    created_by, created_at
ON integration_refs
BEGIN
    SELECT RAISE(ABORT, 'integration ref identity is immutable');
END;
CREATE TRIGGER integration_ref_immutable_delete
BEFORE DELETE ON integration_refs
BEGIN
    SELECT RAISE(ABORT, 'integration refs cannot be deleted');
END;
CREATE TRIGGER integration_queue_identity_immutable
BEFORE UPDATE OF
    id, integration_ref_id, project_id, work_item_id, work_thread_id,
    candidate_id, created_by, created_at
ON integration_queue_items
BEGIN
    SELECT RAISE(ABORT, 'integration queue identity is immutable');
END;
CREATE TRIGGER integration_queue_immutable_delete
BEFORE DELETE ON integration_queue_items
BEGIN
    SELECT RAISE(ABORT, 'integration queue items cannot be deleted');
END;
CREATE TRIGGER integration_fold_immutable_update
BEFORE UPDATE ON integration_folds
BEGIN
    SELECT RAISE(ABORT, 'integration folds are immutable');
END;
CREATE TRIGGER integration_fold_immutable_delete
BEFORE DELETE ON integration_folds
BEGIN
    SELECT RAISE(ABORT, 'integration folds cannot be deleted');
END;
CREATE TRIGGER integration_queue_event_immutable_update
BEFORE UPDATE ON integration_queue_events
BEGIN
    SELECT RAISE(ABORT, 'integration queue events are immutable');
END;
CREATE TRIGGER integration_queue_event_immutable_delete
BEFORE DELETE ON integration_queue_events
BEGIN
    SELECT RAISE(ABORT, 'integration queue events cannot be deleted');
END;
CREATE TRIGGER integration_landing_plan_identity_immutable
BEFORE UPDATE OF
    id, queue_item_id, sequence, schema_version, plan_json, content_sha256,
    nonce_sha256, evidence_snapshot_json, evidence_snapshot_sha256,
    actuator, assurance, issued_by, created_at
ON integration_landing_plans
BEGIN
    SELECT RAISE(ABORT, 'landing plan identity is immutable');
END;
CREATE TRIGGER integration_landing_plan_immutable_delete
BEFORE DELETE ON integration_landing_plans
BEGIN
    SELECT RAISE(ABORT, 'landing plans cannot be deleted');
END;
CREATE TRIGGER integration_landing_receipt_immutable_update
BEFORE UPDATE ON integration_landing_receipts
BEGIN
    SELECT RAISE(ABORT, 'landing receipts are immutable');
END;
CREATE TRIGGER integration_landing_receipt_immutable_delete
BEFORE DELETE ON integration_landing_receipts
BEGIN
    SELECT RAISE(ABORT, 'landing receipts cannot be deleted');
END;
"""

INFERENCE_CONNECTOR_SCHEMA = """
ALTER TABLE provider_grants ADD COLUMN grant_kind TEXT NOT NULL DEFAULT 'direct'
    CHECK (grant_kind IN ('direct', 'connector'));

CREATE TABLE inference_connectors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('active', 'revoked')),
    version INTEGER NOT NULL CHECK (version >= 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    connector_protocol_version INTEGER NOT NULL
        CHECK (connector_protocol_version IN (1, 2)),
    spec_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    deployment_sha256 TEXT NOT NULL CHECK (length(deployment_sha256) = 64),
    transport_kind TEXT NOT NULL CHECK (transport_kind = 'unix'),
    socket_path TEXT NOT NULL,
    request_bytes INTEGER NOT NULL CHECK (request_bytes >= 1),
    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 1),
    timeout_ms INTEGER NOT NULL CHECK (timeout_ms >= 100),
    max_concurrency INTEGER NOT NULL CHECK (max_concurrency >= 1),
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

CREATE TABLE inference_connector_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inference_connector_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN ('inference_connector.created', 'inference_connector.revoked')
    ),
    actor_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (inference_connector_id, sequence),
    FOREIGN KEY (inference_connector_id)
        REFERENCES inference_connectors(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor_id) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE provider_connector_bindings (
    provider_grant_id TEXT PRIMARY KEY,
    inference_connector_id TEXT NOT NULL,
    deployment_sha256 TEXT NOT NULL CHECK (length(deployment_sha256) = 64),
    route TEXT NOT NULL,
    credential_scope_sha256 TEXT NOT NULL
        CHECK (length(credential_scope_sha256) = 64),
    provider TEXT NOT NULL,
    protocol TEXT NOT NULL,
    model TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (provider_grant_id)
        REFERENCES provider_grants(id) ON DELETE RESTRICT,
    FOREIGN KEY (inference_connector_id)
        REFERENCES inference_connectors(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX inference_connector_state
    ON inference_connectors(state, created_at, id);
CREATE INDEX inference_connector_event_stream
    ON inference_connector_events(inference_connector_id, sequence);
CREATE INDEX provider_connector_connector
    ON provider_connector_bindings(inference_connector_id, provider_grant_id);

CREATE TRIGGER inference_connector_identity_immutable
BEFORE UPDATE OF
    id, name, schema_version, connector_protocol_version, spec_json,
    content_sha256, deployment_sha256, transport_kind, socket_path,
    request_bytes, response_bytes, timeout_ms, max_concurrency,
    created_by, created_at
ON inference_connectors
BEGIN
    SELECT RAISE(ABORT, 'inference connector identity is immutable');
END;
CREATE TRIGGER inference_connector_immutable_delete
BEFORE DELETE ON inference_connectors
BEGIN
    SELECT RAISE(ABORT, 'inference connectors cannot be deleted');
END;
CREATE TRIGGER inference_connector_event_immutable_update
BEFORE UPDATE ON inference_connector_events
BEGIN
    SELECT RAISE(ABORT, 'inference connector events are immutable');
END;
CREATE TRIGGER inference_connector_event_immutable_delete
BEFORE DELETE ON inference_connector_events
BEGIN
    SELECT RAISE(ABORT, 'inference connector events cannot be deleted');
END;
CREATE TRIGGER provider_connector_binding_requires_connector_grant
BEFORE INSERT ON provider_connector_bindings
WHEN COALESCE(
    (SELECT grant_kind FROM provider_grants WHERE id = NEW.provider_grant_id),
    ''
) <> 'connector'
BEGIN
    SELECT RAISE(ABORT, 'connector binding requires a connector grant');
END;
CREATE TRIGGER provider_connector_binding_immutable_update
BEFORE UPDATE ON provider_connector_bindings
BEGIN
    SELECT RAISE(ABORT, 'provider connector bindings are immutable');
END;
CREATE TRIGGER provider_connector_binding_immutable_delete
BEFORE DELETE ON provider_connector_bindings
BEGIN
    SELECT RAISE(ABORT, 'provider connector bindings cannot be deleted');
END;
CREATE TRIGGER provider_grant_kind_shape_insert
BEFORE INSERT ON provider_grants
WHEN
    (NEW.grant_kind = 'direct' AND length(NEW.credential_ref) = 0)
    OR (NEW.grant_kind = 'connector' AND length(NEW.credential_ref) <> 0)
BEGIN
    SELECT RAISE(ABORT, 'provider grant kind and credential reference disagree');
END;
CREATE TRIGGER provider_grant_kind_immutable
BEFORE UPDATE OF grant_kind ON provider_grants
BEGIN
    SELECT RAISE(ABORT, 'provider grant kind is immutable');
END;
"""

INFERENCE_DISPATCH_SCHEMA = """
DROP TRIGGER provider_invocation_immutable_update;
DROP TRIGGER provider_invocation_immutable_delete;
DROP INDEX provider_invocation_run;

ALTER TABLE provider_invocations RENAME TO provider_invocations_v15;

CREATE TABLE provider_invocations (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    provider_grant_id TEXT NOT NULL,
    dispatch_kind TEXT NOT NULL CHECK (
        dispatch_kind IN ('direct', 'connector')
    ),
    inference_connector_id TEXT,
    connector_protocol_version INTEGER,
    deployment_sha256 TEXT,
    route TEXT,
    policy_sha256 TEXT,
    credential_scope_sha256 TEXT,
    provider TEXT,
    protocol TEXT,
    model TEXT,
    deadline TEXT,
    outcome TEXT NOT NULL CHECK (
        outcome IN ('succeeded', 'failed', 'denied', 'busy', 'ambiguous')
    ),
    request_bytes INTEGER NOT NULL CHECK (request_bytes >= 0),
    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 0),
    http_status INTEGER CHECK (http_status BETWEEN 100 AND 599),
    error_code TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (provider_grant_id)
        REFERENCES provider_grants(id) ON DELETE RESTRICT,
    FOREIGN KEY (inference_connector_id)
        REFERENCES inference_connectors(id) ON DELETE RESTRICT,
    CHECK (
        (dispatch_kind = 'direct'
            AND inference_connector_id IS NULL
            AND connector_protocol_version IS NULL
            AND deployment_sha256 IS NULL
            AND route IS NULL
            AND policy_sha256 IS NULL
            AND credential_scope_sha256 IS NULL
            AND provider IS NULL
            AND protocol IS NULL
            AND model IS NULL
            AND deadline IS NULL)
        OR
        (dispatch_kind = 'connector'
            AND inference_connector_id IS NOT NULL
            AND connector_protocol_version = 1
            AND length(deployment_sha256) = 64
            AND route IS NOT NULL
            AND length(policy_sha256) = 64
            AND length(credential_scope_sha256) = 64
            AND provider IS NOT NULL
            AND protocol IS NOT NULL
            AND model IS NOT NULL
            AND deadline IS NOT NULL)
    )
) STRICT;

INSERT INTO provider_invocations (
    id, run_id, provider_grant_id, dispatch_kind, outcome, request_bytes,
    response_bytes, http_status, error_code, started_at, finished_at
)
SELECT
    id, run_id, provider_grant_id, 'direct', outcome, request_bytes,
    response_bytes, http_status, error_code, started_at, finished_at
FROM provider_invocations_v15;

DROP TABLE provider_invocations_v15;

CREATE INDEX provider_invocation_run
    ON provider_invocations(run_id, started_at, id);
CREATE INDEX provider_invocation_connector
    ON provider_invocations(inference_connector_id, started_at, id)
    WHERE inference_connector_id IS NOT NULL;

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

PROVIDER_INVOCATION_LIFECYCLE_SCHEMA = """
DROP TRIGGER provider_invocation_immutable_update;
DROP TRIGGER provider_invocation_immutable_delete;
DROP INDEX provider_invocation_run;
DROP INDEX provider_invocation_connector;

ALTER TABLE provider_invocations RENAME TO provider_invocations_v16;

CREATE TABLE provider_invocations (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    provider_grant_id TEXT NOT NULL,
    dispatch_kind TEXT NOT NULL CHECK (
        dispatch_kind IN ('direct', 'connector')
    ),
    inference_connector_id TEXT,
    connector_protocol_version INTEGER,
    deployment_sha256 TEXT,
    route TEXT,
    policy_sha256 TEXT,
    credential_scope_sha256 TEXT,
    provider TEXT,
    protocol TEXT,
    model TEXT,
    deadline TEXT,
    outcome TEXT NOT NULL CHECK (
        outcome IN (
            'dispatching', 'succeeded', 'failed', 'denied', 'busy', 'ambiguous'
        )
    ),
    request_bytes INTEGER NOT NULL CHECK (request_bytes >= 0),
    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 0),
    http_status INTEGER CHECK (http_status BETWEEN 100 AND 599),
    error_code TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (provider_grant_id)
        REFERENCES provider_grants(id) ON DELETE RESTRICT,
    FOREIGN KEY (inference_connector_id)
        REFERENCES inference_connectors(id) ON DELETE RESTRICT,
    CHECK (
        (dispatch_kind = 'direct'
            AND inference_connector_id IS NULL
            AND connector_protocol_version IS NULL
            AND deployment_sha256 IS NULL
            AND route IS NULL
            AND policy_sha256 IS NULL
            AND credential_scope_sha256 IS NULL
            AND provider IS NULL
            AND protocol IS NULL
            AND model IS NULL
            AND deadline IS NULL)
        OR
        (dispatch_kind = 'connector'
            AND inference_connector_id IS NOT NULL
            AND connector_protocol_version = 1
            AND length(deployment_sha256) = 64
            AND route IS NOT NULL
            AND length(policy_sha256) = 64
            AND length(credential_scope_sha256) = 64
            AND provider IS NOT NULL
            AND protocol IS NOT NULL
            AND model IS NOT NULL
            AND deadline IS NOT NULL)
    ),
    CHECK (
        (outcome = 'dispatching'
            AND dispatch_kind = 'connector'
            AND request_bytes = 0
            AND response_bytes = 0
            AND http_status IS NULL
            AND error_code IS NULL
            AND finished_at IS NULL)
        OR
        (outcome = 'succeeded'
            AND http_status BETWEEN 200 AND 299
            AND error_code IS NULL
            AND finished_at IS NOT NULL)
        OR
        (outcome IN ('failed', 'denied', 'busy', 'ambiguous')
            AND error_code IS NOT NULL
            AND finished_at IS NOT NULL)
    )
) STRICT;

INSERT INTO provider_invocations (
    id, run_id, provider_grant_id, dispatch_kind,
    inference_connector_id, connector_protocol_version, deployment_sha256,
    route, policy_sha256, credential_scope_sha256, provider, protocol, model,
    deadline, outcome, request_bytes, response_bytes, http_status, error_code,
    started_at, finished_at
)
SELECT
    id, run_id, provider_grant_id, dispatch_kind,
    inference_connector_id, connector_protocol_version, deployment_sha256,
    route, policy_sha256, credential_scope_sha256, provider, protocol, model,
    deadline, outcome, request_bytes, response_bytes, http_status, error_code,
    started_at, finished_at
FROM provider_invocations_v16;

DROP TABLE provider_invocations_v16;

CREATE INDEX provider_invocation_run
    ON provider_invocations(run_id, started_at, id);
CREATE INDEX provider_invocation_connector
    ON provider_invocations(inference_connector_id, started_at, id)
    WHERE inference_connector_id IS NOT NULL;

CREATE TRIGGER provider_invocation_immutable_update
BEFORE UPDATE ON provider_invocations
WHEN
    OLD.outcome != 'dispatching'
    OR NEW.outcome = 'dispatching'
    OR NEW.finished_at IS NULL
    OR NEW.id IS NOT OLD.id
    OR NEW.run_id IS NOT OLD.run_id
    OR NEW.provider_grant_id IS NOT OLD.provider_grant_id
    OR NEW.dispatch_kind IS NOT OLD.dispatch_kind
    OR NEW.inference_connector_id IS NOT OLD.inference_connector_id
    OR NEW.connector_protocol_version IS NOT OLD.connector_protocol_version
    OR NEW.deployment_sha256 IS NOT OLD.deployment_sha256
    OR NEW.route IS NOT OLD.route
    OR NEW.policy_sha256 IS NOT OLD.policy_sha256
    OR NEW.credential_scope_sha256 IS NOT OLD.credential_scope_sha256
    OR NEW.provider IS NOT OLD.provider
    OR NEW.protocol IS NOT OLD.protocol
    OR NEW.model IS NOT OLD.model
    OR NEW.deadline IS NOT OLD.deadline
    OR NEW.started_at IS NOT OLD.started_at
BEGIN
    SELECT RAISE(ABORT, 'provider invocations are immutable');
END;
CREATE TRIGGER provider_invocation_immutable_delete
BEFORE DELETE ON provider_invocations
BEGIN
    SELECT RAISE(ABORT, 'provider invocations are immutable');
END;
"""

ATTEMPT_OWNER_AUDIT_SCHEMA = """
DROP TRIGGER run_attempt_event_immutable_update;
DROP TRIGGER run_attempt_event_immutable_delete;
DROP INDEX run_attempt_event_stream;

ALTER TABLE run_attempt_events RENAME TO run_attempt_events_v17;

CREATE TABLE run_attempt_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'attempt.created', 'attempt.started', 'attempt.owner_changed',
            'attempt.handoff_submitted', 'attempt.completed',
            'attempt.failed', 'attempt.interrupted'
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

INSERT INTO run_attempt_events (
    id, attempt_id, sequence, kind, from_state, to_state, metadata_json,
    created_at
)
SELECT
    id, attempt_id, sequence, kind, from_state, to_state, metadata_json,
    created_at
FROM run_attempt_events_v17;

DROP TABLE run_attempt_events_v17;

CREATE INDEX run_attempt_event_stream
    ON run_attempt_events(attempt_id, sequence);

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
"""

PROVIDER_BUDGET_SCHEMA = """
DROP TRIGGER provider_invocation_immutable_update;
DROP TRIGGER provider_invocation_immutable_delete;
DROP INDEX provider_invocation_run;
DROP INDEX provider_invocation_connector;
DROP TRIGGER inference_connector_identity_immutable;
DROP TRIGGER inference_connector_immutable_delete;
DROP TRIGGER inference_connector_event_immutable_update;
DROP TRIGGER inference_connector_event_immutable_delete;
DROP TRIGGER provider_connector_binding_requires_connector_grant;
DROP TRIGGER provider_connector_binding_immutable_update;
DROP TRIGGER provider_connector_binding_immutable_delete;
DROP INDEX inference_connector_state;
DROP INDEX inference_connector_event_stream;
DROP INDEX provider_connector_connector;

ALTER TABLE provider_invocations RENAME TO provider_invocations_v18;
ALTER TABLE provider_connector_bindings
    RENAME TO provider_connector_bindings_v18;
ALTER TABLE inference_connector_events RENAME TO inference_connector_events_v18;
ALTER TABLE inference_connectors RENAME TO inference_connectors_v18;

CREATE TABLE inference_connectors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('active', 'revoked')),
    version INTEGER NOT NULL CHECK (version >= 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    connector_protocol_version INTEGER NOT NULL
        CHECK (connector_protocol_version IN (1, 2)),
    spec_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    deployment_sha256 TEXT NOT NULL CHECK (length(deployment_sha256) = 64),
    transport_kind TEXT NOT NULL CHECK (transport_kind = 'unix'),
    socket_path TEXT NOT NULL,
    request_bytes INTEGER NOT NULL CHECK (request_bytes >= 1),
    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 1),
    timeout_ms INTEGER NOT NULL CHECK (timeout_ms >= 100),
    max_concurrency INTEGER NOT NULL CHECK (max_concurrency >= 1),
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

CREATE TABLE inference_connector_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inference_connector_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN ('inference_connector.created', 'inference_connector.revoked')
    ),
    actor_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (inference_connector_id, sequence),
    FOREIGN KEY (inference_connector_id)
        REFERENCES inference_connectors(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor_id) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE provider_connector_bindings (
    provider_grant_id TEXT PRIMARY KEY,
    inference_connector_id TEXT NOT NULL,
    deployment_sha256 TEXT NOT NULL CHECK (length(deployment_sha256) = 64),
    route TEXT NOT NULL,
    credential_scope_sha256 TEXT NOT NULL
        CHECK (length(credential_scope_sha256) = 64),
    provider TEXT NOT NULL,
    protocol TEXT NOT NULL,
    model TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (provider_grant_id)
        REFERENCES provider_grants(id) ON DELETE RESTRICT,
    FOREIGN KEY (inference_connector_id)
        REFERENCES inference_connectors(id) ON DELETE RESTRICT
) STRICT;

INSERT INTO inference_connectors (
    id, name, state, version, schema_version, connector_protocol_version,
    spec_json, content_sha256, deployment_sha256, transport_kind, socket_path,
    request_bytes, response_bytes, timeout_ms, max_concurrency, revoked_at,
    revoked_by, created_by, created_at, updated_at
)
SELECT
    id, name, state, version, schema_version, connector_protocol_version,
    spec_json, content_sha256, deployment_sha256, transport_kind, socket_path,
    request_bytes, response_bytes, timeout_ms, max_concurrency, revoked_at,
    revoked_by, created_by, created_at, updated_at
FROM inference_connectors_v18;

INSERT INTO inference_connector_events (
    id, inference_connector_id, sequence, kind, actor_id, created_at
)
SELECT id, inference_connector_id, sequence, kind, actor_id, created_at
FROM inference_connector_events_v18;

INSERT INTO provider_connector_bindings (
    provider_grant_id, inference_connector_id, deployment_sha256, route,
    credential_scope_sha256, provider, protocol, model, created_at
)
SELECT
    provider_grant_id, inference_connector_id, deployment_sha256, route,
    credential_scope_sha256, provider, protocol, model, created_at
FROM provider_connector_bindings_v18;

CREATE TABLE provider_invocations (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    provider_grant_id TEXT NOT NULL,
    dispatch_kind TEXT NOT NULL CHECK (
        dispatch_kind IN ('direct', 'connector')
    ),
    inference_connector_id TEXT,
    connector_protocol_version INTEGER,
    deployment_sha256 TEXT,
    route TEXT,
    policy_sha256 TEXT,
    credential_scope_sha256 TEXT,
    provider TEXT,
    protocol TEXT,
    model TEXT,
    deadline TEXT,
    outcome TEXT NOT NULL CHECK (
        outcome IN (
            'dispatching', 'succeeded', 'failed', 'denied', 'busy', 'ambiguous'
        )
    ),
    request_bytes INTEGER NOT NULL CHECK (request_bytes >= 0),
    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 0),
    http_status INTEGER CHECK (http_status BETWEEN 100 AND 599),
    error_code TEXT,
    budget_account TEXT,
    budget_revision TEXT,
    budget_policy_sha256 TEXT,
    budget_currency TEXT,
    budget_policy_json TEXT,
    reserved_microusd INTEGER CHECK (reserved_microusd >= 1),
    accounted_microusd INTEGER CHECK (accounted_microusd >= 0),
    estimated_microusd INTEGER CHECK (estimated_microusd >= 0),
    accounting_status TEXT CHECK (
        accounting_status IN ('reserved', 'reported', 'released', 'conservative')
    ),
    input_tokens INTEGER CHECK (input_tokens >= 0),
    cached_input_tokens INTEGER CHECK (cached_input_tokens >= 0),
    output_tokens INTEGER CHECK (output_tokens >= 0),
    reasoning_output_tokens INTEGER CHECK (reasoning_output_tokens >= 0),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (provider_grant_id)
        REFERENCES provider_grants(id) ON DELETE RESTRICT,
    FOREIGN KEY (inference_connector_id)
        REFERENCES inference_connectors(id) ON DELETE RESTRICT,
    CHECK (
        (dispatch_kind = 'direct'
            AND inference_connector_id IS NULL
            AND connector_protocol_version IS NULL
            AND deployment_sha256 IS NULL
            AND route IS NULL
            AND policy_sha256 IS NULL
            AND credential_scope_sha256 IS NULL
            AND provider IS NULL
            AND protocol IS NULL
            AND model IS NULL
            AND deadline IS NULL)
        OR
        (dispatch_kind = 'connector'
            AND inference_connector_id IS NOT NULL
            AND connector_protocol_version IN (1, 2)
            AND length(deployment_sha256) = 64
            AND route IS NOT NULL
            AND length(policy_sha256) = 64
            AND length(credential_scope_sha256) = 64
            AND provider IS NOT NULL
            AND protocol IS NOT NULL
            AND model IS NOT NULL
            AND deadline IS NOT NULL)
    ),
    CHECK (
        (outcome = 'dispatching'
            AND dispatch_kind = 'connector'
            AND request_bytes = 0
            AND response_bytes = 0
            AND http_status IS NULL
            AND error_code IS NULL
            AND finished_at IS NULL)
        OR
        (outcome = 'succeeded'
            AND http_status BETWEEN 200 AND 299
            AND error_code IS NULL
            AND finished_at IS NOT NULL)
        OR
        (outcome IN ('failed', 'denied', 'busy', 'ambiguous')
            AND error_code IS NOT NULL
            AND finished_at IS NOT NULL)
    ),
    CHECK (
        (budget_account IS NULL
            AND budget_revision IS NULL
            AND budget_policy_sha256 IS NULL
            AND budget_currency IS NULL
            AND budget_policy_json IS NULL
            AND reserved_microusd IS NULL
            AND accounted_microusd IS NULL
            AND estimated_microusd IS NULL
            AND accounting_status IS NULL
            AND input_tokens IS NULL
            AND cached_input_tokens IS NULL
            AND output_tokens IS NULL
            AND reasoning_output_tokens IS NULL)
        OR
        (dispatch_kind = 'connector'
            AND connector_protocol_version = 2
            AND budget_account IS NOT NULL
            AND budget_revision IS NOT NULL
            AND length(budget_policy_sha256) = 64
            AND budget_currency = 'USD'
            AND budget_policy_json IS NOT NULL
            AND reserved_microusd IS NOT NULL
            AND (
                (outcome = 'dispatching'
                    AND accounting_status = 'reserved'
                    AND accounted_microusd IS NULL
                    AND estimated_microusd IS NULL
                    AND input_tokens IS NULL
                    AND cached_input_tokens IS NULL
                    AND output_tokens IS NULL
                    AND reasoning_output_tokens IS NULL)
                OR
                (outcome != 'dispatching'
                    AND accounting_status = 'reported'
                    AND accounted_microusd = estimated_microusd
                    AND accounted_microusd <= reserved_microusd
                    AND input_tokens IS NOT NULL
                    AND cached_input_tokens IS NOT NULL
                    AND cached_input_tokens <= input_tokens
                    AND output_tokens IS NOT NULL
                    AND reasoning_output_tokens IS NOT NULL
                    AND reasoning_output_tokens <= output_tokens)
                OR
                (outcome != 'dispatching'
                    AND accounting_status = 'released'
                    AND accounted_microusd = 0
                    AND estimated_microusd IS NULL
                    AND input_tokens IS NULL
                    AND cached_input_tokens IS NULL
                    AND output_tokens IS NULL
                    AND reasoning_output_tokens IS NULL)
                OR
                (outcome != 'dispatching'
                    AND accounting_status = 'conservative'
                    AND accounted_microusd = reserved_microusd
                    AND estimated_microusd IS NULL
                    AND input_tokens IS NULL
                    AND cached_input_tokens IS NULL
                    AND output_tokens IS NULL
                    AND reasoning_output_tokens IS NULL)
            ))
    )
) STRICT;

INSERT INTO provider_invocations (
    id, run_id, provider_grant_id, dispatch_kind,
    inference_connector_id, connector_protocol_version, deployment_sha256,
    route, policy_sha256, credential_scope_sha256, provider, protocol, model,
    deadline, outcome, request_bytes, response_bytes, http_status, error_code,
    started_at, finished_at
)
SELECT
    id, run_id, provider_grant_id, dispatch_kind,
    inference_connector_id, connector_protocol_version, deployment_sha256,
    route, policy_sha256, credential_scope_sha256, provider, protocol, model,
    deadline, outcome, request_bytes, response_bytes, http_status, error_code,
    started_at, finished_at
FROM provider_invocations_v18;

DROP TABLE provider_invocations_v18;
DROP TABLE provider_connector_bindings_v18;
DROP TABLE inference_connector_events_v18;
DROP TABLE inference_connectors_v18;

CREATE INDEX provider_invocation_run
    ON provider_invocations(run_id, started_at, id);
CREATE INDEX provider_invocation_connector
    ON provider_invocations(inference_connector_id, started_at, id)
    WHERE inference_connector_id IS NOT NULL;
CREATE INDEX provider_invocation_budget_account
    ON provider_invocations(budget_account, outcome)
    WHERE budget_account IS NOT NULL;
CREATE INDEX provider_invocation_budget_grant
    ON provider_invocations(provider_grant_id, budget_account)
    WHERE budget_account IS NOT NULL;
CREATE INDEX inference_connector_state
    ON inference_connectors(state, created_at, id);
CREATE INDEX inference_connector_event_stream
    ON inference_connector_events(inference_connector_id, sequence);
CREATE INDEX provider_connector_connector
    ON provider_connector_bindings(inference_connector_id, provider_grant_id);

CREATE TRIGGER inference_connector_identity_immutable
BEFORE UPDATE OF
    id, name, schema_version, connector_protocol_version, spec_json,
    content_sha256, deployment_sha256, transport_kind, socket_path,
    request_bytes, response_bytes, timeout_ms, max_concurrency,
    created_by, created_at
ON inference_connectors
BEGIN
    SELECT RAISE(ABORT, 'inference connector identity is immutable');
END;
CREATE TRIGGER inference_connector_immutable_delete
BEFORE DELETE ON inference_connectors
BEGIN
    SELECT RAISE(ABORT, 'inference connectors cannot be deleted');
END;
CREATE TRIGGER inference_connector_event_immutable_update
BEFORE UPDATE ON inference_connector_events
BEGIN
    SELECT RAISE(ABORT, 'inference connector events are immutable');
END;
CREATE TRIGGER inference_connector_event_immutable_delete
BEFORE DELETE ON inference_connector_events
BEGIN
    SELECT RAISE(ABORT, 'inference connector events cannot be deleted');
END;
CREATE TRIGGER provider_connector_binding_requires_connector_grant
BEFORE INSERT ON provider_connector_bindings
WHEN COALESCE(
    (SELECT grant_kind FROM provider_grants WHERE id = NEW.provider_grant_id),
    ''
) <> 'connector'
BEGIN
    SELECT RAISE(ABORT, 'connector binding requires a connector grant');
END;
CREATE TRIGGER provider_connector_binding_immutable_update
BEFORE UPDATE ON provider_connector_bindings
BEGIN
    SELECT RAISE(ABORT, 'provider connector bindings are immutable');
END;
CREATE TRIGGER provider_connector_binding_immutable_delete
BEFORE DELETE ON provider_connector_bindings
BEGIN
    SELECT RAISE(ABORT, 'provider connector bindings cannot be deleted');
END;

CREATE TRIGGER provider_invocation_immutable_update
BEFORE UPDATE ON provider_invocations
WHEN
    OLD.outcome != 'dispatching'
    OR NEW.outcome = 'dispatching'
    OR NEW.finished_at IS NULL
    OR NEW.id IS NOT OLD.id
    OR NEW.run_id IS NOT OLD.run_id
    OR NEW.provider_grant_id IS NOT OLD.provider_grant_id
    OR NEW.dispatch_kind IS NOT OLD.dispatch_kind
    OR NEW.inference_connector_id IS NOT OLD.inference_connector_id
    OR NEW.connector_protocol_version IS NOT OLD.connector_protocol_version
    OR NEW.deployment_sha256 IS NOT OLD.deployment_sha256
    OR NEW.route IS NOT OLD.route
    OR NEW.policy_sha256 IS NOT OLD.policy_sha256
    OR NEW.credential_scope_sha256 IS NOT OLD.credential_scope_sha256
    OR NEW.provider IS NOT OLD.provider
    OR NEW.protocol IS NOT OLD.protocol
    OR NEW.model IS NOT OLD.model
    OR NEW.deadline IS NOT OLD.deadline
    OR NEW.budget_account IS NOT OLD.budget_account
    OR NEW.budget_revision IS NOT OLD.budget_revision
    OR NEW.budget_policy_sha256 IS NOT OLD.budget_policy_sha256
    OR NEW.budget_currency IS NOT OLD.budget_currency
    OR NEW.budget_policy_json IS NOT OLD.budget_policy_json
    OR NEW.reserved_microusd IS NOT OLD.reserved_microusd
    OR NEW.started_at IS NOT OLD.started_at
BEGIN
    SELECT RAISE(ABORT, 'provider invocations are immutable');
END;
CREATE TRIGGER provider_invocation_immutable_delete
BEFORE DELETE ON provider_invocations
BEGIN
    SELECT RAISE(ABORT, 'provider invocations are immutable');
END;
"""

RUN_PAUSED_STATE_MIGRATION = """
CREATE TABLE runs_v20 (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    context_version_id TEXT NOT NULL,
    profile_version_id TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'queued', 'preparing', 'running', 'waiting_for_input', 'paused',
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
    work_item_version_id TEXT
        REFERENCES work_item_versions(id) ON DELETE RESTRICT,
    work_thread_id TEXT REFERENCES work_threads(id) ON DELETE RESTRICT,
    predecessor_run_id TEXT REFERENCES runs(id) ON DELETE RESTRICT,
    replacement_kind TEXT CHECK (replacement_kind IN ('retry', 'reassign')),
    supervision_policy_json TEXT NOT NULL DEFAULT
        '{"attempt_idle_escalation_seconds":[],"attempt_lease_expired_escalation_seconds":[],"auto_execute":false,"interaction_escalation_seconds":[],"other_attention_escalation_seconds":[],"retry_backoff_seconds":[5,30,300],"schema_version":1}',
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (context_version_id)
        REFERENCES project_context_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (profile_version_id)
        REFERENCES agent_profile_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (requested_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

INSERT INTO runs_v20 (
    id, project_id, context_version_id, profile_version_id, requested_by,
    state, version, source_revision, krab_session_id, krab_claim_id,
    created_at, updated_at, started_at, finished_at, work_item_version_id,
    work_thread_id, predecessor_run_id, replacement_kind,
    supervision_policy_json
)
SELECT
    id, project_id, context_version_id, profile_version_id, requested_by,
    state, version, source_revision, krab_session_id, krab_claim_id,
    created_at, updated_at, started_at, finished_at, work_item_version_id,
    work_thread_id, predecessor_run_id, replacement_kind,
    supervision_policy_json
FROM runs;

DROP TABLE runs;
ALTER TABLE runs_v20 RENAME TO runs;

CREATE INDEX run_project_created ON runs(project_id, created_at DESC);
CREATE INDEX run_state_created ON runs(state, created_at);
CREATE INDEX run_work_item_version ON runs(work_item_version_id);
CREATE INDEX run_work_thread ON runs(work_thread_id);
CREATE INDEX run_predecessor_created
    ON runs(predecessor_run_id, created_at DESC)
    WHERE predecessor_run_id IS NOT NULL;
CREATE UNIQUE INDEX run_one_active_successor
    ON runs(predecessor_run_id)
    WHERE predecessor_run_id IS NOT NULL
      AND state IN (
          'queued', 'preparing', 'running', 'waiting_for_input', 'paused'
      );

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
CREATE TRIGGER run_supervision_policy_immutable
BEFORE UPDATE OF supervision_policy_json ON runs
WHEN NEW.supervision_policy_json IS NOT OLD.supervision_policy_json
BEGIN
    SELECT RAISE(ABORT, 'run supervision policy is immutable');
END;
"""

PROVIDER_RECOVERY_SCHEMA = """
CREATE TABLE provider_recoveries (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    invocation_id TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL CHECK (
        category IN ('quota_exhausted', 'rate_limited')
    ),
    state TEXT NOT NULL CHECK (state IN ('active', 'resumed', 'cancelled')),
    version INTEGER NOT NULL CHECK (version >= 1),
    retry_after_seconds INTEGER NOT NULL CHECK (
        retry_after_seconds BETWEEN 1 AND 2592000
    ),
    eligible_at TEXT NOT NULL,
    error_code TEXT NOT NULL,
    provider_status INTEGER CHECK (provider_status BETWEEN 100 AND 599),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    resolved_at TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT,
    FOREIGN KEY (invocation_id)
        REFERENCES provider_invocations(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'active' AND resolved_at IS NULL)
        OR (state IN ('resumed', 'cancelled') AND resolved_at IS NOT NULL)
    )
) STRICT;

CREATE UNIQUE INDEX provider_recovery_active_run
    ON provider_recoveries(run_id) WHERE state = 'active';
CREATE INDEX provider_recovery_due
    ON provider_recoveries(state, eligible_at, created_at, id);
CREATE INDEX provider_recovery_run
    ON provider_recoveries(run_id, created_at, id);

CREATE TRIGGER provider_recovery_identity_immutable
BEFORE UPDATE OF
    id, run_id, attempt_id, invocation_id, category, retry_after_seconds,
    eligible_at, error_code, provider_status, created_at
ON provider_recoveries
BEGIN
    SELECT RAISE(ABORT, 'provider recovery identity is immutable');
END;
CREATE TRIGGER provider_recovery_immutable_delete
BEFORE DELETE ON provider_recoveries
BEGIN
    SELECT RAISE(ABORT, 'provider recoveries cannot be deleted');
END;
"""

RECOVERY_CHECKPOINT_SCHEMA = """
CREATE UNIQUE INDEX attempt_result_identity
    ON attempt_results(id, attempt_id);

CREATE TABLE attempt_recovery_checkpoints (
    id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL UNIQUE,
    source_attempt_id TEXT NOT NULL,
    result_id TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    changed_paths INTEGER NOT NULL CHECK (changed_paths >= 0),
    changed_bytes INTEGER NOT NULL CHECK (changed_bytes >= 0),
    created_at TEXT NOT NULL,
    UNIQUE (id, attempt_id),
    FOREIGN KEY (attempt_id)
        REFERENCES run_attempts(id) ON DELETE RESTRICT,
    FOREIGN KEY (source_attempt_id)
        REFERENCES run_attempts(id) ON DELETE RESTRICT,
    FOREIGN KEY (result_id, source_attempt_id)
        REFERENCES attempt_results(id, attempt_id) ON DELETE RESTRICT,
    CHECK (attempt_id != source_attempt_id)
) STRICT;

CREATE TABLE attempt_recovery_applications (
    checkpoint_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL UNIQUE,
    applied_at TEXT NOT NULL,
    FOREIGN KEY (checkpoint_id, attempt_id)
        REFERENCES attempt_recovery_checkpoints(id, attempt_id)
        ON DELETE RESTRICT
) STRICT;

CREATE INDEX attempt_recovery_source
    ON attempt_recovery_checkpoints(source_attempt_id, created_at);

CREATE TRIGGER attempt_recovery_checkpoint_immutable_update
BEFORE UPDATE ON attempt_recovery_checkpoints
BEGIN
    SELECT RAISE(ABORT, 'attempt recovery checkpoints are immutable');
END;
CREATE TRIGGER attempt_recovery_checkpoint_immutable_delete
BEFORE DELETE ON attempt_recovery_checkpoints
BEGIN
    SELECT RAISE(ABORT, 'attempt recovery checkpoints are immutable');
END;
CREATE TRIGGER attempt_recovery_application_immutable_update
BEFORE UPDATE ON attempt_recovery_applications
BEGIN
    SELECT RAISE(ABORT, 'attempt recovery applications are immutable');
END;
CREATE TRIGGER attempt_recovery_application_immutable_delete
BEFORE DELETE ON attempt_recovery_applications
BEGIN
    SELECT RAISE(ABORT, 'attempt recovery applications are immutable');
END;
"""

LANDING_PLAN_NONCE_SCHEMA = """
CREATE TABLE integration_landing_plan_nonces (
    plan_id TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 1),
    nonce_sha256 TEXT NOT NULL CHECK (length(nonce_sha256) = 64),
    issued_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (plan_id, generation),
    FOREIGN KEY (plan_id)
        REFERENCES integration_landing_plans(id) ON DELETE RESTRICT,
    FOREIGN KEY (issued_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

INSERT INTO integration_landing_plan_nonces (
    plan_id, generation, nonce_sha256, issued_by, created_at
)
SELECT id, 1, nonce_sha256, issued_by, created_at
FROM integration_landing_plans;

CREATE TRIGGER integration_landing_plan_nonce_immutable_update
BEFORE UPDATE ON integration_landing_plan_nonces
BEGIN
    SELECT RAISE(ABORT, 'landing plan nonces are immutable');
END;
CREATE TRIGGER integration_landing_plan_nonce_immutable_delete
BEFORE DELETE ON integration_landing_plan_nonces
BEGIN
    SELECT RAISE(ABORT, 'landing plan nonces are immutable');
END;
"""

PROVIDER_REPORTED_OVERRUN_SCHEMA = """
CREATE TABLE provider_invocations_v22 AS
SELECT * FROM provider_invocations;

DROP TABLE provider_invocations;

CREATE TABLE provider_invocations (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    provider_grant_id TEXT NOT NULL,
    dispatch_kind TEXT NOT NULL CHECK (
        dispatch_kind IN ('direct', 'connector')
    ),
    inference_connector_id TEXT,
    connector_protocol_version INTEGER,
    deployment_sha256 TEXT,
    route TEXT,
    policy_sha256 TEXT,
    credential_scope_sha256 TEXT,
    provider TEXT,
    protocol TEXT,
    model TEXT,
    deadline TEXT,
    outcome TEXT NOT NULL CHECK (
        outcome IN (
            'dispatching', 'succeeded', 'failed', 'denied', 'busy', 'ambiguous'
        )
    ),
    request_bytes INTEGER NOT NULL CHECK (request_bytes >= 0),
    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 0),
    http_status INTEGER CHECK (http_status BETWEEN 100 AND 599),
    error_code TEXT,
    budget_account TEXT,
    budget_revision TEXT,
    budget_policy_sha256 TEXT,
    budget_currency TEXT,
    budget_policy_json TEXT,
    reserved_microusd INTEGER CHECK (reserved_microusd >= 1),
    accounted_microusd INTEGER CHECK (accounted_microusd >= 0),
    estimated_microusd INTEGER CHECK (estimated_microusd >= 0),
    accounting_status TEXT CHECK (
        accounting_status IN ('reserved', 'reported', 'released', 'conservative')
    ),
    input_tokens INTEGER CHECK (input_tokens >= 0),
    cached_input_tokens INTEGER CHECK (cached_input_tokens >= 0),
    output_tokens INTEGER CHECK (output_tokens >= 0),
    reasoning_output_tokens INTEGER CHECK (reasoning_output_tokens >= 0),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (provider_grant_id)
        REFERENCES provider_grants(id) ON DELETE RESTRICT,
    FOREIGN KEY (inference_connector_id)
        REFERENCES inference_connectors(id) ON DELETE RESTRICT,
    CHECK (
        (dispatch_kind = 'direct'
            AND inference_connector_id IS NULL
            AND connector_protocol_version IS NULL
            AND deployment_sha256 IS NULL
            AND route IS NULL
            AND policy_sha256 IS NULL
            AND credential_scope_sha256 IS NULL
            AND provider IS NULL
            AND protocol IS NULL
            AND model IS NULL
            AND deadline IS NULL)
        OR
        (dispatch_kind = 'connector'
            AND inference_connector_id IS NOT NULL
            AND connector_protocol_version IN (1, 2)
            AND length(deployment_sha256) = 64
            AND route IS NOT NULL
            AND length(policy_sha256) = 64
            AND length(credential_scope_sha256) = 64
            AND provider IS NOT NULL
            AND protocol IS NOT NULL
            AND model IS NOT NULL
            AND deadline IS NOT NULL)
    ),
    CHECK (
        (outcome = 'dispatching'
            AND dispatch_kind = 'connector'
            AND request_bytes = 0
            AND response_bytes = 0
            AND http_status IS NULL
            AND error_code IS NULL
            AND finished_at IS NULL)
        OR
        (outcome = 'succeeded'
            AND http_status BETWEEN 200 AND 299
            AND error_code IS NULL
            AND finished_at IS NOT NULL)
        OR
        (outcome IN ('failed', 'denied', 'busy', 'ambiguous')
            AND error_code IS NOT NULL
            AND finished_at IS NOT NULL)
    ),
    CHECK (
        (budget_account IS NULL
            AND budget_revision IS NULL
            AND budget_policy_sha256 IS NULL
            AND budget_currency IS NULL
            AND budget_policy_json IS NULL
            AND reserved_microusd IS NULL
            AND accounted_microusd IS NULL
            AND estimated_microusd IS NULL
            AND accounting_status IS NULL
            AND input_tokens IS NULL
            AND cached_input_tokens IS NULL
            AND output_tokens IS NULL
            AND reasoning_output_tokens IS NULL)
        OR
        (dispatch_kind = 'connector'
            AND connector_protocol_version = 2
            AND budget_account IS NOT NULL
            AND budget_revision IS NOT NULL
            AND length(budget_policy_sha256) = 64
            AND budget_currency = 'USD'
            AND budget_policy_json IS NOT NULL
            AND reserved_microusd IS NOT NULL
            AND (
                (outcome = 'dispatching'
                    AND accounting_status = 'reserved'
                    AND accounted_microusd IS NULL
                    AND estimated_microusd IS NULL
                    AND input_tokens IS NULL
                    AND cached_input_tokens IS NULL
                    AND output_tokens IS NULL
                    AND reasoning_output_tokens IS NULL)
                OR
                (outcome != 'dispatching'
                    AND accounting_status = 'reported'
                    AND accounted_microusd = estimated_microusd
                    AND input_tokens IS NOT NULL
                    AND cached_input_tokens IS NOT NULL
                    AND cached_input_tokens <= input_tokens
                    AND output_tokens IS NOT NULL
                    AND reasoning_output_tokens IS NOT NULL
                    AND reasoning_output_tokens <= output_tokens)
                OR
                (outcome != 'dispatching'
                    AND accounting_status = 'released'
                    AND accounted_microusd = 0
                    AND estimated_microusd IS NULL
                    AND input_tokens IS NULL
                    AND cached_input_tokens IS NULL
                    AND output_tokens IS NULL
                    AND reasoning_output_tokens IS NULL)
                OR
                (outcome != 'dispatching'
                    AND accounting_status = 'conservative'
                    AND accounted_microusd = reserved_microusd
                    AND estimated_microusd IS NULL
                    AND input_tokens IS NULL
                    AND cached_input_tokens IS NULL
                    AND output_tokens IS NULL
                    AND reasoning_output_tokens IS NULL)
            ))
    )
) STRICT;

INSERT INTO provider_invocations (
    id, run_id, provider_grant_id, dispatch_kind,
    inference_connector_id, connector_protocol_version, deployment_sha256,
    route, policy_sha256, credential_scope_sha256, provider, protocol, model,
    deadline, outcome, request_bytes, response_bytes, http_status, error_code,
    budget_account, budget_revision, budget_policy_sha256, budget_currency,
    budget_policy_json, reserved_microusd, accounted_microusd,
    estimated_microusd, accounting_status, input_tokens, cached_input_tokens,
    output_tokens, reasoning_output_tokens, started_at, finished_at
)
SELECT
    id, run_id, provider_grant_id, dispatch_kind,
    inference_connector_id, connector_protocol_version, deployment_sha256,
    route, policy_sha256, credential_scope_sha256, provider, protocol, model,
    deadline, outcome, request_bytes, response_bytes, http_status, error_code,
    budget_account, budget_revision, budget_policy_sha256, budget_currency,
    budget_policy_json, reserved_microusd, accounted_microusd,
    estimated_microusd, accounting_status, input_tokens, cached_input_tokens,
    output_tokens, reasoning_output_tokens, started_at, finished_at
FROM provider_invocations_v22;

DROP TABLE provider_invocations_v22;

CREATE INDEX provider_invocation_run
    ON provider_invocations(run_id, started_at, id);
CREATE INDEX provider_invocation_connector
    ON provider_invocations(inference_connector_id, started_at, id)
    WHERE inference_connector_id IS NOT NULL;
CREATE INDEX provider_invocation_budget_account
    ON provider_invocations(budget_account, outcome)
    WHERE budget_account IS NOT NULL;
CREATE INDEX provider_invocation_budget_grant
    ON provider_invocations(provider_grant_id, budget_account)
    WHERE budget_account IS NOT NULL;

CREATE TRIGGER provider_invocation_immutable_update
BEFORE UPDATE ON provider_invocations
WHEN
    OLD.outcome != 'dispatching'
    OR NEW.outcome = 'dispatching'
    OR NEW.finished_at IS NULL
    OR NEW.id IS NOT OLD.id
    OR NEW.run_id IS NOT OLD.run_id
    OR NEW.provider_grant_id IS NOT OLD.provider_grant_id
    OR NEW.dispatch_kind IS NOT OLD.dispatch_kind
    OR NEW.inference_connector_id IS NOT OLD.inference_connector_id
    OR NEW.connector_protocol_version IS NOT OLD.connector_protocol_version
    OR NEW.deployment_sha256 IS NOT OLD.deployment_sha256
    OR NEW.route IS NOT OLD.route
    OR NEW.policy_sha256 IS NOT OLD.policy_sha256
    OR NEW.credential_scope_sha256 IS NOT OLD.credential_scope_sha256
    OR NEW.provider IS NOT OLD.provider
    OR NEW.protocol IS NOT OLD.protocol
    OR NEW.model IS NOT OLD.model
    OR NEW.deadline IS NOT OLD.deadline
    OR NEW.budget_account IS NOT OLD.budget_account
    OR NEW.budget_revision IS NOT OLD.budget_revision
    OR NEW.budget_policy_sha256 IS NOT OLD.budget_policy_sha256
    OR NEW.budget_currency IS NOT OLD.budget_currency
    OR NEW.budget_policy_json IS NOT OLD.budget_policy_json
    OR NEW.reserved_microusd IS NOT OLD.reserved_microusd
    OR NEW.started_at IS NOT OLD.started_at
BEGIN
    SELECT RAISE(ABORT, 'provider invocations are immutable');
END;
CREATE TRIGGER provider_invocation_immutable_delete
BEFORE DELETE ON provider_invocations
BEGIN
    SELECT RAISE(ABORT, 'provider invocations are immutable');
END;
"""

SERVICE_SIDECAR_SCHEMA = """
CREATE TABLE service_sidecars (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('active', 'revoked')),
    version INTEGER NOT NULL CHECK (version >= 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    connector_protocol_version INTEGER NOT NULL CHECK (
        connector_protocol_version = 1
    ),
    spec_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE CHECK (length(content_sha256) = 64),
    deployment_sha256 TEXT NOT NULL CHECK (length(deployment_sha256) = 64),
    transport_kind TEXT NOT NULL CHECK (transport_kind = 'unix'),
    socket_path TEXT NOT NULL,
    request_bytes INTEGER NOT NULL CHECK (request_bytes >= 1),
    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 1),
    timeout_ms INTEGER NOT NULL CHECK (timeout_ms >= 100),
    max_concurrency INTEGER NOT NULL CHECK (
        max_concurrency BETWEEN 1 AND 16
    ),
    max_requests_per_minute INTEGER NOT NULL CHECK (
        max_requests_per_minute BETWEEN 1 AND 10000
    ),
    created_by TEXT NOT NULL,
    revoked_by TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    revoked_at TEXT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (revoked_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'active' AND revoked_by IS NULL AND revoked_at IS NULL)
        OR
        (state = 'revoked' AND revoked_by IS NOT NULL AND revoked_at IS NOT NULL)
    )
) STRICT;

CREATE TABLE service_sidecar_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_sidecar_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN ('service_sidecar.created', 'service_sidecar.revoked')
    ),
    actor_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (service_sidecar_id, sequence),
    FOREIGN KEY (service_sidecar_id)
        REFERENCES service_sidecars(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor_id) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE service_operations (
    id TEXT PRIMARY KEY,
    service_sidecar_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'pending', 'dispatching', 'uncertain', 'retryable', 'throttled',
            'succeeded', 'denied', 'conflict', 'cancelled'
        )
    ),
    version INTEGER NOT NULL CHECK (version >= 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    spec_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    sidecar_content_sha256 TEXT NOT NULL CHECK (
        length(sidecar_content_sha256) = 64
    ),
    deployment_sha256 TEXT NOT NULL CHECK (length(deployment_sha256) = 64),
    mapping_name TEXT NOT NULL,
    service TEXT NOT NULL,
    host TEXT NOT NULL,
    repository TEXT NOT NULL,
    account TEXT NOT NULL,
    credential_scope_sha256 TEXT NOT NULL CHECK (
        length(credential_scope_sha256) = 64
    ),
    capability_name TEXT NOT NULL,
    capability_version INTEGER NOT NULL CHECK (capability_version >= 1),
    capability_mode TEXT NOT NULL CHECK (capability_mode IN ('read', 'effect')),
    idempotency_key TEXT NOT NULL,
    input_sha256 TEXT NOT NULL CHECK (length(input_sha256) = 64),
    request_bytes INTEGER NOT NULL CHECK (request_bytes >= 1),
    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 1),
    timeout_ms INTEGER NOT NULL CHECK (timeout_ms >= 100),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    eligible_at TEXT,
    completed_at TEXT,
    UNIQUE (service_sidecar_id, idempotency_key),
    FOREIGN KEY (service_sidecar_id)
        REFERENCES service_sidecars(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (state IN ('pending', 'dispatching', 'uncertain')
            AND eligible_at IS NULL AND completed_at IS NULL)
        OR
        (state IN ('retryable', 'throttled')
            AND eligible_at IS NOT NULL AND completed_at IS NULL)
        OR
        (state IN ('succeeded', 'denied', 'conflict', 'cancelled')
            AND eligible_at IS NULL AND completed_at IS NOT NULL)
    )
) STRICT;

CREATE TABLE service_operation_attempts (
    id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    phase TEXT NOT NULL CHECK (phase IN ('invoke', 'reconcile')),
    outcome TEXT NOT NULL CHECK (
        outcome IN (
            'dispatching', 'succeeded', 'denied', 'conflict', 'retryable',
            'throttled', 'uncertain', 'not_found', 'unavailable',
            'ambiguous', 'abandoned'
        )
    ),
    owner_id TEXT NOT NULL,
    deadline TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    request_bytes INTEGER NOT NULL CHECK (request_bytes >= 0),
    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 0),
    provider_status INTEGER CHECK (provider_status BETWEEN 100 AND 599),
    error_code TEXT,
    retry_after_seconds INTEGER CHECK (
        retry_after_seconds BETWEEN 1 AND 2592000
    ),
    receipt_json TEXT,
    receipt_sha256 TEXT CHECK (length(receipt_sha256) = 64),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE (operation_id, sequence),
    FOREIGN KEY (operation_id)
        REFERENCES service_operations(id) ON DELETE RESTRICT,
    CHECK (
        (outcome = 'dispatching'
            AND request_bytes = 0 AND response_bytes = 0
            AND provider_status IS NULL AND error_code IS NULL
            AND retry_after_seconds IS NULL
            AND receipt_json IS NULL AND receipt_sha256 IS NULL
            AND finished_at IS NULL)
        OR
        (outcome = 'succeeded'
            AND provider_status BETWEEN 200 AND 299
            AND error_code IS NULL AND retry_after_seconds IS NULL
            AND receipt_json IS NOT NULL AND receipt_sha256 IS NOT NULL
            AND finished_at IS NOT NULL)
        OR
        (outcome NOT IN ('dispatching', 'succeeded')
            AND error_code IS NOT NULL
            AND receipt_json IS NULL AND receipt_sha256 IS NULL
            AND finished_at IS NOT NULL)
    )
) STRICT;

CREATE INDEX service_sidecar_state
    ON service_sidecars(state, created_at, id);
CREATE INDEX service_sidecar_event_stream
    ON service_sidecar_events(service_sidecar_id, sequence);
CREATE INDEX service_operation_state
    ON service_operations(state, eligible_at, created_at, id);
CREATE INDEX service_operation_sidecar
    ON service_operations(service_sidecar_id, created_at, id);
CREATE INDEX service_operation_attempt_stream
    ON service_operation_attempts(operation_id, sequence);
CREATE INDEX service_operation_attempt_dispatch
    ON service_operation_attempts(outcome, lease_expires_at, started_at);
CREATE UNIQUE INDEX service_operation_one_dispatch
    ON service_operation_attempts(operation_id) WHERE outcome = 'dispatching';

CREATE TRIGGER service_sidecar_identity_immutable
BEFORE UPDATE OF
    id, name, schema_version, connector_protocol_version, spec_json,
    content_sha256, deployment_sha256, transport_kind, socket_path,
    request_bytes, response_bytes, timeout_ms, max_concurrency,
    max_requests_per_minute, created_by, created_at
ON service_sidecars
BEGIN
    SELECT RAISE(ABORT, 'service sidecar identity is immutable');
END;
CREATE TRIGGER service_sidecar_immutable_delete
BEFORE DELETE ON service_sidecars
BEGIN
    SELECT RAISE(ABORT, 'service sidecars cannot be deleted');
END;
CREATE TRIGGER service_sidecar_event_immutable_update
BEFORE UPDATE ON service_sidecar_events
BEGIN
    SELECT RAISE(ABORT, 'service sidecar events are immutable');
END;
CREATE TRIGGER service_sidecar_event_immutable_delete
BEFORE DELETE ON service_sidecar_events
BEGIN
    SELECT RAISE(ABORT, 'service sidecar events are immutable');
END;
CREATE TRIGGER service_operation_identity_immutable
BEFORE UPDATE OF
    id, service_sidecar_id, schema_version, spec_json, content_sha256,
    sidecar_content_sha256, deployment_sha256, mapping_name, service, host,
    repository, account, credential_scope_sha256, capability_name,
    capability_version, capability_mode, idempotency_key, input_sha256,
    request_bytes, response_bytes, timeout_ms, created_by, created_at
ON service_operations
BEGIN
    SELECT RAISE(ABORT, 'service operation identity is immutable');
END;
CREATE TRIGGER service_operation_immutable_delete
BEFORE DELETE ON service_operations
BEGIN
    SELECT RAISE(ABORT, 'service operations cannot be deleted');
END;
CREATE TRIGGER service_operation_attempt_immutable_update
BEFORE UPDATE ON service_operation_attempts
WHEN
    OLD.outcome != 'dispatching'
    OR NEW.outcome = 'dispatching'
    OR NEW.finished_at IS NULL
    OR NEW.id IS NOT OLD.id
    OR NEW.operation_id IS NOT OLD.operation_id
    OR NEW.sequence IS NOT OLD.sequence
    OR NEW.phase IS NOT OLD.phase
    OR NEW.owner_id IS NOT OLD.owner_id
    OR NEW.deadline IS NOT OLD.deadline
    OR NEW.lease_expires_at IS NOT OLD.lease_expires_at
    OR NEW.started_at IS NOT OLD.started_at
BEGIN
    SELECT RAISE(ABORT, 'service operation attempts are immutable');
END;
CREATE TRIGGER service_operation_attempt_immutable_delete
BEFORE DELETE ON service_operation_attempts
BEGIN
    SELECT RAISE(ABORT, 'service operation attempts are immutable');
END;
"""

CAPABILITY_BINDING_SCHEMA = """
CREATE TABLE project_capability_bindings (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    capability_name TEXT NOT NULL,
    capability_version INTEGER NOT NULL CHECK (capability_version >= 1),
    capability_mode TEXT NOT NULL CHECK (capability_mode IN ('read', 'effect')),
    service_sidecar_id TEXT NOT NULL,
    sidecar_content_sha256 TEXT NOT NULL CHECK (
        length(sidecar_content_sha256) = 64
    ),
    deployment_sha256 TEXT NOT NULL CHECK (length(deployment_sha256) = 64),
    mapping_name TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'unbound')),
    version INTEGER NOT NULL CHECK (version >= 1),
    bound_by TEXT NOT NULL,
    unbound_by TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    unbound_at TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (service_sidecar_id)
        REFERENCES service_sidecars(id) ON DELETE RESTRICT,
    FOREIGN KEY (bound_by) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (unbound_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'active' AND version = 1
            AND unbound_by IS NULL AND unbound_at IS NULL)
        OR
        (state = 'unbound' AND version = 2
            AND unbound_by IS NOT NULL AND unbound_at IS NOT NULL)
    )
) STRICT;

CREATE TABLE project_capability_binding_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    binding_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN ('capability.bound', 'capability.unbound')
    ),
    actor_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (binding_id, sequence),
    FOREIGN KEY (binding_id)
        REFERENCES project_capability_bindings(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor_id) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE service_operation_capability_bindings (
    operation_id TEXT PRIMARY KEY,
    project_capability_binding_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (operation_id)
        REFERENCES service_operations(id) ON DELETE RESTRICT,
    FOREIGN KEY (project_capability_binding_id)
        REFERENCES project_capability_bindings(id) ON DELETE RESTRICT
) STRICT;

CREATE UNIQUE INDEX project_capability_one_active_controller
    ON project_capability_bindings(
        project_id, capability_name, capability_version
    ) WHERE state = 'active';
CREATE INDEX project_capability_binding_project
    ON project_capability_bindings(project_id, state, created_at, id);
CREATE INDEX project_capability_binding_sidecar
    ON project_capability_bindings(service_sidecar_id, state, created_at, id);
CREATE INDEX project_capability_binding_event_stream
    ON project_capability_binding_events(binding_id, sequence);
CREATE INDEX service_operation_capability_binding
    ON service_operation_capability_bindings(
        project_capability_binding_id, operation_id
    );

CREATE TRIGGER project_capability_binding_identity_immutable
BEFORE UPDATE OF
    id, project_id, capability_name, capability_version, capability_mode,
    service_sidecar_id, sidecar_content_sha256, deployment_sha256,
    mapping_name, bound_by, created_at
ON project_capability_bindings
BEGIN
    SELECT RAISE(ABORT, 'project capability binding identity is immutable');
END;
CREATE TRIGGER project_capability_binding_transition
BEFORE UPDATE ON project_capability_bindings
WHEN
    OLD.state != 'active'
    OR NEW.state != 'unbound'
    OR NEW.version != OLD.version + 1
    OR NEW.unbound_by IS NULL
    OR NEW.unbound_at IS NULL
    OR NEW.updated_at IS NOT NEW.unbound_at
BEGIN
    SELECT RAISE(ABORT, 'project capability binding transition is invalid');
END;
CREATE TRIGGER project_capability_binding_immutable_delete
BEFORE DELETE ON project_capability_bindings
BEGIN
    SELECT RAISE(ABORT, 'project capability bindings cannot be deleted');
END;
CREATE TRIGGER project_capability_binding_event_immutable_update
BEFORE UPDATE ON project_capability_binding_events
BEGIN
    SELECT RAISE(ABORT, 'project capability binding events are immutable');
END;
CREATE TRIGGER project_capability_binding_event_immutable_delete
BEFORE DELETE ON project_capability_binding_events
BEGIN
    SELECT RAISE(ABORT, 'project capability binding events cannot be deleted');
END;
CREATE TRIGGER service_operation_capability_binding_immutable_update
BEFORE UPDATE ON service_operation_capability_bindings
BEGIN
    SELECT RAISE(ABORT, 'service operation capability bindings are immutable');
END;
CREATE TRIGGER service_operation_capability_binding_immutable_delete
BEFORE DELETE ON service_operation_capability_bindings
BEGIN
    SELECT RAISE(ABORT, 'service operation capability bindings cannot be deleted');
END;
"""

PROVIDER_LANDING_SCHEMA = """
DROP TRIGGER integration_queue_event_immutable_update;
DROP TRIGGER integration_queue_event_immutable_delete;
DROP INDEX integration_queue_event_stream;

CREATE TABLE integration_queue_events_v25 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_item_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'queue.created', 'queue.claimed', 'queue.claim_expired',
            'queue.claim_released', 'queue.plan_issued',
            'queue.provider_landing_issued', 'queue.provider_landing_failed',
            'queue.not_landed', 'queue.refolded',
            'queue.refold_required', 'queue.landed', 'queue.cancelled'
        )
    ),
    from_state TEXT,
    to_state TEXT NOT NULL,
    actor TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (queue_item_id, sequence),
    FOREIGN KEY (queue_item_id)
        REFERENCES integration_queue_items(id) ON DELETE RESTRICT
) STRICT;

INSERT INTO integration_queue_events_v25 (
    id, queue_item_id, sequence, kind, from_state, to_state, actor,
    metadata_json, created_at
)
SELECT
    id, queue_item_id, sequence, kind, from_state, to_state, actor,
    metadata_json, created_at
FROM integration_queue_events;

DROP TABLE integration_queue_events;
ALTER TABLE integration_queue_events_v25 RENAME TO integration_queue_events;

CREATE INDEX integration_queue_event_stream
    ON integration_queue_events(queue_item_id, sequence);
CREATE TRIGGER integration_queue_event_immutable_update
BEFORE UPDATE ON integration_queue_events
BEGIN
    SELECT RAISE(ABORT, 'integration queue events are immutable');
END;
CREATE TRIGGER integration_queue_event_immutable_delete
BEFORE DELETE ON integration_queue_events
BEGIN
    SELECT RAISE(ABORT, 'integration queue events cannot be deleted');
END;

CREATE TABLE integration_provider_landings (
    id TEXT PRIMARY KEY,
    queue_item_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    request_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    evidence_snapshot_json TEXT NOT NULL,
    evidence_snapshot_sha256 TEXT NOT NULL,
    service_operation_id TEXT NOT NULL UNIQUE,
    capability_binding_id TEXT NOT NULL,
    assurance TEXT NOT NULL CHECK (assurance = 'head_enforced'),
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'landed', 'not_landed', 'failed')
    ),
    issued_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    UNIQUE (queue_item_id, sequence),
    FOREIGN KEY (queue_item_id)
        REFERENCES integration_queue_items(id) ON DELETE RESTRICT,
    FOREIGN KEY (service_operation_id)
        REFERENCES service_operations(id) ON DELETE RESTRICT,
    FOREIGN KEY (capability_binding_id)
        REFERENCES project_capability_bindings(id) ON DELETE RESTRICT,
    FOREIGN KEY (issued_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'pending' AND resolved_at IS NULL)
        OR (state IN ('landed', 'not_landed', 'failed')
            AND resolved_at IS NOT NULL)
    )
) STRICT;

CREATE UNIQUE INDEX integration_provider_one_pending
    ON integration_provider_landings(queue_item_id) WHERE state = 'pending';
CREATE INDEX integration_provider_landing_created
    ON integration_provider_landings(created_at DESC, id DESC);

CREATE TRIGGER integration_provider_landing_identity_immutable
BEFORE UPDATE OF
    id, queue_item_id, sequence, schema_version, request_json, content_sha256,
    evidence_snapshot_json, evidence_snapshot_sha256, service_operation_id,
    capability_binding_id, assurance, issued_by, created_at
ON integration_provider_landings
BEGIN
    SELECT RAISE(ABORT, 'provider landing identity is immutable');
END;
CREATE TRIGGER integration_provider_landing_transition
BEFORE UPDATE ON integration_provider_landings
WHEN
    OLD.state != 'pending'
    OR NEW.state NOT IN ('landed', 'not_landed', 'failed')
    OR NEW.resolved_at IS NULL
BEGIN
    SELECT RAISE(ABORT, 'provider landing transition is invalid');
END;
CREATE TRIGGER integration_provider_landing_immutable_delete
BEFORE DELETE ON integration_provider_landings
BEGIN
    SELECT RAISE(ABORT, 'provider landings cannot be deleted');
END;

DROP TRIGGER integration_landing_receipt_immutable_update;
DROP TRIGGER integration_landing_receipt_immutable_delete;
DROP INDEX integration_receipt_created;

CREATE TABLE integration_landing_receipts_v25 (
    id TEXT PRIMARY KEY,
    plan_id TEXT UNIQUE,
    provider_landing_id TEXT UNIQUE,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    receipt_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    outcome TEXT NOT NULL CHECK (outcome IN ('landed', 'not_landed')),
    observed_revision TEXT NOT NULL,
    assurance TEXT NOT NULL CHECK (
        assurance IN (
            'base_and_head_enforced', 'head_enforced', 'operator_attested'
        )
    ),
    recorded_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (plan_id)
        REFERENCES integration_landing_plans(id) ON DELETE RESTRICT,
    FOREIGN KEY (provider_landing_id)
        REFERENCES integration_provider_landings(id) ON DELETE RESTRICT,
    FOREIGN KEY (recorded_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK ((plan_id IS NULL) != (provider_landing_id IS NULL))
) STRICT;

INSERT INTO integration_landing_receipts_v25 (
    id, plan_id, provider_landing_id, schema_version, receipt_json,
    content_sha256, outcome, observed_revision, assurance, recorded_by, created_at
)
SELECT
    id, plan_id, NULL, schema_version, receipt_json, content_sha256,
    outcome, observed_revision, assurance, recorded_by, created_at
FROM integration_landing_receipts;

DROP TABLE integration_landing_receipts;
ALTER TABLE integration_landing_receipts_v25
    RENAME TO integration_landing_receipts;

CREATE INDEX integration_receipt_created
    ON integration_landing_receipts(created_at DESC, id DESC);
CREATE TRIGGER integration_landing_receipt_immutable_update
BEFORE UPDATE ON integration_landing_receipts
BEGIN
    SELECT RAISE(ABORT, 'landing receipts are immutable');
END;
CREATE TRIGGER integration_landing_receipt_immutable_delete
BEFORE DELETE ON integration_landing_receipts
BEGIN
    SELECT RAISE(ABORT, 'landing receipts cannot be deleted');
END;
"""

WORKFLOW_TEMPLATE_SCHEMA = """
CREATE TABLE workflow_template_definitions (
    id TEXT PRIMARY KEY,
    template_key TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    definition_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    built_in INTEGER NOT NULL CHECK (built_in = 1),
    created_at TEXT NOT NULL,
    UNIQUE (template_key, version)
) STRICT;

INSERT INTO workflow_template_definitions (
    id, template_key, version, schema_version, definition_json,
    content_sha256, built_in, created_at
) VALUES (
    'wtd_b165a0973686c2e87ed8b4150e919e88',
    'implementation-v1',
    1,
    1,
    '{"correction_loop":true,"key":"implementation-v1","roles":[{"name":"producer","workspace_mode":"copy_on_write"},{"name":"reviewer","workspace_mode":"read_only"}],"schema_version":1,"stages":["build","review","maintainer","landing","completion"],"version":1}',
    'b165a0973686c2e87ed8b4150e919e88f826b8558fe560eafcb256a87b5d75ff',
    1,
    '2026-10-23T00:00:00.000000+00:00'
);

CREATE TABLE workflow_configurations (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (project_id, name),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE workflow_configuration_versions (
    id TEXT PRIMARY KEY,
    configuration_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    template_definition_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    spec_json TEXT NOT NULL,
    producer_profile_version_id TEXT NOT NULL,
    reviewer_profile_version_id TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (configuration_id, version),
    UNIQUE (configuration_id, content_sha256),
    FOREIGN KEY (configuration_id)
        REFERENCES workflow_configurations(id) ON DELETE RESTRICT,
    FOREIGN KEY (template_definition_id)
        REFERENCES workflow_template_definitions(id) ON DELETE RESTRICT,
    FOREIGN KEY (producer_profile_version_id)
        REFERENCES agent_profile_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (reviewer_profile_version_id)
        REFERENCES agent_profile_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX workflow_configuration_project
    ON workflow_configurations(project_id, name);
CREATE INDEX workflow_configuration_version_created
    ON workflow_configuration_versions(configuration_id, version DESC);

CREATE TABLE workflow_executions (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    template_definition_id TEXT NOT NULL,
    configuration_version_id TEXT NOT NULL,
    context_version_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL,
    work_item_version_id TEXT NOT NULL,
    work_thread_id TEXT NOT NULL,
    work_thread_generation INTEGER NOT NULL CHECK (work_thread_generation >= 1),
    source_revision TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'cancelled', 'completed')),
    stage TEXT NOT NULL CHECK (
        stage IN ('build', 'review', 'maintainer', 'landing', 'completion')
    ),
    version INTEGER NOT NULL CHECK (version >= 1),
    request_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    cancel_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE (requested_by, idempotency_key),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (template_definition_id)
        REFERENCES workflow_template_definitions(id) ON DELETE RESTRICT,
    FOREIGN KEY (configuration_version_id)
        REFERENCES workflow_configuration_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (context_version_id)
        REFERENCES project_context_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_item_id) REFERENCES work_items(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_item_version_id)
        REFERENCES work_item_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_thread_id) REFERENCES work_threads(id) ON DELETE RESTRICT,
    FOREIGN KEY (requested_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'active' AND finished_at IS NULL AND cancel_reason IS NULL)
        OR (state = 'cancelled' AND finished_at IS NOT NULL
            AND cancel_reason IS NOT NULL)
        OR (state = 'completed' AND finished_at IS NOT NULL
            AND cancel_reason IS NULL)
    )
) STRICT;

CREATE UNIQUE INDEX workflow_execution_one_active_thread
    ON workflow_executions(work_thread_id) WHERE state = 'active';
CREATE INDEX workflow_execution_project_created
    ON workflow_executions(project_id, created_at DESC, id DESC);
CREATE INDEX workflow_execution_state_created
    ON workflow_executions(state, created_at, id);

CREATE TABLE workflow_execution_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN ('workflow.started', 'workflow.cancelled')
    ),
    from_state TEXT,
    to_state TEXT NOT NULL CHECK (
        to_state IN ('active', 'cancelled', 'completed')
    ),
    actor TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (execution_id, sequence),
    FOREIGN KEY (execution_id)
        REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX workflow_execution_event_stream
    ON workflow_execution_events(execution_id, sequence);

CREATE TRIGGER workflow_template_definition_immutable_update
BEFORE UPDATE ON workflow_template_definitions
BEGIN
    SELECT RAISE(ABORT, 'workflow template definitions are immutable');
END;
CREATE TRIGGER workflow_template_definition_immutable_delete
BEFORE DELETE ON workflow_template_definitions
BEGIN
    SELECT RAISE(ABORT, 'workflow template definitions cannot be deleted');
END;
CREATE TRIGGER workflow_configuration_immutable_update
BEFORE UPDATE ON workflow_configurations
BEGIN
    SELECT RAISE(ABORT, 'workflow configurations are immutable');
END;
CREATE TRIGGER workflow_configuration_immutable_delete
BEFORE DELETE ON workflow_configurations
BEGIN
    SELECT RAISE(ABORT, 'workflow configurations cannot be deleted');
END;
CREATE TRIGGER workflow_configuration_version_immutable_update
BEFORE UPDATE ON workflow_configuration_versions
BEGIN
    SELECT RAISE(ABORT, 'workflow configuration versions are immutable');
END;
CREATE TRIGGER workflow_configuration_version_immutable_delete
BEFORE DELETE ON workflow_configuration_versions
BEGIN
    SELECT RAISE(ABORT, 'workflow configuration versions cannot be deleted');
END;
CREATE TRIGGER workflow_execution_identity_immutable
BEFORE UPDATE OF
    id, project_id, template_definition_id, configuration_version_id,
    context_version_id, work_item_id, work_item_version_id, work_thread_id,
    work_thread_generation, source_revision, request_json, content_sha256,
    idempotency_key, requested_by, created_at
ON workflow_executions
BEGIN
    SELECT RAISE(ABORT, 'workflow execution identity is immutable');
END;
CREATE TRIGGER workflow_execution_transition
BEFORE UPDATE ON workflow_executions
WHEN
    OLD.state != 'active'
    OR NEW.version != OLD.version + 1
    OR NEW.state NOT IN ('active', 'cancelled', 'completed')
    OR (NEW.state = 'active'
        AND (NEW.finished_at IS NOT NULL OR NEW.cancel_reason IS NOT NULL))
    OR (NEW.state = 'cancelled'
        AND (NEW.finished_at IS NULL OR NEW.cancel_reason IS NULL))
    OR (NEW.state = 'completed'
        AND (NEW.finished_at IS NULL OR NEW.cancel_reason IS NOT NULL))
BEGIN
    SELECT RAISE(ABORT, 'workflow execution transition is invalid');
END;
CREATE TRIGGER workflow_execution_immutable_delete
BEFORE DELETE ON workflow_executions
BEGIN
    SELECT RAISE(ABORT, 'workflow executions cannot be deleted');
END;
CREATE TRIGGER workflow_execution_event_immutable_update
BEFORE UPDATE ON workflow_execution_events
BEGIN
    SELECT RAISE(ABORT, 'workflow execution events are immutable');
END;
CREATE TRIGGER workflow_execution_event_immutable_delete
BEFORE DELETE ON workflow_execution_events
BEGIN
    SELECT RAISE(ABORT, 'workflow execution events cannot be deleted');
END;
"""

WORKFLOW_DIRECTOR_SCHEMA = """
ALTER TABLE workflow_executions ADD COLUMN correction_round INTEGER NOT NULL
    DEFAULT 0 CHECK (correction_round BETWEEN 0 AND 8);

CREATE TABLE workflow_run_bindings (
    run_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    correction_round INTEGER NOT NULL CHECK (correction_round BETWEEN 0 AND 8),
    role TEXT NOT NULL CHECK (role IN ('producer', 'reviewer')),
    created_at TEXT NOT NULL,
    UNIQUE (execution_id, correction_round, role),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (execution_id) REFERENCES workflow_executions(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE workflow_gate_bindings (
    execution_id TEXT NOT NULL,
    correction_round INTEGER NOT NULL CHECK (correction_round BETWEEN 0 AND 8),
    candidate_id TEXT NOT NULL UNIQUE,
    policy_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    PRIMARY KEY (execution_id, correction_round),
    FOREIGN KEY (execution_id) REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE integration_review_verdicts (
    work_log_entry_id TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    head_revision TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('approved', 'changes_requested')),
    reviewer_run_id TEXT NOT NULL,
    reviewer_attempt_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    FOREIGN KEY (work_log_entry_id)
        REFERENCES work_log_entries(id) ON DELETE RESTRICT,
    FOREIGN KEY (policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (reviewer_run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (reviewer_attempt_id)
        REFERENCES run_attempts(id) ON DELETE RESTRICT
) STRICT;

CREATE UNIQUE INDEX integration_review_verdict_policy_attempt
    ON integration_review_verdicts(policy_id, reviewer_attempt_id);

CREATE TABLE workflow_rulings (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    execution_version INTEGER NOT NULL CHECK (execution_version >= 1),
    correction_round INTEGER NOT NULL CHECK (correction_round BETWEEN 0 AND 8),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'correction_limit', 'producer_failed', 'reviewer_failed',
            'review_verdict_missing', 'validation_failed'
        )
    ),
    state TEXT NOT NULL CHECK (state IN ('open', 'resolved')),
    version INTEGER NOT NULL CHECK (version >= 1),
    policy_id TEXT,
    run_id TEXT,
    gate_name TEXT,
    resolved_by TEXT,
    decision TEXT CHECK (decision IS NULL OR decision IN ('continue', 'cancel')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    resolved_at TEXT,
    UNIQUE (execution_id, execution_version, kind),
    FOREIGN KEY (execution_id) REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (resolved_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'open' AND resolved_by IS NULL AND decision IS NULL
            AND resolved_at IS NULL)
        OR
        (state = 'resolved' AND resolved_by IS NOT NULL AND decision IS NOT NULL
            AND resolved_at IS NOT NULL)
    )
) STRICT;

CREATE UNIQUE INDEX workflow_ruling_one_open
    ON workflow_rulings(execution_id) WHERE state = 'open';

CREATE TRIGGER workflow_run_binding_immutable_update
BEFORE UPDATE ON workflow_run_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow run bindings are immutable');
END;
CREATE TRIGGER workflow_run_binding_immutable_delete
BEFORE DELETE ON workflow_run_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow run bindings cannot be deleted');
END;
CREATE TRIGGER workflow_gate_binding_immutable_update
BEFORE UPDATE ON workflow_gate_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow gate bindings are immutable');
END;
CREATE TRIGGER workflow_gate_binding_immutable_delete
BEFORE DELETE ON workflow_gate_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow gate bindings cannot be deleted');
END;
CREATE TRIGGER integration_review_verdict_immutable_update
BEFORE UPDATE ON integration_review_verdicts
BEGIN
    SELECT RAISE(ABORT, 'integration review verdicts are immutable');
END;
CREATE TRIGGER integration_review_verdict_immutable_delete
BEFORE DELETE ON integration_review_verdicts
BEGIN
    SELECT RAISE(ABORT, 'integration review verdicts cannot be deleted');
END;
CREATE TRIGGER workflow_ruling_identity_immutable
BEFORE UPDATE OF
    id, execution_id, execution_version, correction_round, kind,
    policy_id, run_id, gate_name, created_at
ON workflow_rulings
BEGIN
    SELECT RAISE(ABORT, 'workflow ruling identity is immutable');
END;
CREATE TRIGGER workflow_ruling_immutable_delete
BEFORE DELETE ON workflow_rulings
BEGIN
    SELECT RAISE(ABORT, 'workflow rulings cannot be deleted');
END;

DROP TRIGGER workflow_execution_transition;
CREATE TRIGGER workflow_execution_transition
BEFORE UPDATE ON workflow_executions
WHEN
    OLD.state != 'active'
    OR NEW.version != OLD.version + 1
    OR NEW.state NOT IN ('active', 'cancelled', 'completed')
    OR NEW.correction_round < OLD.correction_round
    OR NEW.correction_round > OLD.correction_round + 1
    OR (NEW.state = 'active'
        AND (NEW.finished_at IS NOT NULL OR NEW.cancel_reason IS NOT NULL))
    OR (NEW.state = 'cancelled'
        AND (NEW.finished_at IS NULL OR NEW.cancel_reason IS NULL
            OR NEW.stage != OLD.stage
            OR NEW.correction_round != OLD.correction_round))
    OR (NEW.state = 'completed'
        AND (NEW.finished_at IS NULL OR NEW.cancel_reason IS NOT NULL))
    OR (NEW.state = 'active' AND NOT (
        (NEW.stage = OLD.stage
            AND NEW.correction_round = OLD.correction_round)
        OR (OLD.stage = 'build' AND NEW.stage = 'review'
            AND NEW.correction_round = OLD.correction_round)
        OR (OLD.stage = 'review' AND NEW.stage IN ('maintainer', 'landing')
            AND NEW.correction_round = OLD.correction_round)
        OR (OLD.stage IN ('review', 'maintainer') AND NEW.stage = 'build'
            AND NEW.correction_round = OLD.correction_round + 1)
        OR (OLD.stage = 'maintainer' AND NEW.stage = 'landing'
            AND NEW.correction_round = OLD.correction_round)
    ))
BEGIN
    SELECT RAISE(ABORT, 'workflow execution transition is invalid');
END;

DROP TRIGGER workflow_execution_event_immutable_update;
DROP TRIGGER workflow_execution_event_immutable_delete;
DROP INDEX workflow_execution_event_stream;
ALTER TABLE workflow_execution_events RENAME TO workflow_execution_events_v28;
CREATE TABLE workflow_execution_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'workflow.started', 'workflow.cancelled',
            'workflow.review_started', 'workflow.correction_started',
            'workflow.maintainer_pending', 'workflow.landing_ready',
            'workflow.ruling_opened', 'workflow.ruling_resolved'
        )
    ),
    from_state TEXT,
    to_state TEXT NOT NULL CHECK (
        to_state IN ('active', 'cancelled', 'completed')
    ),
    actor TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (execution_id, sequence),
    FOREIGN KEY (execution_id)
        REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;
INSERT INTO workflow_execution_events
    (id, execution_id, sequence, kind, from_state, to_state, actor,
     metadata_json, created_at)
SELECT id, execution_id, sequence, kind, from_state, to_state, actor,
       metadata_json, created_at
FROM workflow_execution_events_v28;
DROP TABLE workflow_execution_events_v28;
CREATE INDEX workflow_execution_event_stream
    ON workflow_execution_events(execution_id, sequence);
CREATE TRIGGER workflow_execution_event_immutable_update
BEFORE UPDATE ON workflow_execution_events
BEGIN
    SELECT RAISE(ABORT, 'workflow execution events are immutable');
END;
CREATE TRIGGER workflow_execution_event_immutable_delete
BEFORE DELETE ON workflow_execution_events
BEGIN
    SELECT RAISE(ABORT, 'workflow execution events cannot be deleted');
END;

DROP TRIGGER supervisor_action_identity_immutable;
DROP TRIGGER supervisor_action_immutable_delete;
DROP INDEX supervisor_action_due;
DROP INDEX supervisor_action_run;
ALTER TABLE supervisor_actions RENAME TO supervisor_actions_v28;
CREATE TABLE supervisor_actions (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN (
            'run.execute', 'attempt.reconcile', 'run.reconcile',
            'workflow.advance'
        )
    ),
    subject_id TEXT NOT NULL,
    subject_version INTEGER NOT NULL CHECK (subject_version >= 1),
    run_id TEXT,
    workflow_execution_id TEXT,
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'claimed', 'succeeded', 'exhausted')
    ),
    version INTEGER NOT NULL CHECK (version >= 1),
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 9),
    retry_backoff_json TEXT NOT NULL,
    available_at TEXT NOT NULL,
    owner_id TEXT,
    lease_expires_at TEXT,
    last_error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE (kind, subject_id, subject_version),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (workflow_execution_id)
        REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    CHECK (
        (kind = 'workflow.advance' AND run_id IS NULL
            AND workflow_execution_id = subject_id)
        OR
        (kind != 'workflow.advance' AND run_id IS NOT NULL
            AND workflow_execution_id IS NULL)
    ),
    CHECK (
        (
            state = 'pending' AND owner_id IS NULL
            AND lease_expires_at IS NULL AND finished_at IS NULL
        ) OR (
            state = 'claimed' AND owner_id IS NOT NULL
            AND lease_expires_at IS NOT NULL AND finished_at IS NULL
        ) OR (
            state IN ('succeeded', 'exhausted') AND owner_id IS NULL
            AND lease_expires_at IS NULL AND finished_at IS NOT NULL
        )
    )
) STRICT;
INSERT INTO supervisor_actions
    (id, kind, subject_id, subject_version, run_id, workflow_execution_id,
     state, version, attempt_count, max_attempts, retry_backoff_json,
     available_at, owner_id, lease_expires_at, last_error_code, created_at,
     updated_at, finished_at)
SELECT id, kind, subject_id, subject_version, run_id, NULL,
       state, version, attempt_count, max_attempts, retry_backoff_json,
       available_at, owner_id, lease_expires_at, last_error_code, created_at,
       updated_at, finished_at
FROM supervisor_actions_v28;
DROP TABLE supervisor_actions_v28;
CREATE INDEX supervisor_action_due
    ON supervisor_actions(state, available_at, lease_expires_at);
CREATE INDEX supervisor_action_run
    ON supervisor_actions(run_id, created_at DESC);
CREATE INDEX supervisor_action_workflow
    ON supervisor_actions(workflow_execution_id, created_at DESC);
CREATE TRIGGER supervisor_action_identity_immutable
BEFORE UPDATE OF
    id, kind, subject_id, subject_version, run_id, workflow_execution_id,
    max_attempts, retry_backoff_json, created_at
ON supervisor_actions
BEGIN
    SELECT RAISE(ABORT, 'supervisor action identity is immutable');
END;
CREATE TRIGGER supervisor_action_immutable_delete
BEFORE DELETE ON supervisor_actions
BEGIN
    SELECT RAISE(ABORT, 'supervisor actions cannot be deleted');
END;
"""

WORKFLOW_LANDING_SCHEMA = """
DROP TRIGGER work_log_entry_immutable_update;
DROP TRIGGER work_log_entry_immutable_delete;
DROP INDEX work_log_thread_sequence;
CREATE TABLE work_log_entries_v30 (
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
    author_run_id TEXT,
    author_attempt_id TEXT,
    actor_id TEXT,
    workflow_execution_id TEXT,
    assignment_id TEXT,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (work_thread_id, sequence),
    UNIQUE (work_thread_id, author_run_id, idempotency_key),
    FOREIGN KEY (work_thread_id) REFERENCES work_threads(id) ON DELETE RESTRICT,
    FOREIGN KEY (author_run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (author_attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor_id) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (workflow_execution_id)
        REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (assignment_id) REFERENCES work_assignments(id) ON DELETE RESTRICT,
    CHECK (
        (
            author_run_id IS NOT NULL AND author_attempt_id IS NOT NULL
            AND actor_id IS NULL AND workflow_execution_id IS NULL
        ) OR (
            author_run_id IS NULL AND author_attempt_id IS NULL
            AND actor_id IS NOT NULL AND workflow_execution_id IS NOT NULL
            AND assignment_id IS NULL AND kind = 'outcome'
        )
    )
) STRICT;
INSERT INTO work_log_entries_v30 (
    id, work_thread_id, sequence, kind, content_json, content_sha256,
    author_run_id, author_attempt_id, actor_id, workflow_execution_id,
    assignment_id, idempotency_key, created_at
)
SELECT
    id, work_thread_id, sequence, kind, content_json, content_sha256,
    author_run_id, author_attempt_id, NULL, NULL,
    assignment_id, idempotency_key, created_at
FROM work_log_entries;
DROP TABLE work_log_entries;
ALTER TABLE work_log_entries_v30 RENAME TO work_log_entries;
CREATE INDEX work_log_thread_sequence
    ON work_log_entries(work_thread_id, sequence);
CREATE UNIQUE INDEX work_log_workflow_outcome
    ON work_log_entries(workflow_execution_id, idempotency_key)
    WHERE workflow_execution_id IS NOT NULL;
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

DROP TRIGGER workflow_ruling_identity_immutable;
DROP TRIGGER workflow_ruling_immutable_delete;
DROP INDEX workflow_ruling_one_open;
CREATE TABLE workflow_rulings_v30 (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    execution_version INTEGER NOT NULL CHECK (execution_version >= 1),
    correction_round INTEGER NOT NULL CHECK (correction_round BETWEEN 0 AND 8),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'correction_limit', 'producer_failed', 'reviewer_failed',
            'review_verdict_missing', 'validation_failed',
            'landing_failed', 'landing_authority_mismatch', 'landing_cancelled'
        )
    ),
    state TEXT NOT NULL CHECK (state IN ('open', 'resolved')),
    version INTEGER NOT NULL CHECK (version >= 1),
    policy_id TEXT,
    run_id TEXT,
    gate_name TEXT,
    provider_landing_id TEXT,
    landing_plan_id TEXT,
    resolved_by TEXT,
    decision TEXT CHECK (decision IS NULL OR decision IN ('continue', 'cancel')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    resolved_at TEXT,
    UNIQUE (execution_id, execution_version, kind),
    FOREIGN KEY (execution_id) REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (provider_landing_id)
        REFERENCES integration_provider_landings(id) ON DELETE RESTRICT,
    FOREIGN KEY (landing_plan_id)
        REFERENCES integration_landing_plans(id) ON DELETE RESTRICT,
    FOREIGN KEY (resolved_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'open' AND resolved_by IS NULL AND decision IS NULL
            AND resolved_at IS NULL)
        OR
        (state = 'resolved' AND resolved_by IS NOT NULL AND decision IS NOT NULL
            AND resolved_at IS NOT NULL)
    ),
    CHECK (
        (kind = 'landing_failed' AND provider_landing_id IS NOT NULL
            AND landing_plan_id IS NULL)
        OR
        (kind = 'landing_authority_mismatch'
            AND (provider_landing_id IS NOT NULL) != (landing_plan_id IS NOT NULL))
        OR
        (kind NOT IN ('landing_failed', 'landing_authority_mismatch')
            AND provider_landing_id IS NULL AND landing_plan_id IS NULL)
    )
) STRICT;
INSERT INTO workflow_rulings_v30 (
    id, execution_id, execution_version, correction_round, kind, state,
    version, policy_id, run_id, gate_name, provider_landing_id,
    landing_plan_id, resolved_by, decision, created_at, updated_at, resolved_at
)
SELECT
    id, execution_id, execution_version, correction_round, kind, state,
    version, policy_id, run_id, gate_name, NULL, NULL,
    resolved_by, decision, created_at, updated_at, resolved_at
FROM workflow_rulings;
DROP TABLE workflow_rulings;
ALTER TABLE workflow_rulings_v30 RENAME TO workflow_rulings;
CREATE UNIQUE INDEX workflow_ruling_one_open
    ON workflow_rulings(execution_id) WHERE state = 'open';
CREATE TRIGGER workflow_ruling_identity_immutable
BEFORE UPDATE OF
    id, execution_id, execution_version, correction_round, kind,
    policy_id, run_id, gate_name, provider_landing_id, landing_plan_id, created_at
ON workflow_rulings
BEGIN
    SELECT RAISE(ABORT, 'workflow ruling identity is immutable');
END;
CREATE TRIGGER workflow_ruling_immutable_delete
BEFORE DELETE ON workflow_rulings
BEGIN
    SELECT RAISE(ABORT, 'workflow rulings cannot be deleted');
END;

CREATE TABLE workflow_landing_bindings (
    execution_id TEXT PRIMARY KEY,
    correction_round INTEGER NOT NULL CHECK (correction_round BETWEEN 0 AND 8),
    candidate_id TEXT NOT NULL UNIQUE,
    policy_id TEXT NOT NULL UNIQUE,
    queue_item_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    FOREIGN KEY (execution_id)
        REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
    FOREIGN KEY (queue_item_id)
        REFERENCES integration_queue_items(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE workflow_provider_targets (
    execution_id TEXT PRIMARY KEY,
    queue_item_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL,
    merge_method TEXT NOT NULL CHECK (merge_method IN ('merge', 'rebase', 'squash')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (execution_id)
        REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (queue_item_id)
        REFERENCES integration_queue_items(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE workflow_completions (
    execution_id TEXT PRIMARY KEY,
    queue_item_id TEXT NOT NULL UNIQUE,
    receipt_id TEXT NOT NULL UNIQUE,
    outcome_entry_id TEXT NOT NULL UNIQUE,
    candidate_id TEXT NOT NULL UNIQUE,
    policy_id TEXT NOT NULL,
    observed_revision TEXT NOT NULL,
    completed_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (execution_id)
        REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (queue_item_id)
        REFERENCES integration_queue_items(id) ON DELETE RESTRICT,
    FOREIGN KEY (receipt_id)
        REFERENCES integration_landing_receipts(id) ON DELETE RESTRICT,
    FOREIGN KEY (outcome_entry_id)
        REFERENCES work_log_entries(id) ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
    FOREIGN KEY (completed_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER workflow_landing_binding_immutable_update
BEFORE UPDATE ON workflow_landing_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow landing bindings are immutable');
END;
CREATE TRIGGER workflow_landing_binding_immutable_delete
BEFORE DELETE ON workflow_landing_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow landing bindings cannot be deleted');
END;
CREATE TRIGGER workflow_provider_target_immutable_update
BEFORE UPDATE ON workflow_provider_targets
BEGIN
    SELECT RAISE(ABORT, 'workflow provider targets are immutable');
END;
CREATE TRIGGER workflow_provider_target_immutable_delete
BEFORE DELETE ON workflow_provider_targets
BEGIN
    SELECT RAISE(ABORT, 'workflow provider targets cannot be deleted');
END;
CREATE TRIGGER workflow_completion_immutable_update
BEFORE UPDATE ON workflow_completions
BEGIN
    SELECT RAISE(ABORT, 'workflow completions are immutable');
END;
CREATE TRIGGER workflow_completion_immutable_delete
BEFORE DELETE ON workflow_completions
BEGIN
    SELECT RAISE(ABORT, 'workflow completions cannot be deleted');
END;

DROP TRIGGER workflow_execution_transition;
CREATE TRIGGER workflow_execution_transition
BEFORE UPDATE ON workflow_executions
WHEN
    OLD.state != 'active'
    OR NEW.version != OLD.version + 1
    OR NEW.state NOT IN ('active', 'cancelled', 'completed')
    OR NEW.correction_round < OLD.correction_round
    OR NEW.correction_round > OLD.correction_round + 1
    OR (NEW.state = 'active'
        AND (NEW.finished_at IS NOT NULL OR NEW.cancel_reason IS NOT NULL))
    OR (NEW.state = 'cancelled'
        AND (NEW.finished_at IS NULL OR NEW.cancel_reason IS NULL
            OR NEW.stage != OLD.stage
            OR NEW.correction_round != OLD.correction_round))
    OR (NEW.state = 'completed'
        AND (NEW.finished_at IS NULL OR NEW.cancel_reason IS NOT NULL
            OR NEW.stage != 'completion'
            OR NEW.correction_round != OLD.correction_round))
    OR (NEW.state = 'active' AND NOT (
        (NEW.stage = OLD.stage
            AND NEW.correction_round = OLD.correction_round)
        OR (OLD.stage = 'build' AND NEW.stage = 'review'
            AND NEW.correction_round = OLD.correction_round)
        OR (OLD.stage = 'review' AND NEW.stage IN ('maintainer', 'landing')
            AND NEW.correction_round = OLD.correction_round)
        OR (OLD.stage IN ('review', 'maintainer') AND NEW.stage = 'build'
            AND NEW.correction_round = OLD.correction_round + 1)
        OR (OLD.stage = 'maintainer' AND NEW.stage = 'landing'
            AND NEW.correction_round = OLD.correction_round)
        OR (OLD.stage = 'landing' AND NEW.stage = 'completion'
            AND NEW.correction_round = OLD.correction_round)
    ))
BEGIN
    SELECT RAISE(ABORT, 'workflow execution transition is invalid');
END;

DROP TRIGGER workflow_execution_event_immutable_update;
DROP TRIGGER workflow_execution_event_immutable_delete;
DROP INDEX workflow_execution_event_stream;
CREATE TABLE workflow_execution_events_v30 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'workflow.started', 'workflow.cancelled',
            'workflow.review_started', 'workflow.correction_started',
            'workflow.maintainer_pending', 'workflow.landing_ready',
            'workflow.landing_enqueued', 'workflow.landing_recorded',
            'workflow.completed',
            'workflow.ruling_opened', 'workflow.ruling_resolved'
        )
    ),
    from_state TEXT,
    to_state TEXT NOT NULL CHECK (
        to_state IN ('active', 'cancelled', 'completed')
    ),
    actor TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (execution_id, sequence),
    FOREIGN KEY (execution_id)
        REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;
INSERT INTO workflow_execution_events_v30
    (id, execution_id, sequence, kind, from_state, to_state, actor,
     metadata_json, created_at)
SELECT id, execution_id, sequence, kind, from_state, to_state, actor,
       metadata_json, created_at
FROM workflow_execution_events;
DROP TABLE workflow_execution_events;
ALTER TABLE workflow_execution_events_v30 RENAME TO workflow_execution_events;
CREATE INDEX workflow_execution_event_stream
    ON workflow_execution_events(execution_id, sequence);
CREATE TRIGGER workflow_execution_event_immutable_update
BEFORE UPDATE ON workflow_execution_events
BEGIN
    SELECT RAISE(ABORT, 'workflow execution events are immutable');
END;
CREATE TRIGGER workflow_execution_event_immutable_delete
BEFORE DELETE ON workflow_execution_events
BEGIN
    SELECT RAISE(ABORT, 'workflow execution events cannot be deleted');
END;
"""

WORKFLOW_LANDING_CHAIN_SCHEMA = """
DROP TRIGGER integration_queue_event_immutable_update;
DROP TRIGGER integration_queue_event_immutable_delete;
DROP INDEX integration_queue_event_stream;

CREATE TABLE integration_queue_events_v31 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_item_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'queue.created', 'queue.claimed', 'queue.claim_expired',
            'queue.claim_released', 'queue.plan_issued',
            'queue.provider_landing_issued', 'queue.provider_landing_failed',
            'queue.not_landed', 'queue.refolded',
            'queue.refold_required', 'queue.workflow_chain_accepted',
            'queue.landed', 'queue.cancelled'
        )
    ),
    from_state TEXT,
    to_state TEXT NOT NULL,
    actor TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (queue_item_id, sequence),
    FOREIGN KEY (queue_item_id)
        REFERENCES integration_queue_items(id) ON DELETE RESTRICT
) STRICT;

INSERT INTO integration_queue_events_v31 (
    id, queue_item_id, sequence, kind, from_state, to_state, actor,
    metadata_json, created_at
)
SELECT
    id, queue_item_id, sequence, kind, from_state, to_state, actor,
    metadata_json, created_at
FROM integration_queue_events;

DROP TABLE integration_queue_events;
ALTER TABLE integration_queue_events_v31 RENAME TO integration_queue_events;

CREATE INDEX integration_queue_event_stream
    ON integration_queue_events(queue_item_id, sequence);
CREATE TRIGGER integration_queue_event_immutable_update
BEFORE UPDATE ON integration_queue_events
BEGIN
    SELECT RAISE(ABORT, 'integration queue events are immutable');
END;
CREATE TRIGGER integration_queue_event_immutable_delete
BEFORE DELETE ON integration_queue_events
BEGIN
    SELECT RAISE(ABORT, 'integration queue events cannot be deleted');
END;

CREATE TABLE workflow_landing_chains (
    execution_id TEXT PRIMARY KEY,
    queue_item_id TEXT NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    correction_round INTEGER NOT NULL CHECK (correction_round BETWEEN 1 AND 8),
    candidate_id TEXT NOT NULL UNIQUE,
    policy_id TEXT NOT NULL UNIQUE,
    root_revision TEXT NOT NULL,
    head_revision TEXT NOT NULL,
    object_format TEXT NOT NULL CHECK (object_format IN ('sha1', 'sha256')),
    chain_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    accepted_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (execution_id)
        REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (queue_item_id)
        REFERENCES integration_queue_items(id) ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
    FOREIGN KEY (accepted_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (object_format = 'sha1'
            AND length(root_revision) = 40
            AND length(head_revision) = 40)
        OR
        (object_format = 'sha256'
            AND length(root_revision) = 64
            AND length(head_revision) = 64)
    )
) STRICT;

CREATE TRIGGER workflow_landing_chain_immutable_update
BEFORE UPDATE ON workflow_landing_chains
BEGIN
    SELECT RAISE(ABORT, 'workflow landing chains are immutable');
END;
CREATE TRIGGER workflow_landing_chain_immutable_delete
BEFORE DELETE ON workflow_landing_chains
BEGIN
    SELECT RAISE(ABORT, 'workflow landing chains cannot be deleted');
END;
"""

WORKFLOW_PRESCRIPTION_SCHEMA = """
DROP TRIGGER workflow_configuration_version_immutable_update;
DROP TRIGGER workflow_configuration_version_immutable_delete;

CREATE TABLE workflow_configuration_role_migration (
    configuration_version_id TEXT NOT NULL,
    role TEXT NOT NULL,
    profile_version_id TEXT NOT NULL,
    PRIMARY KEY (configuration_version_id, role)
) STRICT;

INSERT INTO workflow_configuration_role_migration (
    configuration_version_id, role, profile_version_id
)
SELECT id, 'producer', producer_profile_version_id
FROM workflow_configuration_versions
UNION ALL
SELECT id, 'reviewer', reviewer_profile_version_id
FROM workflow_configuration_versions;

CREATE TABLE workflow_configuration_versions_v32 (
    id TEXT PRIMARY KEY,
    configuration_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    template_definition_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    spec_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (configuration_id, version),
    UNIQUE (configuration_id, content_sha256),
    FOREIGN KEY (configuration_id)
        REFERENCES workflow_configurations(id) ON DELETE RESTRICT,
    FOREIGN KEY (template_definition_id)
        REFERENCES workflow_template_definitions(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

INSERT INTO workflow_configuration_versions_v32 (
    id, configuration_id, version, template_definition_id, schema_version,
    spec_json, content_sha256, created_by, created_at
)
SELECT
    id, configuration_id, version, template_definition_id, schema_version,
    spec_json, content_sha256, created_by, created_at
FROM workflow_configuration_versions;

DROP TABLE workflow_configuration_versions;
ALTER TABLE workflow_configuration_versions_v32
    RENAME TO workflow_configuration_versions;

CREATE INDEX workflow_configuration_version_created
    ON workflow_configuration_versions(configuration_id, version DESC);

CREATE TABLE workflow_configuration_role_bindings (
    configuration_version_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (length(role) BETWEEN 1 AND 32),
    profile_version_id TEXT NOT NULL,
    PRIMARY KEY (configuration_version_id, role),
    FOREIGN KEY (configuration_version_id)
        REFERENCES workflow_configuration_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (profile_version_id)
        REFERENCES agent_profile_versions(id) ON DELETE RESTRICT
) STRICT;

INSERT INTO workflow_configuration_role_bindings (
    configuration_version_id, role, profile_version_id
)
SELECT configuration_version_id, role, profile_version_id
FROM workflow_configuration_role_migration;

DROP TABLE workflow_configuration_role_migration;

CREATE INDEX workflow_configuration_role_profile
    ON workflow_configuration_role_bindings(profile_version_id);

CREATE TRIGGER workflow_configuration_version_immutable_update
BEFORE UPDATE ON workflow_configuration_versions
BEGIN
    SELECT RAISE(ABORT, 'workflow configuration versions are immutable');
END;
CREATE TRIGGER workflow_configuration_version_immutable_delete
BEFORE DELETE ON workflow_configuration_versions
BEGIN
    SELECT RAISE(ABORT, 'workflow configuration versions cannot be deleted');
END;
CREATE TRIGGER workflow_configuration_role_binding_immutable_update
BEFORE UPDATE ON workflow_configuration_role_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow configuration role bindings are immutable');
END;
CREATE TRIGGER workflow_configuration_role_binding_immutable_delete
BEFORE DELETE ON workflow_configuration_role_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow configuration role bindings cannot be deleted');
END;
"""

DISCOVERY_PRESCRIPTION_SCHEMA = """
INSERT INTO workflow_template_definitions (
    id, template_key, version, schema_version, definition_json,
    content_sha256, built_in, created_at
) VALUES (
    'wtd_8671ed9e9520e91a5f88d5232d0b77b1',
    'discovery-v1',
    1,
    1,
    '{"correction_loop":false,"key":"discovery-v1","roles":[{"name":"investigator","workspace_mode":"read_only"},{"name":"coordinator","workspace_mode":"read_only"}],"schema_version":1,"stages":["investigate","coordinate","completion"],"version":1}',
    '8671ed9e9520e91a5f88d5232d0b77b1311a076eb80c0d378a3b307cfae82def',
    1,
    '2026-10-25T00:00:00.000000+00:00'
);
"""

ADVISORY_REVIEW_PRESCRIPTION_SCHEMA = """
INSERT INTO workflow_template_definitions (
    id, template_key, version, schema_version, definition_json,
    content_sha256, built_in, created_at
) VALUES (
    'wtd_fceab7475e9212aaa90fbc3055f09307',
    'advisory-review-v1',
    1,
    1,
    '{"correction_loop":false,"key":"advisory-review-v1","roles":[{"name":"reviewer_one","workspace_mode":"read_only"},{"name":"reviewer_two","workspace_mode":"read_only"},{"name":"reviewer_three","workspace_mode":"read_only"},{"name":"coordinator","workspace_mode":"read_only"}],"schema_version":1,"stages":["blind_review","challenge","triangulate","completion"],"version":1}',
    'fceab7475e9212aaa90fbc3055f093071c18c5c0b880a219b513bea62c035382',
    1,
    '2026-10-25T00:00:00.000000+00:00'
);
"""

DISCOVERY_EXECUTION_SCHEMA = """
CREATE TABLE discovery_executions (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    template_definition_id TEXT NOT NULL,
    configuration_version_id TEXT NOT NULL,
    context_version_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL,
    work_item_version_id TEXT NOT NULL,
    work_thread_id TEXT NOT NULL,
    work_thread_generation INTEGER NOT NULL CHECK (work_thread_generation >= 1),
    source_revision TEXT NOT NULL,
    decision_owner_krab_identity_id TEXT NOT NULL,
    krab_session_id TEXT NOT NULL,
    krab_claim_id TEXT,
    state TEXT NOT NULL CHECK (
        state IN ('active', 'cancelled', 'completed', 'failed')
    ),
    stage TEXT NOT NULL CHECK (
        stage IN ('investigate', 'coordinate', 'completion')
    ),
    version INTEGER NOT NULL CHECK (version >= 1),
    request_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    cancel_reason TEXT,
    failure_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE (requested_by, idempotency_key),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (template_definition_id)
        REFERENCES workflow_template_definitions(id) ON DELETE RESTRICT,
    FOREIGN KEY (configuration_version_id)
        REFERENCES workflow_configuration_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (context_version_id)
        REFERENCES project_context_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_item_id) REFERENCES work_items(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_item_version_id)
        REFERENCES work_item_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_thread_id) REFERENCES work_threads(id) ON DELETE RESTRICT,
    FOREIGN KEY (decision_owner_krab_identity_id)
        REFERENCES krab_identities(id) ON DELETE RESTRICT,
    FOREIGN KEY (requested_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'active' AND finished_at IS NULL
            AND cancel_reason IS NULL AND failure_reason IS NULL)
        OR (state = 'cancelled' AND finished_at IS NOT NULL
            AND cancel_reason IS NOT NULL AND failure_reason IS NULL)
        OR (state = 'failed' AND finished_at IS NOT NULL
            AND cancel_reason IS NULL AND failure_reason IS NOT NULL)
        OR (state = 'completed' AND finished_at IS NOT NULL
            AND cancel_reason IS NULL AND failure_reason IS NULL
            AND stage = 'completion')
    )
) STRICT;

CREATE UNIQUE INDEX discovery_execution_one_active_thread
    ON discovery_executions(work_thread_id) WHERE state = 'active';
CREATE INDEX discovery_execution_project_created
    ON discovery_executions(project_id, created_at DESC, id DESC);
CREATE INDEX discovery_execution_state_created
    ON discovery_executions(state, created_at, id);

CREATE TABLE discovery_execution_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'discovery.started',
            'discovery.investigation_completed',
            'discovery.coordination_completed',
            'discovery.completed',
            'discovery.cancelled',
            'discovery.failed'
        )
    ),
    from_state TEXT,
    to_state TEXT NOT NULL CHECK (
        to_state IN ('active', 'cancelled', 'completed', 'failed')
    ),
    actor TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (execution_id, sequence),
    FOREIGN KEY (execution_id)
        REFERENCES discovery_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX discovery_execution_event_stream
    ON discovery_execution_events(execution_id, sequence);

CREATE TABLE discovery_run_bindings (
    run_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('investigator', 'coordinator')),
    created_at TEXT NOT NULL,
    UNIQUE (execution_id, role),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (execution_id)
        REFERENCES discovery_executions(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE discovery_traces (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    trace_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (run_id, idempotency_key),
    FOREIGN KEY (execution_id)
        REFERENCES discovery_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE discovery_rulings (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (
        disposition IN ('GO', 'NO_GO', 'ASK_HUMAN')
    ),
    interaction_id TEXT,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    ruling_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (execution_id, sequence),
    UNIQUE (run_id, idempotency_key),
    FOREIGN KEY (execution_id)
        REFERENCES discovery_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT,
    FOREIGN KEY (interaction_id) REFERENCES interactions(id) ON DELETE RESTRICT,
    CHECK (
        (disposition = 'ASK_HUMAN' AND interaction_id IS NOT NULL)
        OR disposition IN ('GO', 'NO_GO')
    )
) STRICT;

CREATE UNIQUE INDEX discovery_ruling_one_final
    ON discovery_rulings(execution_id)
    WHERE disposition IN ('GO', 'NO_GO');

CREATE TRIGGER discovery_execution_identity_immutable
BEFORE UPDATE OF
    id, project_id, template_definition_id, configuration_version_id,
    context_version_id, work_item_id, work_item_version_id, work_thread_id,
    work_thread_generation, source_revision, decision_owner_krab_identity_id,
    krab_session_id, krab_claim_id, request_json, content_sha256,
    idempotency_key, requested_by, created_at
ON discovery_executions
BEGIN
    SELECT RAISE(ABORT, 'discovery execution identity is immutable');
END;

CREATE TRIGGER discovery_execution_transition
BEFORE UPDATE ON discovery_executions
WHEN
    OLD.state != 'active'
    OR NEW.version != OLD.version + 1
    OR NEW.state NOT IN ('active', 'cancelled', 'completed', 'failed')
    OR (OLD.stage = 'investigate'
        AND NEW.stage NOT IN ('investigate', 'coordinate'))
    OR (OLD.stage = 'coordinate'
        AND NEW.stage NOT IN ('coordinate', 'completion'))
    OR (OLD.stage = 'completion' AND NEW.stage != 'completion')
    OR (NEW.state = 'active'
        AND (NEW.finished_at IS NOT NULL OR NEW.cancel_reason IS NOT NULL
            OR NEW.failure_reason IS NOT NULL))
    OR (NEW.state = 'cancelled'
        AND (NEW.finished_at IS NULL OR NEW.cancel_reason IS NULL
            OR NEW.failure_reason IS NOT NULL))
    OR (NEW.state = 'failed'
        AND (NEW.finished_at IS NULL OR NEW.cancel_reason IS NOT NULL
            OR NEW.failure_reason IS NULL))
    OR (NEW.state = 'completed'
        AND (NEW.finished_at IS NULL OR NEW.cancel_reason IS NOT NULL
            OR NEW.failure_reason IS NOT NULL OR NEW.stage != 'completion'))
BEGIN
    SELECT RAISE(ABORT, 'discovery execution transition is invalid');
END;

CREATE TRIGGER discovery_execution_immutable_delete
BEFORE DELETE ON discovery_executions
BEGIN
    SELECT RAISE(ABORT, 'discovery executions cannot be deleted');
END;
CREATE TRIGGER discovery_execution_event_immutable_update
BEFORE UPDATE ON discovery_execution_events
BEGIN
    SELECT RAISE(ABORT, 'discovery execution events are immutable');
END;
CREATE TRIGGER discovery_execution_event_immutable_delete
BEFORE DELETE ON discovery_execution_events
BEGIN
    SELECT RAISE(ABORT, 'discovery execution events cannot be deleted');
END;
CREATE TRIGGER discovery_run_binding_immutable_update
BEFORE UPDATE ON discovery_run_bindings
BEGIN
    SELECT RAISE(ABORT, 'discovery run bindings are immutable');
END;
CREATE TRIGGER discovery_run_binding_immutable_delete
BEFORE DELETE ON discovery_run_bindings
BEGIN
    SELECT RAISE(ABORT, 'discovery run bindings cannot be deleted');
END;
CREATE TRIGGER discovery_trace_immutable_update
BEFORE UPDATE ON discovery_traces
BEGIN
    SELECT RAISE(ABORT, 'discovery traces are immutable');
END;
CREATE TRIGGER discovery_trace_immutable_delete
BEFORE DELETE ON discovery_traces
BEGIN
    SELECT RAISE(ABORT, 'discovery traces cannot be deleted');
END;
CREATE TRIGGER discovery_ruling_immutable_update
BEFORE UPDATE ON discovery_rulings
BEGIN
    SELECT RAISE(ABORT, 'discovery rulings are immutable');
END;
CREATE TRIGGER discovery_ruling_immutable_delete
BEFORE DELETE ON discovery_rulings
BEGIN
    SELECT RAISE(ABORT, 'discovery rulings cannot be deleted');
END;
"""

DISCOVERY_DIRECTOR_SCHEMA = """
DROP TRIGGER supervisor_action_identity_immutable;
DROP TRIGGER supervisor_action_immutable_delete;
DROP INDEX supervisor_action_due;
DROP INDEX supervisor_action_run;
DROP INDEX supervisor_action_workflow;
ALTER TABLE supervisor_actions RENAME TO supervisor_actions_v34;
CREATE TABLE supervisor_actions (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN (
            'run.execute', 'attempt.reconcile', 'run.reconcile',
            'workflow.advance', 'discovery.advance'
        )
    ),
    subject_id TEXT NOT NULL,
    subject_version INTEGER NOT NULL CHECK (subject_version >= 1),
    run_id TEXT,
    workflow_execution_id TEXT,
    discovery_execution_id TEXT,
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'claimed', 'succeeded', 'exhausted')
    ),
    version INTEGER NOT NULL CHECK (version >= 1),
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 9),
    retry_backoff_json TEXT NOT NULL,
    available_at TEXT NOT NULL,
    owner_id TEXT,
    lease_expires_at TEXT,
    last_error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE (kind, subject_id, subject_version),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (workflow_execution_id)
        REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (discovery_execution_id)
        REFERENCES discovery_executions(id) ON DELETE RESTRICT,
    CHECK (
        (kind = 'workflow.advance' AND run_id IS NULL
            AND workflow_execution_id = subject_id
            AND discovery_execution_id IS NULL)
        OR
        (kind = 'discovery.advance' AND run_id IS NULL
            AND workflow_execution_id IS NULL
            AND discovery_execution_id = subject_id)
        OR
        (kind NOT IN ('workflow.advance', 'discovery.advance')
            AND run_id IS NOT NULL AND workflow_execution_id IS NULL
            AND discovery_execution_id IS NULL)
    ),
    CHECK (
        (
            state = 'pending' AND owner_id IS NULL
            AND lease_expires_at IS NULL AND finished_at IS NULL
        ) OR (
            state = 'claimed' AND owner_id IS NOT NULL
            AND lease_expires_at IS NOT NULL AND finished_at IS NULL
        ) OR (
            state IN ('succeeded', 'exhausted') AND owner_id IS NULL
            AND lease_expires_at IS NULL AND finished_at IS NOT NULL
        )
    )
) STRICT;
INSERT INTO supervisor_actions
    (id, kind, subject_id, subject_version, run_id, workflow_execution_id,
     discovery_execution_id, state, version, attempt_count, max_attempts,
     retry_backoff_json, available_at, owner_id, lease_expires_at,
     last_error_code, created_at, updated_at, finished_at)
SELECT id, kind, subject_id, subject_version, run_id, workflow_execution_id,
       NULL, state, version, attempt_count, max_attempts, retry_backoff_json,
       available_at, owner_id, lease_expires_at, last_error_code, created_at,
       updated_at, finished_at
FROM supervisor_actions_v34;
DROP TABLE supervisor_actions_v34;
CREATE INDEX supervisor_action_due
    ON supervisor_actions(state, available_at, lease_expires_at);
CREATE INDEX supervisor_action_run
    ON supervisor_actions(run_id, created_at DESC);
CREATE INDEX supervisor_action_workflow
    ON supervisor_actions(workflow_execution_id, created_at DESC);
CREATE INDEX supervisor_action_discovery
    ON supervisor_actions(discovery_execution_id, created_at DESC);
CREATE TRIGGER supervisor_action_identity_immutable
BEFORE UPDATE OF
    id, kind, subject_id, subject_version, run_id, workflow_execution_id,
    discovery_execution_id, max_attempts, retry_backoff_json, created_at
ON supervisor_actions
BEGIN
    SELECT RAISE(ABORT, 'supervisor action identity is immutable');
END;
CREATE TRIGGER supervisor_action_immutable_delete
BEFORE DELETE ON supervisor_actions
BEGIN
    SELECT RAISE(ABORT, 'supervisor actions cannot be deleted');
END;
"""

ADVISORY_EXECUTION_SCHEMA = """
CREATE TABLE advisory_executions (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    template_definition_id TEXT NOT NULL,
    configuration_version_id TEXT NOT NULL,
    context_version_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL,
    work_item_version_id TEXT NOT NULL,
    work_thread_id TEXT NOT NULL,
    work_thread_generation INTEGER NOT NULL CHECK (work_thread_generation >= 1),
    candidate_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('active', 'cancelled', 'completed', 'failed')
    ),
    stage TEXT NOT NULL CHECK (
        stage IN ('blind_review', 'challenge', 'triangulate', 'completion')
    ),
    version INTEGER NOT NULL CHECK (version >= 1),
    request_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    cancel_reason TEXT,
    failure_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE (requested_by, idempotency_key),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE RESTRICT,
    FOREIGN KEY (template_definition_id)
        REFERENCES workflow_template_definitions(id) ON DELETE RESTRICT,
    FOREIGN KEY (configuration_version_id)
        REFERENCES workflow_configuration_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (context_version_id)
        REFERENCES project_context_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_item_id) REFERENCES work_items(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_item_version_id)
        REFERENCES work_item_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (work_thread_id) REFERENCES work_threads(id) ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (requested_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'active' AND finished_at IS NULL
            AND cancel_reason IS NULL AND failure_reason IS NULL)
        OR (state = 'cancelled' AND finished_at IS NOT NULL
            AND cancel_reason IS NOT NULL AND failure_reason IS NULL)
        OR (state = 'failed' AND finished_at IS NOT NULL
            AND cancel_reason IS NULL AND failure_reason IS NOT NULL)
        OR (state = 'completed' AND finished_at IS NOT NULL
            AND cancel_reason IS NULL AND failure_reason IS NULL
            AND stage = 'completion')
    )
) STRICT;

CREATE UNIQUE INDEX advisory_execution_one_active_configuration_candidate
    ON advisory_executions(candidate_id, configuration_version_id)
    WHERE state = 'active';
CREATE INDEX advisory_execution_project_created
    ON advisory_executions(project_id, created_at DESC, id DESC);
CREATE INDEX advisory_execution_state_created
    ON advisory_executions(state, created_at, id);

CREATE TABLE advisory_execution_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence >= 1),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'advisory.started',
            'advisory.blind_completed',
            'advisory.challenge_completed',
            'advisory.triangulation_completed',
            'advisory.completed',
            'advisory.cancelled',
            'advisory.failed'
        )
    ),
    from_state TEXT,
    to_state TEXT NOT NULL CHECK (
        to_state IN ('active', 'cancelled', 'completed', 'failed')
    ),
    actor TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (execution_id, sequence),
    FOREIGN KEY (execution_id)
        REFERENCES advisory_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (actor) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX advisory_execution_event_stream
    ON advisory_execution_events(execution_id, sequence);

CREATE TABLE advisory_run_bindings (
    run_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    stage TEXT NOT NULL CHECK (
        stage IN ('blind_review', 'challenge', 'triangulate')
    ),
    role TEXT NOT NULL CHECK (
        role IN ('reviewer_one', 'reviewer_two', 'reviewer_three', 'coordinator')
    ),
    created_at TEXT NOT NULL,
    UNIQUE (execution_id, stage, role),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (execution_id)
        REFERENCES advisory_executions(id) ON DELETE RESTRICT,
    CHECK (
        (stage IN ('blind_review', 'challenge')
            AND role IN ('reviewer_one', 'reviewer_two', 'reviewer_three'))
        OR (stage = 'triangulate' AND role = 'coordinator')
    )
) STRICT;

CREATE TABLE advisory_blind_reviews (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    reviewer_role TEXT NOT NULL CHECK (
        reviewer_role IN ('reviewer_one', 'reviewer_two', 'reviewer_three')
    ),
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    review_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (execution_id, reviewer_role),
    UNIQUE (run_id, idempotency_key),
    FOREIGN KEY (execution_id)
        REFERENCES advisory_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE advisory_challenge_reviews (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    reviewer_role TEXT NOT NULL CHECK (
        reviewer_role IN ('reviewer_one', 'reviewer_two', 'reviewer_three')
    ),
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    review_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (execution_id, reviewer_role),
    UNIQUE (run_id, idempotency_key),
    FOREIGN KEY (execution_id)
        REFERENCES advisory_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE advisory_reports (
    id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    authority TEXT NOT NULL CHECK (authority = 'advisory_only'),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    report_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (run_id, idempotency_key),
    FOREIGN KEY (execution_id)
        REFERENCES advisory_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (attempt_id) REFERENCES run_attempts(id) ON DELETE RESTRICT
) STRICT;

CREATE TRIGGER advisory_execution_identity_immutable
BEFORE UPDATE OF
    id, project_id, template_definition_id, configuration_version_id,
    context_version_id, work_item_id, work_item_version_id, work_thread_id,
    work_thread_generation, candidate_id, request_json, content_sha256,
    idempotency_key, requested_by, created_at
ON advisory_executions
BEGIN
    SELECT RAISE(ABORT, 'advisory execution identity is immutable');
END;

CREATE TRIGGER advisory_execution_transition
BEFORE UPDATE ON advisory_executions
WHEN
    OLD.state != 'active'
    OR NEW.version != OLD.version + 1
    OR NEW.state NOT IN ('active', 'cancelled', 'completed', 'failed')
    OR (OLD.stage = 'blind_review'
        AND NEW.stage NOT IN ('blind_review', 'challenge'))
    OR (OLD.stage = 'challenge'
        AND NEW.stage NOT IN ('challenge', 'triangulate'))
    OR (OLD.stage = 'triangulate'
        AND NEW.stage NOT IN ('triangulate', 'completion'))
    OR (OLD.stage = 'completion' AND NEW.stage != 'completion')
    OR (NEW.state = 'active'
        AND (NEW.finished_at IS NOT NULL OR NEW.cancel_reason IS NOT NULL
            OR NEW.failure_reason IS NOT NULL))
    OR (NEW.state = 'cancelled'
        AND (NEW.finished_at IS NULL OR NEW.cancel_reason IS NULL
            OR NEW.failure_reason IS NOT NULL))
    OR (NEW.state = 'failed'
        AND (NEW.finished_at IS NULL OR NEW.cancel_reason IS NOT NULL
            OR NEW.failure_reason IS NULL))
    OR (NEW.state = 'completed'
        AND (NEW.finished_at IS NULL OR NEW.cancel_reason IS NOT NULL
            OR NEW.failure_reason IS NOT NULL OR NEW.stage != 'completion'))
BEGIN
    SELECT RAISE(ABORT, 'advisory execution transition is invalid');
END;

CREATE TRIGGER advisory_execution_immutable_delete
BEFORE DELETE ON advisory_executions
BEGIN
    SELECT RAISE(ABORT, 'advisory executions cannot be deleted');
END;
CREATE TRIGGER advisory_execution_event_immutable_update
BEFORE UPDATE ON advisory_execution_events
BEGIN
    SELECT RAISE(ABORT, 'advisory execution events are immutable');
END;
CREATE TRIGGER advisory_execution_event_immutable_delete
BEFORE DELETE ON advisory_execution_events
BEGIN
    SELECT RAISE(ABORT, 'advisory execution events cannot be deleted');
END;
CREATE TRIGGER advisory_run_binding_immutable_update
BEFORE UPDATE ON advisory_run_bindings
BEGIN
    SELECT RAISE(ABORT, 'advisory run bindings are immutable');
END;
CREATE TRIGGER advisory_run_binding_immutable_delete
BEFORE DELETE ON advisory_run_bindings
BEGIN
    SELECT RAISE(ABORT, 'advisory run bindings cannot be deleted');
END;
CREATE TRIGGER advisory_blind_review_immutable_update
BEFORE UPDATE ON advisory_blind_reviews
BEGIN
    SELECT RAISE(ABORT, 'advisory blind reviews are immutable');
END;
CREATE TRIGGER advisory_blind_review_immutable_delete
BEFORE DELETE ON advisory_blind_reviews
BEGIN
    SELECT RAISE(ABORT, 'advisory blind reviews cannot be deleted');
END;
CREATE TRIGGER advisory_challenge_review_immutable_update
BEFORE UPDATE ON advisory_challenge_reviews
BEGIN
    SELECT RAISE(ABORT, 'advisory challenge reviews are immutable');
END;
CREATE TRIGGER advisory_challenge_review_immutable_delete
BEFORE DELETE ON advisory_challenge_reviews
BEGIN
    SELECT RAISE(ABORT, 'advisory challenge reviews cannot be deleted');
END;
CREATE TRIGGER advisory_report_immutable_update
BEFORE UPDATE ON advisory_reports
BEGIN
    SELECT RAISE(ABORT, 'advisory reports are immutable');
END;
CREATE TRIGGER advisory_report_immutable_delete
BEFORE DELETE ON advisory_reports
BEGIN
    SELECT RAISE(ABORT, 'advisory reports cannot be deleted');
END;
"""

ADVISORY_DIRECTOR_SCHEMA = """
DROP TRIGGER supervisor_action_identity_immutable;
DROP TRIGGER supervisor_action_immutable_delete;
DROP INDEX supervisor_action_due;
DROP INDEX supervisor_action_run;
DROP INDEX supervisor_action_workflow;
DROP INDEX supervisor_action_discovery;
DROP INDEX IF EXISTS supervisor_action_advisory;
ALTER TABLE supervisor_actions RENAME TO supervisor_actions_v37;
CREATE TABLE supervisor_actions (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN (
            'run.execute', 'attempt.reconcile', 'run.reconcile',
            'workflow.advance', 'discovery.advance', 'advisory.advance'
        )
    ),
    subject_id TEXT NOT NULL,
    subject_version INTEGER NOT NULL CHECK (subject_version >= 1),
    run_id TEXT,
    workflow_execution_id TEXT,
    discovery_execution_id TEXT,
    advisory_execution_id TEXT,
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'claimed', 'succeeded', 'exhausted')
    ),
    version INTEGER NOT NULL CHECK (version >= 1),
    attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 9),
    retry_backoff_json TEXT NOT NULL,
    available_at TEXT NOT NULL,
    owner_id TEXT,
    lease_expires_at TEXT,
    last_error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE (kind, subject_id, subject_version),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (workflow_execution_id)
        REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (discovery_execution_id)
        REFERENCES discovery_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (advisory_execution_id)
        REFERENCES advisory_executions(id) ON DELETE RESTRICT,
    CHECK (
        (kind = 'workflow.advance' AND run_id IS NULL
            AND workflow_execution_id = subject_id
            AND discovery_execution_id IS NULL
            AND advisory_execution_id IS NULL)
        OR
        (kind = 'discovery.advance' AND run_id IS NULL
            AND workflow_execution_id IS NULL
            AND discovery_execution_id = subject_id
            AND advisory_execution_id IS NULL)
        OR
        (kind = 'advisory.advance' AND run_id IS NULL
            AND workflow_execution_id IS NULL
            AND discovery_execution_id IS NULL
            AND advisory_execution_id = subject_id)
        OR
        (kind NOT IN (
                'workflow.advance', 'discovery.advance', 'advisory.advance'
            )
            AND run_id IS NOT NULL AND workflow_execution_id IS NULL
            AND discovery_execution_id IS NULL
            AND advisory_execution_id IS NULL)
    ),
    CHECK (
        (
            state = 'pending' AND owner_id IS NULL
            AND lease_expires_at IS NULL AND finished_at IS NULL
        ) OR (
            state = 'claimed' AND owner_id IS NOT NULL
            AND lease_expires_at IS NOT NULL AND finished_at IS NULL
        ) OR (
            state IN ('succeeded', 'exhausted') AND owner_id IS NULL
            AND lease_expires_at IS NULL AND finished_at IS NOT NULL
        )
    )
) STRICT;
INSERT INTO supervisor_actions
    (id, kind, subject_id, subject_version, run_id, workflow_execution_id,
     discovery_execution_id, advisory_execution_id, state, version,
     attempt_count, max_attempts, retry_backoff_json, available_at, owner_id,
     lease_expires_at, last_error_code, created_at, updated_at, finished_at)
SELECT id, kind, subject_id, subject_version, run_id, workflow_execution_id,
       discovery_execution_id, NULL, state, version, attempt_count,
       max_attempts, retry_backoff_json, available_at, owner_id,
       lease_expires_at, last_error_code, created_at, updated_at, finished_at
FROM supervisor_actions_v37;
DROP TABLE supervisor_actions_v37;
CREATE INDEX supervisor_action_due
    ON supervisor_actions(state, available_at, lease_expires_at);
CREATE INDEX supervisor_action_run
    ON supervisor_actions(run_id, created_at DESC);
CREATE INDEX supervisor_action_workflow
    ON supervisor_actions(workflow_execution_id, created_at DESC);
CREATE INDEX supervisor_action_discovery
    ON supervisor_actions(discovery_execution_id, created_at DESC);
CREATE INDEX supervisor_action_advisory
    ON supervisor_actions(advisory_execution_id, created_at DESC);
CREATE TRIGGER supervisor_action_identity_immutable
BEFORE UPDATE OF
    id, kind, subject_id, subject_version, run_id, workflow_execution_id,
    discovery_execution_id, advisory_execution_id, max_attempts,
    retry_backoff_json, created_at
ON supervisor_actions
BEGIN
    SELECT RAISE(ABORT, 'supervisor action identity is immutable');
END;
CREATE TRIGGER supervisor_action_immutable_delete
BEFORE DELETE ON supervisor_actions
BEGIN
    SELECT RAISE(ABORT, 'supervisor actions cannot be deleted');
END;
"""

WORKFLOW_SCOPE_BINDING_SCHEMA = """
CREATE TABLE workflow_scope_bindings (
    workflow_execution_id TEXT PRIMARY KEY,
    discovery_execution_id TEXT NOT NULL,
    discovery_ruling_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (workflow_execution_id)
        REFERENCES workflow_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (discovery_execution_id)
        REFERENCES discovery_executions(id) ON DELETE RESTRICT,
    FOREIGN KEY (discovery_ruling_id)
        REFERENCES discovery_rulings(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX workflow_scope_binding_discovery
    ON workflow_scope_bindings(discovery_execution_id, workflow_execution_id);

CREATE TRIGGER workflow_scope_binding_immutable_update
BEFORE UPDATE ON workflow_scope_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow scope bindings are immutable');
END;
CREATE TRIGGER workflow_scope_binding_immutable_delete
BEFORE DELETE ON workflow_scope_bindings
BEGIN
    SELECT RAISE(ABORT, 'workflow scope bindings cannot be deleted');
END;
"""

WORKFLOW_SCOPE_REVIEW_SCHEMA = """
CREATE TABLE workflow_scope_reviews (
    id TEXT PRIMARY KEY,
    workflow_execution_id TEXT NOT NULL,
    correction_round INTEGER NOT NULL CHECK (correction_round BETWEEN 0 AND 8),
    candidate_id TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    reviewer_run_id TEXT NOT NULL,
    reviewer_attempt_id TEXT NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    review_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    expected_review_outcome TEXT NOT NULL CHECK (
        expected_review_outcome IN ('approved', 'changes_requested')
    ),
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (workflow_execution_id)
        REFERENCES workflow_scope_bindings(workflow_execution_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (workflow_execution_id, correction_round)
        REFERENCES workflow_gate_bindings(execution_id, correction_round)
        ON DELETE RESTRICT,
    FOREIGN KEY (candidate_id)
        REFERENCES integration_candidates(id) ON DELETE RESTRICT,
    FOREIGN KEY (policy_id)
        REFERENCES integration_gate_policies(id) ON DELETE RESTRICT,
    FOREIGN KEY (reviewer_run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (reviewer_attempt_id)
        REFERENCES run_attempts(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX workflow_scope_review_execution
    ON workflow_scope_reviews(
        workflow_execution_id, correction_round, created_at, id
    );
CREATE INDEX workflow_scope_review_policy_run
    ON workflow_scope_reviews(policy_id, reviewer_run_id, reviewer_attempt_id);

CREATE TRIGGER workflow_scope_review_immutable_update
BEFORE UPDATE ON workflow_scope_reviews
BEGIN
    SELECT RAISE(ABORT, 'workflow scope reviews are immutable');
END;
CREATE TRIGGER workflow_scope_review_immutable_delete
BEFORE DELETE ON workflow_scope_reviews
BEGIN
    SELECT RAISE(ABORT, 'workflow scope reviews cannot be deleted');
END;
"""

PROVIDER_INVOCATION_LIMIT_SCHEMA = """
CREATE TABLE IF NOT EXISTS run_provider_invocation_allowances (
    run_id TEXT NOT NULL,
    provider_grant_id TEXT NOT NULL,
    max_invocations INTEGER NOT NULL CHECK (max_invocations BETWEEN 1 AND 10000),
    invocation_count INTEGER NOT NULL DEFAULT 0 CHECK (
        invocation_count BETWEEN 0 AND max_invocations
    ),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, provider_grant_id),
    FOREIGN KEY (run_id, provider_grant_id)
        REFERENCES run_provider_grants(run_id, provider_grant_id)
        ON DELETE RESTRICT
) STRICT;

INSERT OR IGNORE INTO run_provider_invocation_allowances (
    run_id, provider_grant_id, max_invocations, invocation_count,
    created_at, updated_at
)
SELECT rpg.run_id, rpg.provider_grant_id,
       CASE WHEN COUNT(pi.id) > 64 THEN COUNT(pi.id) ELSE 64 END,
       COUNT(pi.id), rpg.created_at, rpg.created_at
FROM run_provider_grants rpg
LEFT JOIN provider_invocations pi
    ON pi.run_id = rpg.run_id
    AND pi.provider_grant_id = rpg.provider_grant_id
    AND pi.outcome IN ('dispatching', 'succeeded', 'failed', 'ambiguous')
GROUP BY rpg.run_id, rpg.provider_grant_id, rpg.created_at;

CREATE TRIGGER IF NOT EXISTS run_provider_grant_allowance_default
AFTER INSERT ON run_provider_grants
BEGIN
    INSERT INTO run_provider_invocation_allowances (
        run_id, provider_grant_id, max_invocations, invocation_count,
        created_at, updated_at
    ) VALUES (NEW.run_id, NEW.provider_grant_id, 64, 0, NEW.created_at, NEW.created_at);
END;
"""

SUPERVISOR_DISCOVERY_CURSOR_SCHEMA = """
CREATE TABLE IF NOT EXISTS supervisor_discovery_cursors (
    stream TEXT PRIMARY KEY CHECK (
        stream IN (
            'interaction.pending', 'attempt.attention',
            'run.missing_active_attempt', 'run.waiting_without_interaction',
            'authority.unavailable'
        )
    ),
    cursor_timestamp TEXT NOT NULL,
    cursor_id TEXT NOT NULL,
    updated_at TEXT NOT NULL
) STRICT;
"""

OPERABILITY_SCHEMA = (
    PROVIDER_INVOCATION_LIMIT_SCHEMA + SUPERVISOR_DISCOVERY_CURSOR_SCHEMA
)

SERVICE_RESOURCE_SCHEMA = """
DROP TRIGGER service_sidecar_identity_immutable;
DROP TRIGGER service_sidecar_immutable_delete;
DROP TRIGGER service_operation_identity_immutable;
DROP TRIGGER service_operation_immutable_delete;
DROP INDEX service_sidecar_state;
DROP INDEX service_operation_state;
DROP INDEX service_operation_sidecar;

CREATE TABLE service_sidecars_v42 (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('active', 'revoked')),
    version INTEGER NOT NULL CHECK (version >= 1),
    schema_version INTEGER NOT NULL CHECK (schema_version IN (1, 2)),
    connector_protocol_version INTEGER NOT NULL CHECK (
        connector_protocol_version IN (1, 2)
        AND connector_protocol_version = schema_version
    ),
    spec_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE CHECK (length(content_sha256) = 64),
    deployment_sha256 TEXT NOT NULL CHECK (length(deployment_sha256) = 64),
    transport_kind TEXT NOT NULL CHECK (transport_kind = 'unix'),
    socket_path TEXT NOT NULL,
    request_bytes INTEGER NOT NULL CHECK (request_bytes >= 1),
    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 1),
    timeout_ms INTEGER NOT NULL CHECK (timeout_ms >= 100),
    max_concurrency INTEGER NOT NULL CHECK (
        max_concurrency BETWEEN 1 AND 16
    ),
    max_requests_per_minute INTEGER NOT NULL CHECK (
        max_requests_per_minute BETWEEN 1 AND 10000
    ),
    created_by TEXT NOT NULL,
    revoked_by TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    revoked_at TEXT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
    FOREIGN KEY (revoked_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (state = 'active' AND revoked_by IS NULL AND revoked_at IS NULL)
        OR
        (state = 'revoked' AND revoked_by IS NOT NULL AND revoked_at IS NOT NULL)
    )
) STRICT;

CREATE TABLE service_operations_v42 (
    id TEXT PRIMARY KEY,
    service_sidecar_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'pending', 'dispatching', 'uncertain', 'retryable', 'throttled',
            'succeeded', 'denied', 'conflict', 'cancelled'
        )
    ),
    version INTEGER NOT NULL CHECK (version >= 1),
    schema_version INTEGER NOT NULL CHECK (schema_version IN (1, 2)),
    spec_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    sidecar_content_sha256 TEXT NOT NULL CHECK (
        length(sidecar_content_sha256) = 64
    ),
    deployment_sha256 TEXT NOT NULL CHECK (length(deployment_sha256) = 64),
    mapping_name TEXT NOT NULL,
    service TEXT NOT NULL,
    host TEXT NOT NULL,
    resource_kind TEXT NOT NULL CHECK (
        resource_kind IN ('repository', 'work_project')
    ),
    resource_external_id TEXT NOT NULL CHECK (length(resource_external_id) >= 1),
    account TEXT NOT NULL,
    credential_scope_sha256 TEXT NOT NULL CHECK (
        length(credential_scope_sha256) = 64
    ),
    capability_name TEXT NOT NULL,
    capability_version INTEGER NOT NULL CHECK (capability_version >= 1),
    capability_mode TEXT NOT NULL CHECK (capability_mode IN ('read', 'effect')),
    idempotency_key TEXT NOT NULL,
    input_sha256 TEXT NOT NULL CHECK (length(input_sha256) = 64),
    request_bytes INTEGER NOT NULL CHECK (request_bytes >= 1),
    response_bytes INTEGER NOT NULL CHECK (response_bytes >= 1),
    timeout_ms INTEGER NOT NULL CHECK (timeout_ms >= 100),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    eligible_at TEXT,
    completed_at TEXT,
    UNIQUE (service_sidecar_id, idempotency_key),
    FOREIGN KEY (service_sidecar_id)
        REFERENCES service_sidecars_v42(id) ON DELETE RESTRICT,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (
        (state IN ('pending', 'dispatching', 'uncertain')
            AND eligible_at IS NULL AND completed_at IS NULL)
        OR
        (state IN ('retryable', 'throttled')
            AND eligible_at IS NOT NULL AND completed_at IS NULL)
        OR
        (state IN ('succeeded', 'denied', 'conflict', 'cancelled')
            AND eligible_at IS NULL AND completed_at IS NOT NULL)
    )
) STRICT;

INSERT INTO service_sidecars_v42 (
    id, name, state, version, schema_version, connector_protocol_version,
    spec_json, content_sha256, deployment_sha256, transport_kind, socket_path,
    request_bytes, response_bytes, timeout_ms, max_concurrency,
    max_requests_per_minute, created_by, revoked_by, created_at, updated_at,
    revoked_at
)
SELECT
    id, name, state, version, schema_version, connector_protocol_version,
    spec_json, content_sha256, deployment_sha256, transport_kind, socket_path,
    request_bytes, response_bytes, timeout_ms, max_concurrency,
    max_requests_per_minute, created_by, revoked_by, created_at, updated_at,
    revoked_at
FROM service_sidecars;

INSERT INTO service_operations_v42 (
    id, service_sidecar_id, state, version, schema_version, spec_json,
    content_sha256, sidecar_content_sha256, deployment_sha256, mapping_name,
    service, host, resource_kind, resource_external_id, account,
    credential_scope_sha256, capability_name, capability_version,
    capability_mode, idempotency_key, input_sha256, request_bytes,
    response_bytes, timeout_ms, created_by, created_at, updated_at, eligible_at,
    completed_at
)
SELECT
    id, service_sidecar_id, state, version, schema_version, spec_json,
    content_sha256, sidecar_content_sha256, deployment_sha256, mapping_name,
    service, host,
    CASE schema_version
        WHEN 1 THEN 'repository'
        ELSE json_extract(spec_json, '$.mapping.resource.kind')
    END,
    CASE schema_version
        WHEN 1 THEN json_extract(spec_json, '$.mapping.repository')
        ELSE json_extract(spec_json, '$.mapping.resource.external_id')
    END,
    account,
    credential_scope_sha256, capability_name, capability_version,
    capability_mode, idempotency_key, input_sha256, request_bytes,
    response_bytes, timeout_ms, created_by, created_at, updated_at, eligible_at,
    completed_at
FROM service_operations;

DROP TABLE service_operations;
DROP TABLE service_sidecars;
ALTER TABLE service_sidecars_v42 RENAME TO service_sidecars;
ALTER TABLE service_operations_v42 RENAME TO service_operations;

CREATE INDEX service_sidecar_state
    ON service_sidecars(state, created_at, id);
CREATE INDEX service_operation_state
    ON service_operations(state, eligible_at, created_at, id);
CREATE INDEX service_operation_sidecar
    ON service_operations(service_sidecar_id, created_at, id);

CREATE TRIGGER service_sidecar_identity_immutable
BEFORE UPDATE OF
    id, name, schema_version, connector_protocol_version, spec_json,
    content_sha256, deployment_sha256, transport_kind, socket_path,
    request_bytes, response_bytes, timeout_ms, max_concurrency,
    max_requests_per_minute, created_by, created_at
ON service_sidecars
BEGIN
    SELECT RAISE(ABORT, 'service sidecar identity is immutable');
END;
CREATE TRIGGER service_sidecar_immutable_delete
BEFORE DELETE ON service_sidecars
BEGIN
    SELECT RAISE(ABORT, 'service sidecars cannot be deleted');
END;
CREATE TRIGGER service_operation_identity_immutable
BEFORE UPDATE OF
    id, service_sidecar_id, schema_version, spec_json, content_sha256,
    sidecar_content_sha256, deployment_sha256, mapping_name, service, host,
    resource_kind, resource_external_id, account, credential_scope_sha256,
    capability_name, capability_version, capability_mode, idempotency_key,
    input_sha256, request_bytes, response_bytes, timeout_ms, created_by,
    created_at
ON service_operations
BEGIN
    SELECT RAISE(ABORT, 'service operation identity is immutable');
END;
CREATE TRIGGER service_operation_immutable_delete
BEFORE DELETE ON service_operations
BEGIN
    SELECT RAISE(ABORT, 'service operations cannot be deleted');
END;
"""

WORK_INTAKE_SCHEMA = """
CREATE TABLE work_item_service_imports (
    id TEXT PRIMARY KEY,
    work_item_version_id TEXT NOT NULL,
    service_operation_attempt_id TEXT NOT NULL UNIQUE,
    imported_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (work_item_version_id)
        REFERENCES work_item_versions(id) ON DELETE RESTRICT,
    FOREIGN KEY (service_operation_attempt_id)
        REFERENCES service_operation_attempts(id) ON DELETE RESTRICT,
    FOREIGN KEY (imported_by) REFERENCES users(id) ON DELETE RESTRICT
) STRICT;

CREATE INDEX work_item_service_import_version
    ON work_item_service_imports(work_item_version_id, created_at, id);

CREATE TRIGGER work_item_service_import_immutable_update
BEFORE UPDATE ON work_item_service_imports
BEGIN
    SELECT RAISE(ABORT, 'work item service imports are immutable');
END;
CREATE TRIGGER work_item_service_import_immutable_delete
BEFORE DELETE ON work_item_service_imports
BEGIN
    SELECT RAISE(ABORT, 'work item service imports cannot be deleted');
END;
"""

SCHEMA_V37 = (
    SCHEMA_V1
    + INTERACTION_SCHEMA
    + WORK_ITEM_SCHEMA
    + PROVIDER_SCHEMA
    + ATTEMPT_SCHEMA
    + WORK_MEMORY_SCHEMA
    + WORK_THREAD_LIFECYCLE_SCHEMA
    + EXECUTION_INTEGRITY_SCHEMA
    + RUN_REPLACEMENT_SCHEMA
    + SUPERVISION_SCHEMA
    + CANDIDATE_SCHEMA
    + PROVIDER_DISPATCH_SCHEMA
    + GATE_SCHEMA
    + INTEGRATION_SCHEMA
    + INFERENCE_CONNECTOR_SCHEMA
    + INFERENCE_DISPATCH_SCHEMA
    + PROVIDER_INVOCATION_LIFECYCLE_SCHEMA
    + ATTEMPT_OWNER_AUDIT_SCHEMA
    + PROVIDER_BUDGET_SCHEMA
    + RUN_PAUSED_STATE_MIGRATION
    + PROVIDER_RECOVERY_SCHEMA
    + RECOVERY_CHECKPOINT_SCHEMA
    + LANDING_PLAN_NONCE_SCHEMA
    + PROVIDER_REPORTED_OVERRUN_SCHEMA
    + SERVICE_SIDECAR_SCHEMA
    + CAPABILITY_BINDING_SCHEMA
    + PROVIDER_LANDING_SCHEMA
    + GATE_CHANGE_SCHEMA
    + MAINTAINER_REVIEW_SCHEMA
    + WORKFLOW_TEMPLATE_SCHEMA
    + WORKFLOW_DIRECTOR_SCHEMA
    + WORKFLOW_LANDING_SCHEMA
    + WORKFLOW_LANDING_CHAIN_SCHEMA
    + WORKFLOW_PRESCRIPTION_SCHEMA
    + DISCOVERY_PRESCRIPTION_SCHEMA
    + ADVISORY_REVIEW_PRESCRIPTION_SCHEMA
    + DISCOVERY_EXECUTION_SCHEMA
    + DISCOVERY_DIRECTOR_SCHEMA
    + ADVISORY_EXECUTION_SCHEMA
)
SCHEMA_V38 = SCHEMA_V37 + ADVISORY_DIRECTOR_SCHEMA
SCHEMA_V39 = SCHEMA_V38 + WORKFLOW_SCOPE_BINDING_SCHEMA
SCHEMA_V40 = SCHEMA_V39 + WORKFLOW_SCOPE_REVIEW_SCHEMA
SCHEMA_V41 = SCHEMA_V40 + OPERABILITY_SCHEMA
SCHEMA_V42 = SCHEMA_V41 + SERVICE_RESOURCE_SCHEMA
SCHEMA = SCHEMA_V42 + WORK_INTAKE_SCHEMA
MIGRATIONS = {
    1: INTERACTION_SCHEMA,
    2: WORK_ITEM_SCHEMA,
    3: PROVIDER_SCHEMA,
    4: ATTEMPT_SCHEMA,
    5: WORK_MEMORY_SCHEMA,
    6: WORK_THREAD_LIFECYCLE_SCHEMA,
    7: EXECUTION_INTEGRITY_SCHEMA,
    8: RUN_REPLACEMENT_SCHEMA,
    9: SUPERVISION_SCHEMA,
    10: CANDIDATE_SCHEMA,
    11: PROVIDER_DISPATCH_SCHEMA,
    12: GATE_SCHEMA,
    13: INTEGRATION_SCHEMA,
    14: INFERENCE_CONNECTOR_SCHEMA,
    15: INFERENCE_DISPATCH_SCHEMA,
    16: PROVIDER_INVOCATION_LIFECYCLE_SCHEMA,
    17: ATTEMPT_OWNER_AUDIT_SCHEMA,
    18: PROVIDER_BUDGET_SCHEMA,
    19: RUN_PAUSED_STATE_MIGRATION + PROVIDER_RECOVERY_SCHEMA,
    20: RECOVERY_CHECKPOINT_SCHEMA,
    21: LANDING_PLAN_NONCE_SCHEMA + PROVIDER_REPORTED_OVERRUN_SCHEMA,
    22: SERVICE_SIDECAR_SCHEMA,
    23: CAPABILITY_BINDING_SCHEMA,
    24: PROVIDER_LANDING_SCHEMA,
    25: GATE_CHANGE_SCHEMA,
    26: MAINTAINER_REVIEW_SCHEMA,
    27: WORKFLOW_TEMPLATE_SCHEMA,
    28: WORKFLOW_DIRECTOR_SCHEMA,
    29: WORKFLOW_LANDING_SCHEMA,
    30: WORKFLOW_LANDING_CHAIN_SCHEMA,
    31: WORKFLOW_PRESCRIPTION_SCHEMA,
    32: DISCOVERY_PRESCRIPTION_SCHEMA,
    33: DISCOVERY_EXECUTION_SCHEMA,
    34: DISCOVERY_DIRECTOR_SCHEMA,
    35: ADVISORY_REVIEW_PRESCRIPTION_SCHEMA,
    36: ADVISORY_EXECUTION_SCHEMA,
    37: ADVISORY_DIRECTOR_SCHEMA,
    38: WORKFLOW_SCOPE_BINDING_SCHEMA,
    39: WORKFLOW_SCOPE_REVIEW_SCHEMA,
    40: OPERABILITY_SCHEMA,
    41: SERVICE_RESOURCE_SCHEMA,
    42: WORK_INTAKE_SCHEMA,
}
