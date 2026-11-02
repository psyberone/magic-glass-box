from __future__ import annotations

import sqlite3


def downgrade_workflow_policy_activation(connection: sqlite3.Connection) -> None:
    columns = {
        row[1] for row in connection.execute("PRAGMA table_info(workflow_executions)")
    }
    if "workflow_activation_version_id" not in columns:
        return
    connection.executescript(
        """
        PRAGMA foreign_keys = OFF;
        DROP TRIGGER IF EXISTS workflow_manual_wake_event_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_manual_wake_event_immutable_update;
        DROP TRIGGER IF EXISTS workflow_manual_wake_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_manual_wake_identity_immutable;
        DROP INDEX IF EXISTS workflow_manual_wake_recipient_state;
        DROP INDEX IF EXISTS workflow_manual_wake_activation;
        DROP INDEX IF EXISTS workflow_manual_wake_execution;
        DROP TABLE IF EXISTS workflow_manual_wake_events;
        DROP TABLE IF EXISTS workflow_manual_wakes;
        DROP TRIGGER IF EXISTS workflow_execution_activation_immutable;
        DROP INDEX IF EXISTS workflow_execution_activation;
        ALTER TABLE workflow_executions
            DROP COLUMN workflow_activation_version_id;
        DROP TRIGGER IF EXISTS workflow_activation_capability_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_activation_capability_immutable_update;
        DROP TRIGGER IF EXISTS workflow_activation_version_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_activation_version_immutable_update;
        DROP TRIGGER IF EXISTS workflow_activation_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_activation_identity_immutable;
        DROP TRIGGER IF EXISTS workflow_policy_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_policy_immutable_update;
        DROP TABLE workflow_activation_capability_bindings;
        DROP TABLE workflow_activation_versions;
        DROP TABLE workflow_activations;
        DROP TABLE workflow_policies;
        PRAGMA foreign_keys = ON;
        """
    )
