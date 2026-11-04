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
        DROP TRIGGER IF EXISTS workflow_correction_ceiling_extension_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_correction_ceiling_extension_immutable_update;
        DROP INDEX IF EXISTS workflow_correction_ceiling_execution;
        DROP TABLE IF EXISTS workflow_correction_ceiling_extensions;
        DROP TRIGGER IF EXISTS workflow_closeout_proof_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_closeout_proof_immutable_update;
        DROP TRIGGER IF EXISTS workflow_closeout_obligation_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_closeout_obligation_transition;
        DROP TRIGGER IF EXISTS workflow_closeout_obligation_identity_immutable;
        DROP TRIGGER IF EXISTS workflow_closeout_declaration_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_closeout_declaration_immutable_update;
        DROP INDEX IF EXISTS workflow_closeout_obligation_execution;
        DROP INDEX IF EXISTS workflow_closeout_obligation_state_deadline;
        DROP TABLE IF EXISTS workflow_closeout_proofs;
        DROP TABLE IF EXISTS workflow_closeout_obligations;
        DROP TABLE IF EXISTS workflow_closeout_declarations;
        DROP TRIGGER IF EXISTS workflow_provider_target_origin_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_provider_target_origin_immutable_update;
        DROP TRIGGER IF EXISTS workflow_provider_effect_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_provider_effect_identity_immutable;
        DROP TRIGGER IF EXISTS workflow_provider_intent_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_provider_intent_immutable_update;
        DROP TRIGGER IF EXISTS integration_target_hold_immutable_delete;
        DROP TRIGGER IF EXISTS integration_target_hold_identity_immutable;
        DROP TRIGGER IF EXISTS workflow_provider_landing_proof_immutable_delete;
        DROP TRIGGER IF EXISTS workflow_provider_landing_proof_immutable_update;
        DROP INDEX IF EXISTS integration_target_hold_execution;
        DROP INDEX IF EXISTS integration_target_one_open_hold;
        DROP INDEX IF EXISTS workflow_provider_landing_proof_status;
        DROP INDEX IF EXISTS workflow_provider_effect_state;
        DROP TABLE IF EXISTS workflow_provider_target_origins;
        DROP TABLE IF EXISTS workflow_provider_effects;
        DROP TABLE IF EXISTS workflow_provider_intents;
        DROP TABLE IF EXISTS integration_target_holds;
        DROP TABLE IF EXISTS workflow_provider_landing_proofs;
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
