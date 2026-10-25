from __future__ import annotations

from .storage.artifacts import ArtifactStorage
from .storage.attempts import AttemptStorage
from .storage.candidates import CandidateStorage
from .storage.catalog import CatalogStorage
from .storage.core import DatabaseCore, default_db_path
from .storage.discovery import DiscoveryStorage
from .storage.errors import (
    BudgetExceededError,
    ConcurrentUpdateError,
    ConflictError,
    NotFoundError,
    StorageError,
)
from .storage.gates import GateStorage
from .storage.inference import InferenceStorage
from .storage.integration import (
    MAX_INTEGRATION_LEASE_SECONDS,
    MIN_INTEGRATION_LEASE_SECONDS,
    IntegrationStorage,
)
from .storage.interactions import InteractionStorage
from .storage.queries import QueryStorage
from .storage.recovery import RecoveryStorage
from .storage.runs import RunStorage
from .storage.schema import (
    ATTEMPT_SCHEMA,
    CANDIDATE_SCHEMA,
    CAPABILITY_BINDING_SCHEMA,
    DATABASE_FILENAME,
    DISCOVERY_DIRECTOR_SCHEMA,
    DISCOVERY_EXECUTION_SCHEMA,
    DISCOVERY_PRESCRIPTION_SCHEMA,
    EXECUTION_INTEGRITY_SCHEMA,
    GATE_CHANGE_SCHEMA,
    GATE_SCHEMA,
    INFERENCE_CONNECTOR_SCHEMA,
    INFERENCE_DISPATCH_SCHEMA,
    INTEGRATION_SCHEMA,
    INTERACTION_SCHEMA,
    LANDING_PLAN_NONCE_SCHEMA,
    LEGACY_DATABASE_FILENAME,
    LEGACY_STATE_DIRECTORY_NAME,
    MAINTAINER_REVIEW_SCHEMA,
    MAX_ATTEMPT_LEASE_SECONDS,
    MAX_VISIBLE_ASSIGNMENT_EVENTS,
    MAX_WORK_ASSIGNMENTS_PER_THREAD,
    MIGRATIONS,
    MIN_ATTEMPT_LEASE_SECONDS,
    PROVIDER_BUDGET_SCHEMA,
    PROVIDER_DISPATCH_SCHEMA,
    PROVIDER_LANDING_SCHEMA,
    PROVIDER_RECOVERY_SCHEMA,
    PROVIDER_REPORTED_OVERRUN_SCHEMA,
    PROVIDER_SCHEMA,
    RECOVERY_CHECKPOINT_SCHEMA,
    RUN_PAUSED_STATE_MIGRATION,
    RUN_REPLACEMENT_SCHEMA,
    SCHEMA,
    SCHEMA_V1,
    SCHEMA_VERSION,
    SERVICE_SIDECAR_SCHEMA,
    STATE_DIRECTORY_NAME,
    SUPERVISION_SCHEMA,
    WORK_ITEM_SCHEMA,
    WORK_MEMORY_SCHEMA,
    WORK_THREAD_LIFECYCLE_SCHEMA,
    WORKFLOW_DIRECTOR_SCHEMA,
    WORKFLOW_LANDING_CHAIN_SCHEMA,
    WORKFLOW_LANDING_SCHEMA,
    WORKFLOW_PRESCRIPTION_SCHEMA,
    WORKFLOW_TEMPLATE_SCHEMA,
)
from .storage.service_sidecars import ServiceSidecarStorage
from .storage.supervision import SupervisionStorage
from .storage.work import WorkStorage
from .storage.work_items import WorkItemStorage
from .storage.workflows import WorkflowStorage

__all__ = [
    "ATTEMPT_SCHEMA",
    "CAPABILITY_BINDING_SCHEMA",
    "CANDIDATE_SCHEMA",
    "BudgetExceededError",
    "ConcurrentUpdateError",
    "ConflictError",
    "DATABASE_FILENAME",
    "DISCOVERY_DIRECTOR_SCHEMA",
    "DISCOVERY_EXECUTION_SCHEMA",
    "DISCOVERY_PRESCRIPTION_SCHEMA",
    "Database",
    "EXECUTION_INTEGRITY_SCHEMA",
    "GATE_CHANGE_SCHEMA",
    "GATE_SCHEMA",
    "INFERENCE_CONNECTOR_SCHEMA",
    "INFERENCE_DISPATCH_SCHEMA",
    "INTERACTION_SCHEMA",
    "INTEGRATION_SCHEMA",
    "LANDING_PLAN_NONCE_SCHEMA",
    "LEGACY_DATABASE_FILENAME",
    "LEGACY_STATE_DIRECTORY_NAME",
    "MAX_ATTEMPT_LEASE_SECONDS",
    "MAX_INTEGRATION_LEASE_SECONDS",
    "MAX_VISIBLE_ASSIGNMENT_EVENTS",
    "MAX_WORK_ASSIGNMENTS_PER_THREAD",
    "MAINTAINER_REVIEW_SCHEMA",
    "MIGRATIONS",
    "MIN_ATTEMPT_LEASE_SECONDS",
    "MIN_INTEGRATION_LEASE_SECONDS",
    "NotFoundError",
    "PROVIDER_SCHEMA",
    "PROVIDER_LANDING_SCHEMA",
    "PROVIDER_BUDGET_SCHEMA",
    "PROVIDER_DISPATCH_SCHEMA",
    "PROVIDER_RECOVERY_SCHEMA",
    "PROVIDER_REPORTED_OVERRUN_SCHEMA",
    "RECOVERY_CHECKPOINT_SCHEMA",
    "RUN_REPLACEMENT_SCHEMA",
    "RUN_PAUSED_STATE_MIGRATION",
    "SCHEMA",
    "SCHEMA_V1",
    "SCHEMA_VERSION",
    "SERVICE_SIDECAR_SCHEMA",
    "STATE_DIRECTORY_NAME",
    "StorageError",
    "SUPERVISION_SCHEMA",
    "WORK_ITEM_SCHEMA",
    "WORK_MEMORY_SCHEMA",
    "WORK_THREAD_LIFECYCLE_SCHEMA",
    "WORKFLOW_DIRECTOR_SCHEMA",
    "WORKFLOW_LANDING_CHAIN_SCHEMA",
    "WORKFLOW_LANDING_SCHEMA",
    "WORKFLOW_PRESCRIPTION_SCHEMA",
    "WORKFLOW_TEMPLATE_SCHEMA",
    "default_db_path",
]


class Database(
    DiscoveryStorage,
    WorkflowStorage,
    SupervisionStorage,
    ServiceSidecarStorage,
    QueryStorage,
    RecoveryStorage,
    IntegrationStorage,
    InferenceStorage,
    GateStorage,
    CandidateStorage,
    ArtifactStorage,
    WorkStorage,
    InteractionStorage,
    AttemptStorage,
    RunStorage,
    WorkItemStorage,
    CatalogStorage,
    DatabaseCore,
):
    """Stable control-plane persistence facade composed from aggregate stores."""
