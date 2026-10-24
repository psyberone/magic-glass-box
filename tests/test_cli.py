from __future__ import annotations

import argparse
import io
import json
import os
import signal
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

from magic_glass_box.cli import (
    InputError,
    _build_run_coordinator,
    _emit,
    _handle_sigterm,
    _json_file,
    _serve_control_room,
    _serve_supervisor,
    _TerminationRequested,
    build_parser,
    main,
)
from magic_glass_box.db import SCHEMA_VERSION, Database
from magic_glass_box.domain import (
    AgentProfileSpec,
    ProjectContext,
    SupervisionPolicy,
    WorkItemEnvelope,
)
from magic_glass_box.gateway import GatewayError
from magic_glass_box.inference import InferenceConnectorError
from magic_glass_box.krab import ActivityPage
from magic_glass_box.worker_transport import WorkerTransportError
from magic_glass_box.workflows import WorkflowConfigurationSpec


class FakeDatabase:
    def __init__(self) -> None:
        self.path = Path("fake-control-plane.db")
        self.calls: list[tuple[str, dict[str, object]]] = []

    def status(self) -> dict[str, object]:
        self.calls.append(("status", {}))
        return {"operation": "status"}

    def get_run(self, run_id: str) -> dict[str, object]:
        self.calls.append(("run.show", {"run_id": run_id}))
        return {"operation": "run.show", "run_id": run_id}

    def get_provider_grant(self, name: str) -> dict[str, object]:
        self.calls.append(("provider.show", {"name": name}))
        return {"operation": "provider.show", "name": name}

    def get_attempt_result(self, attempt_id: str) -> dict[str, object]:
        self.calls.append(("result.show", {"attempt_id": attempt_id}))
        return {
            "attempt_id": attempt_id,
            "content_sha256": "a" * 64,
            "operation": "result.show",
        }


class FakeControlPlane:
    def __init__(self) -> None:
        self.database = FakeDatabase()
        self.calls: list[tuple[str, dict[str, object]]] = []

    def _called(self, operation: str, **kwargs: object) -> dict[str, object]:
        self.calls.append((operation, kwargs))
        return {"operation": operation}

    def initialize(self) -> dict[str, object]:
        return self._called("init")

    def control_room_snapshot(self, **kwargs: object) -> dict[str, object]:
        return self._called("control-room.show", **kwargs)

    def create_user(self, handle: str, display_name: str | None) -> dict[str, object]:
        return self._called("user.add", handle=handle, display_name=display_name)

    def add_krab_instance(self, name: str, url: str) -> dict[str, object]:
        return self._called("krab.instance-add", name=name, url=url)

    def add_krab_identity(self, **kwargs: object) -> dict[str, object]:
        return self._called("krab.identity-add", **kwargs)

    def project_activity(self, **kwargs: object) -> ActivityPage:
        self.calls.append(("krab.activity", kwargs))
        return ActivityPage("box-project", 0, 0, ())

    def accept_interaction_response(
        self, interaction_id: str, **kwargs: object
    ) -> SimpleNamespace:
        self.calls.append(
            ("interaction.answer", {"interaction_id": interaction_id, **kwargs})
        )
        return SimpleNamespace(
            interaction_id=interaction_id,
            run_id="run_" + "2" * 32,
            response_item_id=kwargs["response_item_id"],
            interaction_state="answered",
            interaction_version=2,
            run_state="queued",
            run_version=5,
            untrusted_title="PRIVATE RESPONSE TITLE",
            untrusted_body="PRIVATE RESPONSE BODY",
            content_risk_flags=("untrusted",),
        )

    def register_project(self, **kwargs: object) -> dict[str, object]:
        return self._called("project.add", **kwargs)

    def revise_project_context(self, **kwargs: object) -> dict[str, object]:
        return self._called("project.revise", **kwargs)

    def create_agent_profile(self, **kwargs: object) -> dict[str, object]:
        return self._called("profile.add", **kwargs)

    def revise_agent_profile(self, **kwargs: object) -> dict[str, object]:
        return self._called("profile.revise", **kwargs)

    def create_workflow_configuration(self, **kwargs: object) -> dict[str, object]:
        return self._called("workflow.config-create", **kwargs)

    def revise_workflow_configuration(self, **kwargs: object) -> dict[str, object]:
        return self._called("workflow.config-revise", **kwargs)

    def get_workflow_configuration(self, **kwargs: object) -> dict[str, object]:
        return self._called("workflow.config-show", **kwargs)

    def list_workflow_configurations(self, **kwargs: object) -> dict[str, object]:
        return self._called("workflow.config-list", **kwargs)

    def start_workflow_execution(self, **kwargs: object) -> dict[str, object]:
        return self._called("workflow.start", **kwargs)

    def get_workflow_execution(self, execution_id: str) -> dict[str, object]:
        return self._called("workflow.show", execution_id=execution_id)

    def list_workflow_executions(self, **kwargs: object) -> dict[str, object]:
        return self._called("workflow.list", **kwargs)

    def cancel_workflow_execution(
        self, execution_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("workflow.cancel", execution_id=execution_id, **kwargs)

    def get_workflow_ruling(self, ruling_id: str) -> dict[str, object]:
        return self._called("workflow.ruling-show", ruling_id=ruling_id)

    def resolve_workflow_ruling(
        self, ruling_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("workflow.ruling-resolve", ruling_id=ruling_id, **kwargs)

    def revoke_provider_grant(self, name: str, **kwargs: object) -> dict[str, object]:
        return self._called("provider.revoke", name=name, **kwargs)

    def import_work_item(self, **kwargs: object) -> dict[str, object]:
        return self._called("work-item.import", **kwargs)

    def get_work_item(self, work_item_id: str) -> dict[str, object]:
        return self._called("work-item.show", work_item_id=work_item_id)

    def complete_work_item(
        self, work_item_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("work-item.complete", work_item_id=work_item_id, **kwargs)

    def reopen_work_item(
        self, work_item_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("work-item.reopen", work_item_id=work_item_id, **kwargs)

    def create_run(self, **kwargs: object) -> dict[str, object]:
        return self._called("run.create", **kwargs)

    def request_run_cancellation(
        self, run_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("run.cancel", run_id=run_id, **kwargs)

    def retry_run(self, run_id: str, **kwargs: object) -> dict[str, object]:
        return self._called("run.retry", run_id=run_id, **kwargs)

    def reassign_run(self, run_id: str, **kwargs: object) -> dict[str, object]:
        return self._called("run.reassign", run_id=run_id, **kwargs)

    def create_candidate(self, attempt_id: str, **kwargs: object) -> dict[str, object]:
        return self._called("candidate.create", attempt_id=attempt_id, **kwargs)

    def get_candidate(self, candidate_id: str, **kwargs: object) -> dict[str, object]:
        return self._called("candidate.show", candidate_id=candidate_id, **kwargs)

    def list_candidates(self, **kwargs: object) -> dict[str, object]:
        return self._called("candidate.list", **kwargs)

    def create_gate_policy(
        self, candidate_id: str, policy: object, **kwargs: object
    ) -> dict[str, object]:
        return self._called(
            "gate.policy-create", candidate_id=candidate_id, policy=policy, **kwargs
        )

    def get_gate_policy(self, policy_id: str) -> dict[str, object]:
        return self._called("gate.policy-show", policy_id=policy_id)

    def list_gate_policies(self, **kwargs: object) -> dict[str, object]:
        return self._called("gate.policy-list", **kwargs)

    def create_review_run(self, policy_id: str, **kwargs: object) -> dict[str, object]:
        return self._called("gate.review-create", policy_id=policy_id, **kwargs)

    def record_review_evidence(
        self, policy_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("gate.review-record", policy_id=policy_id, **kwargs)

    def record_maintainer_review(
        self, policy_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("gate.maintainer-record", policy_id=policy_id, **kwargs)

    def list_maintainer_reviews(
        self, policy_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("gate.maintainer-list", policy_id=policy_id, **kwargs)

    def create_changes_run(self, policy_id: str, **kwargs: object) -> dict[str, object]:
        return self._called("gate.changes-create", policy_id=policy_id, **kwargs)

    def run_local_validation(
        self, policy_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("gate.validate", policy_id=policy_id, **kwargs)

    def list_gate_evidence(self, policy_id: str, **kwargs: object) -> dict[str, object]:
        return self._called("gate.evidence-list", policy_id=policy_id, **kwargs)

    def gate_readiness(self, policy_id: str) -> dict[str, object]:
        return self._called("gate.readiness", policy_id=policy_id)

    def add_landing_dependency(
        self,
        dependent_work_item_id: str,
        prerequisite_work_item_id: str,
        **kwargs: object,
    ) -> dict[str, object]:
        return self._called(
            "integration.dependency-add",
            dependent_work_item_id=dependent_work_item_id,
            prerequisite_work_item_id=prerequisite_work_item_id,
            **kwargs,
        )

    def remove_landing_dependency(
        self, dependency_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called(
            "integration.dependency-remove", dependency_id=dependency_id, **kwargs
        )

    def list_landing_dependencies(self, **kwargs: object) -> dict[str, object]:
        return self._called("integration.dependency-list", **kwargs)

    def enqueue_integration(
        self, policy_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("integration.enqueue", policy_id=policy_id, **kwargs)

    def get_integration_queue_item(self, queue_item_id: str) -> dict[str, object]:
        return self._called("integration.show", queue_item_id=queue_item_id)

    def list_integration_queue(self, **kwargs: object) -> dict[str, object]:
        return self._called("integration.list", **kwargs)

    def refold_integration_item(
        self, queue_item_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("integration.refold", queue_item_id=queue_item_id, **kwargs)

    def activate_integration_fold(
        self, queue_item_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called(
            "integration.activate", queue_item_id=queue_item_id, **kwargs
        )

    def claim_integration_item(
        self, queue_item_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("integration.claim", queue_item_id=queue_item_id, **kwargs)

    def release_integration_claim(
        self, queue_item_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called(
            "integration.release", queue_item_id=queue_item_id, **kwargs
        )

    def cancel_integration_item(
        self, queue_item_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("integration.cancel", queue_item_id=queue_item_id, **kwargs)

    def issue_landing_plan(
        self, queue_item_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("integration.plan", queue_item_id=queue_item_id, **kwargs)

    def get_landing_plan(self, plan_id: str) -> dict[str, object]:
        return self._called("integration.plan-show", plan_id=plan_id)

    def reissue_landing_plan_nonce(
        self, plan_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called("integration.plan-reissue", plan_id=plan_id, **kwargs)

    def record_landing_receipt(
        self, request: object, **kwargs: object
    ) -> dict[str, object]:
        return self._called("integration.receipt", request=request, **kwargs)

    def issue_provider_landing(
        self, queue_item_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called(
            "integration.provider-issue", queue_item_id=queue_item_id, **kwargs
        )

    def get_provider_landing(self, provider_landing_id: str) -> dict[str, object]:
        return self._called(
            "integration.provider-show",
            provider_landing_id=provider_landing_id,
        )

    def execute_provider_landing(
        self, provider_landing_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called(
            "integration.provider-execute",
            provider_landing_id=provider_landing_id,
            **kwargs,
        )

    def settle_provider_landing(
        self, provider_landing_id: str, **kwargs: object
    ) -> dict[str, object]:
        return self._called(
            "integration.provider-settle",
            provider_landing_id=provider_landing_id,
            **kwargs,
        )


class FakeRunCoordinator:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def execute(
        self, run_id: str, *, runtime_adapter: str | None = None
    ) -> dict[str, object]:
        self.calls.append(
            (
                "execute",
                run_id
                if runtime_adapter is None
                else {"run_id": run_id, "runtime_adapter": runtime_adapter},
            )
        )
        return {"operation": "run.execute", "run_id": run_id}

    def reconcile(self, run_id: str | None = None) -> list[dict[str, object]]:
        self.calls.append(("reconcile", {} if run_id is None else run_id))
        return [{"operation": "run.reconcile"}]


class FakeSupervisor:
    def __init__(self) -> None:
        self.supervisor_id = "supervisor-test"
        self.run_calls = 0
        self.close_calls: list[bool] = []

    def run_forever(self) -> None:
        self.run_calls += 1
        raise KeyboardInterrupt

    def close(self, *, wait: bool = True) -> bool:
        self.close_calls.append(wait)
        return True


class FakeResultStore:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def inspect(self, attempt_id: str, **kwargs: object) -> object:
        self.calls.append(("inspect", {"attempt_id": attempt_id, **kwargs}))
        return object()

    def apply(
        self, attempt_id: str, repository: Path, **kwargs: object
    ) -> dict[str, object]:
        self.calls.append(
            (
                "apply",
                {"attempt_id": attempt_id, "repository": repository, **kwargs},
            )
        )
        return {"operation": "result.apply", "attempt_id": attempt_id}


class FakeCandidateStore:
    pass


class CLITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / "control-plane.db"

    def invoke(self, *arguments: str) -> tuple[int, dict[str, object], str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = main(["--db", str(self.database), *arguments])
        output = stdout.getvalue() or stderr.getvalue()
        return status, json.loads(output), stderr.getvalue()

    def test_json_output_preserves_unicode_for_human_readability(self) -> None:
        stream = io.StringIO()
        _emit({"message": "café"}, stream=stream)
        self.assertEqual(stream.getvalue(), '{\n  "message": "café"\n}\n')

    def test_parser_exposes_the_documented_command_help_contract(self) -> None:
        parser = build_parser()
        self.assertEqual(parser.prog, "magic-glass-box")
        self.assertEqual(
            parser.description,
            "Magic Glass Box control-plane administration",
        )
        database_action = next(
            action for action in parser._actions if action.dest == "db"
        )
        self.assertEqual(database_action.help, "control-plane database path")

        expected = {
            (): (
                "command",
                True,
                (
                    ("init", "initialize the private state database"),
                    ("status", "validate state and show object counts"),
                    ("control-room", "inspect redacted operational state"),
                    ("user", "manage local users"),
                    ("krab", "manage and query Krab connections"),
                    ("project", "manage project context"),
                    ("profile", "manage agent profiles"),
                    ("workflow", "manage bounded workflow prescriptions"),
                    (
                        "inference-connector",
                        "manage credential-isolated inference connectors",
                    ),
                    (
                        "service-sidecar",
                        "manage provider-neutral service sidecars and operations",
                    ),
                    (
                        "capability",
                        "bind project capabilities to explicit controllers",
                    ),
                    ("provider", "manage scoped provider grants"),
                    ("work-item", "manage durable work items"),
                    ("interaction", "resolve durable agent interactions"),
                    ("run", "manage agent runs"),
                    ("supervisor", "run the autonomous control loop"),
                    ("result", "inspect and apply retained work"),
                    ("candidate", "materialize and inspect trusted candidates"),
                    ("gate", "manage exact-input integration gates"),
                    (
                        "integration",
                        "order, refold, and land exact candidates",
                    ),
                ),
            ),
            ("user",): (
                "user_command",
                True,
                (("add", "add a local user"),),
            ),
            ("control-room",): (
                "control_room_command",
                True,
                (
                    ("show", "show one consistent redacted snapshot"),
                    ("serve", "serve the interactive loopback control room"),
                ),
            ),
            ("krab",): (
                "krab_command",
                True,
                (
                    ("instance-add", "add a Krab origin"),
                    (
                        "identity-add",
                        "verify and retain a Krab credential reference",
                    ),
                    ("activity", "read metadata-only project activity"),
                ),
            ),
            ("project",): (
                "project_command",
                True,
                (
                    ("add", "verify and register a visible Krab project"),
                    ("revise", "append an immutable project-context revision"),
                ),
            ),
            ("profile",): (
                "profile_command",
                True,
                (
                    ("add", "create a versioned agent profile"),
                    ("revise", "append an immutable profile revision"),
                ),
            ),
            ("workflow",): (
                "workflow_command",
                True,
                (
                    (
                        "config-create",
                        "create a versioned workflow configuration",
                    ),
                    (
                        "config-revise",
                        "append an immutable configuration revision",
                    ),
                    (
                        "config-show",
                        "show one workflow configuration revision",
                    ),
                    ("config-list", "list latest workflow configurations"),
                    ("start", "start one exact pinned workflow execution"),
                    ("show", "show one workflow execution and event stream"),
                    ("list", "list bounded workflow executions"),
                    ("cancel", "cancel one active workflow execution"),
                    ("ruling-show", "show one durable workflow ruling"),
                    ("ruling-resolve", "resolve one durable workflow ruling"),
                ),
            ),
            ("provider",): (
                "provider_command",
                True,
                (
                    ("connector-add", "add a connector-bound provider grant"),
                    ("show", "show provider policy and lifecycle metadata"),
                    ("revoke", "revoke a provider grant"),
                ),
            ),
            ("inference-connector",): (
                "inference_connector_command",
                True,
                (
                    ("register", "register immutable connector authority"),
                    (
                        "show",
                        "show connector authority and lifecycle metadata",
                    ),
                    ("list", "list connector authority and lifecycle metadata"),
                    ("revoke", "revoke an inference connector"),
                ),
            ),
            ("service-sidecar",): (
                "service_sidecar_command",
                True,
                (
                    (
                        "register",
                        "register immutable service-sidecar authority",
                    ),
                    (
                        "show",
                        "show service-sidecar authority and lifecycle metadata",
                    ),
                    (
                        "list",
                        "list service-sidecar authority and lifecycle metadata",
                    ),
                    ("revoke", "revoke a service sidecar"),
                    (
                        "operation-create",
                        "create one exact durable service operation",
                    ),
                    ("operation-show", "show one durable service operation"),
                    (
                        "operation-list",
                        "list recent durable service operations",
                    ),
                    (
                        "operation-execute",
                        "invoke one authorized service operation",
                    ),
                    (
                        "operation-reconcile",
                        "reconcile one uncertain service operation",
                    ),
                ),
            ),
            ("capability",): (
                "capability_command",
                True,
                (
                    ("bind", "bind one project capability to a controller"),
                    ("show", "show one project capability binding"),
                    ("list", "list project capability bindings"),
                    ("unbind", "disable one project capability binding"),
                ),
            ),
            ("work-item",): (
                "work_item_command",
                True,
                (
                    ("import", "create or update a normalized work item"),
                    ("show", "show work-item lifecycle metadata"),
                    ("complete", "seal completed work memory"),
                    ("reopen", "start a fresh work-memory generation"),
                ),
            ),
            ("interaction",): (
                "interaction_command",
                True,
                (("answer", "bind one explicit Krab response item"),),
            ),
            ("run",): (
                "run_command",
                True,
                (
                    ("create", "create a pinned run record"),
                    ("cancel", "request stop for a queued or active run"),
                    (
                        "retry",
                        "create an audited successor with the same pinned profile",
                    ),
                    (
                        "reassign",
                        "create an audited successor for another profile",
                    ),
                    ("show", "show a run and its event stream"),
                    (
                        "execute",
                        "execute one queued run in its pinned sandbox",
                    ),
                    (
                        "reconcile",
                        "clean and fail runs interrupted by an orchestrator restart",
                    ),
                ),
            ),
            ("supervisor",): (
                "supervisor_command",
                True,
                (("run", "advance opted-in work until stopped"),),
            ),
            ("result",): (
                "result_command",
                True,
                (
                    ("show", "verify and show one retained attempt result"),
                    (
                        "apply",
                        "apply one retained result after conflict preflight",
                    ),
                ),
            ),
            ("candidate",): (
                "candidate_command",
                True,
                (
                    ("create", "materialize one retained result as a candidate"),
                    ("show", "verify and show one trusted candidate"),
                    (
                        "list",
                        "list recent durable candidates without reading Git objects",
                    ),
                ),
            ),
            ("gate",): (
                "gate_command",
                True,
                (
                    ("policy-create", "pin a gate policy to one exact candidate"),
                    ("policy-show", "show one immutable gate policy"),
                    ("policy-list", "list recent immutable gate policies"),
                    (
                        "review-create",
                        "create a candidate-backed independent review run",
                    ),
                    ("review-record", "record independent-review evidence"),
                    (
                        "maintainer-record",
                        "record an authorized product disposition for an exact "
                        "candidate",
                    ),
                    (
                        "maintainer-list",
                        "list append-only maintainer dispositions",
                    ),
                    (
                        "changes-create",
                        "return a changes-requested candidate and feedback to a "
                        "developer run",
                    ),
                    ("validate", "run one pinned local validation gate"),
                    ("evidence-list", "list append-only gate evidence"),
                    ("readiness", "derive current exact-input readiness"),
                ),
            ),
            ("integration",): (
                "integration_command",
                True,
                (
                    (
                        "dependency-add",
                        "add a hard work-item landing dependency",
                    ),
                    (
                        "dependency-remove",
                        "remove a hard landing dependency",
                    ),
                    (
                        "dependency-list",
                        "list durable work-item landing dependencies",
                    ),
                    ("enqueue", "queue one gate policy for its integration ref"),
                    (
                        "show",
                        "show one integration queue item and its event stream",
                    ),
                    ("list", "list durable integration queue items"),
                    (
                        "refold",
                        "replay an original result onto the current integration base",
                    ),
                    (
                        "activate",
                        "bind a fresh folded policy and return the item to queue",
                    ),
                    (
                        "claim",
                        "lease one ready item on its serialized ref lane",
                    ),
                    (
                        "release",
                        "release a claimed item before authority is issued",
                    ),
                    (
                        "cancel",
                        "cancel an item before landing authority is issued",
                    ),
                    ("plan", "issue an exact argv-only manual landing plan"),
                    ("plan-show", "show one issued plan without its raw nonce"),
                    (
                        "plan-reissue",
                        "replace a lost plan nonce without releasing its lane",
                    ),
                    (
                        "receipt",
                        "record one nonce-bound landed or not-landed outcome",
                    ),
                    (
                        "provider-issue",
                        "issue exact landing authority to the bound provider",
                    ),
                    (
                        "provider-show",
                        "show one provider landing and its service operation",
                    ),
                    (
                        "provider-execute",
                        "execute and settle one provider landing",
                    ),
                    (
                        "provider-settle",
                        "settle one externally completed provider landing",
                    ),
                ),
            ),
        }

        actual: dict[
            tuple[str, ...], tuple[str, bool, tuple[tuple[str, str | None], ...]]
        ] = {}
        required_options: dict[tuple[str, ...], tuple[str, ...]] = {}
        choices: dict[tuple[tuple[str, ...], str], tuple[str, ...]] = {}

        def collect(
            current: argparse.ArgumentParser, path: tuple[str, ...] = ()
        ) -> None:
            required = tuple(
                action.option_strings[0]
                for action in current._actions
                if action.required and action.option_strings
            )
            if required:
                required_options[path] = required
            for parser_action in current._actions:
                if (
                    not isinstance(parser_action, argparse._SubParsersAction)
                    and parser_action.choices is not None
                ):
                    choices[(path, parser_action.dest)] = tuple(parser_action.choices)
            subparsers = [
                action
                for action in current._actions
                if isinstance(action, argparse._SubParsersAction)
            ]
            if not subparsers:
                return
            self.assertEqual(len(subparsers), 1)
            action = subparsers[0]
            help_by_name = {
                choice.dest: choice.help for choice in action._choices_actions
            }
            actual[path] = (
                action.dest,
                action.required,
                tuple((name, help_by_name[name]) for name in action.choices),
            )
            for name, child in action.choices.items():
                collect(child, (*path, name))

        collect(parser)
        self.assertEqual(actual, expected)
        self.assertEqual(
            required_options,
            {
                ("krab", "instance-add"): ("--url",),
                ("krab", "identity-add"): ("--token-file",),
                ("krab", "activity"): ("--as-user",),
                ("project", "add"): (
                    "--instance",
                    "--as-user",
                    "--name",
                    "--context",
                    "--created-by",
                ),
                ("project", "revise"): ("--context", "--created-by"),
                ("profile", "add"): (
                    "--instance",
                    "--krab-user",
                    "--spec",
                    "--created-by",
                ),
                ("profile", "revise"): (
                    "--instance",
                    "--krab-user",
                    "--spec",
                    "--created-by",
                ),
                ("workflow", "config-create"): (
                    "--project",
                    "--producer",
                    "--reviewer",
                    "--spec",
                    "--created-by",
                ),
                ("workflow", "config-revise"): (
                    "--project",
                    "--producer",
                    "--reviewer",
                    "--spec",
                    "--created-by",
                ),
                ("workflow", "config-show"): ("--project",),
                ("workflow", "start"): (
                    "--project",
                    "--work-item",
                    "--source-revision",
                    "--expected-configuration-version",
                    "--expected-context-version",
                    "--expected-work-item-version",
                    "--expected-thread-generation",
                    "--expected-thread-version",
                    "--idempotency-key",
                    "--requested-by",
                ),
                ("workflow", "cancel"): (
                    "--expected-version",
                    "--cancelled-by",
                    "--reason",
                ),
                ("workflow", "ruling-resolve"): (
                    "--expected-version",
                    "--decision",
                    "--resolved-by",
                ),
                ("inference-connector", "register"): (
                    "--spec",
                    "--created-by",
                ),
                ("inference-connector", "revoke"): (
                    "--expected-version",
                    "--revoked-by",
                ),
                ("service-sidecar", "register"): (
                    "--spec",
                    "--created-by",
                ),
                ("service-sidecar", "revoke"): (
                    "--expected-version",
                    "--revoked-by",
                ),
                ("service-sidecar", "operation-create"): (
                    "--request",
                    "--created-by",
                ),
                ("service-sidecar", "operation-execute"): (
                    "--owner",
                    "--expected-version",
                ),
                ("service-sidecar", "operation-reconcile"): (
                    "--owner",
                    "--expected-version",
                ),
                ("capability", "bind"): (
                    "--version",
                    "--sidecar",
                    "--mapping",
                    "--bound-by",
                ),
                ("capability", "unbind"): (
                    "--expected-version",
                    "--unbound-by",
                ),
                ("provider", "connector-add"): (
                    "--policy",
                    "--created-by",
                ),
                ("provider", "revoke"): (
                    "--expected-version",
                    "--revoked-by",
                ),
                ("work-item", "import"): ("--project", "--imported-by"),
                ("work-item", "complete"): (
                    "--expected-thread-version",
                    "--completed-by",
                ),
                ("work-item", "reopen"): (
                    "--expected-thread-version",
                    "--reopened-by",
                ),
                ("interaction", "answer"): (
                    "--response-item",
                    "--expected-version",
                ),
                ("run", "create"): (
                    "--project",
                    "--profile",
                    "--work-item",
                    "--requested-by",
                ),
                ("run", "cancel"): (
                    "--expected-version",
                    "--requested-by",
                    "--reason",
                ),
                ("run", "retry"): ("--expected-version", "--requested-by"),
                ("run", "reassign"): (
                    "--profile",
                    "--expected-version",
                    "--requested-by",
                ),
                ("result", "apply"): ("--repository",),
                ("candidate", "create"): ("--created-by",),
                ("gate", "policy-create"): ("--policy", "--created-by"),
                ("gate", "review-create"): (
                    "--profile",
                    "--requested-by",
                    "--expected-policy-revision",
                ),
                ("gate", "review-record"): (
                    "--reviewer-attempt",
                    "--outcome",
                    "--recorded-by",
                    "--expected-policy-revision",
                ),
                ("gate", "maintainer-record"): (
                    "--review",
                    "--recorded-by",
                    "--expected-policy-revision",
                ),
                ("gate", "changes-create"): (
                    "--profile",
                    "--requested-by",
                    "--expected-policy-revision",
                ),
                ("gate", "validate"): (
                    "--recorded-by",
                    "--expected-policy-revision",
                ),
                ("integration", "dependency-add"): ("--created-by",),
                ("integration", "dependency-remove"): (
                    "--expected-version",
                    "--removed-by",
                ),
                ("integration", "enqueue"): ("--created-by",),
                ("integration", "refold"): (
                    "--expected-version",
                    "--created-by",
                ),
                ("integration", "activate"): (
                    "--fold",
                    "--policy",
                    "--expected-version",
                    "--actor",
                ),
                ("integration", "claim"): (
                    "--owner",
                    "--expected-version",
                ),
                ("integration", "release"): (
                    "--owner",
                    "--expected-version",
                ),
                ("integration", "cancel"): (
                    "--expected-version",
                    "--cancelled-by",
                ),
                ("integration", "plan"): (
                    "--owner",
                    "--expected-version",
                    "--issued-by",
                ),
                ("integration", "plan-reissue"): (
                    "--expected-generation",
                    "--reissued-by",
                ),
                ("integration", "receipt"): ("--document", "--recorded-by"),
                ("integration", "provider-issue"): (
                    "--owner",
                    "--expected-version",
                    "--resource-id",
                    "--merge-method",
                    "--issued-by",
                ),
                ("integration", "provider-execute"): (
                    "--owner",
                    "--settled-by",
                ),
                ("integration", "provider-settle"): ("--settled-by",),
            },
        )
        self.assertEqual(
            choices,
            {
                (("supervisor", "run"), "log_level"): (
                    "DEBUG",
                    "INFO",
                    "WARNING",
                    "ERROR",
                    "CRITICAL",
                ),
                (("gate", "review-record"), "outcome"): ("passed", "failed"),
                (("workflow", "list"), "state"): (
                    "active",
                    "cancelled",
                    "completed",
                ),
                (("workflow", "ruling-resolve"), "decision"): (
                    "continue",
                    "cancel",
                ),
                (("integration", "provider-issue"), "merge_method"): (
                    "merge",
                    "rebase",
                    "squash",
                ),
            },
        )

    def test_init_user_and_status(self) -> None:
        status, initialized, _ = self.invoke("init")
        self.assertEqual(status, 0)
        self.assertEqual(initialized["schema_version"], SCHEMA_VERSION)
        status, user, _ = self.invoke(
            "user", "add", "operator", "--display-name", "Local Operator"
        )
        self.assertEqual(status, 0)
        self.assertEqual(user["handle"], "operator")
        status, state, _ = self.invoke("status")
        self.assertEqual(status, 0)
        self.assertEqual(state["counts"]["users"], 1)
        status, room, _ = self.invoke("control-room", "show", "--history-limit", "0")
        self.assertEqual(status, 0)
        self.assertEqual(room["summary"]["users"], 1)
        self.assertEqual(room["snapshot"]["history_limit"], 0)

    def test_error_is_structured_and_nonzero(self) -> None:
        self.invoke("init")
        status, error, stderr = self.invoke("user", "add", "x")
        self.assertEqual(status, 2)
        self.assertIn("error", error)
        self.assertIn("ValidationError", stderr)

    def test_boundary_exceptions_are_structured_without_a_traceback(self) -> None:
        self.invoke("init")
        cases = (
            (GatewayError("safe gateway failure"), "safe gateway failure"),
            (WorkerTransportError("safe transport failure"), "safe transport failure"),
            (
                InferenceConnectorError("safe connector failure"),
                "safe connector failure",
            ),
            (sqlite3.DatabaseError("PRIVATE SQL DETAIL"), "database operation failed"),
            (
                UnicodeDecodeError("utf-8", b"\xff", 0, 1, "PRIVATE BYTE DETAIL"),
                "text decoding failed",
            ),
        )
        for error, public_message in cases:
            with (
                self.subTest(error=type(error).__name__),
                mock.patch("magic_glass_box.cli._dispatch", side_effect=error),
            ):
                status, payload, stderr = self.invoke("status")
                self.assertEqual(status, 2)
                self.assertEqual(payload["error"]["message"], public_message)
                self.assertNotIn("Traceback", stderr)
                self.assertNotIn("PRIVATE", stderr)

    def test_all_administrative_routes_dispatch(self) -> None:
        context = Path(self.temporary.name) / "context.json"
        context.write_text('{"purpose":"Build Magic Glass Box"}', encoding="utf-8")
        spec = Path(self.temporary.name) / "profile.json"
        spec.write_text(
            '{"image":"agent@sha256:abc","command":["agent"]}',
            encoding="utf-8",
        )
        work_item = Path(self.temporary.name) / "work-item.json"
        work_item.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "source": "github",
                    "external_id": "psyberone/magic-glass-box#42",
                    "source_updated_at": "2026-10-09T12:00:00Z",
                    "title": "Import work items",
                    "brief": "Add a forge-neutral upsert boundary.",
                }
            ),
            encoding="utf-8",
        )
        supervision_policy = Path(self.temporary.name) / "supervision.json"
        supervision_policy.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "auto_execute": True,
                    "retry_backoff_seconds": [5, 30, 300],
                    "interaction_escalation_seconds": [1800, 7200],
                    "attempt_idle_escalation_seconds": [1800],
                    "attempt_lease_expired_escalation_seconds": [0],
                    "other_attention_escalation_seconds": [3600],
                }
            ),
            encoding="utf-8",
        )
        parsed_supervision_policy = SupervisionPolicy.from_mapping(
            json.loads(supervision_policy.read_text(encoding="utf-8"))
        )
        fake = FakeControlPlane()
        cases: tuple[
            tuple[
                str,
                list[str],
                tuple[str, dict[str, object]],
                dict[str, object],
            ],
            ...,
        ] = (
            ("status", ["status"], ("status", {}), {"operation": "status"}),
            (
                "control-room.show",
                [
                    "control-room",
                    "show",
                    "--history-limit",
                    "17",
                    "--refresh-krab",
                ],
                (
                    "control-room.show",
                    {"history_limit": 17, "refresh_krab": True},
                ),
                {"operation": "control-room.show"},
            ),
            (
                "krab.instance-add",
                ["krab", "instance-add", "local", "--url", "http://localhost:8765"],
                (
                    "krab.instance-add",
                    {"name": "local", "url": "http://localhost:8765"},
                ),
                {"operation": "krab.instance-add"},
            ),
            (
                "krab.identity-add",
                [
                    "krab",
                    "identity-add",
                    "local",
                    "--token-file",
                    "/private/token",
                    "--local-user",
                    "operator",
                ],
                (
                    "krab.identity-add",
                    {
                        "instance_name": "local",
                        "credential_path": Path("/private/token"),
                        "local_user_handle": "operator",
                    },
                ),
                {"operation": "krab.identity-add"},
            ),
            (
                "krab.activity",
                [
                    "krab",
                    "activity",
                    "local",
                    "box-project",
                    "--as-user",
                    "agent_one",
                    "--after",
                    "7",
                    "--limit",
                    "23",
                    "--wait",
                    "9",
                ],
                (
                    "krab.activity",
                    {
                        "instance_name": "local",
                        "krab_handle": "agent_one",
                        "project_key": "box-project",
                        "after": 7,
                        "limit": 23,
                        "wait": 9,
                    },
                ),
                {
                    "after": 0,
                    "events": [],
                    "next_cursor": 0,
                    "project_key": "box-project",
                },
            ),
            (
                "project.add",
                [
                    "project",
                    "add",
                    "box-project",
                    "--instance",
                    "local",
                    "--as-user",
                    "agent_one",
                    "--name",
                    "Magic Glass Box",
                    "--context",
                    str(context),
                    "--created-by",
                    "operator",
                ],
                (
                    "project.add",
                    {
                        "instance_name": "local",
                        "krab_handle": "agent_one",
                        "project_key": "box-project",
                        "display_name": "Magic Glass Box",
                        "context": ProjectContext.from_mapping(
                            {"purpose": "Build Magic Glass Box"}
                        ),
                        "created_by_handle": "operator",
                    },
                ),
                {"operation": "project.add"},
            ),
            (
                "project.revise",
                [
                    "project",
                    "revise",
                    "box-project",
                    "--context",
                    str(context),
                    "--created-by",
                    "operator",
                ],
                (
                    "project.revise",
                    {
                        "project_key": "box-project",
                        "context": ProjectContext.from_mapping(
                            {"purpose": "Build Magic Glass Box"}
                        ),
                        "created_by_handle": "operator",
                    },
                ),
                {"operation": "project.revise"},
            ),
            (
                "profile.add",
                [
                    "profile",
                    "add",
                    "Worker",
                    "--instance",
                    "local",
                    "--krab-user",
                    "agent_one",
                    "--spec",
                    str(spec),
                    "--created-by",
                    "operator",
                ],
                (
                    "profile.add",
                    {
                        "name": "Worker",
                        "instance_name": "local",
                        "krab_handle": "agent_one",
                        "spec": AgentProfileSpec.from_mapping(
                            {"image": "agent@sha256:abc", "command": ["agent"]}
                        ),
                        "created_by_handle": "operator",
                    },
                ),
                {"operation": "profile.add"},
            ),
            (
                "profile.revise",
                [
                    "profile",
                    "revise",
                    "Worker",
                    "--instance",
                    "local",
                    "--krab-user",
                    "agent_one",
                    "--spec",
                    str(spec),
                    "--created-by",
                    "operator",
                ],
                (
                    "profile.revise",
                    {
                        "name": "Worker",
                        "instance_name": "local",
                        "krab_handle": "agent_one",
                        "spec": AgentProfileSpec.from_mapping(
                            {"image": "agent@sha256:abc", "command": ["agent"]}
                        ),
                        "created_by_handle": "operator",
                    },
                ),
                {"operation": "profile.revise"},
            ),
            (
                "provider.show",
                ["provider", "show", "openai-main"],
                ("provider.show", {"name": "openai-main"}),
                {"operation": "provider.show", "name": "openai-main"},
            ),
            (
                "provider.revoke",
                [
                    "provider",
                    "revoke",
                    "openai-main",
                    "--expected-version",
                    "1",
                    "--revoked-by",
                    "operator",
                ],
                (
                    "provider.revoke",
                    {
                        "name": "openai-main",
                        "expected_version": 1,
                        "revoked_by_handle": "operator",
                    },
                ),
                {"operation": "provider.revoke"},
            ),
            (
                "work-item.import",
                [
                    "work-item",
                    "import",
                    str(work_item),
                    "--project",
                    "box-project",
                    "--imported-by",
                    "operator",
                ],
                (
                    "work-item.import",
                    {
                        "project_key": "box-project",
                        "envelope": WorkItemEnvelope.from_mapping(
                            json.loads(work_item.read_text(encoding="utf-8"))
                        ),
                        "imported_by_handle": "operator",
                    },
                ),
                {"operation": "work-item.import"},
            ),
            (
                "work-item.show",
                ["work-item", "show", "wki_" + "6" * 32],
                (
                    "work-item.show",
                    {"work_item_id": "wki_" + "6" * 32},
                ),
                {"operation": "work-item.show"},
            ),
            (
                "work-item.complete",
                [
                    "work-item",
                    "complete",
                    "wki_" + "6" * 32,
                    "--expected-thread-version",
                    "4",
                    "--completed-by",
                    "operator",
                    "--reason",
                    "Acceptance checks passed.",
                ],
                (
                    "work-item.complete",
                    {
                        "work_item_id": "wki_" + "6" * 32,
                        "expected_thread_version": 4,
                        "completed_by_handle": "operator",
                        "reason": "Acceptance checks passed.",
                    },
                ),
                {"operation": "work-item.complete"},
            ),
            (
                "work-item.reopen",
                [
                    "work-item",
                    "reopen",
                    "wki_" + "6" * 32,
                    "--expected-thread-version",
                    "5",
                    "--reopened-by",
                    "operator",
                ],
                (
                    "work-item.reopen",
                    {
                        "work_item_id": "wki_" + "6" * 32,
                        "expected_thread_version": 5,
                        "reopened_by_handle": "operator",
                    },
                ),
                {"operation": "work-item.reopen"},
            ),
            (
                "run.create",
                [
                    "run",
                    "create",
                    "--project",
                    "box-project",
                    "--profile",
                    "Worker",
                    "--work-item",
                    "wki_" + "6" * 32,
                    "--requested-by",
                    "operator",
                    "--source-revision",
                    "abc123",
                    "--krab-session",
                    "ses_" + "4" * 32,
                    "--krab-claim",
                    "clm_" + "5" * 32,
                    "--supervision-policy",
                    str(supervision_policy),
                ],
                (
                    "run.create",
                    {
                        "project_key": "box-project",
                        "profile_name": "Worker",
                        "work_item_id": "wki_" + "6" * 32,
                        "requested_by_handle": "operator",
                        "source_revision": "abc123",
                        "krab_session_id": "ses_" + "4" * 32,
                        "krab_claim_id": "clm_" + "5" * 32,
                        "supervision_policy": parsed_supervision_policy,
                    },
                ),
                {"operation": "run.create"},
            ),
            (
                "run.show",
                ["run", "show", "run_123"],
                ("run.show", {"run_id": "run_123"}),
                {"operation": "run.show", "run_id": "run_123"},
            ),
            (
                "run.retry",
                [
                    "run",
                    "retry",
                    "run_123",
                    "--expected-version",
                    "4",
                    "--requested-by",
                    "operator",
                ],
                (
                    "run.retry",
                    {
                        "run_id": "run_123",
                        "expected_run_version": 4,
                        "requested_by_handle": "operator",
                    },
                ),
                {"operation": "run.retry"},
            ),
            (
                "run.reassign",
                [
                    "run",
                    "reassign",
                    "run_123",
                    "--profile",
                    "Reviewer",
                    "--expected-version",
                    "5",
                    "--requested-by",
                    "operator",
                ],
                (
                    "run.reassign",
                    {
                        "run_id": "run_123",
                        "profile_name": "Reviewer",
                        "expected_run_version": 5,
                        "requested_by_handle": "operator",
                    },
                ),
                {"operation": "run.reassign"},
            ),
        )
        for expected, arguments, expected_call, expected_result in cases:
            with self.subTest(expected=expected):
                stdout = io.StringIO()
                with (
                    mock.patch("magic_glass_box.cli.ControlPlane", return_value=fake),
                    redirect_stdout(stdout),
                ):
                    status = main(["--db", str(self.database), *arguments])
                self.assertEqual(status, 0)
                self.assertEqual(json.loads(stdout.getvalue()), expected_result)
                if expected in {"status", "run.show", "provider.show"}:
                    self.assertEqual(fake.database.calls[-1], expected_call)
                else:
                    self.assertEqual(fake.calls[-1], expected_call)

    def test_parser_contract_and_activity_defaults(self) -> None:
        parser = build_parser()
        self.assertEqual(parser.prog, "magic-glass-box")
        self.assertEqual(
            parser.description, "Magic Glass Box control-plane administration"
        )
        parsed = parser.parse_args(
            [
                "--db",
                str(self.database),
                "krab",
                "activity",
                "local",
                "box-project",
                "--as-user",
                "agent_one",
            ]
        )
        self.assertEqual(parsed.db, self.database)
        self.assertEqual(parsed.after, 0)
        self.assertEqual(parsed.limit, 50)
        self.assertEqual(parsed.wait, 0)
        self.assertEqual(parsed.krab_handle, "agent_one")

        with (
            redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit) as missing_importer,
        ):
            parser.parse_args(
                [
                    "work-item",
                    "import",
                    "work-item.json",
                    "--project",
                    "box-project",
                ]
            )
        self.assertEqual(missing_importer.exception.code, 2)

        timed = parser.parse_args(
            ["run", "execute", "run_" + "1" * 32, "--run-timeout", "12.5"]
        )
        self.assertEqual(timed.run_timeout, 12.5)
        default_timed = parser.parse_args(["run", "execute", "run_" + "1" * 32])
        self.assertEqual(default_timed.run_timeout, 21_600.0)
        candidate_timed = parser.parse_args(
            [
                "candidate",
                "create",
                "att_" + "1" * 32,
                "--created-by",
                "operator",
                "--operation-timeout",
                "12.5",
            ]
        )
        self.assertEqual(candidate_timed.candidate_timeout, 12.5)
        candidate_default = parser.parse_args(["candidate", "show", "cnd_" + "2" * 32])
        self.assertEqual(candidate_default.candidate_timeout, 60.0)
        help_output = io.StringIO()
        with redirect_stdout(help_output), self.assertRaises(SystemExit) as help_exit:
            parser.parse_args(["run", "execute", "run_" + "1" * 32, "--help"])
        self.assertEqual(help_exit.exception.code, 0)
        self.assertIn(
            "maximum agent runtime in seconds (default: 21600)",
            help_output.getvalue(),
        )
        room = parser.parse_args(["control-room", "show"])
        self.assertEqual(room.history_limit, 25)
        self.assertFalse(room.refresh_krab)
        served = parser.parse_args(["control-room", "serve"])
        self.assertEqual(served.host, "127.0.0.1")
        self.assertEqual(served.port, 8766)
        self.assertEqual(served.history_limit, 25)
        supervised = parser.parse_args(["supervisor", "run"])
        self.assertEqual(supervised.poll_interval, 2.0)
        self.assertEqual(supervised.max_workers, 4)
        self.assertEqual(supervised.action_lease_seconds, 60)
        self.assertEqual(supervised.run_timeout, 21_600.0)
        self.assertIsNone(supervised.action_timeout)
        self.assertEqual(supervised.log_level, "WARNING")
        verbose_supervisor = parser.parse_args(
            ["supervisor", "run", "--log-level", "debug"]
        )
        self.assertEqual(verbose_supervisor.log_level, "DEBUG")
        for invalid_limit in ("-1", "501", "1.5"):
            with (
                self.subTest(invalid_limit=invalid_limit),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as invalid,
            ):
                parser.parse_args(
                    ["control-room", "show", "--history-limit", invalid_limit]
                )
            self.assertEqual(invalid.exception.code, 2)
        for invalid_port in ("-1", "65536", "1.5"):
            with (
                self.subTest(invalid_port=invalid_port),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as invalid,
            ):
                parser.parse_args(["control-room", "serve", "--port", invalid_port])
            self.assertEqual(invalid.exception.code, 2)
        for invalid_timeout in ("0", "-1", "nan", "inf", "not-a-number"):
            with (
                self.subTest(invalid_timeout=invalid_timeout),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as invalid,
            ):
                parser.parse_args(
                    [
                        "run",
                        "execute",
                        "run_" + "1" * 32,
                        "--run-timeout",
                        invalid_timeout,
                    ]
                )
            self.assertEqual(invalid.exception.code, 2)
            with (
                self.subTest(candidate_timeout=invalid_timeout),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as invalid_candidate,
            ):
                parser.parse_args(
                    [
                        "candidate",
                        "show",
                        "cnd_" + "2" * 32,
                        "--operation-timeout",
                        invalid_timeout,
                    ]
                )
            self.assertEqual(invalid_candidate.exception.code, 2)
        for option, invalid_values in (
            ("--max-workers", ("0", "33", "1.5")),
            ("--action-lease-seconds", ("9", "3601", "1.5")),
        ):
            for invalid_value in invalid_values:
                with (
                    self.subTest(option=option, invalid_value=invalid_value),
                    redirect_stderr(io.StringIO()),
                    self.assertRaises(SystemExit) as invalid,
                ):
                    parser.parse_args(["supervisor", "run", option, invalid_value])
                self.assertEqual(invalid.exception.code, 2)

        root = Path(self.temporary.name)
        home = root / "home"
        cases = (
            (
                {"MAGIC_GLASS_BOX_DB": str(root / "current.db")},
                root / "current.db",
            ),
            ({"HEX_DB": str(root / "legacy.db")}, root / "legacy.db"),
            (
                {
                    "MAGIC_GLASS_BOX_DB": str(root / "current.db"),
                    "HEX_DB": str(root / "legacy.db"),
                },
                root / "current.db",
            ),
            (
                {"MAGIC_GLASS_BOX_STATE_DIR": str(root / "current-state")},
                root / "current-state" / "control-plane.db",
            ),
            (
                {"HEX_STATE_DIR": str(root / "legacy-state")},
                root / "legacy-state" / "hex.db",
            ),
            (
                {"XDG_STATE_HOME": str(root / "xdg")},
                root / "xdg" / "magic-glass-box" / "control-plane.db",
            ),
            (
                {},
                home / ".local" / "state" / "magic-glass-box" / "control-plane.db",
            ),
        )
        for environment, expected in cases:
            isolated_environment = dict(environment)
            mutant = os.environ.get("MUTANT_UNDER_TEST")
            if mutant is not None:
                isolated_environment["MUTANT_UNDER_TEST"] = mutant
            with (
                self.subTest(environment=environment),
                mock.patch.dict(os.environ, isolated_environment, clear=True),
                mock.patch("magic_glass_box.storage.core.Path.home", return_value=home),
            ):
                parsed = build_parser().parse_args(["status"])
            self.assertEqual(parsed.db, expected)

    def test_interaction_answer_dispatches_only_redacted_confirmation(self) -> None:
        interaction_id = "int_" + "1" * 32
        response_item_id = "itm_" + "2" * 32
        fake = FakeControlPlane()
        stdout = io.StringIO()

        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=fake),
            redirect_stdout(stdout),
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "interaction",
                    "answer",
                    interaction_id,
                    "--response-item",
                    response_item_id,
                    "--expected-version",
                    "1",
                ]
            )

        self.assertEqual(status, 0)
        self.assertEqual(
            fake.calls[-1],
            (
                "interaction.answer",
                {
                    "interaction_id": interaction_id,
                    "response_item_id": response_item_id,
                    "expected_version": 1,
                },
            ),
        )
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "interaction_id": interaction_id,
                "interaction_state": "answered",
                "interaction_version": 2,
                "response_item_id": response_item_id,
                "run_id": "run_" + "2" * 32,
                "run_state": "queued",
                "run_version": 5,
            },
        )
        self.assertNotIn("PRIVATE RESPONSE", stdout.getvalue())

    def test_command_tree_help_and_root_db_option_are_an_explicit_contract(
        self,
    ) -> None:
        parser = build_parser()

        def command_help(
            current: argparse.ArgumentParser,
        ) -> tuple[dict[str, str | None], dict[str, argparse.ArgumentParser]]:
            subcommands = next(
                (
                    action
                    for action in current._actions
                    if isinstance(action, argparse._SubParsersAction)
                ),
                None,
            )
            if subcommands is None:
                return {}, {}
            helps = {
                choice.dest: choice.help for choice in subcommands._choices_actions
            }
            return helps, dict(subcommands.choices)

        root_help, root_commands = command_help(parser)
        self.assertEqual(
            root_help,
            {
                "init": "initialize the private state database",
                "status": "validate state and show object counts",
                "control-room": "inspect redacted operational state",
                "user": "manage local users",
                "krab": "manage and query Krab connections",
                "project": "manage project context",
                "profile": "manage agent profiles",
                "inference-connector": (
                    "manage credential-isolated inference connectors"
                ),
                "service-sidecar": (
                    "manage provider-neutral service sidecars and operations"
                ),
                "capability": ("bind project capabilities to explicit controllers"),
                "workflow": "manage bounded workflow prescriptions",
                "provider": "manage scoped provider grants",
                "work-item": "manage durable work items",
                "interaction": "resolve durable agent interactions",
                "run": "manage agent runs",
                "supervisor": "run the autonomous control loop",
                "result": "inspect and apply retained work",
                "candidate": "materialize and inspect trusted candidates",
                "gate": "manage exact-input integration gates",
                "integration": "order, refold, and land exact candidates",
            },
        )
        expected_nested = {
            "control-room": {
                "show": "show one consistent redacted snapshot",
                "serve": "serve the interactive loopback control room",
            },
            "user": {"add": "add a local user"},
            "krab": {
                "instance-add": "add a Krab origin",
                "identity-add": "verify and retain a Krab credential reference",
                "activity": "read metadata-only project activity",
            },
            "project": {
                "add": "verify and register a visible Krab project",
                "revise": "append an immutable project-context revision",
            },
            "profile": {
                "add": "create a versioned agent profile",
                "revise": "append an immutable profile revision",
            },
            "workflow": {
                "config-create": "create a versioned workflow configuration",
                "config-revise": "append an immutable configuration revision",
                "config-show": "show one workflow configuration revision",
                "config-list": "list latest workflow configurations",
                "start": "start one exact pinned workflow execution",
                "show": "show one workflow execution and event stream",
                "list": "list bounded workflow executions",
                "cancel": "cancel one active workflow execution",
                "ruling-show": "show one durable workflow ruling",
                "ruling-resolve": "resolve one durable workflow ruling",
            },
            "provider": {
                "connector-add": "add a connector-bound provider grant",
                "show": "show provider policy and lifecycle metadata",
                "revoke": "revoke a provider grant",
            },
            "inference-connector": {
                "register": "register immutable connector authority",
                "show": "show connector authority and lifecycle metadata",
                "list": "list connector authority and lifecycle metadata",
                "revoke": "revoke an inference connector",
            },
            "service-sidecar": {
                "register": "register immutable service-sidecar authority",
                "show": "show service-sidecar authority and lifecycle metadata",
                "list": "list service-sidecar authority and lifecycle metadata",
                "revoke": "revoke a service sidecar",
                "operation-create": "create one exact durable service operation",
                "operation-show": "show one durable service operation",
                "operation-list": "list recent durable service operations",
                "operation-execute": "invoke one authorized service operation",
                "operation-reconcile": ("reconcile one uncertain service operation"),
            },
            "capability": {
                "bind": "bind one project capability to a controller",
                "show": "show one project capability binding",
                "list": "list project capability bindings",
                "unbind": "disable one project capability binding",
            },
            "work-item": {
                "import": "create or update a normalized work item",
                "show": "show work-item lifecycle metadata",
                "complete": "seal completed work memory",
                "reopen": "start a fresh work-memory generation",
            },
            "interaction": {
                "answer": "bind one explicit Krab response item",
            },
            "run": {
                "create": "create a pinned run record",
                "cancel": "request stop for a queued or active run",
                "retry": ("create an audited successor with the same pinned profile"),
                "reassign": "create an audited successor for another profile",
                "show": "show a run and its event stream",
                "execute": "execute one queued run in its pinned sandbox",
                "reconcile": (
                    "clean and fail runs interrupted by an orchestrator restart"
                ),
            },
            "supervisor": {"run": "advance opted-in work until stopped"},
            "result": {
                "show": "verify and show one retained attempt result",
                "apply": "apply one retained result after conflict preflight",
            },
            "candidate": {
                "create": "materialize one retained result as a candidate",
                "show": "verify and show one trusted candidate",
                "list": "list recent durable candidates without reading Git objects",
            },
            "gate": {
                "policy-create": "pin a gate policy to one exact candidate",
                "policy-show": "show one immutable gate policy",
                "policy-list": "list recent immutable gate policies",
                "review-create": "create a candidate-backed independent review run",
                "review-record": "record independent-review evidence",
                "maintainer-record": (
                    "record an authorized product disposition for an exact candidate"
                ),
                "maintainer-list": "list append-only maintainer dispositions",
                "changes-create": (
                    "return a changes-requested candidate and feedback to a "
                    "developer run"
                ),
                "validate": "run one pinned local validation gate",
                "evidence-list": "list append-only gate evidence",
                "readiness": "derive current exact-input readiness",
            },
            "integration": {
                "dependency-add": "add a hard work-item landing dependency",
                "dependency-remove": "remove a hard landing dependency",
                "dependency-list": "list durable work-item landing dependencies",
                "enqueue": "queue one gate policy for its integration ref",
                "show": "show one integration queue item and its event stream",
                "list": "list durable integration queue items",
                "refold": (
                    "replay an original result onto the current integration base"
                ),
                "activate": ("bind a fresh folded policy and return the item to queue"),
                "claim": "lease one ready item on its serialized ref lane",
                "release": "release a claimed item before authority is issued",
                "cancel": "cancel an item before landing authority is issued",
                "plan": "issue an exact argv-only manual landing plan",
                "plan-show": "show one issued plan without its raw nonce",
                "plan-reissue": (
                    "replace a lost plan nonce without releasing its lane"
                ),
                "receipt": "record one nonce-bound landed or not-landed outcome",
                "provider-issue": (
                    "issue exact landing authority to the bound provider"
                ),
                "provider-show": (
                    "show one provider landing and its service operation"
                ),
                "provider-execute": "execute and settle one provider landing",
                "provider-settle": ("settle one externally completed provider landing"),
            },
        }
        for command, expected_help in expected_nested.items():
            with self.subTest(command=command):
                nested_help, _ = command_help(root_commands[command])
                self.assertEqual(nested_help, expected_help)

        db_action = next(action for action in parser._actions if action.dest == "db")
        self.assertEqual(db_action.option_strings, ["--db"])
        self.assertIs(db_action.type, Path)
        self.assertIsInstance(db_action.default, Path)
        self.assertEqual(db_action.help, "control-plane database path")

    def test_workflow_commands_parse_documents_and_dispatch_exact_authority(
        self,
    ) -> None:
        policy_path = Path(self.temporary.name) / "workflow.json"
        policy = {
            "schema_version": 1,
            "review_separation": "different_profile_and_identity",
            "maintainer_approval": True,
            "validations": [],
            "max_correction_rounds": 3,
            "landing_mode": "manual",
        }
        policy_path.write_text(json.dumps(policy), encoding="utf-8")
        expected_spec = WorkflowConfigurationSpec.from_mapping(policy)
        fake = FakeControlPlane()
        cases = (
            (
                [
                    "workflow",
                    "config-create",
                    "default",
                    "--project",
                    "box-project",
                    "--producer",
                    "Producer",
                    "--reviewer",
                    "Reviewer",
                    "--spec",
                    str(policy_path),
                    "--created-by",
                    "operator",
                ],
                (
                    "workflow.config-create",
                    {
                        "project_key": "box-project",
                        "name": "default",
                        "spec": expected_spec,
                        "producer_profile_name": "Producer",
                        "reviewer_profile_name": "Reviewer",
                        "created_by_handle": "operator",
                    },
                ),
            ),
            (
                [
                    "workflow",
                    "config-revise",
                    "default",
                    "--project",
                    "box-project",
                    "--producer",
                    "Producer",
                    "--reviewer",
                    "Reviewer",
                    "--spec",
                    str(policy_path),
                    "--created-by",
                    "operator",
                ],
                (
                    "workflow.config-revise",
                    {
                        "project_key": "box-project",
                        "name": "default",
                        "spec": expected_spec,
                        "producer_profile_name": "Producer",
                        "reviewer_profile_name": "Reviewer",
                        "created_by_handle": "operator",
                    },
                ),
            ),
            (
                [
                    "workflow",
                    "config-show",
                    "default",
                    "--project",
                    "box-project",
                    "--version",
                    "2",
                ],
                (
                    "workflow.config-show",
                    {
                        "project_key": "box-project",
                        "name": "default",
                        "version": 2,
                    },
                ),
            ),
            (
                ["workflow", "config-list", "--project", "box-project"],
                ("workflow.config-list", {"project_key": "box-project"}),
            ),
            (
                [
                    "workflow",
                    "start",
                    "default",
                    "--project",
                    "box-project",
                    "--work-item",
                    "wki_" + "1" * 32,
                    "--source-revision",
                    "a" * 40,
                    "--expected-configuration-version",
                    "2",
                    "--expected-context-version",
                    "ctx_" + "2" * 32,
                    "--expected-work-item-version",
                    "wiv_" + "3" * 32,
                    "--expected-thread-generation",
                    "1",
                    "--expected-thread-version",
                    "4",
                    "--idempotency-key",
                    "workflow-start-1",
                    "--requested-by",
                    "operator",
                ],
                (
                    "workflow.start",
                    {
                        "project_key": "box-project",
                        "configuration_name": "default",
                        "work_item_id": "wki_" + "1" * 32,
                        "source_revision": "a" * 40,
                        "expected_configuration_version": 2,
                        "expected_context_version_id": "ctx_" + "2" * 32,
                        "expected_work_item_version_id": "wiv_" + "3" * 32,
                        "expected_thread_generation": 1,
                        "expected_thread_version": 4,
                        "idempotency_key": "workflow-start-1",
                        "requested_by_handle": "operator",
                    },
                ),
            ),
            (
                ["workflow", "show", "wfx_" + "4" * 32],
                (
                    "workflow.show",
                    {"execution_id": "wfx_" + "4" * 32},
                ),
            ),
            (
                [
                    "workflow",
                    "list",
                    "--project",
                    "box-project",
                    "--state",
                    "cancelled",
                    "--limit",
                    "25",
                ],
                (
                    "workflow.list",
                    {
                        "project_key": "box-project",
                        "state": "cancelled",
                        "limit": 25,
                    },
                ),
            ),
            (
                [
                    "workflow",
                    "cancel",
                    "wfx_" + "4" * 32,
                    "--expected-version",
                    "1",
                    "--cancelled-by",
                    "operator",
                    "--reason",
                    "stop now",
                ],
                (
                    "workflow.cancel",
                    {
                        "execution_id": "wfx_" + "4" * 32,
                        "expected_version": 1,
                        "cancelled_by_handle": "operator",
                        "reason": "stop now",
                    },
                ),
            ),
            (
                ["workflow", "ruling-show", "wri_" + "5" * 32],
                (
                    "workflow.ruling-show",
                    {"ruling_id": "wri_" + "5" * 32},
                ),
            ),
            (
                [
                    "workflow",
                    "ruling-resolve",
                    "wri_" + "5" * 32,
                    "--expected-version",
                    "1",
                    "--decision",
                    "continue",
                    "--resolved-by",
                    "operator",
                ],
                (
                    "workflow.ruling-resolve",
                    {
                        "ruling_id": "wri_" + "5" * 32,
                        "expected_version": 1,
                        "decision": "continue",
                        "resolved_by_handle": "operator",
                    },
                ),
            ),
        )
        for arguments, expected_call in cases:
            with self.subTest(arguments=arguments):
                stdout = io.StringIO()
                with (
                    mock.patch("magic_glass_box.cli.ControlPlane", return_value=fake),
                    redirect_stdout(stdout),
                ):
                    status = main(["--db", str(self.database), *arguments])
                self.assertEqual(status, 0)
                self.assertEqual(fake.calls[-1], expected_call)

    def test_execution_cancellation_and_reconciliation_use_the_coordinator(
        self,
    ) -> None:
        control = FakeControlPlane()
        coordinator = FakeRunCoordinator()
        result_store = FakeResultStore()
        cases = (
            (
                [
                    "run",
                    "execute",
                    "run_" + "1" * 32,
                    "--runtime-adapter",
                    "codex",
                ],
                (
                    "execute",
                    {
                        "run_id": "run_" + "1" * 32,
                        "runtime_adapter": "codex",
                    },
                ),
                {"operation": "run.execute", "run_id": "run_" + "1" * 32},
            ),
            (
                ["run", "reconcile"],
                ("reconcile", {}),
                [{"operation": "run.reconcile"}],
            ),
            (
                [
                    "run",
                    "cancel",
                    "run_" + "2" * 32,
                    "--expected-version",
                    "3",
                    "--requested-by",
                    "operator",
                    "--reason",
                    "operator requested stop",
                ],
                ("reconcile", "run_" + "2" * 32),
                {"operation": "run.show", "run_id": "run_" + "2" * 32},
            ),
        )
        for arguments, expected_call, expected_output in cases:
            with self.subTest(arguments=arguments):
                stdout = io.StringIO()
                with (
                    mock.patch(
                        "magic_glass_box.cli.ControlPlane", return_value=control
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_run_coordinator",
                        return_value=coordinator,
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_result_store",
                        return_value=result_store,
                    ),
                    redirect_stdout(stdout),
                ):
                    status = main(["--db", str(self.database), *arguments])
                self.assertEqual(status, 0)
                self.assertEqual(coordinator.calls[-1], expected_call)
                self.assertEqual(json.loads(stdout.getvalue()), expected_output)

        self.assertEqual(
            control.calls[-1],
            (
                "run.cancel",
                {
                    "run_id": "run_" + "2" * 32,
                    "requested_by_handle": "operator",
                    "expected_run_version": 3,
                    "reason": "operator requested stop",
                },
            ),
        )

    def test_queued_run_cancellation_does_not_construct_docker_runtime(self) -> None:
        class QueuedRunControl(FakeControlPlane):
            def request_run_cancellation(
                self, run_id: str, **kwargs: object
            ) -> dict[str, object]:
                self.calls.append(("run.cancel", {"run_id": run_id, **kwargs}))
                return {"id": run_id, "state": "cancelled", "version": 4}

        control = QueuedRunControl()
        stdout = io.StringIO()
        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            mock.patch(
                "magic_glass_box.cli._build_result_store",
                side_effect=AssertionError("result store must remain cold"),
            ),
            mock.patch(
                "magic_glass_box.cli._build_run_coordinator",
                side_effect=AssertionError("Docker must remain cold"),
            ),
            redirect_stdout(stdout),
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "run",
                    "cancel",
                    "run_" + "2" * 32,
                    "--expected-version",
                    "3",
                    "--requested-by",
                    "operator",
                    "--reason",
                    "no longer needed",
                ]
            )

        self.assertEqual(status, 0)
        self.assertEqual(json.loads(stdout.getvalue())["state"], "cancelled")

    def test_control_room_server_prints_the_private_url_and_closes_cleanly(
        self,
    ) -> None:
        class FakeServer:
            control_url = "http://127.0.0.1:8766/private-token/"
            origin = "http://127.0.0.1:8766"

            def __init__(self) -> None:
                self.closed = False

            def serve_forever(self) -> None:
                raise KeyboardInterrupt

            def server_close(self) -> None:
                self.closed = True

        server = FakeServer()
        args = argparse.Namespace(host="127.0.0.1", port=8766, history_limit=25)
        stdout = io.StringIO()
        with (
            mock.patch(
                "magic_glass_box.cli.build_control_room_server",
                return_value=server,
            ) as build,
            redirect_stdout(stdout),
        ):
            status = _serve_control_room(args, FakeControlPlane(), FakeRunCoordinator())

        self.assertEqual(status, 0)
        self.assertTrue(server.closed)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "binding": "http://127.0.0.1:8766",
                "control_room_url": "http://127.0.0.1:8766/private-token/",
                "network_scope": "loopback_only",
            },
        )
        self.assertEqual(build.call_args.kwargs["history_limit"], 25)

        with (
            mock.patch(
                "magic_glass_box.cli.build_control_room_server",
                side_effect=OSError("address already in use"),
            ),
            self.assertRaisesRegex(InputError, "address already in use"),
        ):
            _serve_control_room(args, FakeControlPlane(), FakeRunCoordinator())

    def test_control_room_serve_main_route_builds_one_coordinator(self) -> None:
        control = FakeControlPlane()
        coordinator = FakeRunCoordinator()
        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            mock.patch(
                "magic_glass_box.cli._build_run_coordinator",
                return_value=coordinator,
            ) as build,
            mock.patch("magic_glass_box.cli._build_result_store") as result_store,
            mock.patch(
                "magic_glass_box.cli._serve_control_room", return_value=0
            ) as serve,
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "control-room",
                    "serve",
                    "--port",
                    "0",
                    "--history-limit",
                    "3",
                ]
            )

        self.assertEqual(status, 0)
        result_store.assert_called_once_with(control)
        build.assert_called_once()
        serve.assert_called_once()
        served_args = serve.call_args.args[0]
        self.assertEqual(served_args.port, 0)
        self.assertEqual(served_args.history_limit, 3)

    def test_supervisor_service_announces_policy_and_closes_cleanly(self) -> None:
        supervisor = FakeSupervisor()
        args = argparse.Namespace(
            poll_interval=1.5,
            max_workers=3,
            action_lease_seconds=45,
            run_timeout=90.0,
            action_timeout=None,
            log_level="INFO",
        )
        control = FakeControlPlane()
        stdout = io.StringIO()
        with (
            mock.patch(
                "magic_glass_box.cli.Supervisor", return_value=supervisor
            ) as supervisor_type,
            mock.patch(
                "magic_glass_box.cli._build_workflow_director"
            ) as build_workflow_director,
            mock.patch("magic_glass_box.cli.logging.basicConfig") as configure_logs,
            redirect_stdout(stdout),
        ):
            status = _serve_supervisor(args, control, FakeRunCoordinator())

        self.assertEqual(status, 0)
        self.assertEqual(supervisor.run_calls, 1)
        self.assertEqual(supervisor.close_calls, [True])
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "action_lease_seconds": 45,
                "action_timeout_seconds": 390.0,
                "authority": "explicit_run_policy",
                "max_workers": 3,
                "log_level": "INFO",
                "poll_interval_seconds": 1.5,
                "supervisor_id": "supervisor-test",
            },
        )
        configure_logs.assert_called_once_with(
            level=mock.ANY,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        supervisor_type.assert_called_once_with(
            control.database,
            mock.ANY,
            control,
            workflow_director=build_workflow_director.return_value,
            poll_interval=1.5,
            max_workers=3,
            action_lease_seconds=45,
            action_timeout=390.0,
        )

    def test_sigterm_unwinds_a_service_and_restores_the_previous_handler(self) -> None:
        previous = mock.sentinel.previous_handler
        installed: list[Any] = []

        def install(_signal: int, handler: object) -> None:
            installed.append(handler)

        with (
            mock.patch(
                "magic_glass_box.cli.signal.getsignal", return_value=previous
            ) as get_handler,
            mock.patch("magic_glass_box.cli.signal.signal", side_effect=install),
            self.assertRaises(_TerminationRequested),
            _handle_sigterm(),
        ):
            installed[0](signal.SIGTERM, None)

        get_handler.assert_called_once_with(signal.SIGTERM)
        self.assertEqual(len(installed), 2)
        self.assertIs(installed[1], previous)

    def test_supervisor_main_route_builds_one_coordinator(self) -> None:
        control = FakeControlPlane()
        coordinator = FakeRunCoordinator()
        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            mock.patch(
                "magic_glass_box.cli._build_run_coordinator",
                return_value=coordinator,
            ) as build,
            mock.patch("magic_glass_box.cli._build_result_store") as result_store,
            mock.patch(
                "magic_glass_box.cli._serve_supervisor", return_value=0
            ) as serve,
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "supervisor",
                    "run",
                    "--poll-interval",
                    "0.5",
                    "--max-workers",
                    "2",
                    "--action-lease-seconds",
                    "30",
                    "--run-timeout",
                    "90",
                    "--action-timeout",
                    "120",
                ]
            )

        self.assertEqual(status, 0)
        result_store.assert_called_once_with(control)
        build.assert_called_once_with(
            control,
            run_timeout=90.0,
            result_store=result_store.return_value,
        )
        serve.assert_called_once_with(mock.ANY, control, coordinator)
        self.assertEqual(serve.call_args.args[0].action_timeout, 120.0)

    def test_supervisor_action_timeout_must_exceed_the_agent_runtime(self) -> None:
        args = argparse.Namespace(
            poll_interval=1.5,
            max_workers=3,
            action_lease_seconds=45,
            run_timeout=90.0,
            action_timeout=90.0,
            log_level="WARNING",
        )

        with self.assertRaisesRegex(InputError, "must exceed --run-timeout"):
            _serve_supervisor(args, FakeControlPlane(), FakeRunCoordinator())

    def test_result_routes_verify_database_binding_before_show_or_apply(self) -> None:
        control = FakeControlPlane()
        result_store = FakeResultStore()
        attempt_id = "att_" + "3" * 32
        cases = (
            (
                ["result", "show", attempt_id],
                (
                    "inspect",
                    {"attempt_id": attempt_id, "expected_digest": "a" * 64},
                ),
                {
                    "attempt_id": attempt_id,
                    "content_sha256": "a" * 64,
                    "operation": "result.show",
                },
            ),
            (
                [
                    "result",
                    "apply",
                    attempt_id,
                    "--repository",
                    str(self.database.parent),
                ],
                (
                    "apply",
                    {
                        "attempt_id": attempt_id,
                        "repository": self.database.parent,
                        "expected_digest": "a" * 64,
                    },
                ),
                {"operation": "result.apply", "attempt_id": attempt_id},
            ),
        )
        for arguments, expected_call, expected_output in cases:
            with self.subTest(arguments=arguments):
                stdout = io.StringIO()
                with (
                    mock.patch(
                        "magic_glass_box.cli.ControlPlane", return_value=control
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_result_store",
                        return_value=result_store,
                    ),
                    redirect_stdout(stdout),
                ):
                    status = main(["--db", str(self.database), *arguments])
                self.assertEqual(status, 0)
                self.assertEqual(result_store.calls[-1], expected_call)
                self.assertEqual(json.loads(stdout.getvalue()), expected_output)
                self.assertEqual(
                    control.database.calls[-1],
                    ("result.show", {"attempt_id": attempt_id}),
                )

    def test_candidate_routes_use_both_verified_object_stores(self) -> None:
        control = FakeControlPlane()
        result_store = FakeResultStore()
        candidate_store = FakeCandidateStore()
        attempt_id = "att_" + "3" * 32
        candidate_id = "cnd_" + "4" * 32
        cases = (
            (
                [
                    "candidate",
                    "create",
                    attempt_id,
                    "--created-by",
                    "operator",
                    "--operation-timeout",
                    "12.5",
                ],
                (
                    "candidate.create",
                    {
                        "attempt_id": attempt_id,
                        "created_by_handle": "operator",
                        "result_store": result_store,
                        "candidate_store": candidate_store,
                    },
                ),
            ),
            (
                ["candidate", "show", candidate_id],
                (
                    "candidate.show",
                    {
                        "candidate_id": candidate_id,
                        "result_store": result_store,
                        "candidate_store": candidate_store,
                    },
                ),
            ),
        )
        for arguments, expected_call in cases:
            with self.subTest(arguments=arguments):
                stdout = io.StringIO()
                with (
                    mock.patch(
                        "magic_glass_box.cli.ControlPlane", return_value=control
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_result_store",
                        return_value=result_store,
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_candidate_store",
                        return_value=candidate_store,
                    ) as build_candidate_store,
                    redirect_stdout(stdout),
                ):
                    status = main(["--db", str(self.database), *arguments])
                self.assertEqual(status, 0)
                expected_timeout = 12.5 if arguments[1] == "create" else 60.0
                build_candidate_store.assert_called_once_with(
                    control, timeout=expected_timeout
                )
                self.assertEqual(control.calls[-1], expected_call)
                self.assertEqual(
                    json.loads(stdout.getvalue())["operation"], expected_call[0]
                )

    def test_candidate_list_reads_only_the_durable_ledger(self) -> None:
        control = FakeControlPlane()
        stdout = io.StringIO()
        run_id = "run_" + "5" * 32
        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            mock.patch(
                "magic_glass_box.cli._build_result_store",
                side_effect=AssertionError("result store should stay cold"),
            ),
            mock.patch(
                "magic_glass_box.cli._build_candidate_store",
                side_effect=AssertionError("candidate cache should stay cold"),
            ),
            redirect_stdout(stdout),
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "candidate",
                    "list",
                    "--run",
                    run_id,
                    "--limit",
                    "12",
                ]
            )

        self.assertEqual(status, 0)
        self.assertEqual(
            control.calls[-1],
            (
                "candidate.list",
                {
                    "project_key": None,
                    "producer_run_id": run_id,
                    "work_thread_id": None,
                    "limit": 12,
                },
            ),
        )
        self.assertEqual(json.loads(stdout.getvalue())["operation"], "candidate.list")

    def test_gate_ledger_read_routes_do_not_open_object_stores(self) -> None:
        control = FakeControlPlane()
        policy_id = "gpl_" + "6" * 32
        candidate_id = "cnd_" + "7" * 32
        cases = (
            (
                ["gate", "policy-show", policy_id],
                ("gate.policy-show", {"policy_id": policy_id}),
            ),
            (
                [
                    "gate",
                    "policy-list",
                    "--candidate",
                    candidate_id,
                    "--limit",
                    "17",
                ],
                (
                    "gate.policy-list",
                    {
                        "candidate_id": candidate_id,
                        "project_key": None,
                        "work_thread_id": None,
                        "limit": 17,
                    },
                ),
            ),
            (
                ["gate", "evidence-list", policy_id, "--limit", "19"],
                (
                    "gate.evidence-list",
                    {"policy_id": policy_id, "limit": 19},
                ),
            ),
            (
                ["gate", "maintainer-list", policy_id, "--limit", "23"],
                (
                    "gate.maintainer-list",
                    {"policy_id": policy_id, "limit": 23},
                ),
            ),
            (
                ["gate", "readiness", policy_id],
                ("gate.readiness", {"policy_id": policy_id}),
            ),
        )
        for arguments, expected in cases:
            with self.subTest(arguments=arguments):
                stdout = io.StringIO()
                with (
                    mock.patch(
                        "magic_glass_box.cli.ControlPlane", return_value=control
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_result_store",
                        side_effect=AssertionError("result store should stay cold"),
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_candidate_store",
                        side_effect=AssertionError("candidate store should stay cold"),
                    ),
                    mock.patch(
                        "magic_glass_box.cli.DockerCLI",
                        side_effect=AssertionError("Docker should stay cold"),
                    ),
                    redirect_stdout(stdout),
                ):
                    status = main(["--db", str(self.database), *arguments])
                self.assertEqual(status, 0)
                self.assertEqual(control.calls[-1], expected)
                self.assertEqual(
                    json.loads(stdout.getvalue())["operation"], expected[0]
                )

    def test_gate_policy_create_and_evidence_write_routes_wire_authority(self) -> None:
        control = FakeControlPlane()
        candidate_id = "cnd_" + "7" * 32
        policy_id = "gpl_" + "8" * 32
        reviewer_attempt = "att_" + "9" * 32
        policy_path = Path(self.temporary.name) / "gate-policy.json"
        policy_document = {
            "schema_version": 1,
            "review_separation": "different_profile_and_identity",
            "validations": [
                {
                    "name": "unit",
                    "image": "validator:local",
                    "command": ["python", "-m", "unittest"],
                }
            ],
        }
        policy_path.write_text(json.dumps(policy_document), encoding="utf-8")
        image_engine = mock.Mock()

        stdout = io.StringIO()
        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            mock.patch("magic_glass_box.cli.DockerCLI", return_value=image_engine),
            redirect_stdout(stdout),
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "gate",
                    "policy-create",
                    candidate_id,
                    "--policy",
                    str(policy_path),
                    "--created-by",
                    "operator",
                ]
            )
            _, routed_arguments = control.calls[-1]
            resolved = routed_arguments["image_resolver"]("validator:local")
        self.assertEqual(status, 0)
        operation, arguments = control.calls[-1]
        self.assertEqual(operation, "gate.policy-create")
        self.assertEqual(arguments["candidate_id"], candidate_id)
        self.assertEqual(arguments["policy"], policy_document)
        self.assertEqual(arguments["created_by_handle"], "operator")
        self.assertIs(resolved, image_engine.inspect_image.return_value)
        image_engine.inspect_image.assert_called_once_with("validator:local")

        stdout = io.StringIO()
        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            redirect_stdout(stdout),
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "gate",
                    "review-create",
                    policy_id,
                    "--profile",
                    "Reviewer",
                    "--requested-by",
                    "operator",
                    "--expected-policy-revision",
                    "a" * 64,
                ]
            )
        self.assertEqual(status, 0)
        self.assertEqual(
            control.calls[-1],
            (
                "gate.review-create",
                {
                    "policy_id": policy_id,
                    "profile_name": "Reviewer",
                    "requested_by_handle": "operator",
                    "expected_policy_revision": "a" * 64,
                    "krab_session_id": None,
                    "krab_claim_id": None,
                    "supervision_policy": None,
                },
            ),
        )

        maintainer_review = {
            "schema_version": 1,
            "decision": "changes_requested",
            "summary": "Changes requested, not rejected",
            "details": ["Publish the JSON output schema."],
        }
        maintainer_path = Path(self.temporary.name) / "maintainer-review.json"
        maintainer_path.write_text(json.dumps(maintainer_review), encoding="utf-8")
        stdout = io.StringIO()
        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            redirect_stdout(stdout),
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "gate",
                    "maintainer-record",
                    policy_id,
                    "--review",
                    str(maintainer_path),
                    "--recorded-by",
                    "operator",
                    "--expected-policy-revision",
                    "a" * 64,
                ]
            )
        self.assertEqual(status, 0)
        self.assertEqual(
            control.calls[-1],
            (
                "gate.maintainer-record",
                {
                    "policy_id": policy_id,
                    "review_request": maintainer_review,
                    "recorded_by_handle": "operator",
                    "expected_policy_revision": "a" * 64,
                },
            ),
        )

        stdout = io.StringIO()
        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            redirect_stdout(stdout),
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "gate",
                    "changes-create",
                    policy_id,
                    "--maintainer-review",
                    "mrv_" + "c" * 32,
                    "--profile",
                    "Producer",
                    "--requested-by",
                    "operator",
                    "--expected-policy-revision",
                    "a" * 64,
                ]
            )
        self.assertEqual(status, 0)
        self.assertEqual(
            control.calls[-1],
            (
                "gate.changes-create",
                {
                    "policy_id": policy_id,
                    "review_evidence_id": None,
                    "review_entry_id": None,
                    "maintainer_review_id": "mrv_" + "c" * 32,
                    "profile_name": "Producer",
                    "requested_by_handle": "operator",
                    "expected_policy_revision": "a" * 64,
                    "krab_session_id": None,
                    "krab_claim_id": None,
                    "supervision_policy": None,
                },
            ),
        )

        stdout = io.StringIO()
        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            redirect_stdout(stdout),
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "gate",
                    "review-record",
                    policy_id,
                    "--reviewer-attempt",
                    reviewer_attempt,
                    "--outcome",
                    "passed",
                    "--recorded-by",
                    "operator",
                    "--expected-policy-revision",
                    "a" * 64,
                ]
            )
        self.assertEqual(status, 0)
        self.assertEqual(
            control.calls[-1],
            (
                "gate.review-record",
                {
                    "policy_id": policy_id,
                    "reviewer_attempt_id": reviewer_attempt,
                    "outcome": "passed",
                    "recorded_by_handle": "operator",
                    "expected_policy_revision": "a" * 64,
                },
            ),
        )

        stdout = io.StringIO()
        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            redirect_stdout(stdout),
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "gate",
                    "changes-create",
                    policy_id,
                    "--review-evidence",
                    "gev_" + "a" * 32,
                    "--review-entry",
                    "wle_" + "b" * 32,
                    "--profile",
                    "Producer",
                    "--requested-by",
                    "operator",
                    "--expected-policy-revision",
                    "a" * 64,
                ]
            )
        self.assertEqual(status, 0)
        self.assertEqual(
            control.calls[-1],
            (
                "gate.changes-create",
                {
                    "policy_id": policy_id,
                    "review_evidence_id": "gev_" + "a" * 32,
                    "review_entry_id": "wle_" + "b" * 32,
                    "maintainer_review_id": None,
                    "profile_name": "Producer",
                    "requested_by_handle": "operator",
                    "expected_policy_revision": "a" * 64,
                    "krab_session_id": None,
                    "krab_claim_id": None,
                    "supervision_policy": None,
                },
            ),
        )

    def test_integration_ledger_routes_stay_off_object_stores(self) -> None:
        control = FakeControlPlane()
        queue_id = "iqm_" + "1" * 32
        policy_id = "gpl_" + "2" * 32
        fold_id = "ifd_" + "3" * 32
        dependency_id = "ldp_" + "4" * 32
        plan_id = "lpl_" + "5" * 32
        provider_landing_id = "plr_" + "9" * 32
        receipt_path = Path(self.temporary.name) / "receipt.json"
        receipt_document = {
            "schema_version": 1,
            "plan_id": plan_id,
            "request_sha256": "6" * 64,
            "nonce": "7" * 64,
            "outcome": "landed",
            "observed_revision": "8" * 40,
        }
        receipt_path.write_text(json.dumps(receipt_document), encoding="utf-8")
        cases = (
            (
                [
                    "integration",
                    "dependency-add",
                    "wi-dependent",
                    "wi-prerequisite",
                    "--created-by",
                    "operator",
                ],
                (
                    "integration.dependency-add",
                    {
                        "dependent_work_item_id": "wi-dependent",
                        "prerequisite_work_item_id": "wi-prerequisite",
                        "created_by_handle": "operator",
                    },
                ),
            ),
            (
                [
                    "integration",
                    "dependency-remove",
                    dependency_id,
                    "--expected-version",
                    "2",
                    "--removed-by",
                    "operator",
                ],
                (
                    "integration.dependency-remove",
                    {
                        "dependency_id": dependency_id,
                        "expected_version": 2,
                        "removed_by_handle": "operator",
                    },
                ),
            ),
            (
                [
                    "integration",
                    "dependency-list",
                    "--project",
                    "project",
                    "--active-only",
                    "--limit",
                    "12",
                ],
                (
                    "integration.dependency-list",
                    {"project_key": "project", "active_only": True, "limit": 12},
                ),
            ),
            (
                ["integration", "enqueue", policy_id, "--created-by", "operator"],
                (
                    "integration.enqueue",
                    {"policy_id": policy_id, "created_by_handle": "operator"},
                ),
            ),
            (
                ["integration", "show", queue_id],
                ("integration.show", {"queue_item_id": queue_id}),
            ),
            (
                ["integration", "list", "--project", "project", "--limit", "9"],
                ("integration.list", {"project_key": "project", "limit": 9}),
            ),
            (
                [
                    "integration",
                    "activate",
                    queue_id,
                    "--fold",
                    fold_id,
                    "--policy",
                    policy_id,
                    "--expected-version",
                    "3",
                    "--actor",
                    "operator",
                ],
                (
                    "integration.activate",
                    {
                        "queue_item_id": queue_id,
                        "fold_id": fold_id,
                        "policy_id": policy_id,
                        "expected_version": 3,
                        "actor_handle": "operator",
                    },
                ),
            ),
            (
                [
                    "integration",
                    "claim",
                    queue_id,
                    "--owner",
                    "supervisor",
                    "--expected-version",
                    "3",
                    "--lease-seconds",
                    "90",
                ],
                (
                    "integration.claim",
                    {
                        "queue_item_id": queue_id,
                        "owner_id": "supervisor",
                        "expected_version": 3,
                        "lease_seconds": 90,
                    },
                ),
            ),
            (
                [
                    "integration",
                    "release",
                    queue_id,
                    "--owner",
                    "supervisor",
                    "--expected-version",
                    "4",
                ],
                (
                    "integration.release",
                    {
                        "queue_item_id": queue_id,
                        "owner_id": "supervisor",
                        "expected_version": 4,
                    },
                ),
            ),
            (
                [
                    "integration",
                    "cancel",
                    queue_id,
                    "--expected-version",
                    "4",
                    "--cancelled-by",
                    "operator",
                ],
                (
                    "integration.cancel",
                    {
                        "queue_item_id": queue_id,
                        "expected_version": 4,
                        "cancelled_by_handle": "operator",
                    },
                ),
            ),
            (
                ["integration", "plan-show", plan_id],
                ("integration.plan-show", {"plan_id": plan_id}),
            ),
            (
                [
                    "integration",
                    "plan-reissue",
                    plan_id,
                    "--expected-generation",
                    "1",
                    "--reissued-by",
                    "operator",
                ],
                (
                    "integration.plan-reissue",
                    {
                        "plan_id": plan_id,
                        "expected_generation": 1,
                        "reissued_by_handle": "operator",
                    },
                ),
            ),
            (
                [
                    "integration",
                    "receipt",
                    "--document",
                    str(receipt_path),
                    "--recorded-by",
                    "operator",
                ],
                (
                    "integration.receipt",
                    {
                        "request": receipt_document,
                        "recorded_by_handle": "operator",
                    },
                ),
            ),
            (
                [
                    "integration",
                    "provider-issue",
                    queue_id,
                    "--owner",
                    "supervisor",
                    "--expected-version",
                    "4",
                    "--resource-id",
                    "42",
                    "--merge-method",
                    "squash",
                    "--issued-by",
                    "operator",
                ],
                (
                    "integration.provider-issue",
                    {
                        "queue_item_id": queue_id,
                        "owner_id": "supervisor",
                        "expected_version": 4,
                        "resource_id": "42",
                        "merge_method": "squash",
                        "issued_by_handle": "operator",
                    },
                ),
            ),
            (
                ["integration", "provider-show", provider_landing_id],
                (
                    "integration.provider-show",
                    {"provider_landing_id": provider_landing_id},
                ),
            ),
            (
                [
                    "integration",
                    "provider-execute",
                    provider_landing_id,
                    "--owner",
                    "supervisor",
                    "--settled-by",
                    "operator",
                ],
                (
                    "integration.provider-execute",
                    {
                        "provider_landing_id": provider_landing_id,
                        "owner_id": "supervisor",
                        "settled_by_handle": "operator",
                    },
                ),
            ),
            (
                [
                    "integration",
                    "provider-settle",
                    provider_landing_id,
                    "--settled-by",
                    "operator",
                ],
                (
                    "integration.provider-settle",
                    {
                        "provider_landing_id": provider_landing_id,
                        "settled_by_handle": "operator",
                    },
                ),
            ),
        )
        for arguments, expected in cases:
            with self.subTest(arguments=arguments):
                stdout = io.StringIO()
                with (
                    mock.patch(
                        "magic_glass_box.cli.ControlPlane", return_value=control
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_result_store",
                        side_effect=AssertionError("result store should stay cold"),
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_candidate_store",
                        side_effect=AssertionError("candidate store should stay cold"),
                    ),
                    redirect_stdout(stdout),
                ):
                    status = main(["--db", str(self.database), *arguments])
                self.assertEqual(status, 0)
                self.assertEqual(control.calls[-1], expected)
                self.assertEqual(
                    json.loads(stdout.getvalue())["operation"], expected[0]
                )

    def test_integration_refold_and_plan_routes_open_both_object_stores(self) -> None:
        control = FakeControlPlane()
        result_store = FakeResultStore()
        candidate_store = FakeCandidateStore()
        queue_id = "iqm_" + "1" * 32
        cases = (
            (
                [
                    "integration",
                    "refold",
                    queue_id,
                    "--expected-version",
                    "7",
                    "--created-by",
                    "operator",
                    "--operation-timeout",
                    "12.5",
                ],
                (
                    "integration.refold",
                    {
                        "queue_item_id": queue_id,
                        "expected_version": 7,
                        "created_by_handle": "operator",
                        "result_store": result_store,
                        "candidate_store": candidate_store,
                    },
                ),
            ),
            (
                [
                    "integration",
                    "plan",
                    queue_id,
                    "--owner",
                    "supervisor",
                    "--expected-version",
                    "8",
                    "--remote",
                    "upstream",
                    "--issued-by",
                    "operator",
                ],
                (
                    "integration.plan",
                    {
                        "queue_item_id": queue_id,
                        "owner_id": "supervisor",
                        "expected_version": 8,
                        "remote": "upstream",
                        "issued_by_handle": "operator",
                        "result_store": result_store,
                        "candidate_store": candidate_store,
                    },
                ),
            ),
        )
        for arguments, expected in cases:
            with self.subTest(arguments=arguments):
                stdout = io.StringIO()
                with (
                    mock.patch(
                        "magic_glass_box.cli.ControlPlane", return_value=control
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_result_store",
                        return_value=result_store,
                    ),
                    mock.patch(
                        "magic_glass_box.cli._build_candidate_store",
                        return_value=candidate_store,
                    ) as build_candidate_store,
                    redirect_stdout(stdout),
                ):
                    status = main(["--db", str(self.database), *arguments])
                self.assertEqual(status, 0)
                timeout = 12.5 if arguments[1] == "refold" else 60.0
                build_candidate_store.assert_called_once_with(control, timeout=timeout)
                self.assertEqual(control.calls[-1], expected)
                self.assertEqual(
                    json.loads(stdout.getvalue())["operation"], expected[0]
                )

    def test_review_only_gate_policy_does_not_require_a_docker_client(self) -> None:
        control = FakeControlPlane()
        policy_path = Path(self.temporary.name) / "review-policy.json"
        policy_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "review_separation": "different_profile",
                    "validations": [],
                }
            ),
            encoding="utf-8",
        )

        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            mock.patch(
                "magic_glass_box.cli.DockerCLI",
                side_effect=AssertionError("Docker must remain cold"),
            ),
            redirect_stdout(io.StringIO()),
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "gate",
                    "policy-create",
                    "cnd_" + "7" * 32,
                    "--policy",
                    str(policy_path),
                    "--created-by",
                    "operator",
                ]
            )

        self.assertEqual(status, 0)

    def test_gate_validation_route_builds_only_the_required_runtime(self) -> None:
        control = FakeControlPlane()
        result_store = FakeResultStore()
        candidate_store = FakeCandidateStore()
        executor = object()
        engine = object()
        policy_id = "gpl_" + "8" * 32
        stdout = io.StringIO()
        with (
            mock.patch("magic_glass_box.cli.ControlPlane", return_value=control),
            mock.patch(
                "magic_glass_box.cli._build_result_store",
                return_value=result_store,
            ),
            mock.patch(
                "magic_glass_box.cli._build_candidate_store",
                return_value=candidate_store,
            ) as build_candidate_store,
            mock.patch("magic_glass_box.cli.DockerCLI", return_value=engine) as docker,
            mock.patch(
                "magic_glass_box.cli.DockerExecutor", return_value=executor
            ) as executor_type,
            redirect_stdout(stdout),
        ):
            status = main(
                [
                    "--db",
                    str(self.database),
                    "gate",
                    "validate",
                    policy_id,
                    "unit",
                    "--recorded-by",
                    "operator",
                    "--expected-policy-revision",
                    "b" * 64,
                    "--run-timeout",
                    "75",
                    "--operation-timeout",
                    "11",
                ]
            )

        self.assertEqual(status, 0)
        docker.assert_called_once_with(run_timeout=75.0)
        executor_type.assert_called_once_with(engine)
        build_candidate_store.assert_called_once_with(control, timeout=11.0)
        self.assertEqual(
            control.calls[-1],
            (
                "gate.validate",
                {
                    "policy_id": policy_id,
                    "validation_name": "unit",
                    "recorded_by_handle": "operator",
                    "expected_policy_revision": "b" * 64,
                    "result_store": result_store,
                    "candidate_store": candidate_store,
                    "executor": executor,
                },
            ),
        )

    def test_run_coordinator_builder_wires_the_run_scoped_gateway(self) -> None:
        control = mock.Mock()
        control.database.path = self.database
        workspace = object()
        executor = object()
        gateway = object()
        result_store = object()
        candidate_store = object()
        candidate_workspaces = object()
        coordinator = object()
        runtime_root = Path(self.temporary.name) / "runtime"
        with (
            mock.patch("magic_glass_box.cli.GitSnapshotter", return_value="git"),
            mock.patch(
                "magic_glass_box.cli.WorkspaceManager", return_value=workspace
            ) as workspace_type,
            mock.patch(
                "magic_glass_box.cli.DockerCLI", return_value="docker"
            ) as docker_type,
            mock.patch(
                "magic_glass_box.cli.DockerExecutor", return_value=executor
            ) as executor_type,
            mock.patch(
                "magic_glass_box.cli.default_gateway_root",
                return_value=runtime_root,
            ) as gateway_root,
            mock.patch(
                "magic_glass_box.cli.FileGatewayManager", return_value=gateway
            ) as gateway_type,
            mock.patch(
                "magic_glass_box.cli._build_candidate_store",
                return_value=candidate_store,
            ) as candidate_store_builder,
            mock.patch(
                "magic_glass_box.cli.CandidateReviewWorkspaces",
                return_value=candidate_workspaces,
            ) as candidate_workspace_type,
            mock.patch(
                "magic_glass_box.cli.RunCoordinator", return_value=coordinator
            ) as coordinator_type,
        ):
            result = _build_run_coordinator(
                control, run_timeout=90, result_store=result_store
            )

        self.assertIs(result, coordinator)
        workspace_type.assert_called_once_with(
            self.database.parent / "workspaces", "git"
        )
        docker_type.assert_called_once_with(run_timeout=90)
        executor_type.assert_called_once_with("docker")
        gateway_root.assert_called_once_with(self.database)
        gateway_type.assert_called_once_with(runtime_root, control)
        candidate_store_builder.assert_called_once_with(control)
        candidate_workspace_type.assert_called_once_with(
            control.database,
            workspace,
            result_store,
            candidate_store,
        )
        coordinator_type.assert_called_once_with(
            control.database,
            executor,
            workspace,
            gateway,
            result_store,
            candidate_workspaces=candidate_workspaces,
        )

    def test_emit_has_stable_readable_json(self) -> None:
        output = io.StringIO()
        _emit({"z": "å", "a": 1}, stream=output)
        self.assertEqual(output.getvalue(), '{\n  "a": 1,\n  "z": "å"\n}\n')

    def test_json_input_rejects_empty_duplicate_and_non_object_documents(self) -> None:
        path = Path(self.temporary.name) / "input.json"
        for raw, message in (
            (b"", "between 1 byte"),
            (b'{"a":1,"a":2}', "duplicate JSON key"),
            (b"[]", "must contain an object"),
            (b"{", "malformed JSON"),
            (b'"\xff"', "UTF-8"),
            (b'{"number":NaN}', "invalid JSON number"),
        ):
            with self.subTest(message=message):
                path.write_bytes(raw)
                with self.assertRaisesRegex(InputError, message):
                    _json_file(path)

    def test_json_input_preserves_unicode_and_accepts_exact_size_limit(self) -> None:
        path = Path(self.temporary.name) / "input.json"
        prefix = '{"value":"å'.encode()
        suffix = b'"}'
        raw = prefix + (b"x" * (1024 * 1024 - len(prefix) - len(suffix))) + suffix
        path.write_bytes(raw)
        value = _json_file(path)
        self.assertTrue(value["value"].startswith("å"))
        self.assertEqual(len(raw), 1024 * 1024)

    def test_json_input_reads_at_most_one_byte_past_the_limit(self) -> None:
        path = Path(self.temporary.name) / "input.json"
        reader = mock.MagicMock()
        reader.__enter__.return_value = reader
        reader.read.return_value = b" " * (1024 * 1024 + 1)

        with (
            mock.patch.object(Path, "open", return_value=reader) as opened,
            self.assertRaisesRegex(InputError, "1 MiB"),
        ):
            _json_file(path)

        opened.assert_called_once_with(mode="rb")
        reader.read.assert_called_once_with(1024 * 1024 + 1)

    def test_json_input_rejects_missing_and_oversized_files(self) -> None:
        missing = Path(self.temporary.name) / "missing.json"
        with self.assertRaisesRegex(InputError, "cannot read"):
            _json_file(missing)
        oversized = Path(self.temporary.name) / "oversized.json"
        oversized.write_bytes(b" " * (1024 * 1024 + 1))
        with self.assertRaisesRegex(InputError, "1 MiB"):
            _json_file(oversized)

    def test_inference_connector_commands_create_credentialless_authority(self) -> None:
        connector_path = Path(self.temporary.name) / "connector.json"
        connector_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "connector_protocol_version": 1,
                    "deployment_sha256": "a" * 64,
                    "transport": {
                        "kind": "unix",
                        "socket_path": str(
                            Path(self.temporary.name) / "openai-sidecar.sock"
                        ),
                    },
                    "routes": [
                        {
                            "name": "chatgpt-main",
                            "provider": "openai",
                            "protocol": "openai-responses",
                            "models": ["gpt-test"],
                            "credential_scope_sha256": "b" * 64,
                        }
                    ],
                    "request_bytes": 32768,
                    "response_bytes": 65536,
                    "timeout_ms": 30000,
                    "max_concurrency": 2,
                }
            ),
            encoding="utf-8",
        )
        grant_path = Path(self.temporary.name) / "connector-grant.json"
        grant_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "connector_name": "openai-personal",
                    "deployment_sha256": "a" * 64,
                    "route": "chatgpt-main",
                    "credential_scope_sha256": "b" * 64,
                    "provider": "openai",
                    "protocol": "openai-responses",
                    "model": "gpt-test",
                    "request_bytes": 16384,
                    "response_bytes": 32768,
                    "timeout_ms": 10000,
                    "max_concurrency": 1,
                }
            ),
            encoding="utf-8",
        )
        self.assertEqual(self.invoke("init")[0], 0)
        self.assertEqual(
            self.invoke("user", "add", "operator", "--display-name", "Local Operator")[
                0
            ],
            0,
        )

        status, connector, error = self.invoke(
            "inference-connector",
            "register",
            "openai-personal",
            "--spec",
            str(connector_path),
            "--created-by",
            "operator",
        )
        self.assertEqual((status, error), (0, ""))
        self.assertEqual(connector["state"], "active")

        status, grant, error = self.invoke(
            "provider",
            "connector-add",
            "openai-main",
            "--policy",
            str(grant_path),
            "--created-by",
            "operator",
        )
        self.assertEqual((status, error), (0, ""))
        self.assertEqual(grant["grant_kind"], "connector")
        self.assertNotIn("credential_ref", grant)

        status, connectors, error = self.invoke("inference-connector", "list")
        self.assertEqual((status, error), (0, ""))
        self.assertEqual([item["name"] for item in connectors], ["openai-personal"])

        status, shown, error = self.invoke(
            "inference-connector", "show", "openai-personal"
        )
        self.assertEqual((status, error), (0, ""))
        self.assertEqual(shown, connector)

        status, revoked, error = self.invoke(
            "inference-connector",
            "revoke",
            "openai-personal",
            "--expected-version",
            "1",
            "--revoked-by",
            "operator",
        )
        self.assertEqual((status, error), (0, ""))
        self.assertEqual(revoked["state"], "revoked")

    def test_service_sidecar_commands_manage_durable_operations(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        sidecar_document = json.loads(
            (repository / "examples/service-sidecar.json").read_text()
        )
        sidecar_document["transport"]["socket_path"] = str(
            Path(self.temporary.name) / "service.sock"
        )
        sidecar_path = Path(self.temporary.name) / "service-sidecar.json"
        sidecar_path.write_text(json.dumps(sidecar_document), encoding="utf-8")
        operation_path = repository / "examples/service-operation.json"

        self.assertEqual(self.invoke("init")[0], 0)
        self.assertEqual(
            self.invoke("user", "add", "operator", "--display-name", "Operator")[0],
            0,
        )
        status, sidecar, error = self.invoke(
            "service-sidecar",
            "register",
            "example-sidecar",
            "--spec",
            str(sidecar_path),
            "--created-by",
            "operator",
        )
        self.assertEqual((status, error), (0, ""))
        self.assertEqual(sidecar["state"], "active")

        database = Database(self.database)
        operator = database.get_user("operator")
        instance = database.create_krab_instance("local", "http://localhost:8765")
        database.create_project(
            instance_id=instance["id"],
            krab_project_id="prj_" + "1" * 32,
            krab_project_key="demo-project",
            display_name="Demo Project",
            context=ProjectContext.from_mapping({"purpose": "Prove routing"}),
            created_by=operator["id"],
        )
        status, binding, error = self.invoke(
            "capability",
            "bind",
            "demo-project",
            "change.open",
            "--version",
            "1",
            "--sidecar",
            "example-sidecar",
            "--mapping",
            "primary",
            "--bound-by",
            "operator",
        )
        self.assertEqual((status, error), (0, ""))
        self.assertEqual(binding["state"], "active")
        self.assertEqual(binding["controller"]["sidecar_name"], "example-sidecar")

        status, shown_binding, error = self.invoke("capability", "show", binding["id"])
        self.assertEqual((status, error), (0, ""))
        self.assertEqual(shown_binding, binding)
        status, bindings, error = self.invoke(
            "capability", "list", "--project", "demo-project"
        )
        self.assertEqual((status, error), (0, ""))
        self.assertEqual(bindings, [binding])

        status, operation, error = self.invoke(
            "service-sidecar",
            "operation-create",
            "--request",
            str(operation_path),
            "--binding",
            binding["id"],
            "--created-by",
            "operator",
        )
        self.assertEqual((status, error), (0, ""))
        self.assertEqual(operation["state"], "pending")
        self.assertEqual(operation["project_capability_binding_id"], binding["id"])

        status, unbound, error = self.invoke(
            "capability",
            "unbind",
            binding["id"],
            "--expected-version",
            "1",
            "--unbound-by",
            "operator",
        )
        self.assertEqual((status, error), (0, ""))
        self.assertEqual(unbound["state"], "unbound")
        status, bindings, error = self.invoke(
            "capability",
            "list",
            "--project",
            "demo-project",
            "--include-unbound",
        )
        self.assertEqual((status, error), (0, ""))
        self.assertEqual(bindings, [unbound])

        status, shown, error = self.invoke(
            "service-sidecar", "operation-show", operation["id"]
        )
        self.assertEqual((status, error), (0, ""))
        self.assertEqual(shown, operation)
        status, operations, error = self.invoke("service-sidecar", "operation-list")
        self.assertEqual((status, error), (0, ""))
        self.assertEqual([item["id"] for item in operations], [operation["id"]])

        status, revoked, error = self.invoke(
            "service-sidecar",
            "revoke",
            "example-sidecar",
            "--expected-version",
            "1",
            "--revoked-by",
            "operator",
        )
        self.assertEqual((status, error), (0, ""))
        self.assertEqual(revoked["state"], "revoked")


if __name__ == "__main__":
    unittest.main()
