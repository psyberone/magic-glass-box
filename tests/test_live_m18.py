from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from magic_glass_box.service_sidecar import ServiceSidecarAmbiguousError
from scripts import check_live_m18 as live_m18
from scripts.check_live_m18 import (
    IK_PROJECT,
    MGB_PROJECT,
    _all_runs,
    _drive_one_technical_stop,
    _execution_lineage,
    _existing_executions,
    _github_specs,
    _LostReceiptClientFactory,
    _pin_controller_identity,
    _proof_status,
    _runtime_diagnostics,
    _start_github_providers,
    _usage_totals,
    _wake_evidence,
)


class LiveM18ContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_existing_executions_select_one_active_execution_per_project(self) -> None:
        calls: list[tuple[str, str | None, int]] = []

        class Database:
            @staticmethod
            def list_workflow_executions(
                *, project_key: str, state: str | None = None, limit: int
            ) -> list[dict[str, str]]:
                calls.append((project_key, state, limit))
                executions = [
                    {"id": "wfx_cancelled", "state": "cancelled"},
                    {"id": "wfx_" + project_key, "state": "active"},
                ]
                if state is not None:
                    executions = [item for item in executions if item["state"] == state]
                return executions[:limit]

        self.assertEqual(
            _existing_executions(Database()),  # type: ignore[arg-type]
            {
                "magic-glass-box-m18": "wfx_magic-glass-box-m18",
                "imitation-krab-m18": "wfx_imitation-krab-m18",
            },
        )
        self.assertEqual(
            calls,
            [
                ("magic-glass-box-m18", "active", 2),
                ("imitation-krab-m18", "active", 2),
            ],
        )

    def test_execution_lineage_and_runs_include_cancelled_and_every_generation(
        self,
    ) -> None:
        class Database:
            @staticmethod
            def get_workflow_execution(execution_id: str) -> dict[str, str]:
                return {
                    "id": execution_id,
                    "work_item_id": "wki_target",
                    "created_at": "2026-11-06T12:01:00Z",
                }

            @staticmethod
            def list_workflow_executions(
                *, project_key: str, limit: int
            ) -> list[dict[str, str]]:
                del project_key, limit
                return [
                    {
                        "id": "wfx_current",
                        "work_item_id": "wki_target",
                        "created_at": "2026-11-06T12:01:00Z",
                    },
                    {
                        "id": "wfx_cancelled",
                        "work_item_id": "wki_target",
                        "created_at": "2026-11-06T12:00:00Z",
                    },
                    {
                        "id": "wfx_unrelated",
                        "work_item_id": "wki_other",
                        "created_at": "2026-11-06T11:00:00Z",
                    },
                ]

            @staticmethod
            def list_workflow_runs(execution_id: str) -> list[dict[str, object]]:
                generations = 2 if execution_id == "wfx_cancelled" else 1
                return [
                    {
                        "id": f"run_{execution_id}_{generation}",
                        "workflow_generation": generation,
                    }
                    for generation in range(1, generations + 1)
                ]

        database = Database()
        lineage = _execution_lineage(
            database,  # type: ignore[arg-type]
            {"magic-glass-box-m18": "wfx_current"},
        )

        self.assertEqual(
            [item["id"] for item in lineage["magic-glass-box-m18"]],
            ["wfx_cancelled", "wfx_current"],
        )
        self.assertEqual(
            [run["workflow_generation"] for run in _all_runs(database, lineage)],  # type: ignore[arg-type]
            [1, 2, 1],
        )

    def test_controller_drift_requires_owner_observation(self) -> None:
        first = {
            "commit": "a" * 40,
            "dirty": False,
            "dirty_sha256": "1" * 64,
        }
        changed = {
            "commit": "b" * 40,
            "dirty": False,
            "dirty_sha256": "1" * 64,
        }
        pinned = _pin_controller_identity(self.root, first, existing_state=False)
        self.assertEqual(pinned["commit"], first["commit"])
        self.assertEqual(
            _pin_controller_identity(self.root, first, existing_state=True), pinned
        )

        with self.assertRaisesRegex(RuntimeError, "identity changed"):
            _pin_controller_identity(self.root, changed, existing_state=True)
        with self.assertRaisesRegex(RuntimeError, "every activation owner"):
            _pin_controller_identity(
                self.root,
                changed,
                existing_state=True,
                change_reason="reviewed R9 controller update",
                activation_owners=("operator", "intruder"),
            )

        replacement = _pin_controller_identity(
            self.root,
            changed,
            existing_state=True,
            change_reason="reviewed R9 controller update",
            activation_owners=("operator", "operator"),
        )
        self.assertEqual(replacement["commit"], changed["commit"])
        observation = json.loads(
            (self.root / "observations.jsonl").read_text(encoding="utf-8")
        )
        self.assertEqual(observation["kind"], "controller.code_change")
        self.assertEqual(observation["metadata"]["observed_by"], "operator")

    def test_live_harness_starts_both_exact_registered_github_sidecars(self) -> None:
        specs = _github_specs(
            state_root=self.root,
            account="psyberone",
            credential_scope_sha256="a" * 64,
            base_deployment_sha256="b" * 64,
        )
        names = {
            "github-m18-mgb": MGB_PROJECT,
            "github-m18-ik": IK_PROJECT,
        }

        class Database:
            @staticmethod
            def get_service_sidecar(name: str) -> dict[str, object]:
                spec = specs[names[name]]
                return {
                    "state": "active",
                    "content_sha256": spec.digest,
                    "spec": spec.to_dict(),
                }

        processes = [object(), object()]
        with (
            mock.patch.object(live_m18.workflow, "_clear_stale_socket") as clear,
            mock.patch.object(live_m18.workflow, "_ensure_spec_file") as ensure,
            mock.patch.object(
                live_m18.workflow, "_start_provider", side_effect=processes
            ) as start,
        ):
            observed = _start_github_providers(
                database=Database(),  # type: ignore[arg-type]
                specs=specs,
                state_root=self.root,
                source_root=self.root / "source",
                gh_config_dir=self.root / "gh-config",
                gh_bin=Path("/usr/bin/gh"),
                git_bin=Path("/usr/bin/git"),
            )

        self.assertEqual(observed, processes)
        self.assertEqual(clear.call_count, 2)
        self.assertEqual(ensure.call_count, 2)
        self.assertEqual(start.call_count, 2)
        self.assertEqual(
            {call.kwargs["socket_path"] for call in start.call_args_list},
            {Path(spec.transport.socket_path) for spec in specs.values()},
        )
        self.assertTrue(
            all(
                call.kwargs["candidate_root"] == self.root / "candidates"
                for call in start.call_args_list
            )
        )

    def test_usage_totals_are_complete_across_generations(self) -> None:
        self.assertEqual(
            _usage_totals(
                [
                    {
                        "input_tokens": 10,
                        "output_tokens": 2,
                        "estimated_microusd": 30,
                    },
                    {
                        "input_tokens": 20,
                        "cached_input_tokens": 5,
                        "reasoning_output_tokens": 3,
                        "accounted_microusd": 40,
                    },
                ]
            ),
            {
                "input_tokens": 30,
                "cached_input_tokens": 5,
                "output_tokens": 2,
                "reasoning_output_tokens": 3,
                "estimated_microusd": 30,
                "accounted_microusd": 40,
            },
        )

    def test_wake_evidence_labels_only_harness_authored_facts(self) -> None:
        wake = _wake_evidence(
            {
                "id": "wwk_example",
                "acknowledged_by": "operator",
                "availability": {"observed_by": "operator", "state": "available"},
                "events": [
                    {"kind": "wake.requested", "actor": None},
                    {"kind": "wake.delivered", "actor": "operator"},
                ],
            }
        )

        self.assertEqual(
            wake["proof_fact_sources"],
            {
                "request": "control_plane",
                "availability": "live_harness",
                "delivery": "live_harness",
                "acknowledgement": "live_harness",
            },
        )

    def test_runtime_diagnostics_keep_only_bounded_failures(self) -> None:
        log = "\n".join(
            (
                json.dumps(
                    {
                        "type": "item.completed",
                        "item": {"type": "reasoning", "text": "private reasoning"},
                    }
                ),
                json.dumps(
                    {
                        "type": "item.completed",
                        "item": {
                            "type": "command_execution",
                            "status": "failed",
                            "exit_code": 2,
                            "command": "make test",
                            "aggregated_output": "missing dependency",
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "turn.failed",
                        "error": {"message": "provider stopped"},
                    }
                ),
            )
        )

        diagnostic = _runtime_diagnostics((log,))[0]

        self.assertEqual(diagnostic["event_count"], 3)
        self.assertEqual(diagnostic["last_event_type"], "turn.failed")
        self.assertEqual(
            [failure["type"] for failure in diagnostic["failures"]],
            ["command.failed", "turn.failed"],
        )
        self.assertNotIn("private reasoning", json.dumps(diagnostic))

    def test_lost_receipt_factory_discards_one_effect_per_project(self) -> None:
        class Response:
            outcome = "succeeded"

            def __init__(self, request: SimpleNamespace):
                self.receipt = SimpleNamespace(
                    remote_identity=f"github:example/{request.operation_id}",
                    observed_revision="a" * 40,
                    evidence_sha256="b" * 64,
                )
                self._request = request

            def to_dict(self) -> dict[str, object]:
                return {
                    "operation_id": self._request.operation_id,
                    "result": {"provider_body": "must-not-enter-marker"},
                }

        class Delegate:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def invoke(self, request: SimpleNamespace) -> Response:
                self.calls.append(request.operation_id)
                return Response(request)

        delegates: dict[Path, Delegate] = {}

        def make_delegate(path: Path) -> Delegate:
            return delegates.setdefault(path, Delegate())

        mgb_socket = self.root / "mgb.sock"
        ik_socket = self.root / "ik.sock"
        marker_root = self.root / "lost-receipts"
        factory = _LostReceiptClientFactory(
            marker_root=marker_root,
            socket_projects={mgb_socket: MGB_PROJECT, ik_socket: IK_PROJECT},
            client_factory=make_delegate,
        )

        def request(operation_id: str) -> SimpleNamespace:
            return SimpleNamespace(
                operation_id=operation_id,
                phase="invoke",
                capability=SimpleNamespace(mode="effect", name="refs.publish"),
            )

        with self.assertRaisesRegex(
            ServiceSidecarAmbiguousError, "deliberately discarded"
        ):
            factory(mgb_socket).invoke(request("sop_mgb"))
        accepted = factory(mgb_socket).invoke(request("sop_mgb_second"))
        self.assertEqual(accepted.outcome, "succeeded")
        with self.assertRaises(ServiceSidecarAmbiguousError):
            factory(ik_socket).invoke(request("sop_ik"))

        markers = [
            json.loads((marker_root / f"{project}.json").read_text(encoding="utf-8"))
            for project in (MGB_PROJECT, IK_PROJECT)
        ]
        self.assertEqual(
            [(item["project_key"], item["operation_id"]) for item in markers],
            [(MGB_PROJECT, "sop_mgb"), (IK_PROJECT, "sop_ik")],
        )
        marker_text = json.dumps(markers)
        self.assertNotIn("provider_body", marker_text)
        self.assertNotIn("github:example", marker_text)

    def test_drive_stops_after_a_new_uncertain_provider_effect(self) -> None:
        class Database:
            def __init__(self) -> None:
                self.uncertain = False

            def get_workflow_execution(self, execution_id: str) -> dict[str, object]:
                landing = None
                if self.uncertain and execution_id == "wfx_mgb":
                    landing = {
                        "provider_intent": {
                            "effects": {
                                "refs.publish": {
                                    "operation_state": "uncertain",
                                    "service_operation_id": "sop_lost",
                                }
                            }
                        }
                    }
                return {
                    "id": execution_id,
                    "state": "active",
                    "stage": "landing",
                    "correction_round": 0,
                    "landing": landing,
                    "rulings": [],
                }

        database = Database()

        class Supervisor:
            drained = False

            def tick(self) -> dict[str, bool]:
                database.uncertain = True
                return {"workflow": True}

            def drain(self, *, timeout: int) -> None:
                self.drained = timeout == 30

        class Control:
            @staticmethod
            def list_workflow_manual_wakes(
                *, execution_id: str
            ) -> list[dict[str, object]]:
                del execution_id
                return []

        class Waves:
            calls = 0

            def before_tick(self) -> None:
                self.calls += 1

        supervisor = Supervisor()
        waves = Waves()
        reason = _drive_one_technical_stop(
            supervisor=supervisor,  # type: ignore[arg-type]
            database=database,  # type: ignore[arg-type]
            control=Control(),  # type: ignore[arg-type]
            executions={MGB_PROJECT: "wfx_mgb", IK_PROJECT: "wfx_ik"},
            connector_waves=waves,  # type: ignore[arg-type]
            state_root=self.root,
        )

        self.assertEqual(reason, "planned_restart_after_provider_receipt_loss")
        self.assertTrue(supervisor.drained)
        self.assertEqual(waves.calls, 1)
        observation = json.loads(
            (self.root / "observations.jsonl").read_text(encoding="utf-8")
        )
        self.assertEqual(observation["kind"], "provider.receipt_discarded")
        self.assertEqual(
            observation["metadata"]["effects"][0]["operation_id"], "sop_lost"
        )

    def test_drive_and_report_status_recognize_completed_proof(self) -> None:
        class Database:
            @staticmethod
            def get_workflow_execution(execution_id: str) -> dict[str, str]:
                return {"id": execution_id, "state": "completed"}

        class Supervisor:
            drained = False

            def drain(self, *, timeout: int) -> None:
                self.drained = timeout == 30

            @staticmethod
            def tick() -> dict[str, bool]:
                raise AssertionError("completed proof must not tick")

        executions = {MGB_PROJECT: "wfx_mgb", IK_PROJECT: "wfx_ik"}
        database = Database()
        supervisor = Supervisor()
        reason = _drive_one_technical_stop(
            supervisor=supervisor,  # type: ignore[arg-type]
            database=database,  # type: ignore[arg-type]
            control=object(),  # type: ignore[arg-type]
            executions=executions,
            connector_waves=object(),  # type: ignore[arg-type]
            state_root=self.root,
        )

        self.assertEqual(reason, "both_completed")
        self.assertTrue(supervisor.drained)
        self.assertEqual(_proof_status(database, executions), "completed")  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
