from __future__ import annotations

import hashlib
import json
import shutil
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
    _provider_invocation_evidence,
    _registered_github_specs,
    _resolve_github_source_root,
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

    def _init_controller_repository(self) -> tuple[str, str]:
        git_raw = shutil.which("git")
        assert git_raw is not None
        git_bin = str(Path(git_raw).resolve(strict=True))
        live_m18._run_checked((git_bin, "init", "--quiet", str(self.root)))
        (self.root / "tracked.txt").write_text("base\n", encoding="utf-8")
        live_m18._run_checked((git_bin, "-C", str(self.root), "add", "tracked.txt"))
        live_m18._run_checked(
            (
                git_bin,
                "-C",
                str(self.root),
                "-c",
                "user.name=M18 Test",
                "-c",
                "user.email=m18@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "base",
            )
        )
        commit = live_m18._run_checked(
            (git_bin, "-C", str(self.root), "rev-parse", "HEAD")
        )
        return git_bin, commit

    def test_cli_accepts_explicit_ten_dollar_ceiling_and_rejects_more(self) -> None:
        required = [
            "--profile-dir",
            "/private/profile",
            "--mgb-repository",
            "/source/mgb",
            "--ik-repository",
            "/source/ik",
            "--gh-config-dir",
            "/private/gh",
            "--state-dir",
            "/private/state",
        ]

        parsed = live_m18.parse_args(
            [
                *required,
                "--budget-ceiling-microusd",
                "10000000",
            ]
        )

        self.assertEqual(parsed.budget_ceiling_microusd, 10_000_000)
        with mock.patch("sys.stderr"), self.assertRaises(SystemExit):
            live_m18.parse_args(
                [
                    *required,
                    "--budget-ceiling-microusd",
                    "10000001",
                ]
            )

    def test_live_policy_uses_the_approved_bounded_operator_window(self) -> None:
        bindings = {
            name: {"id": "cpb_" + character * 32}
            for name, character in zip(
                ("change.open", "landing.invoke", "refs.publish"),
                ("a", "b", "c"),
                strict=True,
            )
        }
        policy_resources = live_m18._policy().to_dict()["resource_maxima"]
        activations = [
            live_m18._activation(
                project_key=project_key,
                policy_sha256="d" * 64,
                configuration_version_id="wfv_" + "e" * 32,
                bindings=bindings,
            ).to_dict()["resources"]
            for project_key in (MGB_PROJECT, IK_PROJECT)
        ]

        self.assertEqual(live_m18.LIVE_WORKFLOW_WALL_TIME_SECONDS, 604_800)
        self.assertEqual(policy_resources["wall_time_seconds"], 604_800)
        for resources in activations:
            self.assertEqual(resources["wall_time_seconds"], 604_800)
            self.assertEqual(resources["seat_count"], 2)
            self.assertEqual(resources["shared_host_concurrency"], 1)

    def test_observation_reader_is_bounded_and_structural(self) -> None:
        self.assertEqual(live_m18._read_observations(self.root), [])
        live_m18._append_observation(
            self.root,
            "first",
            "First observation",
            project_key=MGB_PROJECT,
        )
        live_m18._append_observation(
            self.root,
            "second",
            "Second observation",
            project_key=IK_PROJECT,
        )

        observed = live_m18._read_observations(self.root)

        self.assertEqual([item["kind"] for item in observed], ["first", "second"])
        self.assertEqual(
            [item["metadata"]["project_key"] for item in observed],
            [MGB_PROJECT, IK_PROJECT],
        )
        path = self.root / live_m18.OBSERVATIONS_NAME
        invalid_values = (
            b"not-json\n",
            b"[]\n",
            json.dumps({"kind": 7, "metadata": {}}).encode() + b"\n",
            json.dumps({"kind": "valid", "metadata": []}).encode() + b"\n",
        )
        for raw in invalid_values:
            with self.subTest(raw=raw):
                path.write_bytes(raw)
                with self.assertRaises(RuntimeError) as caught:
                    live_m18._read_observations(self.root)
                self.assertEqual(str(caught.exception), "M18 observations are invalid")
        prefix = b'{"kind":"k","metadata":{}}'
        path.write_bytes(
            prefix
            + b" " * (live_m18.MAX_CONTROLLER_OBSERVATIONS_BYTES - len(prefix) - 1)
            + b"\n"
        )
        self.assertEqual(len(live_m18._read_observations(self.root)), 1)
        path.write_bytes(b"{" + b"x" * live_m18.MAX_CONTROLLER_OBSERVATIONS_BYTES)
        with self.assertRaises(RuntimeError) as caught:
            live_m18._read_observations(self.root)
        self.assertEqual(
            str(caught.exception), "M18 observations exceed their recovery bound"
        )
        path.write_bytes(b'{"kind":"k","metadata":{}}\n' * 256)
        self.assertEqual(len(live_m18._read_observations(self.root)), 256)
        path.write_bytes(b'{"kind":"k","metadata":{}}\n' * 257)
        with self.assertRaises(RuntimeError) as caught:
            live_m18._read_observations(self.root)
        self.assertEqual(
            str(caught.exception), "M18 observations exceed their recovery bound"
        )

    def test_observation_reader_bounds_the_physical_read(self) -> None:
        path = self.root / live_m18.OBSERVATIONS_NAME
        path.write_bytes(b'{"kind":"k","metadata":{}}\n')
        opened = mock.MagicMock()
        reader = opened.__enter__.return_value
        reader.read.return_value = b'{"kind":"k","metadata":{}}\n'

        with mock.patch.object(Path, "open", return_value=opened):
            observed = live_m18._read_observations(self.root)

        self.assertEqual([item["kind"] for item in observed], ["k"])
        reader.read.assert_called_once_with(
            live_m18.MAX_CONTROLLER_OBSERVATIONS_BYTES + 1
        )

    def test_observation_reader_reports_an_unavailable_log_exactly(self) -> None:
        path = self.root / live_m18.OBSERVATIONS_NAME
        path.touch()

        with (
            mock.patch.object(Path, "open", side_effect=OSError("unavailable")),
            self.assertRaises(RuntimeError) as caught,
        ):
            live_m18._read_observations(self.root)

        self.assertEqual(str(caught.exception), "M18 observations are unavailable")
        self.assertIsInstance(caught.exception.__cause__, OSError)

    def test_bounded_command_capture_preserves_its_execution_contract(self) -> None:
        observed: dict[str, object] = {}

        def run(command: list[str], **kwargs: object) -> SimpleNamespace:
            observed["command"] = command
            observed.update(kwargs)
            kwargs["stdout"].write(b"abc")  # type: ignore[union-attr]
            kwargs["stderr"].write(b"ignored")  # type: ignore[union-attr]
            return SimpleNamespace(returncode=0)

        with mock.patch.object(live_m18.subprocess, "run", side_effect=run):
            output = live_m18._run_bounded_bytes(
                ("proof-command", "argument"),
                limit=3,
                failure_message="proof failed",
                overflow_message="proof overflowed",
                cwd=self.root,
            )

        self.assertEqual(output, b"abc")
        self.assertEqual(observed["command"], ["proof-command", "argument"])
        self.assertEqual(observed["cwd"], self.root)
        self.assertIs(observed["stdin"], live_m18.subprocess.DEVNULL)
        self.assertEqual(observed["timeout"], 120)
        self.assertIs(observed["check"], False)

    def test_bounded_command_capture_reports_failures_and_overflow(self) -> None:
        def failing(command: list[str], **kwargs: object) -> SimpleNamespace:
            del command
            kwargs["stderr"].write(b"provider detail")  # type: ignore[union-attr]
            return SimpleNamespace(returncode=7)

        with (
            mock.patch.object(live_m18.subprocess, "run", side_effect=failing),
            self.assertRaises(RuntimeError) as caught,
        ):
            live_m18._run_bounded_bytes(
                ("proof-command",),
                limit=4,
                failure_message="proof failed",
                overflow_message="proof overflowed",
            )
        self.assertEqual(str(caught.exception), "proof failed: provider detail")

        def overflowing(command: list[str], **kwargs: object) -> SimpleNamespace:
            del command
            kwargs["stdout"].write(b"12345")  # type: ignore[union-attr]
            return SimpleNamespace(returncode=0)

        with (
            mock.patch.object(live_m18.subprocess, "run", side_effect=overflowing),
            self.assertRaises(RuntimeError) as caught,
        ):
            live_m18._run_bounded_bytes(
                ("proof-command",),
                limit=4,
                failure_message="proof failed",
                overflow_message="proof overflowed",
            )
        self.assertEqual(str(caught.exception), "proof overflowed")

    def test_bounded_command_capture_bounds_and_normalizes_failure_detail(
        self,
    ) -> None:
        failure_details = (
            (b"", "proof failed"),
            (b"\xff", "proof failed: \ufffd"),
            (
                b"x" * (live_m18.MAX_DIAGNOSTIC_TEXT + 7),
                "proof failed: " + "x" * live_m18.MAX_DIAGNOSTIC_TEXT,
            ),
        )
        for detail, expected in failure_details:

            def failing(
                command: list[str], detail: bytes = detail, **kwargs: object
            ) -> SimpleNamespace:
                del command
                kwargs["stderr"].write(detail)  # type: ignore[union-attr]
                return SimpleNamespace(returncode=7)

            with (
                self.subTest(detail_length=len(detail)),
                mock.patch.object(live_m18.subprocess, "run", side_effect=failing),
                self.assertRaises(RuntimeError) as caught,
            ):
                live_m18._run_bounded_bytes(
                    ("proof-command",),
                    limit=4,
                    failure_message="proof failed",
                    overflow_message="proof overflowed",
                )
            self.assertEqual(str(caught.exception), expected)

    def test_bounded_command_capture_normalizes_local_execution_errors(self) -> None:
        failures = (
            OSError("missing executable"),
            live_m18.subprocess.TimeoutExpired(("proof-command",), 120),
        )
        for failure in failures:
            with (
                self.subTest(failure=type(failure).__name__),
                mock.patch.object(live_m18.subprocess, "run", side_effect=failure),
                self.assertRaises(RuntimeError) as caught,
            ):
                live_m18._run_bounded_bytes(
                    ("proof-command",),
                    limit=4,
                    failure_message="proof failed",
                    overflow_message="proof overflowed",
                )
            self.assertEqual(str(caught.exception), "proof failed")
            self.assertIs(caught.exception.__cause__, failure)

    @unittest.skipUnless(shutil.which("git"), "git is required")
    def test_controller_identity_hashes_clean_tracked_and_untracked_state(self) -> None:
        git_bin, commit = self._init_controller_repository()

        clean = live_m18._controller_identity(self.root)
        self.assertEqual(
            clean,
            {
                "commit": commit,
                "dirty": False,
                "dirty_sha256": hashlib.sha256(b"tracked-diff\0").hexdigest(),
            },
        )

        (self.root / "tracked.txt").write_text("changed\n", encoding="utf-8")
        diff = live_m18._run_bounded_bytes(
            (
                git_bin,
                "-C",
                str(self.root),
                "diff",
                "--binary",
                "HEAD",
                "--",
            ),
            limit=live_m18.MAX_CONTROLLER_SOURCE_ARCHIVE_BYTES,
            failure_message="test diff failed",
            overflow_message="test diff overflowed",
        )
        tracked = live_m18._controller_identity(self.root)
        self.assertTrue(tracked["dirty"])
        self.assertEqual(
            tracked["dirty_sha256"],
            hashlib.sha256(b"tracked-diff\0" + diff).hexdigest(),
        )

        (self.root / "tracked.txt").write_text("base\n", encoding="utf-8")
        (self.root / "z.txt").write_bytes(b"last")
        (self.root / "a.txt").write_bytes(b"first")
        expected = hashlib.sha256()
        expected.update(b"tracked-diff\0")
        for path, raw in ((b"a.txt", b"first"), (b"z.txt", b"last")):
            expected.update(b"untracked\0" + path + b"\0" + raw)
        untracked = live_m18._controller_identity(self.root)
        self.assertTrue(untracked["dirty"])
        self.assertEqual(untracked["dirty_sha256"], expected.hexdigest())

    @unittest.skipUnless(shutil.which("git"), "git is required")
    def test_controller_identity_binds_exact_git_inventory_commands(self) -> None:
        git_raw = shutil.which("git")
        assert git_raw is not None
        git_bin = str(Path(git_raw).resolve(strict=True))
        root = self.root.resolve(strict=True)
        (root / "a.txt").write_bytes(b"first")
        (root / "b.txt").write_bytes(b"second")
        commit = "a" * 40
        with (
            mock.patch.object(live_m18, "_git", return_value=commit) as git,
            mock.patch.object(
                live_m18,
                "_run_bounded_bytes",
                side_effect=(b"diff", b"b.txt\0a.txt\0"),
            ) as bounded,
        ):
            observed = live_m18._controller_identity(root)

        expected = hashlib.sha256()
        expected.update(b"tracked-diff\0diff")
        for path, raw in ((b"a.txt", b"first"), (b"b.txt", b"second")):
            expected.update(b"untracked\0" + path + b"\0" + raw)
        self.assertEqual(
            observed,
            {"commit": commit, "dirty": True, "dirty_sha256": expected.hexdigest()},
        )
        git.assert_called_once_with(root, "rev-parse", "HEAD")
        self.assertEqual(
            bounded.call_args_list,
            [
                mock.call(
                    (
                        git_bin,
                        "-C",
                        str(root),
                        "diff",
                        "--binary",
                        "HEAD",
                        "--",
                    ),
                    limit=live_m18.MAX_CONTROLLER_SOURCE_ARCHIVE_BYTES,
                    failure_message="could not inventory tracked controller changes",
                    overflow_message="controller inventory exceeds its size bound",
                ),
                mock.call(
                    (
                        git_bin,
                        "-C",
                        str(root),
                        "ls-files",
                        "--others",
                        "--exclude-standard",
                        "-z",
                    ),
                    limit=live_m18.MAX_CONTROLLER_SOURCE_ARCHIVE_BYTES,
                    failure_message="could not inventory untracked controller files",
                    overflow_message="controller inventory exceeds its size bound",
                ),
            ],
        )

    def test_controller_identity_requires_git(self) -> None:
        with (
            mock.patch.object(live_m18.shutil, "which", return_value=None),
            self.assertRaises(RuntimeError) as caught,
        ):
            live_m18._controller_identity(self.root)
        self.assertEqual(
            str(caught.exception), "git is required to identify the M18 controller"
        )

    @unittest.skipUnless(shutil.which("git"), "git is required")
    def test_controller_identity_fails_closed_on_inventory_shape_and_drift(
        self,
    ) -> None:
        git_raw = shutil.which("git")
        assert git_raw is not None
        root = self.root.resolve(strict=True)
        (root / "a").write_bytes(b"12345")

        with (
            mock.patch.object(live_m18, "_git", return_value="a" * 40),
            mock.patch.object(
                live_m18, "_run_bounded_bytes", side_effect=(b"", b"a\0b\0")
            ),
            mock.patch.object(live_m18, "MAX_CONTROLLER_SOURCE_ARCHIVE_MEMBERS", 1),
            self.assertRaises(RuntimeError) as caught,
        ):
            live_m18._controller_identity(root)
        self.assertEqual(
            str(caught.exception), "controller inventory exceeds its member bound"
        )

        with (
            mock.patch.object(live_m18, "_git", return_value="a" * 40),
            mock.patch.object(
                live_m18, "_run_bounded_bytes", side_effect=(b"", b"a\0")
            ),
            mock.patch.object(live_m18, "MAX_CONTROLLER_SOURCE_ARCHIVE_BYTES", 4),
            self.assertRaises(RuntimeError) as caught,
        ):
            live_m18._controller_identity(root)
        self.assertEqual(
            str(caught.exception), "controller inventory exceeds its size bound"
        )

        opened = mock.MagicMock()
        opened.__enter__.return_value.read.side_effect = (b"1234", b"")
        with (
            mock.patch.object(live_m18, "_git", return_value="a" * 40),
            mock.patch.object(
                live_m18, "_run_bounded_bytes", side_effect=(b"", b"a\0")
            ),
            mock.patch.object(Path, "open", return_value=opened),
            self.assertRaises(RuntimeError) as caught,
        ):
            live_m18._controller_identity(root)
        self.assertEqual(
            str(caught.exception), "controller inventory changed while it was read"
        )

    @unittest.skipUnless(shutil.which("git"), "git is required")
    def test_controller_identity_accepts_exact_inventory_bounds(self) -> None:
        root = self.root.resolve(strict=True)
        (root / "a").write_bytes(b"12")
        (root / "b").write_bytes(b"34")

        with (
            mock.patch.object(live_m18, "_git", return_value="a" * 40),
            mock.patch.object(
                live_m18, "_run_bounded_bytes", side_effect=(b"", b"a\0b\0")
            ),
            mock.patch.object(live_m18, "MAX_CONTROLLER_SOURCE_ARCHIVE_MEMBERS", 2),
            mock.patch.object(live_m18, "MAX_CONTROLLER_SOURCE_ARCHIVE_BYTES", 4),
        ):
            observed = live_m18._controller_identity(root)

        self.assertTrue(observed["dirty"])

    @unittest.skipUnless(shutil.which("git"), "git is required")
    def test_controller_identity_applies_the_byte_bound_to_the_aggregate(self) -> None:
        root = self.root.resolve(strict=True)
        (root / "a").write_bytes(b"123")
        (root / "b").write_bytes(b"456")

        with (
            mock.patch.object(live_m18, "_git", return_value="a" * 40),
            mock.patch.object(
                live_m18, "_run_bounded_bytes", side_effect=(b"", b"a\0b\0")
            ),
            mock.patch.object(live_m18, "MAX_CONTROLLER_SOURCE_ARCHIVE_BYTES", 5),
            self.assertRaises(RuntimeError) as caught,
        ):
            live_m18._controller_identity(root)

        self.assertEqual(
            str(caught.exception), "controller inventory exceeds its size bound"
        )

    @unittest.skipUnless(shutil.which("git"), "git is required")
    def test_controller_identity_streams_and_counts_multi_chunk_files(self) -> None:
        root = self.root.resolve(strict=True)
        chunk_size = 64 * 1024
        (root / "chunked").write_bytes(b"x" * (chunk_size + 1))
        opened = mock.MagicMock()
        reader = opened.__enter__.return_value
        reader.read.side_effect = (b"x" * chunk_size, b"y", b"")

        with (
            mock.patch.object(live_m18, "_git", return_value="a" * 40),
            mock.patch.object(
                live_m18,
                "_run_bounded_bytes",
                side_effect=(b"", b"chunked\0"),
            ),
            mock.patch.object(Path, "open", return_value=opened),
        ):
            observed = live_m18._controller_identity(root)

        self.assertTrue(observed["dirty"])
        self.assertEqual(
            reader.read.call_args_list,
            [mock.call(chunk_size), mock.call(chunk_size), mock.call(chunk_size)],
        )

    @unittest.skipUnless(shutil.which("git"), "git is required")
    def test_controller_identity_rejects_untrusted_inventory_entries_exactly(
        self,
    ) -> None:
        root = self.root.resolve(strict=True)
        (root / "directory").mkdir()
        outside_temporary = tempfile.TemporaryDirectory()
        self.addCleanup(outside_temporary.cleanup)
        outside = Path(outside_temporary.name) / "outside"
        outside.write_bytes(b"outside")
        (root / "link").symlink_to(outside)
        cases = (
            (b"missing\0", "controller inventory is unavailable"),
            (b"directory\0", "controller inventory contains a non-regular entry"),
            (b"link\0", "controller inventory escaped the repository"),
        )
        for inventory, expected in cases:
            with (
                self.subTest(inventory=inventory),
                mock.patch.object(live_m18, "_git", return_value="a" * 40),
                mock.patch.object(
                    live_m18,
                    "_run_bounded_bytes",
                    side_effect=(b"", inventory),
                ),
                self.assertRaises(RuntimeError) as caught,
            ):
                live_m18._controller_identity(root)
            self.assertEqual(str(caught.exception), expected)

    @unittest.skipUnless(shutil.which("git"), "git is required")
    def test_controller_identity_refuses_an_oversized_untracked_file(self) -> None:
        self._init_controller_repository()
        (self.root / "oversized.bin").write_bytes(b"12345")

        with (
            mock.patch.object(live_m18, "MAX_CONTROLLER_SOURCE_ARCHIVE_BYTES", 4),
            self.assertRaisesRegex(RuntimeError, "inventory exceeds its size bound"),
        ):
            live_m18._controller_identity(self.root)

    def test_report_projects_only_the_two_live_proof_observation_kinds(self) -> None:
        records = (
            (
                "activation.missing_binding_refused",
                {"project_key": MGB_PROJECT, "capability": "landing.invoke"},
            ),
            ("unrelated", {"ignored": True}),
            (
                "target_hold.isolation_readback",
                {"lifted_hold": {"id": "lth_mgb"}, "other_hold": {"id": "lth_ik"}},
            ),
            (
                "target_hold.isolation_complete",
                {"holds": [{"id": "lth_mgb"}, {"id": "lth_ik"}]},
            ),
        )
        for kind, metadata in records:
            live_m18._append_observation(
                self.root,
                kind,
                "Live proof observation",
                **metadata,
            )

        self.assertEqual(
            live_m18._proof_observation_evidence(self.root),
            {
                "missing_binding_negatives": [
                    {"project_key": MGB_PROJECT, "capability": "landing.invoke"}
                ],
                "target_hold_isolation": [
                    {
                        "kind": "target_hold.isolation_readback",
                        "lifted_hold": {"id": "lth_mgb"},
                        "other_hold": {"id": "lth_ik"},
                    },
                    {
                        "kind": "target_hold.isolation_complete",
                        "holds": [{"id": "lth_mgb"}, {"id": "lth_ik"}],
                    },
                ],
            },
        )

    def test_preparation_proves_a_missing_binding_negative_per_project(self) -> None:
        state = self.root / "state"
        targets = {
            MGB_PROJECT: self.root / "mgb-target",
            IK_PROJECT: self.root / "ik-target",
        }
        for target in targets.values():
            target.mkdir()
        database = live_m18.Database(state / "control-plane.db")
        control = live_m18.ControlPlane(database)
        specs = _github_specs(
            state_root=state,
            account="psyberone",
            credential_scope_sha256="a" * 64,
            base_deployment_sha256="b" * 64,
        )

        executions = live_m18._prepare_control_plane(
            control,
            state_root=state,
            targets=targets,
            revisions={MGB_PROJECT: "c" * 40, IK_PROJECT: "d" * 40},
            worker_image="worker:test",
            validation_image_id="sha256:" + "e" * 64,
            connector_socket=state / "connector.sock",
            connector_deployment_sha256="f" * 64,
            credential_scope_sha256="1" * 64,
            github_specs=specs,
            budget_ceiling_microusd=1_700_000,
        )

        observations = [
            json.loads(line)
            for line in (state / "observations.jsonl").read_text().splitlines()
        ]
        negatives = [
            item
            for item in observations
            if item["kind"] == "activation.missing_binding_refused"
        ]
        self.assertEqual(
            [item["metadata"]["project_key"] for item in negatives],
            [MGB_PROJECT, IK_PROJECT],
        )
        for item in negatives:
            metadata = item["metadata"]
            self.assertEqual(metadata["capability"], "landing.invoke")
            self.assertEqual(
                metadata["availability"],
                {
                    "status": "unavailable",
                    "reason": "activation_requirements_unavailable",
                },
            )
            self.assertEqual(metadata["unavailable_activation_version"], 1)
            self.assertRegex(metadata["unavailable_binding_id"], r"^cpb_[0-9a-f]{32}$")
            self.assertEqual(
                metadata["refusal"],
                "workflow activation capability is unavailable",
            )
        for project_key, execution_id in executions.items():
            activation = database.get_workflow_activation(project_key)
            execution = database.get_workflow_execution(execution_id)
            self.assertEqual(activation["version"], 2)
            self.assertEqual(
                activation["availability"],
                {"status": "available", "reason": None},
            )
            self.assertEqual(
                execution["workflow_activation_version_id"],
                activation["workflow_activation_version_id"],
            )

    def test_harness_declares_genuine_deadline_bearing_closeouts(self) -> None:
        executions = {MGB_PROJECT: "wfx_mgb", IK_PROJECT: "wfx_ik"}

        class Database:
            def __init__(self) -> None:
                self.rows = {
                    execution_id: {
                        "id": execution_id,
                        "state": "active",
                        "stage": "completion",
                        "version": 7,
                        "closeout": None,
                    }
                    for execution_id in executions.values()
                }

            def get_workflow_execution(self, execution_id: str) -> dict[str, object]:
                return self.rows[execution_id]

        database = Database()

        class Control:
            def __init__(self) -> None:
                self.declarations: list[tuple[str, dict[str, object]]] = []

            def declare_workflow_closeout(
                self,
                execution_id: str,
                *,
                expected_version: int,
                declaration: object,
                declared_by_handle: str,
            ) -> dict[str, object]:
                self.assertions(execution_id, expected_version, declared_by_handle)
                value = declaration.to_dict()  # type: ignore[attr-defined]
                self.declarations.append((execution_id, value))
                database.rows[execution_id]["closeout"] = {"summary": "pending"}
                return {"id": "wcd_example", **value}

            @staticmethod
            def assertions(
                execution_id: str, expected_version: int, declared_by_handle: str
            ) -> None:
                if execution_id not in executions.values():
                    raise AssertionError("unexpected execution")
                if expected_version != 7 or declared_by_handle != "operator":
                    raise AssertionError("unexpected closeout authority")

        control = Control()
        fixed_now = "2026-11-11T12:00:00.000000Z"
        with mock.patch.object(live_m18, "_now", return_value=fixed_now):
            changed = live_m18._declare_ready_closeouts(
                control,  # type: ignore[arg-type]
                database,  # type: ignore[arg-type]
                executions,
                state_root=self.root,
            )

        self.assertTrue(changed)
        self.assertEqual(len(control.declarations), 2)
        expected_reasons = {
            "wfx_mgb": (
                "Re-run the full quality and isolated wheel proof from the landed "
                "primary on the remote validation host."
            ),
            "wfx_ik": (
                "Re-run the complete branch-aware suite from the landed primary on "
                "the remote validation host."
            ),
        }
        for execution_id, declaration in control.declarations:
            self.assertEqual(declaration["schema_version"], 1)
            self.assertEqual(len(declaration["obligations"]), 1)
            obligation = declaration["obligations"][0]
            self.assertEqual(obligation["kind"], "deferred_validation")
            self.assertEqual(obligation["obligation"], "full_quality")
            self.assertEqual(obligation["deadline"], "2026-11-18T12:00:00.000000Z")
            self.assertEqual(obligation["reason"], expected_reasons[execution_id])
        observation = json.loads(
            (self.root / live_m18.OBSERVATIONS_NAME).read_text(encoding="utf-8")
        )
        self.assertEqual(observation["kind"], "workflow.closeout_declared")
        self.assertEqual(
            [item["project_key"] for item in observation["metadata"]["declarations"]],
            [MGB_PROJECT, IK_PROJECT],
        )
        self.assertTrue(
            all(
                len(item["content_sha256"]) == 64
                and item["deadline"] == "2026-11-18T12:00:00.000000Z"
                for item in observation["metadata"]["declarations"]
            )
        )
        self.assertFalse(
            live_m18._declare_ready_closeouts(
                control,  # type: ignore[arg-type]
                database,  # type: ignore[arg-type]
                executions,
                state_root=self.root,
            )
        )

    def test_harness_waits_for_active_completion_before_closeout(self) -> None:
        executions = {MGB_PROJECT: "wfx_mgb", IK_PROJECT: "wfx_ik"}

        class Database:
            @staticmethod
            def get_workflow_execution(execution_id: str) -> dict[str, object]:
                return {
                    "id": execution_id,
                    "state": "active" if execution_id == "wfx_mgb" else "completed",
                    "stage": "landing" if execution_id == "wfx_mgb" else "completion",
                    "version": 7,
                    "closeout": None,
                }

        class Control:
            @staticmethod
            def declare_workflow_closeout(*args: object, **kwargs: object) -> None:
                raise AssertionError("ineligible executions must not close")

        self.assertFalse(
            live_m18._declare_ready_closeouts(
                Control(),  # type: ignore[arg-type]
                Database(),  # type: ignore[arg-type]
                executions,
                state_root=self.root,
            )
        )
        self.assertFalse((self.root / live_m18.OBSERVATIONS_NAME).exists())

    def test_harness_reads_the_other_hold_before_lifting_it(self) -> None:
        executions = {MGB_PROJECT: "wfx_mgb", IK_PROJECT: "wfx_ik"}

        class Database:
            @staticmethod
            def get_workflow_execution(execution_id: str) -> dict[str, object]:
                return {
                    "id": execution_id,
                    "state": "active",
                    "stage": "landing",
                    "version": 4,
                    "landing": {
                        "provider_landing_id": None,
                        "target_hold": None,
                    },
                }

        class Control:
            def __init__(self) -> None:
                self.holds: dict[str, dict[str, object]] = {}
                self.calls: list[str] = []
                self.reason_summaries: list[str] = []

            def place_integration_target_hold(
                self,
                execution_id: str,
                *,
                expected_execution_version: int,
                reason_summary: str,
                placed_by_handle: str,
                idempotency_key: str,
            ) -> dict[str, object]:
                self.calls.append(f"place:{execution_id}")
                self.reason_summaries.append(reason_summary)
                self.assert_authority(expected_execution_version, placed_by_handle)
                hold = {
                    "id": f"lth_{execution_id}",
                    "workflow_execution_id": execution_id,
                    "idempotency_key": idempotency_key,
                    "state": "open",
                    "version": 1,
                }
                self.holds[idempotency_key] = hold
                return dict(hold)

            def lift_integration_target_hold(
                self,
                hold_id: str,
                *,
                expected_version: int,
                lifted_by_handle: str,
            ) -> dict[str, object]:
                self.calls.append(f"lift:{hold_id}")
                self.assert_authority(expected_version, lifted_by_handle)
                hold = next(
                    item for item in self.holds.values() if item["id"] == hold_id
                )
                hold.update(state="lifted", version=2)
                return dict(hold)

            def list_integration_target_holds(
                self, *, open_only: bool = False
            ) -> list[dict[str, object]]:
                self.calls.append("list")
                return [
                    dict(hold)
                    for hold in self.holds.values()
                    if not open_only or hold["state"] == "open"
                ]

            @staticmethod
            def assert_authority(version: int, handle: str) -> None:
                if version not in {1, 4} or handle != "operator":
                    raise AssertionError("unexpected hold authority")

        control = Control()
        changed = live_m18._exercise_target_hold_isolation(
            control,  # type: ignore[arg-type]
            Database(),  # type: ignore[arg-type]
            executions,
            state_root=self.root,
        )

        self.assertTrue(changed)
        self.assertEqual(
            control.calls,
            [
                "list",
                "place:wfx_mgb",
                "place:wfx_ik",
                "list",
                "lift:lth_wfx_mgb",
                "list",
                "lift:lth_wfx_ik",
                "list",
            ],
        )
        self.assertEqual(
            control.reason_summaries,
            [
                (
                    "M18 live proof: demonstrate independent target-hold "
                    "isolation before provider dispatch."
                )
            ]
            * 2,
        )
        observations = [
            json.loads(line)
            for line in (self.root / "observations.jsonl").read_text().splitlines()
        ]
        readback = next(
            item
            for item in observations
            if item["kind"] == "target_hold.isolation_readback"
        )
        self.assertEqual(
            readback["metadata"],
            {
                "lifted_hold": {
                    "id": "lth_wfx_mgb",
                    "workflow_execution_id": "wfx_mgb",
                    "idempotency_key": live_m18.HOLD_ISOLATION_KEYS[MGB_PROJECT],
                    "state": "lifted",
                    "version": 2,
                },
                "other_hold": {
                    "id": "lth_wfx_ik",
                    "workflow_execution_id": "wfx_ik",
                    "idempotency_key": live_m18.HOLD_ISOLATION_KEYS[IK_PROJECT],
                    "state": "open",
                    "version": 1,
                },
            },
        )
        complete = next(
            item
            for item in observations
            if item["kind"] == "target_hold.isolation_complete"
        )
        self.assertEqual(
            [item["id"] for item in complete["metadata"]["holds"]],
            ["lth_wfx_mgb", "lth_wfx_ik"],
        )
        self.assertFalse(
            live_m18._exercise_target_hold_isolation(
                control,  # type: ignore[arg-type]
                Database(),  # type: ignore[arg-type]
                executions,
                state_root=self.root,
            )
        )

    def test_harness_recovers_after_both_isolation_holds_were_lifted(self) -> None:
        executions = {MGB_PROJECT: "wfx_mgb", IK_PROJECT: "wfx_ik"}

        class Database:
            @staticmethod
            def get_workflow_execution(execution_id: str) -> dict[str, object]:
                return {
                    "id": execution_id,
                    "state": "active",
                    "stage": "landing",
                    "version": 4,
                    "landing": {
                        "provider_landing_id": None,
                        "target_hold": None,
                    },
                }

        holds = {
            live_m18.HOLD_ISOLATION_KEYS[project_key]: {
                "id": f"lth_{execution_id}",
                "workflow_execution_id": execution_id,
                "idempotency_key": live_m18.HOLD_ISOLATION_KEYS[project_key],
                "state": "lifted",
                "version": 2,
            }
            for project_key, execution_id in executions.items()
        }

        class Control:
            @staticmethod
            def list_integration_target_holds(
                *, open_only: bool = False
            ) -> list[dict[str, object]]:
                return [
                    dict(hold)
                    for hold in holds.values()
                    if not open_only or hold["state"] == "open"
                ]

            @staticmethod
            def place_integration_target_hold(*args: object, **kwargs: object) -> None:
                raise AssertionError("existing proof holds must be reused")

            @staticmethod
            def lift_integration_target_hold(*args: object, **kwargs: object) -> None:
                raise AssertionError("lifted proof holds must not be lifted again")

        with self.assertRaisesRegex(RuntimeError, "read-back is missing"):
            live_m18._exercise_target_hold_isolation(
                Control(),  # type: ignore[arg-type]
                Database(),  # type: ignore[arg-type]
                executions,
                state_root=self.root,
            )
        live_m18._append_observation(
            self.root,
            "target_hold.isolation_readback",
            "Lifting one exact hold left the other project's hold open",
            lifted_hold={"id": "lth_wfx_mgb", "state": "lifted"},
            other_hold={"id": "lth_wfx_ik", "state": "open"},
        )

        self.assertTrue(
            live_m18._exercise_target_hold_isolation(
                Control(),  # type: ignore[arg-type]
                Database(),  # type: ignore[arg-type]
                executions,
                state_root=self.root,
            )
        )
        observations = [
            json.loads(line)
            for line in (self.root / "observations.jsonl").read_text().splitlines()
        ]
        self.assertEqual(
            [item["kind"] for item in observations],
            ["target_hold.isolation_readback", "target_hold.isolation_complete"],
        )

    def test_harness_waits_until_both_targets_are_ready_for_hold_isolation(
        self,
    ) -> None:
        executions = {MGB_PROJECT: "wfx_mgb", IK_PROJECT: "wfx_ik"}

        class Database:
            @staticmethod
            def get_workflow_execution(execution_id: str) -> dict[str, object]:
                return {
                    "id": execution_id,
                    "state": "active",
                    "stage": "landing" if execution_id == "wfx_mgb" else "review",
                    "version": 4,
                    "landing": {
                        "provider_landing_id": None,
                        "target_hold": None,
                    },
                }

        class Control:
            def __getattr__(self, name: str) -> object:
                raise AssertionError(f"control method {name} must not be called")

        self.assertFalse(
            live_m18._exercise_target_hold_isolation(
                Control(),  # type: ignore[arg-type]
                Database(),  # type: ignore[arg-type]
                executions,
                state_root=self.root,
            )
        )
        self.assertFalse((self.root / live_m18.OBSERVATIONS_NAME).exists())

    def test_harness_does_not_place_proof_holds_after_provider_authority_issues(
        self,
    ) -> None:
        executions = {MGB_PROJECT: "wfx_mgb", IK_PROJECT: "wfx_ik"}

        class Database:
            @staticmethod
            def get_workflow_execution(execution_id: str) -> dict[str, object]:
                return {
                    "id": execution_id,
                    "state": "active",
                    "stage": "landing",
                    "version": 4,
                    "landing": {
                        "provider_landing_id": (
                            "ipl_already_issued" if execution_id == "wfx_ik" else None
                        ),
                        "target_hold": None,
                    },
                }

        class Control:
            def __getattr__(self, name: str) -> object:
                raise AssertionError(f"control method {name} must not be called")

        self.assertFalse(
            live_m18._exercise_target_hold_isolation(
                Control(),  # type: ignore[arg-type]
                Database(),  # type: ignore[arg-type]
                executions,
                state_root=self.root,
            )
        )
        self.assertFalse((self.root / live_m18.OBSERVATIONS_NAME).exists())

    def test_existing_executions_retain_one_terminal_sibling(self) -> None:
        calls: list[tuple[str, str | None, int]] = []

        class Database:
            @staticmethod
            def list_workflow_executions(
                *, project_key: str, state: str | None = None, limit: int
            ) -> list[dict[str, str]]:
                calls.append((project_key, state, limit))
                executions = (
                    [{"id": "wfx_mgb_cancelled", "state": "cancelled"}]
                    if project_key == MGB_PROJECT
                    else [
                        {"id": "wfx_ik_cancelled", "state": "cancelled"},
                        {"id": "wfx_ik_active", "state": "active"},
                    ]
                )
                if state is not None:
                    executions = [item for item in executions if item["state"] == state]
                return executions[:limit]

        self.assertEqual(
            _existing_executions(Database()),  # type: ignore[arg-type]
            {
                "magic-glass-box-m18": "wfx_mgb_cancelled",
                "imitation-krab-m18": "wfx_ik_active",
            },
        )
        self.assertEqual(
            calls,
            [
                ("magic-glass-box-m18", None, 2),
                ("imitation-krab-m18", None, 2),
            ],
        )

    def test_execution_lineage_and_runs_include_cancelled_and_every_generation(
        self,
    ) -> None:
        calls: list[tuple[object, ...]] = []

        class Database:
            @staticmethod
            def get_workflow_execution(execution_id: str) -> dict[str, str]:
                calls.append(("get", execution_id))
                return {
                    "id": execution_id,
                    "work_item_id": "wki_target",
                    "created_at": "2026-11-06T12:01:00Z",
                }

            @staticmethod
            def list_workflow_executions(
                *, project_key: str, limit: int
            ) -> list[dict[str, str]]:
                calls.append(("list", project_key, limit))
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
        self.assertEqual(
            calls[:2],
            [
                ("get", "wfx_current"),
                ("list", MGB_PROJECT, live_m18.EXECUTION_LINEAGE_LIMIT),
            ],
        )

    def test_execution_lineage_fails_closed_when_its_bound_is_reached(self) -> None:
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
                del project_key
                return [
                    {
                        "id": f"wfx_{index:03d}",
                        "work_item_id": "wki_target",
                        "created_at": "2026-11-06T12:00:00Z",
                    }
                    for index in range(limit)
                ]

        with self.assertRaisesRegex(RuntimeError, "lineage reached its bounded limit"):
            _execution_lineage(
                Database(),  # type: ignore[arg-type]
                {"magic-glass-box-m18": "wfx_000"},
            )

    def test_provider_evidence_keeps_bounded_transport_byte_counts(self) -> None:
        evidence = _provider_invocation_evidence(
            {
                "id": "pinv_diagnostic",
                "outcome": "ambiguous",
                "error_code": "upstream_stream_duplicate_response_completed",
                "request_bytes": 43_432,
                "response_bytes": 109_238,
            },
            execution_id="wfx_diagnostic",
            run_id="run_diagnostic",
        )

        self.assertEqual(evidence["request_bytes"], 43_432)
        self.assertEqual(evidence["response_bytes"], 109_238)

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

    def test_mgb_wheel_validation_builds_from_bounded_scratch_copy(self) -> None:
        configuration = live_m18._configuration(
            MGB_PROJECT,
            "sha256:" + "a" * 64,
        )
        command = configuration.validations[0].command

        self.assertEqual(command[:2], ("/bin/sh", "-c"))
        recipe = command[2]
        self.assertIn(
            'cp -R /workspace/. "$TMPDIR/m18-build-source"',
            recipe,
        )
        self.assertIn('cd "$TMPDIR/m18-build-source"', recipe)
        self.assertIn('--outdir "$TMPDIR/m18-dist"', recipe)
        self.assertNotIn("--outdir /tmp/m18-dist", recipe)
        self.assertLess(recipe.index("make PYTHON="), recipe.index("cp -R "))

    def test_ik_work_item_tracks_the_open_participant_help_issue(self) -> None:
        work_item = live_m18._work_item(IK_PROJECT)

        self.assertEqual(work_item.source, "github")
        self.assertEqual(work_item.external_id, "11")
        self.assertEqual(
            work_item.title,
            "Clarify participant handles and message body limits in CLI help",
        )
        self.assertIn(
            "session-create --participant accepts a handle",
            work_item.brief,
        )
        self.assertIn(
            "Boundary-size and oversize UTF-8 bodies produce documented outcomes.",
            work_item.acceptance_criteria,
        )
        self.assertIn(
            "Do not accept user IDs as a second participant identifier.",
            work_item.constraints,
        )

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

        class CandidateStore:
            calls = 0

            def ensure_root(self) -> Path:
                self.calls += 1
                root = self_root / "candidates"
                root.mkdir()
                return root

        self_root = self.root
        candidate_store = CandidateStore()

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
                candidate_store=candidate_store,  # type: ignore[arg-type]
                state_root=self.root,
                source_root=self.root / "source",
                gh_config_dir=self.root / "gh-config",
                gh_bin=Path("/usr/bin/gh"),
                git_bin=Path("/usr/bin/git"),
            )

        self.assertEqual(observed, processes)
        self.assertEqual(candidate_store.calls, 1)
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

    def test_existing_state_retains_registered_github_authority(self) -> None:
        registered = _github_specs(
            state_root=self.root,
            account="psyberone",
            credential_scope_sha256="a" * 64,
            base_deployment_sha256="b" * 64,
        )
        current = _github_specs(
            state_root=self.root,
            account="psyberone",
            credential_scope_sha256="a" * 64,
            base_deployment_sha256="c" * 64,
        )
        names = {
            "github-m18-mgb": MGB_PROJECT,
            "github-m18-ik": IK_PROJECT,
        }

        class Database:
            @staticmethod
            def get_service_sidecar(name: str) -> dict[str, object]:
                spec = registered[names[name]]
                return {
                    "state": "active",
                    "content_sha256": spec.digest,
                    "spec": spec.to_dict(),
                }

        selected = _registered_github_specs(
            Database(),  # type: ignore[arg-type]
            expected=current,
        )

        self.assertEqual(selected, registered)
        invalid = registered[MGB_PROJECT].to_dict()
        invalid["mappings"][0]["repository"] = "intruder/repository"
        with (
            mock.patch.object(
                Database,
                "get_service_sidecar",
                return_value={
                    "state": "active",
                    "content_sha256": live_m18.ServiceSidecarSpec.from_mapping(
                        invalid
                    ).digest,
                    "spec": invalid,
                },
            ),
            self.assertRaisesRegex(RuntimeError, "authority changed"),
        ):
            _registered_github_specs(
                Database(),  # type: ignore[arg-type]
                expected=current,
            )

    def test_changed_controller_recovers_exact_registered_github_source(self) -> None:
        repository = self.root / "controller"
        state = self.root / "state"
        git_raw = shutil.which("git")
        if git_raw is None:
            self.skipTest("git is unavailable")
        git = str(Path(git_raw).resolve())
        package = repository / "src" / "magic_glass_box"
        storage = package / "storage"
        storage.mkdir(parents=True)
        (package / "__init__.py").write_text("VERSION = 'old'\n", encoding="utf-8")
        (package / "github_provider.py").write_text(
            "PROVIDER = 'old'\n", encoding="utf-8"
        )
        (storage / "__init__.py").write_text("", encoding="utf-8")
        (storage / "core.py").write_text("STATE = 'old'\n", encoding="utf-8")
        live_m18._run_checked(
            (git, "init", "--quiet", "--initial-branch", "main"), cwd=repository
        )
        live_m18._run_checked((git, "add", "."), cwd=repository)
        live_m18._run_checked(
            (
                git,
                "-c",
                "user.name=MGB Tests",
                "-c",
                "user.email=mgb@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "old",
            ),
            cwd=repository,
        )
        old_commit = live_m18._run_checked((git, "rev-parse", "HEAD"), cwd=repository)
        old_deployment = live_m18.workflow._github_deployment_sha256(repository / "src")
        specs = _github_specs(
            state_root=state,
            account="psyberone",
            credential_scope_sha256="a" * 64,
            base_deployment_sha256=old_deployment,
        )
        (package / "github_provider.py").write_text(
            "PROVIDER = 'new'\n", encoding="utf-8"
        )
        live_m18._run_checked((git, "add", "."), cwd=repository)
        live_m18._run_checked(
            (
                git,
                "-c",
                "user.name=MGB Tests",
                "-c",
                "user.email=mgb@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "new",
            ),
            cwd=repository,
        )
        new_commit = live_m18._run_checked((git, "rev-parse", "HEAD"), cwd=repository)
        state.mkdir()
        (state / "observations.jsonl").write_text(
            json.dumps(
                {
                    "kind": "controller.code_change",
                    "metadata": {
                        "previous": {"commit": old_commit},
                        "replacement": {"commit": new_commit},
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )

        with mock.patch.object(
            live_m18.tarfile.TarFile,
            "extractall",
            side_effect=AssertionError("unbounded archive extraction is forbidden"),
        ):
            selected = _resolve_github_source_root(
                project_root=repository,
                current_source_root=repository / "src",
                state_root=state,
                current_commit=new_commit,
                specs=specs,
            )

        self.assertNotEqual(selected, repository / "src")
        self.assertEqual(
            (selected / "magic_glass_box" / "github_provider.py").read_text(),
            "PROVIDER = 'old'\n",
        )
        self.assertEqual(
            (selected / "magic_glass_box" / "storage" / "core.py").read_text(),
            "STATE = 'old'\n",
        )
        self.assertFalse((selected.parent / ".git").exists())
        self.assertEqual(
            _resolve_github_source_root(
                project_root=repository,
                current_source_root=repository / "src",
                state_root=state,
                current_commit=new_commit,
                specs=specs,
            ),
            selected,
        )
        recovery_observations = [
            json.loads(line)
            for line in (state / "observations.jsonl").read_text().splitlines()
            if json.loads(line).get("kind") == "sidecar.deployment_pinned"
        ]
        self.assertEqual(len(recovery_observations), 1)
        self.assertEqual(recovery_observations[0]["metadata"]["commit"], old_commit)

    def test_legacy_controller_archive_fallback_rejects_traversal(self) -> None:
        raw = live_m18.io.BytesIO()
        with live_m18.tarfile.open(fileobj=raw, mode="w:") as archive:
            member = live_m18.tarfile.TarInfo("src/magic_glass_box/../../escape.py")
            member.size = 1
            archive.addfile(member, live_m18.io.BytesIO(b"x"))
        raw.seek(0)

        with (
            live_m18.tarfile.open(fileobj=raw, mode="r:") as archive,
            self.assertRaisesRegex(RuntimeError, "archive is unsafe"),
        ):
            live_m18._controller_archive_members(archive)

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

        self.assertEqual(delegates[mgb_socket].calls, ["sop_mgb", "sop_mgb_second"])
        self.assertEqual(delegates[ik_socket].calls, ["sop_ik"])

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
                self.uncertain: set[str] = set()

            def get_workflow_execution(self, execution_id: str) -> dict[str, object]:
                landing = None
                if execution_id in self.uncertain:
                    landing = {
                        "provider_intent": {
                            "effects": {
                                "refs.publish": {
                                    "operation_state": "uncertain",
                                    "service_operation_id": f"sop_lost_{execution_id}",
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
                database.uncertain.add("wfx_mgb")
                return {"workflow": True}

            def drain(self, *, timeout: int) -> None:
                self.drained = timeout == 30
                database.uncertain.add("wfx_ik")

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
            [effect["operation_id"] for effect in observation["metadata"]["effects"]],
            ["sop_lost_wfx_mgb", "sop_lost_wfx_ik"],
        )

    def test_drive_stops_after_workflow_requires_control_room_attention(self) -> None:
        class Database:
            def __init__(self) -> None:
                self.attention = False

            def get_workflow_execution(self, execution_id: str) -> dict[str, object]:
                responsibility = {
                    "status": "action_required",
                    "holder": "control_plane",
                    "reason": "landing_dispatch",
                }
                if self.attention and execution_id == "wfx_mgb":
                    responsibility = {
                        "status": "attention",
                        "holder": "control_room",
                        "reason": "provider_effect_unresolved",
                    }
                return {
                    "id": execution_id,
                    "state": "active",
                    "stage": "landing",
                    "correction_round": 0,
                    "landing": None,
                    "responsibility": responsibility,
                    "rulings": [],
                }

        database = Database()

        class Supervisor:
            drained = False

            def tick(self) -> dict[str, bool]:
                database.attention = True
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
            def before_tick(self) -> None:
                return None

        supervisor = Supervisor()
        with mock.patch.object(live_m18, "PROOF_TIMEOUT_SECONDS", 0.01):
            reason = _drive_one_technical_stop(
                supervisor=supervisor,  # type: ignore[arg-type]
                database=database,  # type: ignore[arg-type]
                control=Control(),  # type: ignore[arg-type]
                executions={MGB_PROJECT: "wfx_mgb", IK_PROJECT: "wfx_ik"},
                connector_waves=Waves(),  # type: ignore[arg-type]
                state_root=self.root,
            )

        self.assertEqual(reason, "workflow_attention_required")
        self.assertTrue(supervisor.drained)
        observation = json.loads(
            (self.root / "observations.jsonl").read_text(encoding="utf-8")
        )
        self.assertEqual(observation["kind"], "workflow.attention_required")
        self.assertEqual(
            observation["metadata"]["attention"],
            [
                {
                    "execution_id": "wfx_mgb",
                    "holder": "control_room",
                    "project_key": MGB_PROJECT,
                    "reason": "provider_effect_unresolved",
                }
            ],
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

    def test_drive_and_report_status_preserve_terminal_incomplete_proof(self) -> None:
        class Database:
            @staticmethod
            def get_workflow_execution(execution_id: str) -> dict[str, str]:
                return {
                    "id": execution_id,
                    "state": "cancelled" if execution_id == "wfx_mgb" else "completed",
                }

        class Supervisor:
            drained = False

            def drain(self, *, timeout: int) -> None:
                self.drained = timeout == 30

            @staticmethod
            def tick() -> dict[str, bool]:
                raise AssertionError("terminal proof must not tick")

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

        self.assertEqual(reason, "all_terminal_incomplete")
        self.assertTrue(supervisor.drained)
        self.assertEqual(
            _proof_status(database, executions),  # type: ignore[arg-type]
            "terminal_incomplete",
        )


if __name__ == "__main__":
    unittest.main()
