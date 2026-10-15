from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from magic_glass_box.domain import (
    AgentProfileSpec,
    GatewayCapability,
    NetworkMode,
    WorkspaceMode,
)
from magic_glass_box.executor import (
    DockerCLI,
    DockerExecutor,
    DockerImage,
    DockerLimits,
    ExecutionError,
    SandboxPlan,
    SandboxRequest,
    SandboxResult,
    build_sandbox_plan,
)
from magic_glass_box.gateway import (
    GATEWAY_REQUEST_NAME,
    GATEWAY_REQUEST_READY_NAME,
    GATEWAY_RESPONSE_NAME,
    GATEWAY_RESPONSE_READY_NAME,
)
from magic_glass_box.runtime import RuntimeLaunch

RUN_ID = "run_" + "1" * 32
IMAGE_ID = "sha256:" + "a" * 64
CONTAINER_ID = "b" * 64


class SandboxPlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir(mode=0o700)
        self.token = self.root / "krab.token"
        self.token.write_text("secret-material", encoding="ascii")
        os.chmod(self.token, 0o600)

    def request(
        self,
        *,
        workspace_mode: WorkspaceMode = WorkspaceMode.READ_ONLY,
        network_mode: NetworkMode = NetworkMode.NONE,
        credential_names: tuple[str, ...] = (),
        credential_files: dict[str, Path] | None = None,
        gateway_capabilities: tuple[GatewayCapability, ...] = (),
        gateway_directory: Path | None = None,
    ) -> SandboxRequest:
        profile = AgentProfileSpec.create(
            image="agent:local",
            command=["agent", "--run"],
            workspace_mode=workspace_mode,
            network_mode=network_mode,
            credential_names=credential_names,
            gateway_capabilities=gateway_capabilities,
        )
        return SandboxRequest(
            run_id=RUN_ID,
            profile=profile,
            workspace=self.workspace,
            launch=RuntimeLaunch(
                ("runtime-agent", "--headless"),
                (("RUNTIME_MODE", "strict"),),
            ),
            credential_files={} if credential_files is None else credential_files,
            gateway_directory=gateway_directory,
        )

    def private_gateway_directory(self) -> Path:
        root = self.root / "gateway"
        root.mkdir(mode=0o700)
        for name in (
            GATEWAY_REQUEST_NAME,
            GATEWAY_REQUEST_READY_NAME,
            GATEWAY_RESPONSE_NAME,
            GATEWAY_RESPONSE_READY_NAME,
        ):
            path = root / name
            path.touch(mode=0o600)
            os.chmod(path, 0o600)
        return root

    def test_read_only_plan_has_complete_security_policy(self) -> None:
        plan = build_sandbox_plan(
            self.request(),
            DockerImage(IMAGE_ID, ()),
            uid=os.getuid(),
            gid=os.getgid(),
        )
        self.assertEqual(plan.run_id, RUN_ID)
        self.assertEqual(plan.container_name, "magic-glass-box-" + "1" * 32)
        self.assertEqual(plan.image_id, IMAGE_ID)
        self.assertEqual(
            plan.create_args,
            (
                "container",
                "create",
                "--name",
                "magic-glass-box-" + "1" * 32,
                "--label",
                "io.magicglassbox.managed=true",
                "--label",
                f"io.magicglassbox.run-id={RUN_ID}",
                "--pull",
                "never",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges=true",
                "--network",
                "none",
                "--ipc",
                "none",
                "--pids-limit",
                "256",
                "--memory",
                str(2 * 1024 * 1024 * 1024),
                "--cpus",
                "2",
                "--ulimit",
                "nofile=1024:1024",
                "--stop-timeout",
                "10",
                "--init",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "--hostname",
                "magic-glass-box-agent",
                "--workdir",
                "/workspace",
                "--tmpfs",
                f"/tmp:rw,nosuid,nodev,noexec,size={256 * 1024 * 1024}",  # noqa: S108
                "--log-driver",
                "local",
                "--log-opt",
                "max-size=10m",
                "--log-opt",
                "max-file=2",
                "--no-healthcheck",
                "--env",
                "CI=1",
                "--env",
                "HOME=/tmp",
                "--env",
                f"MAGIC_GLASS_BOX_RUN_ID={RUN_ID}",
                "--env",
                "RUNTIME_MODE=strict",
                "--mount",
                (f"type=bind,src={self.workspace.resolve()},dst=/workspace,readonly"),
                "--entrypoint",
                "runtime-agent",
                IMAGE_ID,
                "--headless",
            ),
        )
        self.assertNotIn("secret-material", "\n".join(plan.create_args))

    def test_copy_on_write_workspace_is_the_only_writable_bind(self) -> None:
        request = self.request(
            workspace_mode=WorkspaceMode.COPY_ON_WRITE,
            credential_names=(),
            credential_files={},
        )
        plan = build_sandbox_plan(
            request,
            DockerImage(IMAGE_ID, ()),
            uid=os.getuid(),
            gid=os.getgid(),
        )
        mounts = [
            plan.create_args[index + 1]
            for index, value in enumerate(plan.create_args)
            if value == "--mount"
        ]
        self.assertEqual(
            mounts,
            [f"type=bind,src={self.workspace.resolve()},dst=/workspace"],
        )
        self.assertNotIn(
            "KRAB_TOKEN_FILE=/run/secrets/magic-glass-box/krab", plan.create_args
        )

    def test_gateway_only_plan_mounts_only_the_mailbox_and_no_credentials(
        self,
    ) -> None:
        gateway_directory = self.private_gateway_directory()
        plan = build_sandbox_plan(
            self.request(
                network_mode=NetworkMode.GATEWAY_ONLY,
                credential_names=("krab",),
                credential_files={},
                gateway_capabilities=(
                    GatewayCapability.RUN_DESCRIBE,
                    GatewayCapability.INTERACTION_OPEN,
                    GatewayCapability.INTERACTION_READ,
                ),
                gateway_directory=gateway_directory,
            ),
            DockerImage(IMAGE_ID, ()),
            uid=os.getuid(),
            gid=os.getgid(),
        )

        arguments = "\n".join(plan.create_args)
        self.assertIn(
            "MAGIC_GLASS_BOX_GATEWAY_DIR=/run/magic-glass-box",
            plan.create_args,
        )
        self.assertIn(
            f"type=bind,src={gateway_directory.resolve()},"
            "dst=/run/magic-glass-box,readonly",
            plan.create_args,
        )
        for name in (GATEWAY_REQUEST_NAME, GATEWAY_REQUEST_READY_NAME):
            self.assertIn(
                f"type=bind,src={gateway_directory.resolve() / name},"
                f"dst=/run/magic-glass-box/{name}",
                plan.create_args,
            )
        for name in (GATEWAY_RESPONSE_NAME, GATEWAY_RESPONSE_READY_NAME):
            self.assertNotIn(
                f"type=bind,src={gateway_directory.resolve() / name},"
                f"dst=/run/magic-glass-box/{name}",
                plan.create_args,
            )
        self.assertNotIn("KRAB_TOKEN_FILE", arguments)
        self.assertNotIn("MAGIC_GLASS_BOX_CREDENTIAL_DIR", arguments)
        self.assertNotIn(str(self.token), arguments)
        network_index = plan.create_args.index("--network")
        self.assertEqual(plan.create_args[network_index + 1], "none")

    def test_gateway_policy_rejects_missing_or_competing_authority_paths(self) -> None:
        gateway_directory = self.private_gateway_directory()
        gateway_profile = {
            "network_mode": NetworkMode.GATEWAY_ONLY,
            "credential_names": ("krab",),
            "gateway_capabilities": (GatewayCapability.INTERACTION_READ,),
        }
        cases = (
            (
                self.request(
                    **gateway_profile,
                    credential_files={},
                ),
                "gateway endpoint is required",
            ),
            (
                self.request(
                    **gateway_profile,
                    credential_files={"krab": self.token},
                    gateway_directory=gateway_directory,
                ),
                "must not receive reusable credential files",
            ),
            (
                self.request(
                    network_mode=NetworkMode.NONE,
                    credential_names=(),
                    credential_files={},
                    gateway_directory=gateway_directory,
                ),
                "requires gateway_only",
            ),
            (
                self.request(network_mode=NetworkMode.KRAB_ONLY),
                "krab_only networking is retired",
            ),
        )
        for candidate, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(ExecutionError, message),
            ):
                build_sandbox_plan(
                    candidate,
                    DockerImage(IMAGE_ID, ()),
                    uid=os.getuid(),
                    gid=os.getgid(),
                )

    def test_executor_rejects_reusable_credentials_even_for_legacy_profiles(
        self,
    ) -> None:
        legacy = AgentProfileSpec(
            image="agent:local",
            command=("agent", "--run"),
            workspace_mode=WorkspaceMode.READ_ONLY,
            network_mode=NetworkMode.NONE,
            credential_names=("krab",),
        )
        request = SandboxRequest(
            run_id=RUN_ID,
            profile=legacy,
            workspace=self.workspace,
            launch=RuntimeLaunch(("runtime-agent", "--headless")),
            credential_files={"krab": self.token},
        )

        with self.assertRaisesRegex(ExecutionError, "reusable credential"):
            build_sandbox_plan(
                request,
                DockerImage(IMAGE_ID, ()),
                uid=os.getuid(),
                gid=os.getgid(),
            )

    def test_runtime_environment_cannot_override_sandbox_policy(self) -> None:
        request = self.request(credential_names=(), credential_files={})
        request = SandboxRequest(
            run_id=request.run_id,
            profile=request.profile,
            workspace=request.workspace,
            launch=RuntimeLaunch(("worker",), (("HOME", "/workspace"),)),
            credential_files={},
        )

        with self.assertRaisesRegex(ExecutionError, "reserved environment"):
            build_sandbox_plan(
                request,
                DockerImage(IMAGE_ID, ()),
                uid=os.getuid(),
                gid=os.getgid(),
            )

    def test_gateway_mailbox_rejects_unsafe_filesystem_shapes(self) -> None:
        def request_for(path: Path) -> SandboxRequest:
            return self.request(
                network_mode=NetworkMode.GATEWAY_ONLY,
                credential_names=("krab",),
                credential_files={},
                gateway_capabilities=(GatewayCapability.INTERACTION_READ,),
                gateway_directory=path,
            )

        missing = self.root / "missing-gateway"
        with self.assertRaisesRegex(ExecutionError, "endpoint is unavailable"):
            build_sandbox_plan(
                request_for(missing),
                DockerImage(IMAGE_ID, ()),
                uid=os.getuid(),
                gid=os.getgid(),
            )

        regular = self.root / "gateway-file"
        regular.touch()
        with self.assertRaisesRegex(ExecutionError, "private directory"):
            build_sandbox_plan(
                request_for(regular),
                DockerImage(IMAGE_ID, ()),
                uid=os.getuid(),
                gid=os.getgid(),
            )

        gateway = self.private_gateway_directory()
        os.chmod(gateway, 0o755)  # noqa: S103 - exercise rejection
        with self.assertRaisesRegex(ExecutionError, "mode 0700"):
            build_sandbox_plan(
                request_for(gateway),
                DockerImage(IMAGE_ID, ()),
                uid=os.getuid(),
                gid=os.getgid(),
            )
        os.chmod(gateway, 0o700)

        response = gateway / GATEWAY_RESPONSE_NAME
        os.chmod(response, 0o644)
        with self.assertRaisesRegex(ExecutionError, "mode 0600"):
            build_sandbox_plan(
                request_for(gateway),
                DockerImage(IMAGE_ID, ()),
                uid=os.getuid(),
                gid=os.getgid(),
            )
        os.chmod(response, 0o600)

        response.unlink()
        with self.assertRaisesRegex(ExecutionError, "mailbox shape"):
            build_sandbox_plan(
                request_for(gateway),
                DockerImage(IMAGE_ID, ()),
                uid=os.getuid(),
                gid=os.getgid(),
            )

    def test_plan_rejects_network_or_identity_policy_weakening(self) -> None:
        for request, uid, gid, message in (
            (
                self.request(network_mode=NetworkMode.KRAB_ONLY),
                501,
                20,
                "krab_only networking is retired; use the run-scoped gateway",
            ),
            (self.request(), 0, 20, "sandbox user must not be root"),
            (self.request(), 501, 0, "sandbox group must not be root"),
        ):
            with (
                self.subTest(message=message),
                self.assertRaises(ExecutionError) as caught,
            ):
                build_sandbox_plan(
                    request,
                    DockerImage(IMAGE_ID, ()),
                    uid=uid,
                    gid=gid,
                )
            self.assertEqual(str(caught.exception), message)

    def test_positive_numeric_id_boundaries_and_types_are_enforced(self) -> None:
        request = self.request(credential_names=(), credential_files={})
        plan = build_sandbox_plan(
            request,
            DockerImage(IMAGE_ID, ()),
            uid=1,
            gid=1,
        )
        user_index = plan.create_args.index("--user")
        self.assertEqual(plan.create_args[user_index + 1], "1:1")

        for uid, gid, message in (
            (1.5, 1, "sandbox user must not be root"),
            (1, "20", "sandbox group must not be root"),
        ):
            with (
                self.subTest(uid=uid, gid=gid),
                self.assertRaises(ExecutionError) as caught,
            ):
                build_sandbox_plan(
                    request,
                    DockerImage(IMAGE_ID, ()),
                    uid=uid,  # type: ignore[arg-type]
                    gid=gid,  # type: ignore[arg-type]
                )
            self.assertEqual(str(caught.exception), message)

    def test_plan_rejects_bad_run_and_workspace_shapes(self) -> None:
        missing_workspace = self.root / "missing"
        workspace_link = self.root / "workspace-link"
        workspace_link.symlink_to(self.workspace, target_is_directory=True)
        workspace_file = self.root / "workspace-file"
        workspace_file.write_text("not a directory", encoding="utf-8")
        comma_workspace = self.root / "work,space"
        comma_workspace.mkdir()
        cases = (
            (
                SandboxRequest(
                    "run_bad",
                    self.request().profile,
                    self.workspace,
                    self.request().launch,
                    {},
                ),
                "invalid run identifier",
            ),
            (
                SandboxRequest(
                    RUN_ID,
                    self.request().profile,
                    missing_workspace,
                    self.request().launch,
                    {},
                ),
                f"workspace is unavailable: {missing_workspace.absolute()}",
            ),
            (
                SandboxRequest(
                    RUN_ID,
                    self.request().profile,
                    workspace_link,
                    self.request().launch,
                    {},
                ),
                "workspace must not be a symlink",
            ),
            (
                SandboxRequest(
                    RUN_ID,
                    self.request().profile,
                    workspace_file,
                    self.request().launch,
                    {},
                ),
                "workspace must be a directory",
            ),
            (
                SandboxRequest(
                    RUN_ID,
                    self.request(credential_names=(), credential_files={}).profile,
                    Path("/"),
                    self.request().launch,
                    {},
                ),
                "workspace must not be a filesystem root",
            ),
            (
                SandboxRequest(
                    RUN_ID,
                    self.request(credential_names=(), credential_files={}).profile,
                    comma_workspace,
                    self.request().launch,
                    {},
                ),
                "workspace path must not contain a comma",
            ),
        )
        for request, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaises(ExecutionError) as caught,
            ):
                build_sandbox_plan(
                    request,
                    DockerImage(IMAGE_ID, ()),
                    uid=os.getuid(),
                    gid=os.getgid(),
                )
            self.assertEqual(str(caught.exception), message)

    def test_copy_on_write_workspace_must_be_owned_and_writable(self) -> None:
        with self.assertRaises(ExecutionError) as caught:
            build_sandbox_plan(
                self.request(
                    workspace_mode=WorkspaceMode.COPY_ON_WRITE,
                    credential_names=(),
                    credential_files={},
                ),
                DockerImage(IMAGE_ID, ()),
                uid=os.getuid() + 1,
                gid=os.getgid(),
            )
        self.assertEqual(
            str(caught.exception),
            "copy-on-write workspace is not writable by the run user",
        )

    def test_plan_rejects_invalid_or_volume_declaring_image(self) -> None:
        for image, message in (
            (
                DockerImage("not-an-image-id", ()),
                "Docker returned an invalid immutable image ID",
            ),
            (
                DockerImage(None, ()),  # type: ignore[arg-type]
                "Docker returned an invalid immutable image ID",
            ),
            (
                DockerImage(IMAGE_ID, ("/data",)),
                "agent image declares writable volumes",
            ),
        ):
            with self.subTest(image=image), self.assertRaises(ExecutionError) as caught:
                build_sandbox_plan(
                    self.request(),
                    image,
                    uid=os.getuid(),
                    gid=os.getgid(),
                )
            self.assertEqual(str(caught.exception), message)

    def test_limits_reject_values_outside_hard_policy_bounds(self) -> None:
        invalid = (
            {"memory_bytes": 64 * 1024 * 1024 - 1},
            {"memory_bytes": 64 * 1024 * 1024 * 1024 + 1},
            {"cpus": 0},
            {"cpus": 65},
            {"cpus": True},
            {"pids_limit": 15},
            {"pids_limit": 4097},
            {"tmpfs_bytes": 16 * 1024 * 1024 - 1},
            {"tmpfs_bytes": 4 * 1024 * 1024 * 1024 + 1},
            {"stop_timeout": 0},
            {"stop_timeout": 301},
            {"memory_bytes": "67108864"},
        )
        for kwargs in invalid:
            with self.subTest(kwargs=kwargs), self.assertRaises(ExecutionError):
                DockerLimits(**kwargs)

    def test_limits_accept_the_inclusive_policy_boundaries(self) -> None:
        minimum = DockerLimits(
            memory_bytes=64 * 1024 * 1024,
            cpus=0.1,
            pids_limit=16,
            tmpfs_bytes=16 * 1024 * 1024,
            stop_timeout=1,
        )
        maximum = DockerLimits(
            memory_bytes=64 * 1024 * 1024 * 1024,
            cpus=64,
            pids_limit=4096,
            tmpfs_bytes=4 * 1024 * 1024 * 1024,
            stop_timeout=300,
        )
        self.assertEqual(minimum.pids_limit, 16)
        self.assertEqual(maximum.pids_limit, 4096)


class FakeDockerEngine:
    def __init__(self) -> None:
        self.image = DockerImage(IMAGE_ID, ())
        self.exit_code = 0
        self.stdout = "runtime output\n"
        self.failure: tuple[str, Exception] | None = None
        self.managed: tuple[str, ...] = ()
        self.calls: list[tuple[str, object]] = []

    def _call(self, operation: str, value: object) -> None:
        self.calls.append((operation, value))
        if self.failure is not None and self.failure[0] == operation:
            raise self.failure[1]

    def inspect_image(self, reference: str) -> DockerImage:
        self._call("inspect", reference)
        return self.image

    def create(self, plan: SandboxPlan) -> str:
        self._call("create", plan)
        return CONTAINER_ID

    def start(self, container_id: str) -> None:
        self._call("start", container_id)

    def wait(self, container_id: str) -> int:
        self._call("wait", container_id)
        return self.exit_code

    def logs(self, container_id: str) -> str:
        self._call("logs", container_id)
        return self.stdout

    def stop(self, container_id: str) -> None:
        self._call("stop", container_id)

    def remove(self, container_id: str) -> None:
        self._call("remove", container_id)

    def managed_containers(self, run_id: str) -> tuple[str, ...]:
        self._call("managed", run_id)
        return self.managed


class DockerExecutorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.workspace = Path(self.temporary.name) / "workspace"
        self.workspace.mkdir()
        self.profile = AgentProfileSpec.create(
            image="agent:local",
            command=["agent"],
            workspace_mode=WorkspaceMode.READ_ONLY,
            network_mode=NetworkMode.NONE,
        )
        self.request = SandboxRequest(
            RUN_ID,
            self.profile,
            self.workspace,
            RuntimeLaunch(self.profile.command),
            {},
        )
        self.engine = FakeDockerEngine()
        self.executor = DockerExecutor(
            self.engine,
            uid=os.getuid(),
            gid=os.getgid(),
        )

    def test_success_and_agent_failure_both_remove_the_container(self) -> None:
        for exit_code in (0, 17):
            with self.subTest(exit_code=exit_code):
                self.engine.calls.clear()
                self.engine.exit_code = exit_code
                result = self.executor.execute(self.request)
                self.assertEqual(
                    result,
                    SandboxResult(
                        container_id=CONTAINER_ID,
                        container_name="magic-glass-box-" + "1" * 32,
                        image_id=IMAGE_ID,
                        exit_code=exit_code,
                        stdout="runtime output\n",
                    ),
                )
                self.assertEqual(
                    [operation for operation, _ in self.engine.calls],
                    ["inspect", "create", "start", "wait", "logs", "remove"],
                )

    def test_declared_image_volumes_are_rejected_before_create(self) -> None:
        self.engine.image = DockerImage(IMAGE_ID, ("/data",))
        with self.assertRaisesRegex(ExecutionError, "declares writable volumes"):
            self.executor.execute(self.request)
        self.assertEqual(self.engine.calls, [("inspect", "agent:local")])

    def test_start_failure_preserves_error_and_still_removes(self) -> None:
        failure = ExecutionError("start failed")
        self.engine.failure = ("start", failure)
        with self.assertRaises(ExecutionError) as raised:
            self.executor.execute(self.request)
        self.assertIs(raised.exception, failure)
        self.assertEqual(
            [operation for operation, _ in self.engine.calls],
            ["inspect", "create", "start", "remove"],
        )

    def test_wait_timeout_preserves_error_and_still_removes(self) -> None:
        failure = ExecutionError("Docker command timed out")
        self.engine.failure = ("wait", failure)

        with self.assertRaisesRegex(ExecutionError, "timed out") as raised:
            self.executor.execute(self.request)

        self.assertIs(raised.exception, failure)
        self.assertEqual(
            [operation for operation, _ in self.engine.calls],
            ["inspect", "create", "start", "wait", "remove"],
        )

    def test_primary_failure_records_a_secondary_cleanup_failure(self) -> None:
        class DoubleFailureEngine(FakeDockerEngine):
            def start(self, container_id: str) -> None:
                self.calls.append(("start", container_id))
                raise ExecutionError("start failed")

            def remove(self, container_id: str) -> None:
                self.calls.append(("remove", container_id))
                raise ExecutionError("remove failed")

        engine = DoubleFailureEngine()
        executor = DockerExecutor(engine, uid=os.getuid(), gid=os.getgid())
        with self.assertRaisesRegex(ExecutionError, "start failed") as raised:
            executor.execute(self.request)
        self.assertEqual(
            raised.exception.__notes__,
            ["container cleanup also failed: remove failed"],
        )

    def test_create_failure_does_not_remove_an_unknown_container(self) -> None:
        self.engine.failure = ("create", ExecutionError("create failed"))
        with self.assertRaisesRegex(ExecutionError, "create failed"):
            self.executor.execute(self.request)
        self.assertEqual(
            [operation for operation, _ in self.engine.calls],
            ["inspect", "create"],
        )

    def test_before_start_callback_is_ordered_and_callback_failure_cleans_up(
        self,
    ) -> None:
        observed: list[tuple[SandboxPlan, str]] = []

        def observe(plan: SandboxPlan, container_id: str) -> None:
            self.engine.calls.append(("callback", container_id))
            observed.append((plan, container_id))

        self.executor.execute(self.request, before_start=observe)
        self.assertEqual(observed[0][0].run_id, RUN_ID)
        self.assertEqual(observed[0][1], CONTAINER_ID)
        self.assertEqual(
            [operation for operation, _ in self.engine.calls],
            ["inspect", "create", "callback", "start", "wait", "logs", "remove"],
        )

        self.engine.calls.clear()

        def fail(_plan: SandboxPlan, _container_id: str) -> None:
            raise RuntimeError("database unavailable")

        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            self.executor.execute(self.request, before_start=fail)
        self.assertEqual(
            [operation for operation, _ in self.engine.calls],
            ["inspect", "create", "remove"],
        )

    def test_log_capture_failure_still_removes_the_container(self) -> None:
        self.engine.failure = ("logs", ExecutionError("logs failed"))

        with self.assertRaisesRegex(ExecutionError, "logs failed"):
            self.executor.execute(self.request)

        self.assertEqual(
            [operation for operation, _ in self.engine.calls],
            ["inspect", "create", "start", "wait", "logs", "remove"],
        )

    def test_cleanup_run_removes_every_managed_container(self) -> None:
        second = "c" * 64
        self.engine.managed = (CONTAINER_ID, second)
        self.assertEqual(self.executor.cleanup_run(RUN_ID), 2)
        self.assertEqual(
            self.engine.calls,
            [
                ("managed", RUN_ID),
                ("remove", CONTAINER_ID),
                ("remove", second),
            ],
        )
        self.engine.calls.clear()
        with self.assertRaisesRegex(ExecutionError, "run identifier"):
            self.executor.cleanup_run("run_bad")
        self.assertEqual(self.engine.calls, [])

    def test_cleanup_run_attempts_all_containers_and_preserves_failures(self) -> None:
        class CleanupFailureEngine(FakeDockerEngine):
            def remove(self, container_id: str) -> None:
                self.calls.append(("remove", container_id))
                raise ExecutionError(f"cannot remove {container_id[0]}")

        engine = CleanupFailureEngine()
        engine.managed = (CONTAINER_ID, "c" * 64)
        executor = DockerExecutor(engine, uid=os.getuid(), gid=os.getgid())

        with self.assertRaisesRegex(ExecutionError, "cannot remove b") as raised:
            executor.cleanup_run(RUN_ID)

        self.assertEqual(
            [value for operation, value in engine.calls if operation == "remove"],
            [CONTAINER_ID, "c" * 64],
        )
        self.assertEqual(
            raised.exception.__notes__,
            ["additional container cleanup failed: cannot remove c"],
        )

    def test_stop_run_stops_every_managed_container(self) -> None:
        second = "c" * 64
        self.engine.managed = (CONTAINER_ID, second)

        self.assertEqual(self.executor.stop_run(RUN_ID), 2)

        self.assertEqual(
            self.engine.calls,
            [
                ("managed", RUN_ID),
                ("stop", CONTAINER_ID),
                ("stop", second),
            ],
        )


class DockerCLITests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = mock.Mock()
        self.client = DockerCLI(
            binary=Path("/usr/local/bin/docker"),
            runner=self.runner,
            command_timeout=12.5,
        )

    def completed(
        self, stdout: str = "", stderr: str = "", returncode: int = 0
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            ["docker"], returncode, stdout=stdout, stderr=stderr
        )

    def test_image_inspection_is_typed_and_rejects_implicit_volumes(self) -> None:
        payload = [
            {
                "Id": IMAGE_ID,
                "Config": {"Volumes": {"/cache": {}, "/data": {}}},
            }
        ]
        self.runner.return_value = self.completed(json.dumps(payload))
        image = self.client.inspect_image("agent:local")
        self.assertEqual(image, DockerImage(IMAGE_ID, ("/cache", "/data")))
        self.runner.assert_called_once_with(
            ["/usr/local/bin/docker", "image", "inspect", "agent:local"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=12.5,
            check=False,
        )

    def test_image_without_declared_volumes_is_accepted(self) -> None:
        self.runner.return_value = self.completed(
            json.dumps([{"Id": IMAGE_ID, "Config": {"Volumes": None}}])
        )
        self.assertEqual(
            self.client.inspect_image("agent:local"),
            DockerImage(IMAGE_ID, ()),
        )

    def test_constructor_discovers_absolute_binary_and_rejects_bad_settings(
        self,
    ) -> None:
        with mock.patch(
            "magic_glass_box.executor.shutil.which", return_value="/bin/docker"
        ):
            discovered = DockerCLI(runner=self.runner)
        self.assertEqual(discovered.binary, Path("/bin/docker"))
        with (
            mock.patch("magic_glass_box.executor.shutil.which", return_value=None),
            self.assertRaisesRegex(ExecutionError, "not found"),
        ):
            DockerCLI(runner=self.runner)
        with self.assertRaisesRegex(ExecutionError, "absolute"):
            DockerCLI(binary=Path("docker"), runner=self.runner)
        for timeout in (0, -1, True, "30"):
            with (
                self.subTest(timeout=timeout),
                self.assertRaisesRegex(ExecutionError, "timeout must be positive"),
            ):
                DockerCLI(
                    binary=Path("/bin/docker"),
                    runner=self.runner,
                    command_timeout=timeout,  # type: ignore[arg-type]
                )
            with (
                self.subTest(run_timeout=timeout),
                self.assertRaisesRegex(ExecutionError, "timeout must be positive"),
            ):
                DockerCLI(
                    binary=Path("/bin/docker"),
                    runner=self.runner,
                    run_timeout=timeout,  # type: ignore[arg-type]
                )
        subsecond = DockerCLI(
            binary=Path("/bin/docker"),
            runner=self.runner,
            command_timeout=0.5,
            run_timeout=0.5,
        )
        self.assertEqual(subsecond.command_timeout, 0.5)
        self.assertEqual(subsecond.run_timeout, 0.5)

    def test_wait_uses_default_run_deadline_and_allows_an_override(
        self,
    ) -> None:
        self.runner.return_value = self.completed("0\n")

        self.assertEqual(self.client.wait(CONTAINER_ID), 0)
        self.assertEqual(self.runner.call_args.kwargs["timeout"], 21_600.0)

        bounded = DockerCLI(
            binary=Path("/usr/local/bin/docker"),
            runner=self.runner,
            command_timeout=12.5,
            run_timeout=3600,
        )
        self.assertEqual(bounded.wait(CONTAINER_ID), 0)
        self.assertEqual(self.runner.call_args.kwargs["timeout"], 3600.0)

    def test_lifecycle_commands_parse_exact_outputs(self) -> None:
        plan = SandboxPlan(
            RUN_ID,
            "magic-glass-box-" + "1" * 32,
            IMAGE_ID,
            ("container", "create"),
        )
        self.runner.side_effect = (
            self.completed(CONTAINER_ID + "\n"),
            self.completed(CONTAINER_ID + "\n"),
            self.completed("17\n"),
            self.completed("structured output\n"),
            self.completed(CONTAINER_ID + "\n"),
            self.completed(CONTAINER_ID + "\n"),
        )
        self.assertEqual(self.client.create(plan), CONTAINER_ID)
        self.client.start(CONTAINER_ID)
        self.assertEqual(self.client.wait(CONTAINER_ID), 17)
        self.assertEqual(self.client.logs(CONTAINER_ID), "structured output\n")
        self.client.stop(CONTAINER_ID)
        self.client.remove(CONTAINER_ID)
        commands = [call.args[0][1:] for call in self.runner.call_args_list]
        self.assertEqual(
            commands,
            [
                ["container", "create"],
                ["container", "start", CONTAINER_ID],
                ["container", "wait", CONTAINER_ID],
                ["container", "logs", CONTAINER_ID],
                ["container", "stop", CONTAINER_ID],
                ["container", "rm", "--force", "--volumes", CONTAINER_ID],
            ],
        )

    def test_lists_managed_containers_by_exact_run_label(self) -> None:
        second = "c" * 64
        self.runner.side_effect = (
            self.completed(f"{CONTAINER_ID}\n"),
            self.completed(f"{second}\n"),
        )
        self.assertEqual(
            self.client.managed_containers(RUN_ID),
            (CONTAINER_ID, second),
        )
        self.assertEqual(
            [call.args[0][1:] for call in self.runner.call_args_list],
            [
                [
                    "container",
                    "ls",
                    "--all",
                    "--no-trunc",
                    "--quiet",
                    "--filter",
                    "label=io.magicglassbox.managed=true",
                    "--filter",
                    f"label=io.magicglassbox.run-id={RUN_ID}",
                ],
                [
                    "container",
                    "ls",
                    "--all",
                    "--no-trunc",
                    "--quiet",
                    "--filter",
                    "label=io.hexadecapus.managed=true",
                    "--filter",
                    f"label=io.hexadecapus.run-id={RUN_ID}",
                ],
            ],
        )

    def test_managed_container_listing_rejects_invalid_output(self) -> None:
        for output in ("short\n", f"{CONTAINER_ID}\n{CONTAINER_ID}\n"):
            with self.subTest(output=output):
                self.runner.side_effect = (self.completed(output), self.completed(""))
                with self.assertRaisesRegex(ExecutionError, "container listing"):
                    self.client.managed_containers(RUN_ID)
        self.runner.side_effect = (self.completed("\n"), self.completed("\n"))
        self.assertEqual(self.client.managed_containers(RUN_ID), ())
        with self.assertRaisesRegex(ExecutionError, "run identifier"):
            self.client.managed_containers("run_bad")

    def test_cli_rejects_bad_json_ids_exit_codes_and_command_failures(self) -> None:
        cases = (
            ("inspect", self.completed("{}"), "image inspection"),
            ("create", self.completed("short\n"), "container ID"),
            ("wait", self.completed("not-an-int\n"), "exit code"),
            ("wait", self.completed("256\n"), "exit code"),
            ("wait", self.completed("-1\n"), "exit code"),
            (
                "start",
                self.completed(stderr="daemon denied", returncode=1),
                "daemon denied",
            ),
        )
        plan = SandboxPlan(
            RUN_ID,
            "magic-glass-box-" + "1" * 32,
            IMAGE_ID,
            ("container", "create"),
        )
        for operation, completed, message in cases:
            with self.subTest(operation=operation, message=message):
                self.runner.reset_mock()
                self.runner.return_value = completed
                with self.assertRaisesRegex(ExecutionError, message):
                    if operation == "inspect":
                        self.client.inspect_image("agent:local")
                    elif operation == "create":
                        self.client.create(plan)
                    elif operation == "wait":
                        self.client.wait(CONTAINER_ID)
                    else:
                        self.client.start(CONTAINER_ID)

    def test_cli_rejects_malformed_image_inspection_shapes(self) -> None:
        payloads = (
            "not-json",
            "{}",
            "[]",
            json.dumps([1]),
            json.dumps([{"Id": None, "Config": {"Volumes": None}}]),
            json.dumps([{"Id": IMAGE_ID, "Config": None}]),
            json.dumps([{"Id": IMAGE_ID, "Config": {"Volumes": []}}]),
            json.dumps([{"Id": IMAGE_ID, "Config": {"Volumes": {"/data": None}}}]),
        )
        for payload in payloads:
            with self.subTest(payload=payload):
                self.runner.return_value = self.completed(payload)
                with self.assertRaises(ExecutionError):
                    self.client.inspect_image("agent:local")

    def test_cli_bounds_output_and_uses_redacted_fallback_errors(self) -> None:
        self.runner.return_value = self.completed("x" * (1024 * 1024 + 1))
        with self.assertRaisesRegex(ExecutionError, "size limit"):
            self.client.inspect_image("agent:local")

        for completed, expected in (
            (self.completed(returncode=1), "unknown daemon error"),
            (self.completed(stdout="stdout detail", returncode=1), "stdout detail"),
        ):
            with self.subTest(expected=expected):
                self.runner.return_value = completed
                with self.assertRaisesRegex(ExecutionError, expected):
                    self.client.start(CONTAINER_ID)

    def test_verbose_container_logs_use_the_separate_rotated_log_limit(self) -> None:
        verbose = "x" * (2 * 1024 * 1024)
        self.runner.return_value = self.completed(verbose)

        self.assertEqual(self.client.logs(CONTAINER_ID), verbose)

        self.runner.return_value = self.completed("x" * (24 * 1024 * 1024 + 1))
        with self.assertRaisesRegex(ExecutionError, "size limit"):
            self.client.logs(CONTAINER_ID)

    def test_cli_wraps_missing_binary_and_timeout_without_leaking_arguments(
        self,
    ) -> None:
        for failure, message in (
            (OSError("not found"), "cannot execute Docker"),
            (
                subprocess.TimeoutExpired(["docker"], 12.5),
                "timed out",
            ),
        ):
            with self.subTest(message=message):
                self.runner.side_effect = failure
                with self.assertRaisesRegex(ExecutionError, message):
                    self.client.inspect_image("secret-looking-image-reference")


if __name__ == "__main__":
    unittest.main()
