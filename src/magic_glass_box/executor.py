from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from .domain import AgentProfileSpec, NetworkMode, WorkspaceMode
from .gateway import (
    CONTAINER_GATEWAY_ROOT,
    GATEWAY_FILE_NAMES,
    GATEWAY_WRITABLE_NAMES,
)
from .runtime import RuntimeLaunch

RUN_ID_RE = re.compile(r"^run_[0-9a-f]{32}$")
IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
CONTAINER_ID_RE = re.compile(r"^[0-9a-f]{64}$")
MAX_DOCKER_OUTPUT_BYTES = 1024 * 1024
MAX_DOCKER_LOG_BYTES = 24 * 1024 * 1024
DEFAULT_RUN_TIMEOUT_SECONDS = 6 * 60 * 60
MIN_MEMORY_BYTES = 64 * 1024 * 1024
MAX_MEMORY_BYTES = 64 * 1024 * 1024 * 1024
MIN_TMPFS_BYTES = 16 * 1024 * 1024
MAX_TMPFS_BYTES = 4 * 1024 * 1024 * 1024
MANAGED_LABEL = "io.magicglassbox.managed"
RUN_ID_LABEL = "io.magicglassbox.run-id"
LEGACY_MANAGED_LABEL = "io.hexadecapus.managed"
LEGACY_RUN_ID_LABEL = "io.hexadecapus.run-id"
RESERVED_RUNTIME_ENVIRONMENT = {
    "CI",
    "HOME",
    "KRAB_TOKEN_FILE",
    "MAGIC_GLASS_BOX_CREDENTIAL_DIR",
    "MAGIC_GLASS_BOX_GATEWAY_DIR",
    "MAGIC_GLASS_BOX_RUN_ID",
}


class ExecutionError(RuntimeError):
    """A sandbox could not be created or managed without weakening policy."""


def _bounded_integer(value: object, *, minimum: int, maximum: int, label: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise ExecutionError(f"{label} must be between {minimum} and {maximum}")
    return value


@dataclass(frozen=True)
class DockerLimits:
    memory_bytes: int = 2 * 1024 * 1024 * 1024
    cpus: float = 2.0
    pids_limit: int = 256
    tmpfs_bytes: int = 256 * 1024 * 1024
    stop_timeout: int = 10

    def __post_init__(self) -> None:
        _bounded_integer(
            self.memory_bytes,
            minimum=MIN_MEMORY_BYTES,
            maximum=MAX_MEMORY_BYTES,
            label="memory limit",
        )
        if (
            isinstance(self.cpus, bool)
            or not isinstance(self.cpus, (int, float))
            or not 0.1 <= self.cpus <= 64
        ):
            raise ExecutionError("CPU limit must be between 0.1 and 64")
        _bounded_integer(
            self.pids_limit,
            minimum=16,
            maximum=4096,
            label="PID limit",
        )
        _bounded_integer(
            self.tmpfs_bytes,
            minimum=MIN_TMPFS_BYTES,
            maximum=MAX_TMPFS_BYTES,
            label="tmpfs limit",
        )
        _bounded_integer(
            self.stop_timeout,
            minimum=1,
            maximum=300,
            label="stop timeout",
        )


@dataclass(frozen=True)
class SandboxRequest:
    run_id: str
    profile: AgentProfileSpec
    workspace: Path
    launch: RuntimeLaunch
    credential_files: Mapping[str, Path] = field(default_factory=dict)
    gateway_directory: Path | None = None


@dataclass(frozen=True)
class DockerImage:
    image_id: str
    declared_volumes: tuple[str, ...]


@dataclass(frozen=True)
class SandboxPlan:
    run_id: str
    container_name: str
    image_id: str
    create_args: tuple[str, ...]


@dataclass(frozen=True)
class SandboxResult:
    container_id: str
    container_name: str
    image_id: str
    exit_code: int
    stdout: str = ""


def _workspace_path(path: Path, *, writable: bool, uid: int) -> Path:
    candidate = Path(path).expanduser().absolute()
    try:
        info = candidate.lstat()
    except OSError as exc:
        raise ExecutionError(f"workspace is unavailable: {candidate}") from exc
    if stat.S_ISLNK(info.st_mode):
        raise ExecutionError("workspace must not be a symlink")
    if not stat.S_ISDIR(info.st_mode):
        raise ExecutionError("workspace must be a directory")
    resolved = candidate.resolve(strict=True)
    if resolved == Path(resolved.anchor):
        raise ExecutionError("workspace must not be a filesystem root")
    if "," in str(resolved):
        raise ExecutionError("workspace path must not contain a comma")
    if writable and (info.st_uid != uid or not os.access(resolved, os.W_OK)):
        raise ExecutionError("copy-on-write workspace is not writable by the run user")
    return resolved


def _credential_path(path: Path, *, uid: int, workspace: Path) -> Path:
    candidate = Path(path).expanduser().absolute()
    try:
        info = candidate.lstat()
        parent = candidate.parent.lstat()
    except OSError as exc:
        raise ExecutionError(f"credential file is unavailable: {candidate}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ExecutionError("credential must be a regular file, not a symlink")
    if stat.S_ISLNK(parent.st_mode) or not stat.S_ISDIR(parent.st_mode):
        raise ExecutionError("credential parent must be a private directory")
    if info.st_uid != uid or parent.st_uid != uid:
        raise ExecutionError("credential path must be owned by the run user")
    if info.st_nlink != 1:
        raise ExecutionError("credential file must not be hard-linked")
    if info.st_mode & 0o777 != 0o600:
        raise ExecutionError("credential file must use mode 0600")
    if parent.st_mode & 0o777 != 0o700:
        raise ExecutionError("credential parent must use mode 0700")
    resolved = candidate.resolve(strict=True)
    if "," in str(resolved):
        raise ExecutionError("credential path must not contain a comma")
    if resolved.is_relative_to(workspace):
        raise ExecutionError("credential files must remain outside the workspace")
    return resolved


def _gateway_directory(path: Path, *, uid: int, workspace: Path) -> Path:
    candidate = Path(path).expanduser().absolute()
    try:
        info = candidate.lstat()
    except OSError as exc:
        raise ExecutionError(f"gateway endpoint is unavailable: {candidate}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ExecutionError("gateway endpoint must be a private directory")
    if info.st_uid != uid:
        raise ExecutionError("gateway endpoint must be owned by the run user")
    if info.st_mode & 0o777 != 0o700:
        raise ExecutionError("gateway endpoint must use mode 0700")
    resolved = candidate.resolve(strict=True)
    if "," in str(resolved):
        raise ExecutionError("gateway path must not contain a comma")
    if resolved.is_relative_to(workspace):
        raise ExecutionError("gateway endpoint must remain outside the workspace")
    entries = {entry.name: entry for entry in resolved.iterdir()}
    if set(entries) != GATEWAY_FILE_NAMES:
        raise ExecutionError("gateway endpoint has an invalid mailbox shape")
    for entry in entries.values():
        entry_info = entry.lstat()
        if stat.S_ISLNK(entry_info.st_mode) or not stat.S_ISREG(entry_info.st_mode):
            raise ExecutionError("gateway mailbox entries must be regular files")
        if entry_info.st_uid != uid:
            raise ExecutionError("gateway mailbox must be owned by the run user")
        if entry_info.st_nlink != 1:
            raise ExecutionError("gateway mailbox files must not be hard-linked")
        if entry_info.st_mode & 0o777 != 0o600:
            raise ExecutionError("gateway mailbox files must use mode 0600")
    return resolved


def build_sandbox_plan(
    request: SandboxRequest,
    image: DockerImage,
    *,
    uid: int,
    gid: int,
    limits: DockerLimits | None = None,
) -> SandboxPlan:
    if (
        not isinstance(request.run_id, str)
        or RUN_ID_RE.fullmatch(request.run_id) is None
    ):
        raise ExecutionError("invalid run identifier")
    if isinstance(uid, bool) or not isinstance(uid, int) or uid <= 0:
        raise ExecutionError("sandbox user must not be root")
    if isinstance(gid, bool) or not isinstance(gid, int) or gid <= 0:
        raise ExecutionError("sandbox group must not be root")
    if request.profile.network_mode is NetworkMode.KRAB_ONLY:
        raise ExecutionError(
            "krab_only networking is retired; use the run-scoped gateway"
        )
    if (
        not isinstance(image.image_id, str)
        or IMAGE_ID_RE.fullmatch(image.image_id) is None
    ):
        raise ExecutionError("Docker returned an invalid immutable image ID")
    if image.declared_volumes:
        raise ExecutionError("agent image declares writable volumes")
    gateway_enabled = request.profile.network_mode is NetworkMode.GATEWAY_ONLY
    if gateway_enabled:
        if request.gateway_directory is None:
            raise ExecutionError("gateway endpoint is required for gateway_only")
        if request.credential_files:
            raise ExecutionError(
                "gateway-only sandboxes must not receive credential files"
            )
    else:
        if request.gateway_directory is not None:
            raise ExecutionError("gateway endpoint requires gateway_only networking")
        expected_credentials = set(request.profile.credential_names)
        if set(request.credential_files) != expected_credentials:
            raise ExecutionError(
                "credential set does not match the pinned agent profile"
            )
    runtime_environment = dict(request.launch.environment)
    reserved_environment = set(runtime_environment).intersection(
        RESERVED_RUNTIME_ENVIRONMENT
    )
    if reserved_environment:
        raise ExecutionError(
            "runtime adapter cannot override reserved environment: "
            + ", ".join(sorted(reserved_environment))
        )

    policy = limits or DockerLimits()
    workspace = _workspace_path(
        request.workspace,
        writable=request.profile.workspace_mode is WorkspaceMode.COPY_ON_WRITE,
        uid=uid,
    )
    credentials = (
        {}
        if gateway_enabled
        else {
            name: _credential_path(path, uid=uid, workspace=workspace)
            for name, path in request.credential_files.items()
        }
    )
    gateway = (
        _gateway_directory(request.gateway_directory, uid=uid, workspace=workspace)
        if request.gateway_directory is not None
        else None
    )
    container_name = "magic-glass-box-" + request.run_id.removeprefix("run_")
    workspace_mount = f"type=bind,src={workspace},dst=/workspace"
    if request.profile.workspace_mode is WorkspaceMode.READ_ONLY:
        workspace_mount += ",readonly"

    args = [
        "container",
        "create",
        "--name",
        container_name,
        "--label",
        f"{MANAGED_LABEL}=true",
        "--label",
        f"{RUN_ID_LABEL}={request.run_id}",
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
        str(policy.pids_limit),
        "--memory",
        str(policy.memory_bytes),
        "--cpus",
        f"{policy.cpus:g}",
        "--ulimit",
        "nofile=1024:1024",
        "--stop-timeout",
        str(policy.stop_timeout),
        "--init",
        "--user",
        f"{uid}:{gid}",
        "--hostname",
        "magic-glass-box-agent",
        "--workdir",
        "/workspace",
        "--tmpfs",
        f"/tmp:rw,nosuid,nodev,noexec,size={policy.tmpfs_bytes}",  # noqa: S108
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
        f"MAGIC_GLASS_BOX_RUN_ID={request.run_id}",
    ]
    for name in sorted(runtime_environment):
        args.extend(("--env", f"{name}={runtime_environment[name]}"))
    if gateway is not None:
        args.extend(("--env", f"MAGIC_GLASS_BOX_GATEWAY_DIR={CONTAINER_GATEWAY_ROOT}"))
    elif credentials:
        args.extend(
            ("--env", "MAGIC_GLASS_BOX_CREDENTIAL_DIR=/run/secrets/magic-glass-box")
        )
    if "krab" in credentials:
        args.extend(("--env", "KRAB_TOKEN_FILE=/run/secrets/magic-glass-box/krab"))
    args.extend(("--mount", workspace_mount))
    if gateway is not None:
        args.extend(
            (
                "--mount",
                f"type=bind,src={gateway},dst={CONTAINER_GATEWAY_ROOT},readonly",
            )
        )
        for name in GATEWAY_WRITABLE_NAMES:
            args.extend(
                (
                    "--mount",
                    (
                        f"type=bind,src={gateway / name},"
                        f"dst={CONTAINER_GATEWAY_ROOT}/{name}"
                    ),
                )
            )
    for name in sorted(credentials):
        args.extend(
            (
                "--mount",
                (
                    f"type=bind,src={credentials[name]},"
                    f"dst=/run/secrets/magic-glass-box/{name},readonly"
                ),
            )
        )
    args.extend(("--entrypoint", request.launch.command[0], image.image_id))
    args.extend(request.launch.command[1:])
    return SandboxPlan(
        run_id=request.run_id,
        container_name=container_name,
        image_id=image.image_id,
        create_args=tuple(args),
    )


class DockerEngine(Protocol):
    def inspect_image(self, reference: str) -> DockerImage: ...

    def create(self, plan: SandboxPlan) -> str: ...

    def start(self, container_id: str) -> None: ...

    def wait(self, container_id: str) -> int: ...

    def logs(self, container_id: str) -> str: ...

    def stop(self, container_id: str) -> None: ...

    def remove(self, container_id: str) -> None: ...

    def managed_containers(self, run_id: str) -> tuple[str, ...]: ...


class DockerExecutor:
    def __init__(
        self,
        engine: DockerEngine,
        *,
        uid: int | None = None,
        gid: int | None = None,
        limits: DockerLimits | None = None,
    ):
        self.engine = engine
        self.uid = os.getuid() if uid is None else uid
        self.gid = os.getgid() if gid is None else gid
        self.limits = limits or DockerLimits()

    def execute(
        self,
        request: SandboxRequest,
        *,
        before_start: Callable[[SandboxPlan, str], None] | None = None,
    ) -> SandboxResult:
        image = self.engine.inspect_image(request.profile.image)
        if image.declared_volumes:
            raise ExecutionError(
                "agent image declares writable volumes: "
                + ", ".join(image.declared_volumes)
            )
        plan = build_sandbox_plan(
            request,
            image,
            uid=self.uid,
            gid=self.gid,
            limits=self.limits,
        )
        container_id = self.engine.create(plan)
        try:
            if before_start is not None:
                before_start(plan, container_id)
            self.engine.start(container_id)
            exit_code = self.engine.wait(container_id)
            stdout = self.engine.logs(container_id)
        except BaseException as exc:
            try:
                self.engine.remove(container_id)
            except BaseException as cleanup_error:
                exc.add_note(f"container cleanup also failed: {cleanup_error}")
            raise
        self.engine.remove(container_id)
        return SandboxResult(
            container_id=container_id,
            container_name=plan.container_name,
            image_id=image.image_id,
            exit_code=exit_code,
            stdout=stdout,
        )

    def cleanup_run(self, run_id: str) -> int:
        return self._operate_managed(run_id, self.engine.remove)

    def stop_run(self, run_id: str) -> int:
        return self._operate_managed(run_id, self.engine.stop)

    def _operate_managed(self, run_id: str, operation: Callable[[str], None]) -> int:
        if not isinstance(run_id, str) or RUN_ID_RE.fullmatch(run_id) is None:
            raise ExecutionError("invalid run identifier")
        container_ids = self.engine.managed_containers(run_id)
        failures: list[BaseException] = []
        for container_id in container_ids:
            try:
                operation(container_id)
            except BaseException as exc:
                failures.append(exc)
        if failures:
            primary = failures[0]
            for failure in failures[1:]:
                primary.add_note(f"additional container cleanup failed: {failure}")
            raise primary
        return len(container_ids)


Runner = Callable[..., subprocess.CompletedProcess[str]]


class DockerCLI:
    def __init__(
        self,
        *,
        binary: Path | None = None,
        runner: Runner | None = None,
        command_timeout: float = 30.0,
        run_timeout: float | None = DEFAULT_RUN_TIMEOUT_SECONDS,
    ):
        if binary is None:
            discovered = shutil.which("docker")
            if discovered is None:
                raise ExecutionError("Docker CLI was not found")
            binary = Path(discovered)
        if not Path(binary).is_absolute():
            raise ExecutionError("Docker CLI path must be absolute")
        for value, label in (
            (command_timeout, "Docker command timeout"),
            (run_timeout, "agent run timeout"),
        ):
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or value <= 0
            ):
                raise ExecutionError(f"{label} must be positive")
        self.binary = Path(binary)
        self.runner = runner or subprocess.run
        self.command_timeout = float(command_timeout)
        self.run_timeout = float(
            DEFAULT_RUN_TIMEOUT_SECONDS if run_timeout is None else run_timeout
        )

    def _run(
        self,
        args: tuple[str, ...],
        *,
        timeout: float | None = None,
        use_command_timeout: bool = True,
        max_output_bytes: int = MAX_DOCKER_OUTPUT_BYTES,
    ) -> str:
        effective_timeout = self.command_timeout if use_command_timeout else timeout
        try:
            completed = self.runner(
                [str(self.binary), *args],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=effective_timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise ExecutionError("Docker command timed out") from None
        except OSError:
            raise ExecutionError("cannot execute Docker CLI") from None
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout).strip()
            if not detail:
                detail = "unknown daemon error"
            raise ExecutionError(f"Docker command failed: {detail[:4096]}")
        if len(completed.stdout.encode("utf-8")) > max_output_bytes:
            raise ExecutionError("Docker command output exceeds the size limit")
        return completed.stdout

    def inspect_image(self, reference: str) -> DockerImage:
        raw = self._run(("image", "inspect", reference))
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, RecursionError) as exc:
            raise ExecutionError("invalid Docker image inspection response") from exc
        if not isinstance(payload, list) or len(payload) != 1:
            raise ExecutionError("invalid Docker image inspection response")
        item = payload[0]
        if not isinstance(item, dict):
            raise ExecutionError("invalid Docker image inspection response")
        image_id = item.get("Id")
        config = item.get("Config")
        if (
            not isinstance(image_id, str)
            or IMAGE_ID_RE.fullmatch(image_id) is None
            or not isinstance(config, dict)
        ):
            raise ExecutionError("invalid Docker image inspection response")
        raw_volumes = config.get("Volumes")
        if raw_volumes is None:
            volumes: tuple[str, ...] = ()
        elif isinstance(raw_volumes, dict) and all(
            isinstance(value, dict) for value in raw_volumes.values()
        ):
            volumes = tuple(sorted(raw_volumes))
        else:
            raise ExecutionError("invalid Docker image volume declaration")
        return DockerImage(image_id=image_id, declared_volumes=volumes)

    def create(self, plan: SandboxPlan) -> str:
        container_id = self._run(plan.create_args).strip()
        if CONTAINER_ID_RE.fullmatch(container_id) is None:
            raise ExecutionError("Docker returned an invalid container ID")
        return container_id

    def start(self, container_id: str) -> None:
        self._run(("container", "start", container_id))

    def wait(self, container_id: str) -> int:
        raw = self._run(
            ("container", "wait", container_id),
            timeout=self.run_timeout,
            use_command_timeout=False,
        ).strip()
        try:
            exit_code = int(raw)
        except ValueError as exc:
            raise ExecutionError(
                "Docker returned an invalid container exit code"
            ) from exc
        if not 0 <= exit_code <= 255:
            raise ExecutionError("Docker returned an invalid container exit code")
        return exit_code

    def logs(self, container_id: str) -> str:
        # The local Docker logging policy retains at most two 10 MiB files. Keep
        # a small rotation margin while preserving a tighter cap for every
        # control-plane command whose output is expected to be tiny.
        return self._run(
            ("container", "logs", container_id),
            max_output_bytes=MAX_DOCKER_LOG_BYTES,
        )

    def stop(self, container_id: str) -> None:
        self._run(("container", "stop", container_id))

    def remove(self, container_id: str) -> None:
        self._run(("container", "rm", "--force", "--volumes", container_id))

    def managed_containers(self, run_id: str) -> tuple[str, ...]:
        if not isinstance(run_id, str) or RUN_ID_RE.fullmatch(run_id) is None:
            raise ExecutionError("invalid run identifier")
        discovered: list[str] = []
        for managed_label, run_id_label in (
            (MANAGED_LABEL, RUN_ID_LABEL),
            (LEGACY_MANAGED_LABEL, LEGACY_RUN_ID_LABEL),
        ):
            raw = self._run(
                (
                    "container",
                    "ls",
                    "--all",
                    "--no-trunc",
                    "--quiet",
                    "--filter",
                    f"label={managed_label}=true",
                    "--filter",
                    f"label={run_id_label}={run_id}",
                )
            )
            listed = tuple(line.strip() for line in raw.splitlines() if line.strip())
            if len(set(listed)) != len(listed) or any(
                CONTAINER_ID_RE.fullmatch(container_id) is None
                for container_id in listed
            ):
                raise ExecutionError("Docker returned an invalid container listing")
            discovered.extend(listed)
        return tuple(dict.fromkeys(discovered))
