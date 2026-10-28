#!/usr/local/bin/python
from __future__ import annotations

import gzip
import io
import json
import os
import signal
import stat
import subprocess
import sys
import tarfile
import time
from collections.abc import Mapping, Sequence
from pathlib import Path

MAX_EVIDENCE_BYTES = 1024 * 1024
MAX_PROC_NET_BYTES = 64 * 1024
WORKSPACE = Path("/workspace")
ARTIFACT_ENV = "MGB_ARTIFACT_PATH"
INERT_NETWORK_INTERFACES = frozenset(
    {
        "erspan0",
        "gre0",
        "gretap0",
        "ip6gre0",
        "ip6tnl0",
        "ip6_vti0",
        "ip_vti0",
        "sit0",
        "tunl0",
    }
)
FORBIDDEN_ENVIRONMENT = frozenset(
    {
        "ANTHROPIC_API_KEY",
        "AZURE_CLIENT_SECRET",
        "AZURE_STORAGE_KEY",
        "DOCKER_AUTH_CONFIG",
        "DOCKER_CONFIG",
        "DOCKER_HOST",
        "GH_TOKEN",
        "GITHUB_TOKEN",
        "GOOGLE_API_KEY",
        "OPENAI_API_KEY",
        "SSH_AGENT_PID",
        "SSH_AUTH_SOCK",
    }
)
TEST_MODULES = (
    "tests.test_execution_operations",
    "tests.test_execution_provider",
    "tests.test_execution_provider_cli",
    "tests.test_remote_runner",
)


class ProofError(RuntimeError):
    pass


def forbidden_environment(environment: Mapping[str, str]) -> tuple[str, ...]:
    return tuple(sorted(FORBIDDEN_ENVIRONMENT.intersection(environment)))


def network_interfaces(document: str) -> tuple[str, ...]:
    if (
        not isinstance(document, str)
        or len(document.encode("utf-8")) > MAX_PROC_NET_BYTES
    ):
        raise ProofError("network interface inventory is invalid")
    interfaces: list[str] = []
    for line in document.splitlines():
        if ":" not in line:
            continue
        name = line.split(":", 1)[0].strip()
        if not name or any(mark.isspace() for mark in name):
            raise ProofError("network interface inventory is invalid")
        interfaces.append(name)
    if not interfaces or len(set(interfaces)) != len(interfaces):
        raise ProofError("network interface inventory is invalid")
    if "lo" not in interfaces or not set(interfaces).issubset(
        {"lo", *INERT_NETWORK_INTERFACES}
    ):
        raise ProofError("unexpected network interface is available")
    return tuple(interfaces)


def assert_loopback_routes(ipv4_document: str, ipv6_document: str) -> None:
    for document in (ipv4_document, ipv6_document):
        if (
            not isinstance(document, str)
            or len(document.encode("utf-8")) > MAX_PROC_NET_BYTES
        ):
            raise ProofError("network route inventory is invalid")
    ipv4_lines = [line.split() for line in ipv4_document.splitlines() if line.strip()]
    if not ipv4_lines or ipv4_lines[0][0] != "Iface":
        raise ProofError("network route inventory is invalid")
    if any(not fields or fields[0] != "lo" for fields in ipv4_lines[1:]):
        raise ProofError("unexpected network route is available")
    ipv6_lines = [line.split() for line in ipv6_document.splitlines() if line.strip()]
    if any(len(fields) != 10 or fields[-1] != "lo" for fields in ipv6_lines):
        raise ProofError("unexpected network route is available")


def evidence_archive(summary: Mapping[str, object]) -> bytes:
    payload = (
        json.dumps(summary, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    output = io.BytesIO()
    with (
        gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as compressed,
        tarfile.open(
            fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT
        ) as archive,
    ):
        member = tarfile.TarInfo("m15-proof.json")
        member.size = len(payload)
        member.mode = 0o644
        member.uid = 0
        member.gid = 0
        member.uname = ""
        member.gname = ""
        member.mtime = 0
        archive.addfile(member, io.BytesIO(payload))
    value = output.getvalue()
    if not value or len(value) > MAX_EVIDENCE_BYTES:
        raise ProofError("proof evidence archive is invalid")
    return value


def test_command(workspace: Path) -> tuple[str, ...]:
    if workspace != WORKSPACE:
        raise ProofError("proof workspace is invalid")
    return (sys.executable, "-m", "unittest", *TEST_MODULES, "-v")


def _read_text_bounded(source: Path, maximum: int) -> str:
    try:
        with source.open("rb") as stream:
            raw = stream.read(maximum + 1)
    except OSError as exc:
        raise ProofError("boundary inventory is unavailable") from exc
    if not raw or len(raw) > maximum:
        raise ProofError("boundary inventory is invalid")
    try:
        return raw.decode("utf-8", "strict")
    except UnicodeDecodeError as exc:
        raise ProofError("boundary inventory is invalid") from exc


def _assert_unwritable(directory: Path, name: str) -> None:
    probe = directory / name
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(probe, flags, 0o600)
    except OSError:
        return
    try:
        os.close(descriptor)
        probe.unlink(missing_ok=True)
    finally:
        raise ProofError("protected container path is writable")


def _write_artifact(destination: Path, content: bytes) -> None:
    try:
        info = destination.lstat()
    except OSError as exc:
        raise ProofError("artifact upload is unavailable") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ProofError("artifact upload is invalid")
    flags = os.O_WRONLY | os.O_TRUNC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = -1
    try:
        descriptor = os.open(destination, flags)
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short artifact write")
            view = view[written:]
        os.fsync(descriptor)
    except OSError as exc:
        raise ProofError("artifact upload failed") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _run_proof() -> int:
    if os.geteuid() == 0:
        raise ProofError("proof image must run without root authority")
    try:
        workspace_info = WORKSPACE.lstat()
    except OSError as exc:
        raise ProofError("proof workspace is unavailable") from exc
    if stat.S_ISLNK(workspace_info.st_mode) or not stat.S_ISDIR(workspace_info.st_mode):
        raise ProofError("proof workspace is invalid")
    forbidden = forbidden_environment(os.environ)
    if forbidden:
        raise ProofError(
            "forbidden credential environment is present: " + ",".join(forbidden)
        )
    for socket_path in (Path("/var/run/docker.sock"), Path("/run/docker.sock")):
        if socket_path.exists() or socket_path.is_symlink():
            raise ProofError("Docker authority is visible inside the proof image")
    interfaces = network_interfaces(
        _read_text_bounded(Path("/proc/net/dev"), MAX_PROC_NET_BYTES)
    )
    assert_loopback_routes(
        _read_text_bounded(Path("/proc/net/route"), MAX_PROC_NET_BYTES),
        _read_text_bounded(Path("/proc/net/ipv6_route"), MAX_PROC_NET_BYTES),
    )
    _assert_unwritable(WORKSPACE, ".mgb-m15-write-probe")
    _assert_unwritable(Path("/etc"), ".mgb-m15-write-probe")

    home = Path("/tmp/mgb-home")  # noqa: S108 - the runner supplies bounded tmpfs.
    home.mkdir(mode=0o700, exist_ok=True)
    environment = {
        "HOME": str(home),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": os.defpath,
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(WORKSPACE / "src"),
        "TMPDIR": "/tmp",  # noqa: S108 - the runner supplies bounded tmpfs.
    }
    completed = subprocess.run(  # noqa: S603 - command and modules are fixed above.
        test_command(WORKSPACE),
        cwd=WORKSPACE,
        env=environment,
        stdin=subprocess.DEVNULL,
        check=False,
        timeout=600,
    )
    if completed.returncode != 0:
        raise ProofError("focused M15 test suite failed")
    artifact_value = os.environ.get(ARTIFACT_ENV)
    if artifact_value != "/mgb-artifacts.tar.gz":
        raise ProofError("artifact upload authority is invalid")
    python_version = (
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
    summary: dict[str, object] = {
        "boundary": {
            "docker_authority": "absent",
            "network_interfaces": list(interfaces),
            "network_routes": "loopback-only",
            "root_filesystem": "read-only",
            "runtime_user": "non-root",
            "workspace": "read-only",
        },
        "python": python_version,
        "schema_version": 1,
        "status": "passed",
        "test_modules": list(TEST_MODULES),
    }
    _write_artifact(Path(artifact_value), evidence_archive(summary))
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")), flush=True)
    return 0


def _cancel_proof() -> int:
    def terminate(_signum: int, _frame: object) -> None:
        print('{"status":"cancelled"}', flush=True)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        time.sleep(1)
    raise ProofError("cancellation proof was not cancelled")


def main(argv: Sequence[str] | None = None) -> int:
    arguments = tuple(sys.argv[1:] if argv is None else argv)
    try:
        if arguments == ("prove",):
            return _run_proof()
        if arguments == ("cancel",):
            return _cancel_proof()
        raise ProofError("expected exactly one proof mode")
    except ProofError as exc:
        print(f"m15-remote-proof: {exc}", file=sys.stderr)
        return 2
    except (OSError, subprocess.SubprocessError):
        print("m15-remote-proof: operation failed", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
