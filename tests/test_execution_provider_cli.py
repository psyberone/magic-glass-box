from __future__ import annotations

import base64
import contextlib
import hashlib
import io
import json
import os
import subprocess
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path

from magic_glass_box.domain import canonical_json
from magic_glass_box.execution_provider import (
    ControlledExecutionProvider,
    ExecutionBackendAmbiguous,
    ExecutionBackendUnavailable,
    ExecutionJobSpec,
)
from magic_glass_box.execution_provider_cli import (
    BoundedCommandResult,
    BoundedCommandRunner,
    ExecutionProviderCLIError,
    GitCandidateArchiveSource,
    SSHExecutionBackend,
    build_parser,
    load_config,
    load_spec,
    main,
    serve_unix,
    validate_spec_config,
)
from magic_glass_box.remote_runner import CandidateArchiveMaterializer
from magic_glass_box.service_sidecar import (
    ServiceOperationRequest,
    ServiceOperationSpec,
    ServiceSidecarSpec,
    UnixServiceSidecarClient,
)
from magic_glass_box.workspace import SnapshotResult

DEPLOYMENT = "d" * 64
SCOPE = "e" * 64
BASE = "a" * 40
HEAD = "b" * 40
RECIPE_SHA256 = "4" * 64
IMAGE_ID = "sha256:" + "5" * 64
EXECUTION_ID = "rex_" + "3" * 32
DEADLINE = "2099-01-01T00:00:00.000000+00:00"


def job(**overrides: object) -> ExecutionJobSpec:
    values: dict[str, object] = {
        "operation_id": "sop_" + "1" * 32,
        "mapping_name": "azure-quality",
        "repository": "acme/widgets",
        "candidate_id": "cnd_" + "2" * 32,
        "object_format": "sha1",
        "base_revision": BASE,
        "candidate_revision": HEAD,
        "recipe_id": "quality",
        "recipe_sha256": RECIPE_SHA256,
        "image_id": IMAGE_ID,
    }
    values.update(overrides)
    return ExecutionJobSpec(**values)


def status_document(*, state: str = "running") -> dict[str, object]:
    document: dict[str, object] = {
        "schema_version": 1,
        "execution_id": EXECUTION_ID,
        "state": state,
        "candidate_revision": HEAD,
        "object_format": "sha1",
        "recipe_id": "quality",
        "recipe_sha256": RECIPE_SHA256,
        "image_id": IMAGE_ID,
        "source_bytes": len(b"candidate archive"),
        "source_sha256": hashlib.sha256(b"candidate archive").hexdigest(),
        "submitted_at": "2026-10-28T12:00:00.000000+00:00",
        "started_at": "2026-10-28T12:00:01.000000+00:00",
        "finished_at": None,
        "exit_code": None,
    }
    if state == "succeeded":
        document.update(
            {
                "finished_at": "2026-10-28T12:00:05.000000+00:00",
                "exit_code": 0,
            }
        )
    return document


def evidence_document() -> dict[str, object]:
    bundle = b"evidence"
    return {
        "schema_version": 1,
        "execution": status_document(state="succeeded"),
        "duration_ms": 4000,
        "output_bytes": 0,
        "output_sha256": hashlib.sha256(b"").hexdigest(),
        "artifact_bytes": len(bundle),
        "artifact_count": 1,
        "artifact_sha256": hashlib.sha256(bundle).hexdigest(),
        "artifact_encoding": "tar+gzip+base64",
        "artifact_bundle": base64.b64encode(bundle).decode("ascii"),
    }


class FakeCommandRunner:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response
        self.returncode = 0
        self.calls: list[dict[str, object]] = []

    def run(
        self,
        command: list[str],
        *,
        input_bytes: bytes,
        timeout: float,
        stdout_limit: int,
        environment: dict[str, str],
    ) -> BoundedCommandResult:
        self.calls.append(
            {
                "command": list(command),
                "input": input_bytes,
                "timeout": timeout,
                "stdout_limit": stdout_limit,
                "environment": dict(environment),
            }
        )
        return BoundedCommandResult(
            self.returncode,
            canonical_json(self.response).encode("utf-8"),
            b"remote failure" if self.returncode else b"",
        )


class FakeCandidateSource:
    def __init__(self, archive: bytes = b"candidate archive") -> None:
        self.bundle = archive
        self.calls: list[ExecutionJobSpec] = []

    def archive(self, selected: ExecutionJobSpec) -> bytes:
        self.calls.append(selected)
        return self.bundle


class FailingCandidateSource:
    def archive(self, selected: ExecutionJobSpec) -> bytes:
        raise ExecutionProviderCLIError("candidate archive export failed")


class FakeCandidateSnapshotter:
    def __init__(self, *, revision: str = HEAD) -> None:
        self.revision = revision
        self.calls: list[tuple[Path, str | None, Path]] = []

    def snapshot(
        self, repository: Path, revision: str | None, destination: Path
    ) -> SnapshotResult:
        self.calls.append((repository, revision, destination))
        (destination / "candidate.txt").write_bytes(b"raw candidate")
        return SnapshotResult(self.revision, ())


class ExecutionProviderCLITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
        self.identity_file = self.root / "id_ed25519"
        self.identity_file.write_text("private-key-material", encoding="utf-8")
        os.chmod(self.identity_file, 0o600)
        self.known_hosts = self.root / "known_hosts"
        self.known_hosts.write_text(
            "runner.example.test ssh-ed25519 AAAATEST", encoding="utf-8"
        )
        os.chmod(self.known_hosts, 0o600)
        self.ssh_binary = self.root / "ssh"
        self.ssh_binary.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        os.chmod(self.ssh_binary, 0o700)
        self.git_binary = self.root / "git"
        self.git_binary.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        os.chmod(self.git_binary, 0o700)
        self.repository = self.root / "repository.git"
        self.repository.mkdir(mode=0o700)
        self.config_file = self.root / "runner-config.json"
        self.config_file.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "ssh_binary": str(self.ssh_binary),
                    "git_binary": str(self.git_binary),
                    "host": "runner.example.test",
                    "port": 22,
                    "account": "runner",
                    "identity_file": str(self.identity_file),
                    "known_hosts_file": str(self.known_hosts),
                    "remote_helper": "/usr/local/bin/magic-glass-box-remote-runner",
                    "repositories": {"acme/widgets": str(self.repository)},
                }
            ),
            encoding="utf-8",
        )
        os.chmod(self.config_file, 0o600)

    def spec(self, scope: str) -> ServiceSidecarSpec:
        return ServiceSidecarSpec.from_mapping(
            {
                "schema_version": 1,
                "connector_protocol_version": 1,
                "deployment_sha256": DEPLOYMENT,
                "transport": {
                    "kind": "unix",
                    "socket_path": str(self.root / "runner.sock"),
                },
                "capabilities": [
                    {
                        "name": "execution.submit",
                        "version": 1,
                        "mode": "effect",
                    },
                    {
                        "name": "execution.inspect",
                        "version": 1,
                        "mode": "read",
                    },
                ],
                "mappings": [
                    {
                        "name": "azure-quality",
                        "service": "ssh-runner",
                        "host": "runner.example.test",
                        "repository": "acme/widgets",
                        "account": "runner",
                        "credential_scope_sha256": scope,
                    }
                ],
                "request_bytes": 128 * 1024,
                "response_bytes": 4 * 1024 * 1024,
                "timeout_ms": 30000,
                "max_concurrency": 2,
                "max_requests_per_minute": 60,
            }
        )

    def rewrite_config(self, **changes: object) -> None:
        document = json.loads(self.config_file.read_text(encoding="utf-8"))
        document.update(changes)
        self.config_file.write_text(json.dumps(document), encoding="utf-8")
        os.chmod(self.config_file, 0o600)

    def test_private_config_identity_binds_spec_without_exposing_key(self) -> None:
        config = load_config(self.config_file)
        identity = config.identity.to_dict()

        candidate_sources_sha256 = hashlib.sha256(
            canonical_json({"acme/widgets": str(self.repository)}).encode("utf-8")
        ).hexdigest()
        expected_scope = hashlib.sha256(
            canonical_json(
                {
                    "account": "runner",
                    "candidate_sources_sha256": candidate_sources_sha256,
                    "host": "runner.example.test",
                    "identity_sha256": hashlib.sha256(
                        b"private-key-material"
                    ).hexdigest(),
                    "known_hosts_sha256": hashlib.sha256(
                        b"runner.example.test ssh-ed25519 AAAATEST"
                    ).hexdigest(),
                    "port": 22,
                    "remote_helper": ("/usr/local/bin/magic-glass-box-remote-runner"),
                }
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(config.ssh_binary, self.ssh_binary)
        self.assertEqual(config.git_binary, self.git_binary)
        self.assertEqual(config.host, "runner.example.test")
        self.assertEqual(config.port, 22)
        self.assertEqual(config.account, "runner")
        self.assertEqual(config.identity_file, self.identity_file)
        self.assertEqual(config.known_hosts_file, self.known_hosts)
        self.assertEqual(
            config.remote_helper,
            "/usr/local/bin/magic-glass-box-remote-runner",
        )
        self.assertEqual(config.repositories, {"acme/widgets": self.repository})
        self.assertEqual(config.credential_scope_sha256, expected_scope)
        self.assertEqual(
            identity,
            {
                "account": "runner",
                "credential_scope_sha256": expected_scope,
                "host": "runner.example.test",
            },
        )
        self.assertNotIn("private-key-material", canonical_json(identity))
        self.assertNotIn(str(self.repository), canonical_json(identity))
        validate_spec_config(self.spec(identity["credential_scope_sha256"]), config)

        with self.assertRaisesRegex(ExecutionProviderCLIError, "credential scope"):
            validate_spec_config(self.spec(SCOPE), config)

    def test_config_and_credential_files_must_be_private_regular_files(self) -> None:
        os.chmod(self.config_file, 0o644)
        with self.assertRaisesRegex(ExecutionProviderCLIError, "accessible"):
            load_config(self.config_file)

    def test_config_rejects_unbounded_routes_and_unsafe_local_sources(self) -> None:
        original = self.config_file.read_bytes()
        cases = (
            ({"host": "-oProxyCommand=bad"}, "configuration"),
            ({"account": "UPPER"}, "configuration"),
            ({"port": 0}, "configuration"),
            ({"remote_helper": "relative/helper"}, "configuration"),
            ({"repositories": {}}, "configuration"),
            ({"repositories": {"bad@repo": str(self.repository)}}, "mapping"),
            ({"repositories": {"acme/widgets": "relative.git"}}, "absolute"),
        )
        for changes, message in cases:
            self.config_file.write_bytes(original)
            os.chmod(self.config_file, 0o600)
            self.rewrite_config(**changes)
            with (
                self.subTest(changes=changes),
                self.assertRaisesRegex(ExecutionProviderCLIError, message),
            ):
                load_config(self.config_file)

        self.config_file.write_bytes(original)
        os.chmod(self.config_file, 0o600)
        self.ssh_binary.chmod(0o722)
        with self.assertRaisesRegex(ExecutionProviderCLIError, "unsafe"):
            load_config(self.config_file)

    def test_spec_binding_rejects_wrong_host_account_and_repository(self) -> None:
        config = load_config(self.config_file)
        base = self.spec(config.credential_scope_sha256).to_dict()
        cases = (
            ("host", "elsewhere.example.test", "host"),
            ("account", "other", "account"),
            ("repository", "other/repository", "repository"),
        )
        for field, value, message in cases:
            document = json.loads(json.dumps(base))
            document["mappings"][0][field] = value
            changed = ServiceSidecarSpec.from_mapping(document)
            with (
                self.subTest(field=field),
                self.assertRaisesRegex(ExecutionProviderCLIError, message),
            ):
                validate_spec_config(changed, config)

        with self.assertRaisesRegex(ExecutionProviderCLIError, "authority"):
            validate_spec_config(None, config)  # type: ignore[arg-type]
        os.chmod(self.config_file, 0o600)

        os.chmod(self.identity_file, 0o644)
        with self.assertRaisesRegex(ExecutionProviderCLIError, "accessible"):
            load_config(self.config_file)

    def test_ssh_backend_uses_fixed_hardened_command_and_json_request(self) -> None:
        config = load_config(self.config_file)
        runner = FakeCommandRunner(
            {"schema_version": 1, "outcome": "found", "result": status_document()}
        )
        source = FakeCandidateSource()
        backend = SSHExecutionBackend(config, runner=runner, candidate_source=source)

        result = backend.submit(job(), deadline=DEADLINE)

        self.assertEqual(result.execution_id, EXECUTION_ID)
        call = runner.calls[0]
        command = call["command"]
        self.assertEqual(
            command,
            [
                str(self.ssh_binary),
                "-F",
                os.devnull,
                "-o",
                "BatchMode=yes",
                "-o",
                "IdentitiesOnly=yes",
                "-o",
                "StrictHostKeyChecking=yes",
                "-o",
                "PasswordAuthentication=no",
                "-o",
                "KbdInteractiveAuthentication=no",
                "-o",
                "ClearAllForwardings=yes",
                "-o",
                "ForwardAgent=no",
                "-o",
                "RequestTTY=no",
                "-o",
                "PermitLocalCommand=no",
                "-o",
                "LogLevel=ERROR",
                "-o",
                "ConnectionAttempts=1",
                "-o",
                "ConnectTimeout=30",
                "-o",
                "ServerAliveInterval=15",
                "-o",
                "ServerAliveCountMax=2",
                "-o",
                f"UserKnownHostsFile={self.known_hosts}",
                "-o",
                f"GlobalKnownHostsFile={os.devnull}",
                "-i",
                str(self.identity_file),
                "-p",
                "22",
                "runner@runner.example.test",
                config.remote_helper,
                "invoke",
            ],
        )
        payload = json.loads(call["input"])
        self.assertEqual(payload["action"], "submit")
        self.assertEqual(payload["job"], job().to_dict())
        self.assertIsNone(payload["execution_id"])
        self.assertEqual(
            base64.b64decode(payload["candidate_archive"]["bundle"]),
            b"candidate archive",
        )
        self.assertEqual(
            payload["candidate_archive"]["sha256"],
            hashlib.sha256(b"candidate archive").hexdigest(),
        )
        self.assertEqual(source.calls, [job()])
        self.assertNotIn("private-key-material", call["input"].decode("utf-8"))
        self.assertEqual(call["timeout"], 120.0)
        self.assertEqual(call["stdout_limit"], 4 * 1024 * 1024)
        self.assertEqual(
            call["environment"],
            {
                "HOME": str(self.root),
                "LANG": "C",
                "LC_ALL": "C",
                "PATH": os.defpath,
            },
        )

    def test_parser_keeps_configuration_and_transport_inputs_explicit(self) -> None:
        parser = build_parser()
        self.assertEqual(parser.prog, "magic-glass-box-execution-provider")
        self.assertIn("credential-isolated", parser.description)
        self.assertIn("Show non-secret SSH identity", parser.format_help())
        self.assertIn("Serve the bounded provider carrier", parser.format_help())
        inspected = parser.parse_args(["inspect", "--config", str(self.config_file)])
        self.assertEqual(
            vars(inspected), {"command": "inspect", "config": self.config_file}
        )
        served = parser.parse_args(
            [
                "serve",
                "--config",
                str(self.config_file),
                "--spec",
                str(self.root / "spec.json"),
                "--socket-path",
                str(self.root / "provider.sock"),
                "--max-invocations",
                "3",
            ]
        )
        self.assertEqual(
            vars(served),
            {
                "command": "serve",
                "config": self.config_file,
                "max_invocations": 3,
                "socket_path": self.root / "provider.sock",
                "spec": self.root / "spec.json",
            },
        )

    def test_candidate_source_exports_only_the_exact_pinned_commit(self) -> None:
        config = load_config(self.config_file)
        snapshotter = FakeCandidateSnapshotter()
        source = GitCandidateArchiveSource(config, snapshotter=snapshotter)

        archive = source.archive(job())

        self.assertGreater(len(archive), 0)
        self.assertEqual(snapshotter.calls[0][0], self.repository)
        self.assertEqual(snapshotter.calls[0][1], HEAD)
        destination = self.root / "candidate-extracted"
        destination.mkdir(mode=0o700)
        CandidateArchiveMaterializer().materialize(archive, destination)
        self.assertEqual((destination / "candidate.txt").read_bytes(), b"raw candidate")

        changed = GitCandidateArchiveSource(
            config,
            snapshotter=FakeCandidateSnapshotter(revision="c" * 40),
        )
        with self.assertRaisesRegex(ExecutionProviderCLIError, "identity"):
            changed.archive(job())

    def test_candidate_source_retains_raw_git_bytes_and_export_ignored_files(
        self,
    ) -> None:
        repository = self.root / "raw-source"
        repository.mkdir(mode=0o700)

        def git(*arguments: str) -> bytes:
            return subprocess.run(  # noqa: S603 - fixed test Git binary.
                ["/usr/bin/git", "-C", str(repository), *arguments],
                check=True,
                capture_output=True,
            ).stdout

        git("init", "-q")
        git("config", "user.name", "M15 Test")
        git("config", "user.email", "m15@example.invalid")
        (repository / ".gitattributes").write_bytes(
            b"hidden export-ignore\nsubst export-subst\nfiltered filter=hostile\n"
        )
        (repository / "hidden").write_bytes(b"must remain\n")
        (repository / "subst").write_bytes(b"$Format:%H$\n")
        (repository / "filtered").write_bytes(b"raw bytes\n")
        git("add", ".")
        git("commit", "-qm", "raw source")
        revision = git("rev-parse", "HEAD").decode("ascii").strip()
        git("config", "filter.hostile.smudge", "false")
        candidate_repository = self.root / "raw-source.git"
        subprocess.run(  # noqa: S603 - fixed test Git binary.
            [
                "/usr/bin/git",
                "clone",
                "--bare",
                "--no-local",
                str(repository),
                str(candidate_repository),
            ],
            check=True,
            capture_output=True,
        )
        config = replace(
            load_config(self.config_file),
            git_binary=Path("/usr/bin/git"),
            repositories={"acme/widgets": candidate_repository},
        )

        archive = GitCandidateArchiveSource(config).archive(
            job(candidate_revision=revision)
        )
        destination = self.root / "raw-extracted"
        destination.mkdir(mode=0o700)
        CandidateArchiveMaterializer().materialize(archive, destination)

        self.assertEqual((destination / "hidden").read_bytes(), b"must remain\n")
        self.assertEqual((destination / "subst").read_bytes(), b"$Format:%H$\n")
        self.assertEqual((destination / "filtered").read_bytes(), b"raw bytes\n")

    def test_candidate_package_preserves_modes_symlinks_and_rejects_specials(
        self,
    ) -> None:
        source = self.root / "package-source"
        source.mkdir(mode=0o700)
        nested = source / "nested"
        nested.mkdir()
        executable = nested / "run"
        executable.write_bytes(b"#!/bin/sh\n")
        executable.chmod(0o755)
        (source / "link").symlink_to("nested/run")

        archive = GitCandidateArchiveSource._package(source)
        destination = self.root / "package-destination"
        destination.mkdir(mode=0o700)
        CandidateArchiveMaterializer().materialize(archive, destination)

        self.assertTrue((destination / "nested/run").stat().st_mode & 0o111)
        self.assertEqual(os.readlink(destination / "link"), "nested/run")

        special = source / "pipe"
        os.mkfifo(special)
        with self.assertRaisesRegex(ExecutionProviderCLIError, "unsupported"):
            GitCandidateArchiveSource._package(source)

    def test_not_found_is_distinct_from_transport_failure(self) -> None:
        config = load_config(self.config_file)
        runner = FakeCommandRunner(
            {"schema_version": 1, "outcome": "not_found", "result": None}
        )
        backend = SSHExecutionBackend(
            config, runner=runner, candidate_source=FakeCandidateSource()
        )
        self.assertIsNone(backend.reconcile_submit(job(), deadline=DEADLINE))
        reconcile_payload = json.loads(runner.calls[0]["input"])
        self.assertEqual(reconcile_payload["action"], "reconcile_submit")
        self.assertEqual(
            base64.b64decode(reconcile_payload["candidate_archive"]["bundle"]),
            b"candidate archive",
        )
        self.assertIsNone(backend.inspect(job(), EXECUTION_ID, deadline=DEADLINE))

        runner.returncode = 255
        with self.assertRaises(ExecutionBackendAmbiguous):
            backend.submit(job(), deadline=DEADLINE)
        with self.assertRaises(ExecutionBackendUnavailable):
            backend.inspect(job(), EXECUTION_ID, deadline=DEADLINE)

    def test_backend_collects_evidence_and_checks_source_provenance(self) -> None:
        config = load_config(self.config_file)
        runner = FakeCommandRunner(
            {
                "schema_version": 1,
                "outcome": "found",
                "result": evidence_document(),
            }
        )
        backend = SSHExecutionBackend(
            config, runner=runner, candidate_source=FakeCandidateSource()
        )

        evidence = backend.collect(job(), EXECUTION_ID, deadline=DEADLINE)

        self.assertEqual(evidence.artifact_bundle, b"evidence")
        self.assertEqual(json.loads(runner.calls[0]["input"])["action"], "collect")

        mismatched = status_document()
        mismatched["source_sha256"] = "0" * 64
        runner.response = {
            "schema_version": 1,
            "outcome": "found",
            "result": mismatched,
        }
        with self.assertRaises(ExecutionBackendAmbiguous):
            backend.submit(job(), deadline=DEADLINE)

    def test_deadlines_and_backend_configuration_fail_before_unsafe_dispatch(
        self,
    ) -> None:
        with self.assertRaisesRegex(ExecutionProviderCLIError, "configuration"):
            SSHExecutionBackend(None)  # type: ignore[arg-type]
        with self.assertRaises(ExecutionBackendUnavailable):
            SSHExecutionBackend._remaining("not-a-time")
        with self.assertRaisesRegex(ExecutionBackendUnavailable, "expired"):
            SSHExecutionBackend._remaining("2000-01-01T00:00:00+00:00")

    def test_candidate_export_failure_is_known_pre_dispatch(self) -> None:
        backend = SSHExecutionBackend(
            load_config(self.config_file),
            runner=FakeCommandRunner(
                {
                    "schema_version": 1,
                    "outcome": "found",
                    "result": status_document(),
                }
            ),
            candidate_source=FailingCandidateSource(),
        )

        with self.assertRaises(ExecutionBackendUnavailable):
            backend.submit(job(), deadline=DEADLINE)

    def test_remote_response_is_exact_and_bounded(self) -> None:
        config = load_config(self.config_file)
        runner = FakeCommandRunner(
            {
                "schema_version": 1,
                "outcome": "found",
                "result": status_document(),
                "extra": True,
            }
        )
        backend = SSHExecutionBackend(
            config, runner=runner, candidate_source=FakeCandidateSource()
        )
        with self.assertRaises(ExecutionBackendAmbiguous):
            backend.submit(job(), deadline=DEADLINE)
        with self.assertRaises(ExecutionBackendUnavailable):
            backend.inspect(job(), EXECUTION_ID, deadline=DEADLINE)

    def test_bounded_command_runner_enforces_input_output_and_deadline(self) -> None:
        runner = BoundedCommandRunner()
        environment = {"PATH": os.defpath, "LANG": "C", "LC_ALL": "C"}
        completed = runner.run(
            ["/bin/cat"],
            input_bytes=b"hello",
            timeout=1.0,
            stdout_limit=16,
            environment=environment,
        )
        self.assertEqual(completed.stdout, b"hello")

        with self.assertRaisesRegex(ExecutionProviderCLIError, "request"):
            runner.run(
                ["relative"],
                input_bytes=b"",
                timeout=1.0,
                stdout_limit=1,
                environment=environment,
            )
        with self.assertRaisesRegex(ExecutionProviderCLIError, "output"):
            runner.run(
                ["/bin/echo", "oversized"],
                input_bytes=b"",
                timeout=1.0,
                stdout_limit=2,
                environment=environment,
            )
        with self.assertRaises(subprocess.TimeoutExpired):
            runner.run(
                ["/bin/sleep", "1"],
                input_bytes=b"",
                timeout=0.01,
                stdout_limit=1,
                environment=environment,
            )

    def test_spec_loader_and_inspect_cli_emit_only_non_secret_identity(self) -> None:
        config = load_config(self.config_file)
        spec_file = self.root / "execution-spec.json"
        spec_file.write_text(
            json.dumps(self.spec(config.credential_scope_sha256).to_dict()),
            encoding="utf-8",
        )
        self.assertEqual(
            load_spec(spec_file).digest,
            self.spec(config.credential_scope_sha256).digest,
        )

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            status = main(["inspect", "--config", str(self.config_file)])
        self.assertEqual(status, 0)
        self.assertNotIn("private-key-material", output.getvalue())

        os.chmod(self.config_file, 0o644)
        errors = io.StringIO()
        with contextlib.redirect_stderr(errors):
            status = main(["inspect", "--config", str(self.config_file)])
        self.assertEqual(status, 2)
        self.assertIn("accessible", errors.getvalue())

    def test_unix_carrier_serves_one_bounded_execution_request(self) -> None:
        config = load_config(self.config_file)
        spec = self.spec(config.credential_scope_sha256)
        operation = ServiceOperationSpec.from_mapping(
            {
                "schema_version": 1,
                "connector_name": "remote-main",
                "deployment_sha256": DEPLOYMENT,
                "mapping": spec.mappings[0].to_dict(),
                "capability": {
                    "name": "execution.submit",
                    "version": 1,
                    "mode": "effect",
                },
                "idempotency_key": "m15-carrier-test",
                "target": {
                    "repository": "acme/widgets",
                    "resource_id": "cnd_" + "2" * 32,
                    "object_format": "sha1",
                    "expected_revision": BASE,
                    "desired_revision": HEAD,
                },
                "parameters": {
                    "recipe_id": "quality",
                    "recipe_sha256": RECIPE_SHA256,
                    "image_id": IMAGE_ID,
                },
                "request_bytes": 128 * 1024,
                "response_bytes": 4 * 1024 * 1024,
                "timeout_ms": 30000,
            }
        )
        request = ServiceOperationRequest.create(
            operation_id="sop_" + "7" * 32,
            connector_id="ssc_" + "1" * 32,
            connector_protocol_version=1,
            operation=operation,
            phase="invoke",
            deadline=DEADLINE,
        )
        command = FakeCommandRunner(
            {"schema_version": 1, "outcome": "found", "result": status_document()}
        )
        provider = ControlledExecutionProvider(
            spec,
            SSHExecutionBackend(
                config,
                runner=command,
                candidate_source=FakeCandidateSource(),
            ),
        )
        failures: list[BaseException] = []

        def target() -> None:
            try:
                serve_unix(Path(spec.transport.socket_path), 1, provider)
            except BaseException as exc:  # pragma: no cover - asserted below
                failures.append(exc)

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        for _ in range(100):
            if Path(spec.transport.socket_path).exists():
                break
            time.sleep(0.01)
        response = UnixServiceSidecarClient(Path(spec.transport.socket_path)).invoke(
            request
        )
        thread.join(timeout=2)

        self.assertEqual(failures, [])
        self.assertFalse(thread.is_alive())
        self.assertEqual(response.outcome, "succeeded")
        self.assertEqual(response.receipt.result, status_document())


if __name__ == "__main__":
    unittest.main()
