from __future__ import annotations

import base64
import contextlib
import gzip
import hashlib
import io
import json
import os
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from magic_glass_box.domain import canonical_json
from magic_glass_box.execution import ExecutionJobSpec
from magic_glass_box.execution_provider_cli import BoundedCommandResult
from magic_glass_box.remote_runner import (
    MAX_DOCKER_OUTPUT_BYTES,
    ArtifactArchiveMaterializer,
    CandidateArchiveMaterializer,
    DockerCLI,
    DockerState,
    RemoteRunner,
    RemoteRunnerError,
    handle_request,
    load_remote_config,
)
from magic_glass_box.remote_runner import build_parser as build_remote_parser
from magic_glass_box.remote_runner import main as remote_runner_main

BASE = "a" * 40
HEAD = "b" * 40
IMAGE_ID = "sha256:" + "5" * 64
EXECUTION_ID = "rex_" + "9" * 32
DEADLINE = "2099-01-01T00:00:00.000000+00:00"


def artifact_archive(entries: list[tuple[str, bytes | str, str]]) -> bytes:
    output = io.BytesIO()
    with (
        gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as compressed,
        tarfile.open(fileobj=compressed, mode="w") as archive,
    ):
        for name, content, kind in entries:
            info = tarfile.TarInfo(name)
            info.mtime = 0
            if kind == "file":
                assert isinstance(content, bytes)
                info.size = len(content)
                archive.addfile(info, io.BytesIO(content))
            elif kind == "symlink":
                assert isinstance(content, str)
                info.type = tarfile.SYMTYPE
                info.linkname = content
                archive.addfile(info)
            else:
                info.type = tarfile.FIFOTYPE
                archive.addfile(info)
    return output.getvalue()


class FakeMaterializer:
    def __init__(self) -> None:
        self.calls: list[tuple[bytes, Path]] = []

    def materialize(self, archive: bytes, destination: Path) -> None:
        self.calls.append((archive, destination))
        (destination / "source.txt").write_text("exact candidate", encoding="utf-8")


class FailsOnceMaterializer(FakeMaterializer):
    def materialize(self, archive: bytes, destination: Path) -> None:
        if not self.calls:
            self.calls.append((archive, destination))
            (destination / "partial.txt").write_text("partial", encoding="utf-8")
            raise RemoteRunnerError("simulated boundary failure")
        super().materialize(archive, destination)


class FakeDocker:
    def __init__(self) -> None:
        self.created: list[dict[str, object]] = []
        self.started: list[str] = []
        self.cancelled: list[str] = []
        self.state: DockerState | None = None
        self.stdout = b"all tests passed\n"
        self.stderr = b""
        self.artifact_bundle = artifact_archive([])

    def create(self, **values: object) -> None:
        self.created.append(dict(values))
        self.state = DockerState(
            status="created", started_at=None, finished_at=None, exit_code=None
        )

    def start(self, container_name: str, *, timeout_seconds: int) -> None:
        self.started.append(container_name)
        self.state = DockerState(
            status="running",
            started_at="2026-10-28T12:00:01.000000+00:00",
            finished_at=None,
            exit_code=None,
        )

    def inspect(self, container_name: str) -> DockerState | None:
        return self.state

    def cancel(
        self, container_name: str, *, stop_seconds: int, timeout_seconds: int
    ) -> None:
        self.cancelled.append(container_name)
        self.state = DockerState(
            status="exited",
            started_at="2026-10-28T12:00:01.000000+00:00",
            finished_at="2026-10-28T12:00:05.000000+00:00",
            exit_code=143,
        )

    def logs(self, container_name: str, *, timeout_seconds: int) -> tuple[bytes, bytes]:
        return self.stdout, self.stderr

    def artifacts(
        self,
        container_name: str,
        *,
        timeout_seconds: int,
        output_limit: int,
    ) -> bytes:
        if len(self.artifact_bundle) > output_limit:
            raise RemoteRunnerError("artifact archive exceeds its limit")
        return self.artifact_bundle


class FailsOnceStartDocker(FakeDocker):
    def __init__(self) -> None:
        super().__init__()
        self.failed = False

    def start(self, container_name: str, *, timeout_seconds: int) -> None:
        if not self.failed:
            self.failed = True
            raise RemoteRunnerError("simulated start reply loss")
        super().start(container_name, timeout_seconds=timeout_seconds)


class FailsOnceCancelDocker(FakeDocker):
    def __init__(self) -> None:
        super().__init__()
        self.failed = False

    def cancel(
        self, container_name: str, *, stop_seconds: int, timeout_seconds: int
    ) -> None:
        if not self.failed:
            self.failed = True
            raise RemoteRunnerError("simulated cancellation reply loss")
        super().cancel(
            container_name,
            stop_seconds=stop_seconds,
            timeout_seconds=timeout_seconds,
        )


class ZeroExitCancelDocker(FakeDocker):
    def cancel(
        self, container_name: str, *, stop_seconds: int, timeout_seconds: int
    ) -> None:
        self.cancelled.append(container_name)
        self.state = DockerState(
            status="exited",
            started_at="2026-10-28T12:00:01.000000+00:00",
            finished_at="2026-10-28T12:00:05.000000+00:00",
            exit_code=0,
        )


class CapturingCommandRunner:
    def __init__(
        self,
        result: BoundedCommandResult | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.calls: list[dict[str, object]] = []
        self.result = result or BoundedCommandResult(0, b"", b"")
        self.error = error

    def run(self, command: list[str], **values: object) -> BoundedCommandResult:
        self.calls.append({"command": list(command), **values})
        if self.error is not None:
            raise self.error
        return self.result


class RemoteRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        os.chmod(self.root, 0o700)
        self.state = self.root / "state"
        self.state.mkdir(mode=0o700)
        self.docker_binary = self.root / "docker"
        self.git_binary = self.root / "git"
        for binary in (self.docker_binary, self.git_binary):
            binary.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
            os.chmod(binary, 0o700)
        self.config_path = self.root / "remote-runner.json"
        self.config_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "state_directory": str(self.state),
                    "docker_binary": str(self.docker_binary),
                    "git_binary": str(self.git_binary),
                    "repositories": ["acme/widgets"],
                    "recipes": {
                        "quality": {
                            "image_id": IMAGE_ID,
                            "command": ["make", "quality"],
                            "timeout_seconds": 1800,
                            "stop_seconds": 10,
                            "cpu_millis": 2000,
                            "memory_bytes": 2147483648,
                            "pids_limit": 256,
                            "max_artifact_bytes": 1048576,
                            "max_artifacts": 1000,
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        os.chmod(self.config_path, 0o600)
        self.config = load_remote_config(self.config_path)
        self.materializer = FakeMaterializer()
        self.docker = FakeDocker()
        self.runner = RemoteRunner(
            self.config,
            docker=self.docker,
            materializer=self.materializer,
            clock=lambda: "2026-10-28T12:00:00.000000+00:00",
        )
        self.candidate_archive = b"candidate archive"

    def job(self, **overrides: object) -> ExecutionJobSpec:
        values: dict[str, object] = {
            "operation_id": "sop_" + "1" * 32,
            "mapping_name": "azure-quality",
            "repository": "acme/widgets",
            "candidate_id": "cnd_" + "2" * 32,
            "object_format": "sha1",
            "base_revision": BASE,
            "candidate_revision": HEAD,
            "recipe_id": "quality",
            "recipe_sha256": self.config.recipes["quality"].digest,
            "image_id": IMAGE_ID,
        }
        values.update(overrides)
        return ExecutionJobSpec(**values)

    def request(
        self,
        action: str,
        *,
        execution_id: str | None = None,
        archive: bool = False,
    ) -> dict[str, object]:
        candidate: dict[str, object] | None = None
        if archive:
            candidate = {
                "encoding": "tar+gzip+base64",
                "bytes": len(self.candidate_archive),
                "sha256": hashlib.sha256(self.candidate_archive).hexdigest(),
                "bundle": base64.b64encode(self.candidate_archive).decode("ascii"),
            }
        return {
            "schema_version": 1,
            "action": action,
            "deadline": DEADLINE,
            "execution_id": execution_id,
            "job": self.job().to_dict(),
            "candidate_archive": candidate,
        }

    def test_config_derives_recipe_identity_and_hides_command_from_identity(
        self,
    ) -> None:
        recipe = self.config.recipes["quality"]
        expected_digest = hashlib.sha256(
            canonical_json(recipe.to_dict()).encode("utf-8")
        ).hexdigest()
        self.assertEqual(recipe.digest, expected_digest)
        self.assertEqual(recipe.image_id, IMAGE_ID)
        self.assertEqual(recipe.command, ("make", "quality"))
        self.assertEqual(self.config.state_directory, self.state)
        self.assertEqual(self.config.docker_binary, self.docker_binary)
        self.assertEqual(self.config.git_binary, self.git_binary)
        self.assertEqual(self.config.repositories, frozenset({"acme/widgets"}))
        identity = self.config.identity()
        self.assertEqual(
            identity["recipes"],
            [
                {
                    "image_id": IMAGE_ID,
                    "recipe_id": "quality",
                    "recipe_sha256": recipe.digest,
                }
            ],
        )
        self.assertNotIn("command", json.dumps(identity))

    def test_config_rejects_unbounded_authority_and_recipe_values(self) -> None:
        original = json.loads(self.config_path.read_text(encoding="utf-8"))
        cases: tuple[tuple[dict[str, object], str], ...] = (
            ({"schema_version": 2}, "version"),
            ({"repositories": []}, "configuration"),
            ({"repositories": ["acme/widgets", "acme/widgets"]}, "unique"),
            ({"repositories": ["acme/widgets?admin=true"]}, "mapping"),
            ({"recipes": {}}, "configuration"),
        )
        for changes, message in cases:
            document = json.loads(json.dumps(original))
            document.update(changes)
            self.config_path.write_text(json.dumps(document), encoding="utf-8")
            os.chmod(self.config_path, 0o600)
            with (
                self.subTest(changes=changes),
                self.assertRaisesRegex(RemoteRunnerError, message),
            ):
                load_remote_config(self.config_path)

        recipe_cases: tuple[tuple[str, object], ...] = (
            ("image_id", "quality:latest"),
            ("command", []),
            ("command", ["make\0quality"]),
            ("timeout_seconds", 9),
            ("stop_seconds", 0),
            ("cpu_millis", True),
            ("memory_bytes", 1024),
            ("pids_limit", 2),
            ("max_artifact_bytes", 0),
            ("max_artifacts", 0),
        )
        for field, value in recipe_cases:
            document = json.loads(json.dumps(original))
            document["recipes"]["quality"][field] = value
            self.config_path.write_text(json.dumps(document), encoding="utf-8")
            os.chmod(self.config_path, 0o600)
            with self.subTest(field=field), self.assertRaises(RemoteRunnerError):
                load_remote_config(self.config_path)

    def test_config_and_host_tools_must_be_private_and_trusted(self) -> None:
        os.chmod(self.config_path, 0o644)
        with self.assertRaisesRegex(RemoteRunnerError, "mode 0600"):
            load_remote_config(self.config_path)

        os.chmod(self.config_path, 0o600)
        os.chmod(self.docker_binary, 0o722)  # noqa: S103 - proves unsafe mode fails.
        with self.assertRaisesRegex(RemoteRunnerError, "unsafe"):
            load_remote_config(self.config_path)

        os.chmod(self.docker_binary, 0o700)
        os.chmod(self.state, 0o755)  # noqa: S103 - proves private state is required.
        with self.assertRaisesRegex(RemoteRunnerError, "private directory"):
            load_remote_config(self.config_path)

    def test_submit_uses_exact_candidate_and_pinned_docker_recipe(self) -> None:
        result = self.runner.submit(self.job(), self.candidate_archive)

        execution_id = self.runner._execution_id(self.job())
        self.assertEqual(
            result.to_dict(),
            {
                "candidate_revision": HEAD,
                "execution_id": execution_id,
                "exit_code": None,
                "finished_at": None,
                "image_id": IMAGE_ID,
                "object_format": "sha1",
                "recipe_id": "quality",
                "recipe_sha256": self.config.recipes["quality"].digest,
                "schema_version": 1,
                "source_bytes": len(self.candidate_archive),
                "source_sha256": hashlib.sha256(self.candidate_archive).hexdigest(),
                "started_at": "2026-10-28T12:00:01.000000+00:00",
                "state": "running",
                "submitted_at": "2026-10-28T12:00:00.000000+00:00",
            },
        )
        self.assertEqual(len(self.materializer.calls), 1)
        self.assertEqual(self.materializer.calls[0][0], self.candidate_archive)
        created = self.docker.created[0]
        directory = self.state / "jobs" / execution_id
        self.assertEqual(
            created,
            {
                "artifact_upload": directory / "artifact-upload.tar.gz",
                "command": ("make", "quality"),
                "container_name": "mgb-" + execution_id.removeprefix("rex_"),
                "cpu_millis": 2000,
                "image_id": IMAGE_ID,
                "max_artifact_bytes": 1048576,
                "memory_bytes": 2147483648,
                "network_disabled": True,
                "pids_limit": 256,
                "read_only_root": True,
                "timeout_seconds": 1800,
                "workspace": directory / "workspace",
            },
        )
        self.assertEqual(len(self.docker.started), 1)

    def test_runner_rejects_ungranted_jobs_and_invalid_submission_inputs(self) -> None:
        for selected in (
            self.job(repository="other/widgets"),
            self.job(recipe_id="other"),
            self.job(recipe_sha256="0" * 64),
            self.job(image_id="sha256:" + "0" * 64),
        ):
            with (
                self.subTest(job=selected),
                self.assertRaisesRegex(RemoteRunnerError, "authority"),
            ):
                self.runner.submit(selected, self.candidate_archive)
        with self.assertRaisesRegex(RemoteRunnerError, "job is invalid"):
            self.runner.submit(object(), self.candidate_archive)  # type: ignore[arg-type]
        with self.assertRaisesRegex(RemoteRunnerError, "empty or oversized"):
            self.runner.submit(self.job(), b"")

    def test_absent_and_nonterminal_jobs_have_explicit_outcomes(self) -> None:
        execution_id = self.runner._execution_id(self.job())
        self.assertIsNone(self.runner.find(self.job()))
        self.assertIsNone(self.runner.inspect(self.job(), execution_id))
        self.assertIsNone(self.runner.cancel(self.job(), execution_id))
        self.assertIsNone(self.runner.collect(self.job(), execution_id))

        submitted = self.runner.submit(self.job(), self.candidate_archive)
        with self.assertRaisesRegex(RemoteRunnerError, "not terminal"):
            self.runner.collect(self.job(), submitted.execution_id)
        self.assertEqual(self.runner.find(self.job()), submitted)

    def test_inspect_maps_created_running_failed_and_missing_docker_states(
        self,
    ) -> None:
        submitted = self.runner.submit(self.job(), self.candidate_archive)
        self.docker.state = DockerState("created", None, None, None)
        queued = self.runner.inspect(self.job(), submitted.execution_id)
        self.assertEqual(queued.state, "queued")

        self.docker.state = DockerState(
            "exited",
            "2026-10-28T12:00:01.000000+00:00",
            "2026-10-28T12:00:05.000000+00:00",
            7,
        )
        failed = self.runner.inspect(self.job(), submitted.execution_id)
        self.assertEqual(failed.state, "failed")
        self.assertEqual(failed.exit_code, 7)

        self.docker.state = None
        self.assertIsNone(self.runner.inspect(self.job(), submitted.execution_id))

    def test_submit_is_idempotent_and_changed_job_is_rejected(self) -> None:
        first = self.runner.submit(self.job(), self.candidate_archive)
        second = self.runner.submit(self.job(), self.candidate_archive)

        self.assertEqual(second.execution_id, first.execution_id)
        self.assertEqual(len(self.materializer.calls), 1)
        self.assertEqual(len(self.docker.created), 1)
        self.assertEqual(len(self.docker.started), 1)

        with self.assertRaisesRegex(RemoteRunnerError, "different job"):
            self.runner.submit(
                self.job(candidate_revision="c" * 40), self.candidate_archive
            )

        with self.assertRaisesRegex(RemoteRunnerError, "archive"):
            self.runner.submit(self.job(), b"different archive")

    def test_safe_pre_effect_submission_failure_can_resume_exactly_once(self) -> None:
        materializer = FailsOnceMaterializer()
        runner = RemoteRunner(
            self.config,
            docker=self.docker,
            materializer=materializer,
            clock=lambda: "2026-10-28T12:00:00.000000+00:00",
        )

        with self.assertRaisesRegex(RemoteRunnerError, "submission"):
            runner.submit(self.job(), self.candidate_archive)
        result = runner.submit(self.job(), self.candidate_archive)

        self.assertEqual(result.state, "running")
        self.assertEqual(len(self.docker.created), 1)
        workspace = materializer.calls[-1][1]
        self.assertFalse((workspace / "partial.txt").exists())

    def test_submission_retry_restores_a_missing_artifact_baseline(self) -> None:
        staging = self.runner.evidence / ".staging"
        staging.mkdir(mode=0o700)
        execution_id = self.runner._execution_id(self.job())
        blocker = staging / (self.runner._attempt_id(execution_id) + ".json")
        blocker.mkdir(mode=0o700)

        with self.assertRaisesRegex(RemoteRunnerError, "submission"):
            self.runner.submit(self.job(), self.candidate_archive)

        blocker.rmdir()
        submitted = self.runner.submit(self.job(), self.candidate_archive)
        self.docker.state = DockerState(
            status="exited",
            started_at="2026-10-28T12:00:01.000000+00:00",
            finished_at="2026-10-28T12:00:05.000000+00:00",
            exit_code=0,
        )

        evidence = self.runner.collect(self.job(), submitted.execution_id)

        self.assertEqual(evidence.artifact_count, 0)

    def test_reconcile_resumes_only_an_existing_exact_submission(self) -> None:
        docker = FailsOnceStartDocker()
        runner = RemoteRunner(
            self.config,
            docker=docker,
            materializer=self.materializer,
            clock=lambda: "2026-10-28T12:00:00.000000+00:00",
        )

        with self.assertRaisesRegex(RemoteRunnerError, "submission"):
            runner.submit(self.job(), self.candidate_archive)

        reconciled = runner.reconcile_submission(self.job(), self.candidate_archive)
        absent = runner.reconcile_submission(
            self.job(operation_id="sop_" + "9" * 32), self.candidate_archive
        )

        self.assertEqual(reconciled.state, "running")
        self.assertIsNone(absent)
        self.assertEqual(len(docker.created), 1)

    def test_cancel_is_idempotent_and_inspect_reports_cancelled(self) -> None:
        submitted = self.runner.submit(self.job(), self.candidate_archive)

        cancelled = self.runner.cancel(self.job(), submitted.execution_id)
        repeated = self.runner.cancel(self.job(), submitted.execution_id)

        self.assertEqual(cancelled.state, "cancelled")
        self.assertEqual(repeated.state, "cancelled")
        self.assertEqual(len(self.docker.cancelled), 1)

    def test_cancel_reconcile_resumes_only_a_recorded_cancel_intent(self) -> None:
        docker = FailsOnceCancelDocker()
        runner = RemoteRunner(
            self.config,
            docker=docker,
            materializer=self.materializer,
            clock=lambda: "2026-10-28T12:00:00.000000+00:00",
        )
        submitted = runner.submit(self.job(), self.candidate_archive)

        self.assertIsNone(
            runner.reconcile_cancellation(self.job(), submitted.execution_id)
        )
        with self.assertRaisesRegex(RemoteRunnerError, "reply loss"):
            runner.cancel(self.job(), submitted.execution_id)

        reconciled = runner.reconcile_cancellation(self.job(), submitted.execution_id)

        self.assertEqual(reconciled.state, "cancelled")
        self.assertEqual(len(docker.cancelled), 1)

    def test_recipe_timeout_is_enforced_when_the_job_is_observed(self) -> None:
        now = ["2026-10-28T12:00:00.000000+00:00"]
        runner = RemoteRunner(
            self.config,
            docker=self.docker,
            materializer=self.materializer,
            clock=lambda: now[0],
        )
        submitted = runner.submit(self.job(), self.candidate_archive)
        now[0] = "2026-10-28T12:31:00.000000+00:00"

        observed = runner.inspect(self.job(), submitted.execution_id)

        self.assertEqual(observed.state, "failed")
        self.assertEqual(observed.exit_code, 143)
        self.assertEqual(len(self.docker.cancelled), 1)

    def test_timeout_cannot_be_reported_as_success_when_stop_exits_zero(self) -> None:
        now = ["2026-10-28T12:00:00.000000+00:00"]
        docker = ZeroExitCancelDocker()
        runner = RemoteRunner(
            self.config,
            docker=docker,
            materializer=self.materializer,
            clock=lambda: now[0],
        )
        submitted = runner.submit(self.job(), self.candidate_archive)
        now[0] = "2026-10-28T12:31:00.000000+00:00"

        observed = runner.inspect(self.job(), submitted.execution_id)

        self.assertEqual(observed.state, "failed")
        self.assertEqual(observed.exit_code, 124)

    def test_collect_captures_hostile_artifacts_through_result_boundary(self) -> None:
        submitted = self.runner.submit(self.job(), self.candidate_archive)
        self.runner.artifact_upload_path(submitted.execution_id).write_bytes(
            artifact_archive([("coverage.json", b"{}", "file")])
        )
        self.docker.state = DockerState(
            status="exited",
            started_at="2026-10-28T12:00:01.000000+00:00",
            finished_at="2026-10-28T12:00:05.000000+00:00",
            exit_code=0,
        )

        evidence = self.runner.collect(self.job(), submitted.execution_id)

        self.assertEqual(evidence.execution.state, "succeeded")
        self.assertEqual(evidence.duration_ms, 4000)
        self.assertEqual(evidence.output_bytes, len(self.docker.stdout))
        self.assertEqual(evidence.artifact_count, 1)
        self.assertGreater(evidence.artifact_bytes, 0)
        self.assertNotIn(b"all tests passed", evidence.artifact_bundle)

    def test_collect_rejects_unsafe_worker_artifacts(self) -> None:
        submitted = self.runner.submit(self.job(), self.candidate_archive)
        self.runner.artifact_upload_path(submitted.execution_id).write_bytes(
            artifact_archive([("escape", "../outside", "symlink")])
        )
        self.docker.state = DockerState(
            status="exited",
            started_at="2026-10-28T12:00:01.000000+00:00",
            finished_at="2026-10-28T12:00:05.000000+00:00",
            exit_code=0,
        )

        with self.assertRaisesRegex(RemoteRunnerError, "capture"):
            self.runner.collect(self.job(), submitted.execution_id)

    def test_request_handler_exposes_only_bounded_actions(self) -> None:
        submitted = handle_request(self.runner, self.request("submit", archive=True))
        self.assertEqual(submitted["outcome"], "found")
        execution_id = submitted["result"]["execution_id"]

        inspected = handle_request(
            self.runner,
            self.request("inspect", execution_id=execution_id),
        )
        self.assertEqual(inspected["result"]["state"], "running")

        reconciled = handle_request(
            self.runner,
            self.request("reconcile_submit", archive=True),
        )
        self.assertEqual(reconciled["result"]["execution_id"], execution_id)

        cancelled = handle_request(
            self.runner,
            self.request("cancel", execution_id=execution_id),
        )
        self.assertEqual(cancelled["result"]["state"], "cancelled")
        repeated = handle_request(
            self.runner,
            self.request("reconcile_cancel", execution_id=execution_id),
        )
        self.assertEqual(repeated["result"]["state"], "cancelled")
        collected = handle_request(
            self.runner,
            self.request("collect", execution_id=execution_id),
        )
        self.assertEqual(collected["result"]["execution"]["state"], "cancelled")

        with self.assertRaisesRegex(RemoteRunnerError, "action"):
            handle_request(self.runner, self.request("shell"))

    def test_request_handler_rejects_malformed_deadlines_identity_and_archives(
        self,
    ) -> None:
        cases: list[tuple[dict[str, object], str]] = []
        version = self.request("submit", archive=True)
        version["schema_version"] = 2
        cases.append((version, "version"))
        expired = self.request("submit", archive=True)
        expired["deadline"] = "2000-01-01T00:00:00+00:00"
        cases.append((expired, "expired"))
        bad_job = self.request("submit", archive=True)
        bad_job["job"] = {}
        cases.append((bad_job, "job"))
        submit_identity = self.request(
            "submit", execution_id=EXECUTION_ID, archive=True
        )
        cases.append((submit_identity, "identity"))
        inspect_identity = self.request("inspect")
        cases.append((inspect_identity, "identity"))
        inspect_archive = self.request(
            "inspect", execution_id=EXECUTION_ID, archive=True
        )
        cases.append((inspect_archive, "archive"))
        bad_digest = self.request("submit", archive=True)
        bad_digest["candidate_archive"]["sha256"] = "0" * 64
        cases.append((bad_digest, "archive"))
        bad_bundle = self.request("submit", archive=True)
        bad_bundle["candidate_archive"]["bundle"] = "***"
        cases.append((bad_bundle, "archive"))

        for request, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(RemoteRunnerError, message),
            ):
                handle_request(self.runner, request)

    def test_cli_inspection_is_non_secret_and_invoke_fails_closed(self) -> None:
        parser = build_remote_parser()
        self.assertEqual(parser.prog, "magic-glass-box-remote-runner")
        self.assertIn("pinned validation recipes", parser.description)
        self.assertIn("Show non-secret runner identity", parser.format_help())
        self.assertIn("Handle one bounded provider request", parser.format_help())
        parsed = parser.parse_args(["inspect", "--config", str(self.config_path)])
        self.assertEqual(
            vars(parsed), {"command": "inspect", "config": self.config_path}
        )
        invoked = parser.parse_args(["invoke", "--config", str(self.config_path)])
        self.assertEqual(
            vars(invoked), {"command": "invoke", "config": self.config_path}
        )

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = remote_runner_main(["inspect", "--config", str(self.config_path)])
        self.assertEqual(result, 0)
        identity = json.loads(stdout.getvalue())
        self.assertEqual(identity["status"], "configured")
        self.assertNotIn("command", stdout.getvalue())

        class Input:
            buffer = io.BytesIO(b"{}")

        stderr = io.StringIO()
        with (
            patch("sys.stdin", Input()),
            contextlib.redirect_stderr(stderr),
        ):
            result = remote_runner_main(["invoke", "--config", str(self.config_path)])
        self.assertEqual(result, 2)
        self.assertIn("request fields are invalid", stderr.getvalue())


class CandidateArchiveBoundaryTests(unittest.TestCase):
    @staticmethod
    def archive(entries: list[tuple[str, bytes | str, str]]) -> bytes:
        output = io.BytesIO()
        with (
            gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as compressed,
            tarfile.open(fileobj=compressed, mode="w") as archive,
        ):
            for name, content, kind in entries:
                info = tarfile.TarInfo(name)
                info.mtime = 0
                if kind == "file":
                    assert isinstance(content, bytes)
                    info.size = len(content)
                    archive.addfile(info, io.BytesIO(content))
                elif kind == "symlink":
                    assert isinstance(content, str)
                    info.type = tarfile.SYMTYPE
                    info.linkname = content
                    archive.addfile(info)
                else:
                    info.type = tarfile.FIFOTYPE
                    archive.addfile(info)
        return output.getvalue()

    def test_safe_archive_extracts_without_tarfile_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            destination = Path(raw)
            CandidateArchiveMaterializer().materialize(
                self.archive([("nested/file.txt", b"safe", "file")]),
                destination,
            )
            self.assertEqual((destination / "nested/file.txt").read_bytes(), b"safe")
            self.assertEqual(destination.stat().st_mode & 0o777, 0o755)
            self.assertEqual((destination / "nested").stat().st_mode & 0o777, 0o755)

    def test_archive_members_are_streamed_before_the_path_limit_is_enforced(
        self,
    ) -> None:
        with (
            tempfile.TemporaryDirectory() as raw,
            patch.object(
                tarfile.TarFile,
                "getmembers",
                side_effect=AssertionError("untrusted member list was materialized"),
            ),
        ):
            destination = Path(raw)
            CandidateArchiveMaterializer().materialize(
                self.archive([("nested/file.txt", b"safe", "file")]),
                destination,
            )
            self.assertEqual((destination / "nested/file.txt").read_bytes(), b"safe")

    def test_archive_rejects_traversal_special_files_and_leaf_aliases(self) -> None:
        cases = (
            [("../escape", b"bad", "file")],
            [("pipe", b"", "fifo")],
            [("same", b"one", "file"), ("same", b"two", "file")],
            [("alias", b"file", "file"), ("alias/child", b"bad", "file")],
            [("alias", "target", "symlink"), ("alias/child", b"bad", "file")],
            [("a", "b", "symlink"), ("b", "a", "symlink")],
            [("absolute", "/etc/passwd", "symlink")],
        )
        for entries in cases:
            with (
                self.subTest(entries=entries),
                tempfile.TemporaryDirectory() as raw,
                self.assertRaises(RemoteRunnerError),
            ):
                CandidateArchiveMaterializer().materialize(
                    self.archive(entries), Path(raw)
                )

    def test_archive_rejects_invalid_encoding_and_explicit_resource_overruns(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            destination = Path(raw)
            with self.assertRaisesRegex(RemoteRunnerError, "invalid"):
                CandidateArchiveMaterializer().materialize(b"not-gzip", destination)
            with self.assertRaisesRegex(RemoteRunnerError, "empty"):
                CandidateArchiveMaterializer().materialize(b"", destination)
            with self.assertRaisesRegex(RemoteRunnerError, "too many paths"):
                CandidateArchiveMaterializer._safe_extract(
                    self.archive([("one", b"1", "file"), ("two", b"2", "file")]),
                    destination,
                    mode="r:gz",
                    max_bytes=10,
                    max_paths=1,
                )
            with self.assertRaisesRegex(RemoteRunnerError, "oversized"):
                CandidateArchiveMaterializer._safe_extract(
                    self.archive([("two-bytes", b"12", "file")]),
                    destination,
                    mode="r:gz",
                    max_bytes=1,
                    max_paths=10,
                )

    def test_artifact_materializer_applies_its_own_transport_bounds(self) -> None:
        materializer = ArtifactArchiveMaterializer()
        archive = self.archive([("result.json", b"{}", "file")])
        with tempfile.TemporaryDirectory() as raw:
            destination = Path(raw)
            materializer.materialize(
                archive,
                destination,
                max_bytes=len(archive),
                max_paths=1,
            )
            self.assertEqual((destination / "result.json").read_bytes(), b"{}")
            with self.assertRaisesRegex(RemoteRunnerError, "empty or oversized"):
                materializer.materialize(
                    b"",
                    destination,
                    max_bytes=len(archive),
                    max_paths=1,
                )
            with self.assertRaisesRegex(RemoteRunnerError, "empty or oversized"):
                materializer.materialize(
                    archive,
                    destination,
                    max_bytes=len(archive) - 1,
                    max_paths=1,
                )


class DockerCommandBoundaryTests(unittest.TestCase):
    def test_create_uses_no_network_pinned_image_and_read_only_source(self) -> None:
        runner = CapturingCommandRunner()
        docker = DockerCLI(Path("/usr/bin/docker"), Path("/private/tmp/docker-client"))
        docker.runner = runner

        docker.create(
            artifact_upload=Path("/private/tmp/artifact-upload.tar.gz"),
            command=("make", "quality"),
            container_name="mgb-test",
            cpu_millis=2000,
            image_id=IMAGE_ID,
            max_artifact_bytes=1048576,
            memory_bytes=2147483648,
            network_disabled=True,
            pids_limit=256,
            read_only_root=True,
            timeout_seconds=1800,
            workspace=Path("/private/tmp/workspace"),
        )

        command = runner.calls[0]["command"]
        assert isinstance(command, list)
        self.assertEqual(
            command,
            [
                "/usr/bin/docker",
                "create",
                "--name",
                "mgb-test",
                "--pull",
                "never",
                "--network",
                "none",
                "--read-only",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--pids-limit",
                "256",
                "--memory",
                "2147483648",
                "--cpus",
                "2.000",
                "--workdir",
                "/workspace",
                "--mount",
                "type=bind,src=/private/tmp/workspace,dst=/workspace,readonly",
                "--mount",
                "type=bind,src=/private/tmp/artifact-upload.tar.gz,"
                "dst=/mgb-artifacts.tar.gz",
                "--env",
                "MGB_ARTIFACT_PATH=/mgb-artifacts.tar.gz",
                "--ulimit",
                "fsize=1048576:1048576",
                "--log-driver",
                "local",
                "--log-opt",
                "max-size=4m",
                "--log-opt",
                "max-file=1",
                "--log-opt",
                "compress=false",
                "--tmpfs",
                "/tmp:rw,noexec,nosuid,nodev,size=268435456",  # noqa: S108
                IMAGE_ID,
                "make",
                "quality",
            ],
        )
        self.assertEqual(
            {key: value for key, value in runner.calls[0].items() if key != "command"},
            {
                "environment": {
                    "DOCKER_CONFIG": "/private/tmp/docker-client",
                    "HOME": "/private/tmp/docker-client",
                    "LANG": "C",
                    "LC_ALL": "C",
                    "PATH": os.defpath,
                },
                "input_bytes": b"",
                "stdout_limit": 1024 * 1024,
                "timeout": 30.0,
            },
        )

    def test_docker_lifecycle_commands_parse_only_bounded_state(self) -> None:
        state = {
            "Status": "exited",
            "StartedAt": "2026-10-28T12:00:01Z",
            "FinishedAt": "2026-10-28T12:00:05Z",
            "ExitCode": 7,
        }
        runner = CapturingCommandRunner(
            BoundedCommandResult(0, json.dumps(state).encode("utf-8"), b"")
        )
        docker = DockerCLI(Path("/usr/bin/docker"), Path("/private/tmp/docker-client"))
        docker.runner = runner

        observed = docker.inspect("mgb-test")

        self.assertEqual(observed.status, "exited")
        self.assertEqual(observed.exit_code, 7)
        self.assertEqual(observed.started_at, "2026-10-28T12:00:01.000000+00:00")

        runner.result = BoundedCommandResult(1, b"", b"not found")
        self.assertIsNone(docker.inspect("mgb-missing"))

        runner.result = BoundedCommandResult(0, b"stdout", b"stderr")
        docker.start("mgb-test", timeout_seconds=1800)
        docker.cancel("mgb-test", stop_seconds=10, timeout_seconds=1800)
        self.assertEqual(
            docker.logs("mgb-test", timeout_seconds=1800), (b"stdout", b"stderr")
        )
        self.assertEqual(runner.calls[-3]["command"][-2:], ["start", "mgb-test"])
        self.assertEqual(
            runner.calls[-2]["command"][-4:], ["stop", "--time", "10", "mgb-test"]
        )

    def test_docker_cli_rejects_malformed_state_failures_and_oversized_logs(
        self,
    ) -> None:
        docker = DockerCLI(Path("/usr/bin/docker"), Path("/private/tmp/docker-client"))
        malformed = CapturingCommandRunner(
            BoundedCommandResult(0, b'{"Status":"running"}', b"")
        )
        docker.runner = malformed
        with self.assertRaisesRegex(RemoteRunnerError, "state is invalid"):
            docker.inspect("mgb-test")

        malformed.result = BoundedCommandResult(2, b"", b"denied")
        with self.assertRaisesRegex(RemoteRunnerError, "command failed"):
            docker.start("mgb-test", timeout_seconds=30)

        docker.runner = CapturingCommandRunner(error=OSError("lost Docker"))
        with self.assertRaisesRegex(RemoteRunnerError, "command failed"):
            docker.start("mgb-test", timeout_seconds=30)

        docker.runner = CapturingCommandRunner(
            BoundedCommandResult(0, b"x" * (MAX_DOCKER_OUTPUT_BYTES + 1), b"")
        )
        with self.assertRaisesRegex(RemoteRunnerError, "logs exceed"):
            docker.logs("mgb-test", timeout_seconds=30)

        docker.runner = CapturingCommandRunner()
        with self.assertRaisesRegex(RemoteRunnerError, "recipe command"):
            docker.create(command=["make"])
        with self.assertRaisesRegex(RemoteRunnerError, "CPU limit"):
            docker.create(command=("make",), cpu_millis="two")

    def test_docker_state_rejects_unknown_status_timestamps_and_exit_codes(
        self,
    ) -> None:
        cases = (
            ("unknown", None, None, None),
            ("running", "not-a-time", None, None),
            ("exited", None, "2026-10-28T12:00:05Z", 256),
        )
        for values in cases:
            with (
                self.subTest(values=values),
                self.assertRaisesRegex(RemoteRunnerError, "state is invalid"),
            ):
                DockerState(*values)


if __name__ == "__main__":
    unittest.main()
