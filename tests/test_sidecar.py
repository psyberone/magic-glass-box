from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from magic_glass_box.sidecar import (
    UnixSidecarError,
    _staging_socket_path,
    serve_unix_socket,
)


class UnixSidecarTests(unittest.TestCase):
    def test_staging_path_skips_target_and_occupied_siblings_without_growth(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            root = Path(temporary)
            socket_path = root / "a"
            (root / "b").write_text("occupied", encoding="utf-8")
            (root / "c").symlink_to(root / "missing")

            with mock.patch(
                "magic_glass_box.sidecar.secrets.choice",
                side_effect=list("abcd"),
            ):
                staging_path = _staging_socket_path(socket_path)

            self.assertEqual(staging_path, root / "d")
            self.assertLessEqual(
                len(os.fsencode(staging_path)), len(os.fsencode(socket_path))
            )

            with (
                mock.patch("magic_glass_box.sidecar.secrets.choice", return_value="a"),
                self.assertRaises(UnixSidecarError),
            ):
                _staging_socket_path(socket_path)

    def test_listener_publishes_only_after_socket_is_private_and_listening(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            socket_path = Path(temporary) / "sidecar.sock"
            chmod_started = threading.Event()
            allow_chmod = threading.Event()
            completed: list[int] = []
            failures: list[BaseException] = []
            real_chmod = os.chmod

            def delayed_chmod(path: os.PathLike[str] | str, mode: int) -> None:
                chmod_started.set()
                if not allow_chmod.wait(2):  # pragma: no cover - asserted below
                    raise AssertionError("test did not release socket publication")
                real_chmod(path, mode)

            def run() -> None:
                try:
                    completed.append(
                        serve_unix_socket(
                            socket_path,
                            1,
                            lambda connection: connection.recv(1),
                            recoverable_errors=(OSError,),
                        )
                    )
                except BaseException as exc:  # pragma: no cover - asserted below
                    failures.append(exc)

            with mock.patch("magic_glass_box.sidecar.os.chmod", delayed_chmod):
                server = threading.Thread(target=run, daemon=True)
                server.start()
                self.assertTrue(chmod_started.wait(1))
                self.assertFalse(socket_path.exists())
                allow_chmod.set()
                deadline = time.monotonic() + 2
                while not socket_path.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(socket_path.exists())
                self.assertEqual(socket_path.stat().st_mode & 0o777, 0o600)
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.connect(str(socket_path))
                    client.sendall(b"x")
                server.join(2)

            self.assertFalse(server.is_alive())
            self.assertEqual(failures, [])
            self.assertEqual(completed, [1])
            self.assertFalse(socket_path.exists())

    def test_listener_cleans_unpublished_socket_after_publication_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            root = Path(temporary)
            socket_path = root / "sidecar.sock"
            staging_path = root / "staging.sock"
            cases = (
                (FileExistsError(), "already exists"),
                (OSError(), "could not be published"),
            )
            for failure, message in cases:
                with (
                    self.subTest(message=message),
                    mock.patch(
                        "magic_glass_box.sidecar._staging_socket_path",
                        return_value=staging_path,
                    ),
                    mock.patch("magic_glass_box.sidecar.os.link", side_effect=failure),
                    self.assertRaisesRegex(UnixSidecarError, message),
                ):
                    serve_unix_socket(
                        socket_path,
                        1,
                        lambda connection: connection.recv(1),
                        recoverable_errors=(OSError,),
                    )
                self.assertFalse(staging_path.exists())
                self.assertFalse(socket_path.exists())

    def test_listener_executes_advertised_concurrency_without_queue_timeout(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            socket_path = Path(temporary) / "sidecar.sock"
            first_started = threading.Event()
            second_started = threading.Event()
            release = threading.Event()
            completed: list[int] = []

            def handler(connection: socket.socket) -> None:
                marker = connection.recv(1)
                if marker == b"1":
                    first_started.set()
                elif marker == b"2":
                    second_started.set()
                else:  # pragma: no cover - test input is fixed
                    raise AssertionError("unexpected marker")
                if not release.wait(2):  # pragma: no cover - asserted by test
                    raise AssertionError("test did not release handler")
                connection.sendall(marker)

            server = threading.Thread(
                target=lambda: completed.append(
                    serve_unix_socket(
                        socket_path,
                        2,
                        handler,
                        recoverable_errors=(OSError,),
                        max_concurrency=2,
                    )
                ),
                daemon=True,
            )
            server.start()
            deadline = time.monotonic() + 2
            while not socket_path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)

            first = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            second = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                first.connect(str(socket_path))
                first.sendall(b"1")
                self.assertTrue(first_started.wait(1))
                second.connect(str(socket_path))
                second.sendall(b"2")
                self.assertTrue(second_started.wait(0.5))
            finally:
                release.set()
            try:
                self.assertEqual(first.recv(1), b"1")
                self.assertEqual(second.recv(1), b"2")
            finally:
                first.close()
                second.close()
            server.join(2)

            self.assertFalse(server.is_alive())
            self.assertEqual(completed, [2])
            self.assertFalse(socket_path.exists())

    def test_listener_default_concurrency_is_serial(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            socket_path = Path(temporary) / "sidecar.sock"
            first_started = threading.Event()
            second_started = threading.Event()
            release_first = threading.Event()
            completed: list[int] = []

            def handler(connection: socket.socket) -> None:
                marker = connection.recv(1)
                if marker == b"1":
                    first_started.set()
                    if not release_first.wait(2):  # pragma: no cover - asserted below
                        raise AssertionError("test did not release first handler")
                elif marker == b"2":
                    second_started.set()
                connection.sendall(marker)

            server = threading.Thread(
                target=lambda: completed.append(
                    serve_unix_socket(
                        socket_path,
                        2,
                        handler,
                        recoverable_errors=(OSError,),
                    )
                ),
                daemon=True,
            )
            server.start()
            deadline = time.monotonic() + 2
            while not socket_path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)

            first = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            second = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                first.connect(str(socket_path))
                first.sendall(b"1")
                self.assertTrue(first_started.wait(1))
                second.connect(str(socket_path))
                second.sendall(b"2")
                self.assertFalse(second_started.wait(0.2))
                release_first.set()
                self.assertEqual(first.recv(1), b"1")
                self.assertTrue(second_started.wait(1))
                self.assertEqual(second.recv(1), b"2")
            finally:
                release_first.set()
                first.close()
                second.close()
            server.join(2)

            self.assertFalse(server.is_alive())
            self.assertEqual(completed, [2])
            self.assertFalse(socket_path.exists())

    def test_sigterm_stops_accepting_and_drains_an_inflight_handler(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as temporary:  # noqa: S108
            root = Path(temporary)
            socket_path = root / "sidecar.sock"
            started_path = root / "started"
            script = "\n".join(
                (
                    "import socket, sys, time",
                    "from pathlib import Path",
                    "from magic_glass_box.sidecar import serve_unix_socket",
                    "def handler(connection):",
                    "    connection.recv(1)",
                    "    Path(sys.argv[2]).write_text('started')",
                    "    time.sleep(0.25)",
                    "    connection.sendall(b'done')",
                    "serve_unix_socket(Path(sys.argv[1]), 1, handler, "
                    "recoverable_errors=(OSError,), max_concurrency=1)",
                )
            )
            environment = os.environ.copy()
            source_root = str(Path(__file__).resolve().parents[1] / "src")
            environment["PYTHONPATH"] = os.pathsep.join(
                value
                for value in (source_root, environment.get("PYTHONPATH", ""))
                if value
            )
            process = subprocess.Popen(  # noqa: S603 - fixed interpreter argv
                [
                    sys.executable,
                    "-c",
                    script,
                    str(socket_path),
                    str(started_path),
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=environment,
            )
            self.addCleanup(lambda: process.kill() if process.poll() is None else None)
            deadline = time.monotonic() + 2
            while not socket_path.exists() and time.monotonic() < deadline:
                if process.poll() is not None:
                    break
                time.sleep(0.01)
            self.assertTrue(socket_path.exists())

            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(2)
                client.connect(str(socket_path))
                client.sendall(b"x")
                while not started_path.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(started_path.exists())
                process.send_signal(signal.SIGTERM)
                self.assertEqual(client.recv(4), b"done")

            stdout, stderr = process.communicate(timeout=2)
            self.assertEqual(process.returncode, 0, (stdout, stderr))
            self.assertFalse(socket_path.exists())


if __name__ == "__main__":
    unittest.main()
