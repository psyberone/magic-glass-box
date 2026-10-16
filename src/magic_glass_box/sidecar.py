from __future__ import annotations

import math
import os
import signal
import socket
import stat
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from types import FrameType

DEFAULT_CLIENT_TIMEOUT_SECONDS = 30.0
MAX_CLIENT_TIMEOUT_SECONDS = 5 * 60


class UnixSidecarError(ValueError):
    """A local sidecar listener configuration is unsafe or invalid."""


class _TerminationRequested(BaseException):
    """Unwind a sidecar listener so its private socket can be removed."""


@contextmanager
def _handle_sigterm() -> Iterator[None]:
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = signal.getsignal(signal.SIGTERM)

    def terminate(_signum: int, _frame: FrameType | None) -> None:
        raise _TerminationRequested

    signal.signal(signal.SIGTERM, terminate)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def _validate_listener(
    socket_path: Path,
    max_invocations: int,
    client_timeout: float,
) -> Path:
    path = Path(socket_path)
    if not path.is_absolute():
        raise UnixSidecarError("socket path must be absolute")
    if (
        isinstance(max_invocations, bool)
        or not isinstance(max_invocations, int)
        or max_invocations < 1
    ):
        raise UnixSidecarError("max invocations must be positive")
    if (
        isinstance(client_timeout, bool)
        or not isinstance(client_timeout, (int, float))
        or not math.isfinite(client_timeout)
        or not 0 < client_timeout <= MAX_CLIENT_TIMEOUT_SECONDS
    ):
        raise UnixSidecarError("client timeout is invalid")
    try:
        path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        parent = path.parent.lstat()
    except OSError as exc:
        raise UnixSidecarError("socket directory is unavailable") from exc
    if (
        stat.S_ISLNK(parent.st_mode)
        or not stat.S_ISDIR(parent.st_mode)
        or parent.st_mode & 0o077
        or (hasattr(os, "getuid") and parent.st_uid != os.getuid())
    ):
        raise UnixSidecarError("socket directory must be private")
    if path.exists() or path.is_symlink():
        raise UnixSidecarError("socket path already exists")
    return path


def serve_unix_socket(
    socket_path: Path,
    max_invocations: int,
    handler: Callable[[socket.socket], None],
    *,
    recoverable_errors: tuple[type[BaseException], ...],
    client_timeout: float = DEFAULT_CLIENT_TIMEOUT_SECONDS,
) -> int:
    """Serve successful frames while isolating malformed or abandoned clients."""

    path = _validate_listener(socket_path, max_invocations, client_timeout)
    completed = 0
    bound = False
    try:
        try:
            with (
                _handle_sigterm(),
                socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener,
            ):
                listener.bind(str(path))
                bound = True
                os.chmod(path, 0o600)
                listener.listen(1)
                while completed < max_invocations:
                    connection, _ = listener.accept()
                    with connection:
                        connection.settimeout(float(client_timeout))
                        try:
                            handler(connection)
                        except recoverable_errors:
                            continue
                        completed += 1
        except _TerminationRequested:
            pass
    finally:
        if bound:
            path.unlink(missing_ok=True)
    return completed
