from __future__ import annotations

import math
import os
import secrets
import signal
import socket
import stat
import threading
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from types import FrameType

DEFAULT_CLIENT_TIMEOUT_SECONDS = 30.0
MAX_CLIENT_TIMEOUT_SECONDS = 5 * 60
MAX_SIDECAR_CONCURRENCY = 16
_STAGING_NAME_ALPHABET = (
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
)


class UnixSidecarError(ValueError):
    """A local sidecar listener configuration is unsafe or invalid."""


@contextmanager
def _handle_sigterm(stop_event: threading.Event) -> Iterator[None]:
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = signal.getsignal(signal.SIGTERM)

    def terminate(_signum: int, _frame: FrameType | None) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, terminate)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def _validate_listener(
    socket_path: Path,
    max_invocations: int,
    client_timeout: float,
    max_concurrency: int,
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
    if (
        isinstance(max_concurrency, bool)
        or not isinstance(max_concurrency, int)
        or not 1 <= max_concurrency <= MAX_SIDECAR_CONCURRENCY
    ):
        raise UnixSidecarError("max concurrency is invalid")
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


def _staging_socket_path(path: Path) -> Path:
    """Choose an unpublished sibling without lengthening the Unix socket path."""

    name_bytes = len(os.fsencode(path.name))
    if name_bytes < 1:  # pragma: no cover - an absolute root path already exists
        raise UnixSidecarError("socket path has no filename")
    for _ in range(64):
        name = "".join(
            secrets.choice(_STAGING_NAME_ALPHABET) for _ in range(name_bytes)
        )
        candidate = path.with_name(name)
        if candidate != path and not candidate.exists() and not candidate.is_symlink():
            return candidate
    raise UnixSidecarError("socket staging path is unavailable")


def serve_unix_socket(
    socket_path: Path,
    max_invocations: int,
    handler: Callable[[socket.socket], None],
    *,
    recoverable_errors: tuple[type[BaseException], ...],
    client_timeout: float = DEFAULT_CLIENT_TIMEOUT_SECONDS,
    max_concurrency: int = 1,
) -> int:
    """Serve successful frames while isolating malformed or abandoned clients."""

    path = _validate_listener(
        socket_path,
        max_invocations,
        client_timeout,
        max_concurrency,
    )
    completed = 0
    active = 0
    staging_path = _staging_socket_path(path)
    stop_event = threading.Event()
    condition = threading.Condition()
    fatal: list[BaseException] = []
    workers: list[threading.Thread] = []

    def serve_connection(connection: socket.socket) -> None:
        nonlocal active, completed
        succeeded = False
        try:
            with connection:
                connection.settimeout(float(client_timeout))
                handler(connection)
                succeeded = True
        except recoverable_errors:
            pass
        except BaseException as exc:
            with condition:
                if not fatal:
                    fatal.append(exc)
            stop_event.set()
        finally:
            with condition:
                active -= 1
                if succeeded:
                    completed += 1
                condition.notify_all()

    with _handle_sigterm(stop_event), ExitStack() as cleanup:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind(str(staging_path))
            cleanup.callback(staging_path.unlink, missing_ok=True)
            os.chmod(staging_path, 0o600)
            listener.listen(max_concurrency)
            try:
                os.link(staging_path, path, follow_symlinks=False)
            except FileExistsError as exc:
                raise UnixSidecarError("socket path already exists") from exc
            except OSError as exc:
                raise UnixSidecarError("socket path could not be published") from exc
            cleanup.callback(path.unlink, missing_ok=True)
            staging_path.unlink()
            listener.settimeout(0.1)
            while True:
                with condition:
                    while (
                        not stop_event.is_set()
                        and not fatal
                        and completed < max_invocations
                        and (
                            active >= max_concurrency
                            or completed + active >= max_invocations
                        )
                    ):
                        condition.wait(timeout=0.1)
                    if stop_event.is_set() or fatal or completed >= max_invocations:
                        break
                try:
                    connection, _ = listener.accept()
                except TimeoutError:
                    continue
                with condition:
                    if (
                        stop_event.is_set()
                        or fatal
                        or completed + active >= max_invocations
                    ):
                        connection.close()
                        continue
                    active += 1
                worker = threading.Thread(
                    target=serve_connection,
                    args=(connection,),
                    name="magic-glass-box-sidecar-client",
                )
                workers.append(worker)
                try:
                    worker.start()
                except BaseException:
                    connection.close()
                    with condition:
                        active -= 1
                        condition.notify_all()
                    raise
        for worker in workers:
            worker.join()
        if fatal:
            raise fatal[0]
    return completed
