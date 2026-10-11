from __future__ import annotations


class StorageError(RuntimeError):
    """Control-plane persistence failed."""


class NotFoundError(StorageError):
    pass


class ConflictError(StorageError):
    pass


class ConcurrentUpdateError(ConflictError):
    pass
