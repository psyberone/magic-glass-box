from __future__ import annotations


class StorageError(RuntimeError):
    """Control-plane persistence failed."""


class NotFoundError(StorageError):
    pass


class ConflictError(StorageError):
    pass


class ConcurrentUpdateError(ConflictError):
    pass


class BudgetExceededError(ConflictError):
    """A durable reservation would exceed pinned provider budget authority."""


class InvocationLimitExceededError(ConflictError):
    """A run consumed its pinned provider invocation allowance."""
