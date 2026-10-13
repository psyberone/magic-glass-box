from __future__ import annotations

from collections import deque
from collections.abc import Callable, Mapping
from pathlib import PurePosixPath


class UnsafeSymlinkError(ValueError):
    """A relative symlink graph can resolve outside its declared root."""


LinkLookup = Callable[[PurePosixPath], str | None]


def validate_symlink_graph(
    links: Mapping[PurePosixPath, str],
    *,
    lookup: LinkLookup | None = None,
) -> None:
    """Validate symlink targets after recursively expanding sibling links.

    A target that looks safe lexically can still escape through another
    symlink (for example ``x -> ..`` followed by ``y -> x/..``).  Resolution
    therefore walks every path component and expands known links before
    interpreting the remaining ``..`` components.
    """

    normalized = dict(links)

    def target_parts(target: object) -> tuple[str, ...]:
        if not isinstance(target, str) or not target or "\x00" in target:
            raise UnsafeSymlinkError("symlink target is invalid")
        expanded = PurePosixPath(target)
        if expanded.is_absolute():
            raise UnsafeSymlinkError("symlink target is absolute")
        return expanded.parts

    for source, target in normalized.items():
        if (
            not isinstance(source, PurePosixPath)
            or not source.parts
            or source.is_absolute()
            or ".." in source.parts
        ):
            raise UnsafeSymlinkError("symlink path is invalid")
        target_parts(target)

    def target_for(path: PurePosixPath) -> str | None:
        if path in normalized:
            return normalized[path]
        return None if lookup is None else lookup(path)

    # Bound hostile graphs without imposing a small fixed chain limit.  A
    # normal repository consumes roughly one step per target component.
    remaining_steps = max(4096, len(normalized) * 32)
    for source, initial_target in normalized.items():
        resolved: list[str] = []
        pending = deque((*source.parent.parts, *target_parts(initial_target)))
        while pending:
            remaining_steps -= 1
            if remaining_steps < 0:
                raise UnsafeSymlinkError("symlink graph is too complex")

            part = pending.popleft()
            if part == "..":
                if not resolved:
                    raise UnsafeSymlinkError("symlink graph escapes its root")
                resolved.pop()
                continue

            resolved.append(part)
            candidate = PurePosixPath(*resolved)
            target = target_for(candidate)
            if target is None:
                continue
            resolved.pop()
            pending.extendleft(reversed(target_parts(target)))
