from __future__ import annotations

import unicodedata
from collections import deque
from collections.abc import Callable, Mapping
from pathlib import PurePosixPath

MAX_PATH_BYTES = 4096
MAX_PATH_DEPTH = 128
HFS_GIT_IGNORABLES = frozenset(
    {
        "\u200c",
        "\u200d",
        "\u200e",
        "\u200f",
        "\u202a",
        "\u202b",
        "\u202c",
        "\u202d",
        "\u202e",
        "\u206a",
        "\u206b",
        "\u206c",
        "\u206d",
        "\u206e",
        "\u206f",
        "\ufeff",
    }
)


class UnsafePathError(ValueError):
    """A repository path is not safe to materialize on supported hosts."""


class UnsafeSymlinkError(ValueError):
    """A relative symlink graph can resolve outside its declared root."""


LinkLookup = Callable[[PurePosixPath], str | None]


def portable_path_identity(path: PurePosixPath) -> tuple[str, ...]:
    """Return the conservative identity used by supported checkout filesystems.

    Default macOS filesystems compare names without case or canonical-Unicode
    distinctions.  Git additionally ignores a small set of formatting code
    points when protecting its administrative directory on HFS/APFS.  Treating
    those spellings as one namespace keeps a Linux worker from producing paths
    that collapse or resolve differently when applied on macOS.
    """

    return tuple(
        unicodedata.normalize(
            "NFD",
            "".join(
                character
                for character in component
                if character not in HFS_GIT_IGNORABLES
            ).casefold(),
        )
        for component in path.parts
    )


def reserved_git_component(value: str) -> bool:
    """Match Git's cross-platform protections for aliases of ``.git``."""

    hfs_name = "".join(
        character for character in value if character not in HFS_GIT_IGNORABLES
    )
    if hfs_name.casefold() == ".git":
        return True

    ntfs_name = hfs_name.casefold()
    if ntfs_name.startswith(".git"):
        remainder = ntfs_name[4:]
    elif ntfs_name.startswith("git~1"):
        remainder = ntfs_name[5:]
    else:
        return False
    for character in remainder:
        if character == ":":
            return True
        if character not in {".", " "}:
            return False
    return True


def validate_repo_path(value: object) -> str:
    """Return one canonical relative POSIX path or reject it."""

    if not isinstance(value, str) or not value or "\x00" in value:
        raise UnsafePathError("path must be a non-empty string")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        raise UnsafePathError("path is not valid UTF-8") from None
    path = PurePosixPath(value)
    if any(reserved_git_component(part) for part in path.parts):
        raise UnsafePathError("path contains a reserved .git component")
    if (
        len(encoded) > MAX_PATH_BYTES
        or len(path.parts) > MAX_PATH_DEPTH
        or value.startswith("/")
        or "\\" in value
        or value != path.as_posix()
        or path.as_posix() == "."
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise UnsafePathError("path must be a safe relative POSIX path")
    return value


def validate_symlink_target(path: str, target: object) -> str:
    """Validate one relative target lexically before graph validation."""

    if not isinstance(target, str) or not target or "\x00" in target:
        raise UnsafeSymlinkError("symlink target is invalid")
    try:
        encoded = target.encode("utf-8")
    except UnicodeEncodeError:
        raise UnsafeSymlinkError("symlink target is invalid") from None
    link = PurePosixPath(target)
    if (
        link.is_absolute()
        or len(encoded) > MAX_PATH_BYTES
        or "\\" in target
        or any(reserved_git_component(part) for part in link.parts)
    ):
        raise UnsafeSymlinkError("symlink target is unsafe")
    depth = len(PurePosixPath(path).parent.parts)
    for part in link.parts:
        if part == "..":
            depth -= 1
            if depth < 0:
                raise UnsafeSymlinkError("symlink target escapes its root")
        elif part not in {"", "."}:
            depth += 1
    return target


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

    normalized: dict[tuple[str, ...], str] = {}

    def target_parts(target: object) -> tuple[str, ...]:
        if not isinstance(target, str) or not target or "\x00" in target:
            raise UnsafeSymlinkError("symlink target is invalid")
        try:
            encoded = target.encode("utf-8")
        except UnicodeEncodeError:
            raise UnsafeSymlinkError("symlink target is invalid") from None
        expanded = PurePosixPath(target)
        if expanded.is_absolute():
            raise UnsafeSymlinkError("symlink target is absolute")
        if (
            len(encoded) > MAX_PATH_BYTES
            or "\\" in target
            or any(reserved_git_component(part) for part in expanded.parts)
        ):
            raise UnsafeSymlinkError("symlink target is unsafe")
        return expanded.parts

    for source, target in links.items():
        if (
            not isinstance(source, PurePosixPath)
            or not source.parts
            or source.is_absolute()
            or ".." in source.parts
        ):
            raise UnsafeSymlinkError("symlink path is invalid")
        target_parts(target)
        identity = portable_path_identity(source)
        if identity in normalized:
            raise UnsafeSymlinkError("symlink graph contains portable path aliases")
        normalized[identity] = target

    def target_for(path: PurePosixPath) -> str | None:
        identity = portable_path_identity(path)
        if identity in normalized:
            return normalized[identity]
        return None if lookup is None else lookup(path)

    # Bound hostile graphs without imposing a small fixed chain limit.  A
    # normal repository consumes roughly one step per target component.
    remaining_steps = max(4096, len(normalized) * 32)
    for source, initial_target in links.items():
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
