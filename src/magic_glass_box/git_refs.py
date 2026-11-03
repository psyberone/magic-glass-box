from __future__ import annotations

from .domain import ValidationError, require_string

BRANCH_REF_PREFIX = "refs/heads/"
MAX_BRANCH_REF_BYTES = 256


def normalize_branch_ref(
    value: object,
    *,
    field: str,
    allow_short: bool = False,
) -> str:
    """Return one canonical branch ref accepted at every Git boundary."""

    text = require_string(value, field=field, max_bytes=MAX_BRANCH_REF_BYTES)
    if text.startswith(BRANCH_REF_PREFIX):
        full = text
    elif allow_short:
        full = f"{BRANCH_REF_PREFIX}{text}"
        if len(full.encode("utf-8")) > MAX_BRANCH_REF_BYTES:
            raise ValidationError(f"{field} exceeds {MAX_BRANCH_REF_BYTES} UTF-8 bytes")
    else:
        raise ValidationError(f"{field} must be a branch ref")

    branch = full.removeprefix(BRANCH_REF_PREFIX)
    forbidden = set(" ~^:?*[\\")
    components = branch.split("/")
    if (
        not branch
        or branch == "@"
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in branch)
        or any(character in forbidden for character in branch)
        or ".." in branch
        or "@{" in branch
        or "//" in branch
        or branch.startswith("/")
        or branch.endswith(("/", "."))
        or any(
            not component or component.startswith(".") or component.endswith(".lock")
            for component in components
        )
    ):
        raise ValidationError(f"{field} is invalid")
    return full
