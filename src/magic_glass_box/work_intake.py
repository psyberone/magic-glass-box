from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC
from typing import Any

from .domain import (
    ValidationError,
    canonical_json,
    parse_rfc3339_timestamp,
    require_string,
)

WORK_ITEM_LIST_SCHEMA_VERSION = 1
MAX_WORK_ITEM_REFERENCES = 100
MAX_WORK_ITEM_LIST_BYTES = 256 * 1024


def _nonblank(value: object, *, field: str, max_bytes: int) -> str:
    result = require_string(value, field=field, max_bytes=max_bytes)
    if not result.strip():
        raise ValidationError(f"{field} must not be blank")
    return result


def _strings(
    value: object, *, field: str, maximum: int, item_bytes: int
) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) > maximum:
        raise ValidationError(f"{field} must be a list of at most {maximum} entries")
    result = tuple(
        _nonblank(entry, field=f"{field} entry", max_bytes=item_bytes)
        for entry in value
    )
    if len(set(result)) != len(result):
        raise ValidationError(f"{field} entries must be unique")
    return result


def _source_timestamp(value: object) -> str:
    parsed = parse_rfc3339_timestamp(value, field="source_updated_at")
    return (
        parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


@dataclass(frozen=True)
class WorkItemReference:
    """Bounded discovery metadata for one provider-owned work object."""

    object_id: str
    external_id: str
    source_updated_at: str
    title: str
    assignees: tuple[str, ...]
    labels: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkItemReference:
        if not isinstance(value, dict):
            raise ValidationError("work item reference must be an object")
        expected = {
            "object_id",
            "external_id",
            "source_updated_at",
            "title",
            "assignees",
            "labels",
        }
        unknown = set(value) - expected
        if unknown:
            raise ValidationError(
                "work item reference contains unknown fields: "
                + ", ".join(sorted(unknown))
            )
        missing = expected - set(value)
        if missing:
            raise ValidationError(
                "work item reference is missing fields: " + ", ".join(sorted(missing))
            )
        return cls(
            object_id=_nonblank(value["object_id"], field="object_id", max_bytes=512),
            external_id=_nonblank(
                value["external_id"], field="external_id", max_bytes=512
            ),
            source_updated_at=_source_timestamp(value["source_updated_at"]),
            title=_nonblank(value["title"], field="title", max_bytes=512),
            assignees=_strings(
                value["assignees"],
                field="assignees",
                maximum=64,
                item_bytes=256,
            ),
            labels=_strings(
                value["labels"], field="labels", maximum=64, item_bytes=128
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "assignees": list(self.assignees),
            "external_id": self.external_id,
            "labels": list(self.labels),
            "object_id": self.object_id,
            "source_updated_at": self.source_updated_at,
            "title": self.title,
        }


@dataclass(frozen=True)
class WorkItemListResult:
    """Core-owned result of one bounded ``work.list`` controller read."""

    schema_version: int
    items: tuple[WorkItemReference, ...]
    truncated: bool

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WorkItemListResult:
        if not isinstance(value, dict):
            raise ValidationError("work item list result must be an object")
        expected = {"schema_version", "items", "truncated"}
        unknown = set(value) - expected
        if unknown:
            raise ValidationError(
                "work item list result contains unknown fields: "
                + ", ".join(sorted(unknown))
            )
        missing = expected - set(value)
        if missing:
            raise ValidationError(
                "work item list result is missing fields: " + ", ".join(sorted(missing))
            )
        schema_version = value["schema_version"]
        if (
            isinstance(schema_version, bool)
            or schema_version != WORK_ITEM_LIST_SCHEMA_VERSION
        ):
            raise ValidationError(
                f"schema_version must be {WORK_ITEM_LIST_SCHEMA_VERSION}"
            )
        raw_items = value["items"]
        if not isinstance(raw_items, list) or len(raw_items) > MAX_WORK_ITEM_REFERENCES:
            raise ValidationError(
                f"items must be a list of at most {MAX_WORK_ITEM_REFERENCES} entries"
            )
        items = tuple(WorkItemReference.from_mapping(item) for item in raw_items)
        object_ids = [item.object_id for item in items]
        external_ids = [item.external_id for item in items]
        if len(set(object_ids)) != len(object_ids) or len(set(external_ids)) != len(
            external_ids
        ):
            raise ValidationError("work item references must be unique")
        truncated = value["truncated"]
        if not isinstance(truncated, bool):
            raise ValidationError("truncated must be a boolean")
        result = cls(
            schema_version=schema_version,
            items=items,
            truncated=truncated,
        )
        if len(result.canonical.encode("utf-8")) > MAX_WORK_ITEM_LIST_BYTES:
            raise ValidationError("work item list result exceeds its byte limit")
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "items": [item.to_dict() for item in self.items],
            "schema_version": self.schema_version,
            "truncated": self.truncated,
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())
