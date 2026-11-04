from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .domain import ValidationError, canonical_json, content_sha256, require_string
from .git_refs import normalize_branch_ref

PROVIDER_EFFECT_READBACK_SCHEMA_VERSION = 1
OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION = 2
MAX_SECONDARY_EFFECTS = 64

_CHANGE_ID_RE = re.compile(r"^[1-9][0-9]{0,19}$")
_OBJECT_ID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_SECONDARY_EFFECT_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,31}:[^\s]{1,223}$")
_REMOTE_REFERENCE_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,31}:[^\s]{1,223}$")
_WORKFLOW_INTENT_ID_PATTERNS = {
    "project_id": re.compile(r"^boxp_[0-9a-f]{32}$"),
    "work_item_id": re.compile(r"^wki_[0-9a-f]{32}$"),
    "work_item_version_id": re.compile(r"^wiv_[0-9a-f]{32}$"),
    "candidate_id": re.compile(r"^cnd_[0-9a-f]{32}$"),
    "gate_policy_id": re.compile(r"^gpl_[0-9a-f]{32}$"),
}


def _exact_fields(
    value: Mapping[str, Any], *, expected: set[str], subject: str
) -> None:
    unknown = set(value) - expected
    missing = expected - set(value)
    if unknown:
        raise ValidationError(
            f"{subject} contains unknown fields: " + ", ".join(sorted(unknown))
        )
    if missing:
        raise ValidationError(
            f"{subject} is missing fields: " + ", ".join(sorted(missing))
        )


def _document(value: object, *, subject: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{subject} must be an object")
    return value


def _schema_version(
    value: object,
    *,
    subject: str,
    supported: frozenset[int] = frozenset({PROVIDER_EFFECT_READBACK_SCHEMA_VERSION}),
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value not in supported:
        versions = " or ".join(str(item) for item in sorted(supported))
        raise ValidationError(f"{subject} schema_version must be {versions}")
    return value


def _identifier(value: object, *, field: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValidationError(f"{field} is invalid")
    return value


def _revision(value: object, *, field: str) -> str:
    return _identifier(value, field=field, pattern=_OBJECT_ID_RE)


def _branch_ref(value: object, *, field: str) -> str:
    return normalize_branch_ref(value, field=field)


def _secondary_effects(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) > MAX_SECONDARY_EFFECTS:
        raise ValidationError(
            "provider secondary_effects must contain at most 64 entries"
        )
    effects = tuple(
        _identifier(
            item,
            field="provider secondary effect",
            pattern=_SECONDARY_EFFECT_RE,
        )
        for item in value
    )
    if len(set(effects)) != len(effects):
        raise ValidationError("provider secondary_effects must be unique")
    return effects


def _closure_references(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or len(value) > MAX_SECONDARY_EFFECTS:
        raise ValidationError(
            "provider closure_references must contain at most 64 entries"
        )
    references = tuple(
        _identifier(
            item,
            field="provider closure reference",
            pattern=_REMOTE_REFERENCE_RE,
        )
        for item in value
    )
    if len(set(references)) != len(references):
        raise ValidationError("provider closure_references must be unique")
    return references


def _validate_closure_effects(
    closure_references: tuple[str, ...], secondary_effects: tuple[str, ...]
) -> None:
    observed = {
        effect.removeprefix("issue.close:")
        for effect in secondary_effects
        if effect.startswith("issue.close:")
    }
    if observed != set(closure_references):
        raise ValidationError(
            "provider closure references do not match secondary effects"
        )


@dataclass(frozen=True)
class WorkflowProviderIntent:
    schema_version: int
    project_id: str
    work_item_id: str
    work_item_version_id: str
    candidate_id: str
    gate_policy_id: str
    base_ref: str
    head_ref: str
    title: str
    body: str
    draft: bool
    merge_method: str
    secondary_effects: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: object) -> WorkflowProviderIntent:
        subject = "workflow provider intent"
        document = _document(value, subject=subject)
        _exact_fields(
            document,
            expected={
                "schema_version",
                "project_id",
                "work_item_id",
                "work_item_version_id",
                "candidate_id",
                "gate_policy_id",
                "base_ref",
                "head_ref",
                "title",
                "body",
                "draft",
                "merge_method",
                "secondary_effects",
            },
            subject=subject,
        )
        identifiers = {
            field: _identifier(document[field], field=field, pattern=pattern)
            for field, pattern in _WORKFLOW_INTENT_ID_PATTERNS.items()
        }
        draft = document["draft"]
        if not isinstance(draft, bool):
            raise ValidationError("workflow provider intent draft must be a boolean")
        merge_method = document["merge_method"]
        if merge_method not in {"merge", "rebase", "squash"}:
            raise ValidationError("workflow provider intent merge_method is invalid")
        secondary_effects = _secondary_effects(document["secondary_effects"])
        if secondary_effects:
            raise ValidationError(
                "workflow provider intent secondary_effects must be empty"
            )
        return cls(
            schema_version=_schema_version(document["schema_version"], subject=subject),
            project_id=identifiers["project_id"],
            work_item_id=identifiers["work_item_id"],
            work_item_version_id=identifiers["work_item_version_id"],
            candidate_id=identifiers["candidate_id"],
            gate_policy_id=identifiers["gate_policy_id"],
            base_ref=_branch_ref(document["base_ref"], field="intent base_ref"),
            head_ref=_branch_ref(document["head_ref"], field="intent head_ref"),
            title=require_string(
                document["title"], field="workflow provider intent title", max_bytes=512
            ),
            body=require_string(
                document["body"],
                field="workflow provider intent body",
                max_bytes=32768,
                allow_empty=True,
            ),
            draft=draft,
            merge_method=merge_method,
            secondary_effects=secondary_effects,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "project_id": self.project_id,
            "work_item_id": self.work_item_id,
            "work_item_version_id": self.work_item_version_id,
            "candidate_id": self.candidate_id,
            "gate_policy_id": self.gate_policy_id,
            "base_ref": self.base_ref,
            "head_ref": self.head_ref,
            "title": self.title,
            "body": self.body,
            "draft": self.draft,
            "merge_method": self.merge_method,
            "secondary_effects": list(self.secondary_effects),
        }

    @property
    def canonical(self) -> str:
        return canonical_json(self.to_dict())

    @property
    def digest(self) -> str:
        return content_sha256(self.canonical)


@dataclass(frozen=True)
class RefPublicationReadback:
    schema_version: int
    candidate_id: str
    target_ref: str
    published_revision: str
    secondary_effects: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: object) -> RefPublicationReadback:
        subject = "ref publication readback"
        document = _document(value, subject=subject)
        _exact_fields(
            document,
            expected={
                "schema_version",
                "candidate_id",
                "target_ref",
                "published_revision",
                "secondary_effects",
            },
            subject=subject,
        )
        candidate_id = document["candidate_id"]
        if (
            not isinstance(candidate_id, str)
            or re.fullmatch(r"cnd_[0-9a-f]{32}", candidate_id) is None
        ):
            raise ValidationError("ref publication candidate_id is invalid")
        return cls(
            schema_version=_schema_version(document["schema_version"], subject=subject),
            candidate_id=candidate_id,
            target_ref=_branch_ref(
                document["target_ref"], field="ref publication target_ref"
            ),
            published_revision=_revision(
                document["published_revision"],
                field="ref publication published_revision",
            ),
            secondary_effects=_secondary_effects(document["secondary_effects"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "target_ref": self.target_ref,
            "published_revision": self.published_revision,
            "secondary_effects": list(self.secondary_effects),
        }


@dataclass(frozen=True)
class ChangeOpenReadback:
    schema_version: int
    change_id: str
    base_ref: str
    head_ref: str
    base_revision: str
    head_revision: str
    title: str
    body: str
    draft: bool
    state: str
    closure_references: tuple[str, ...]
    secondary_effects: tuple[str, ...]

    @classmethod
    def from_mapping(cls, value: object) -> ChangeOpenReadback:
        subject = "change-open readback"
        document = _document(value, subject=subject)
        schema_version = _schema_version(
            document.get("schema_version"),
            subject=subject,
            supported=frozenset(
                {
                    PROVIDER_EFFECT_READBACK_SCHEMA_VERSION,
                    OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION,
                }
            ),
        )
        expected = {
            "schema_version",
            "change_id",
            "base_ref",
            "head_ref",
            "base_revision",
            "head_revision",
            "title",
            "body",
            "draft",
            "state",
            "secondary_effects",
        }
        if schema_version == OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION:
            expected.add("closure_references")
        _exact_fields(
            document,
            expected=expected,
            subject=subject,
        )
        draft = document["draft"]
        if not isinstance(draft, bool):
            raise ValidationError("change-open draft must be a boolean")
        state = document["state"]
        if state not in {"open", "closed"}:
            raise ValidationError("change-open state is invalid")
        closure_references = (
            _closure_references(document["closure_references"])
            if schema_version == OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION
            else ()
        )
        secondary_effects = _secondary_effects(document["secondary_effects"])
        if schema_version == OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION:
            _validate_closure_effects(closure_references, secondary_effects)
        return cls(
            schema_version=schema_version,
            change_id=_identifier(
                document["change_id"],
                field="change-open change_id",
                pattern=_CHANGE_ID_RE,
            ),
            base_ref=_branch_ref(document["base_ref"], field="change-open base_ref"),
            head_ref=_branch_ref(document["head_ref"], field="change-open head_ref"),
            base_revision=_revision(
                document["base_revision"], field="change-open base_revision"
            ),
            head_revision=_revision(
                document["head_revision"], field="change-open head_revision"
            ),
            title=require_string(
                document["title"], field="change-open title", max_bytes=512
            ),
            body=require_string(
                document["body"],
                field="change-open body",
                max_bytes=32768,
                allow_empty=True,
            ),
            draft=draft,
            state=state,
            closure_references=closure_references,
            secondary_effects=secondary_effects,
        )

    def to_dict(self) -> dict[str, Any]:
        result = {
            "schema_version": self.schema_version,
            "change_id": self.change_id,
            "base_ref": self.base_ref,
            "head_ref": self.head_ref,
            "base_revision": self.base_revision,
            "head_revision": self.head_revision,
            "title": self.title,
            "body": self.body,
            "draft": self.draft,
            "state": self.state,
            "secondary_effects": list(self.secondary_effects),
        }
        if self.schema_version == OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION:
            result["closure_references"] = list(self.closure_references)
        return result


@dataclass(frozen=True)
class LandingReadback:
    schema_version: int
    change_id: str
    base_ref: str
    head_ref: str
    candidate_revision: str
    landing_revision: str
    landing_tree: str
    landing_parent_revisions: tuple[str, ...]
    landing_commit_message: str
    primary_ref: str
    primary_revision: str
    first_parent_status: str
    title: str
    body: str
    draft: bool
    state: str
    closure_references: tuple[str, ...]
    secondary_effects: tuple[str, ...]
    _legacy_landing_revision_reachable: bool | None = None
    _legacy_merge_method: str | None = None

    @classmethod
    def from_mapping(cls, value: object) -> LandingReadback:
        subject = "landing readback"
        document = _document(value, subject=subject)
        schema_version = _schema_version(
            document.get("schema_version"),
            subject=subject,
            supported=frozenset(
                {
                    PROVIDER_EFFECT_READBACK_SCHEMA_VERSION,
                    OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION,
                }
            ),
        )
        common = {
            "schema_version",
            "change_id",
            "base_ref",
            "head_ref",
            "candidate_revision",
            "landing_revision",
            "landing_tree",
            "primary_ref",
            "primary_revision",
            "title",
            "body",
            "draft",
            "state",
            "secondary_effects",
        }
        if schema_version == PROVIDER_EFFECT_READBACK_SCHEMA_VERSION:
            expected = common | {"landing_revision_reachable", "merge_method"}
        else:
            expected = common | {
                "landing_parent_revisions",
                "landing_commit_message",
                "first_parent_status",
                "closure_references",
            }
        _exact_fields(
            document,
            expected=expected,
            subject=subject,
        )
        legacy_reachable: bool | None = None
        legacy_merge_method: str | None = None
        if schema_version == PROVIDER_EFFECT_READBACK_SCHEMA_VERSION:
            legacy_reachable = document["landing_revision_reachable"]
            if not isinstance(legacy_reachable, bool):
                raise ValidationError(
                    "landing readback landing_revision_reachable must be a boolean"
                )
            legacy_merge_method = document["merge_method"]
            if legacy_merge_method not in {"merge", "rebase", "squash"}:
                raise ValidationError("landing readback merge_method is invalid")
        draft = document["draft"]
        if not isinstance(draft, bool):
            raise ValidationError("landing readback draft must be a boolean")
        state = document["state"]
        if state not in {"open", "closed"}:
            raise ValidationError("landing readback state is invalid")
        if (
            schema_version == OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION
            and state != "closed"
        ):
            raise ValidationError("landing readback v2 state must be closed")
        landing_revision = _revision(
            document["landing_revision"], field="landing landing_revision"
        )
        primary_revision = _revision(
            document["primary_revision"], field="landing primary_revision"
        )
        if schema_version == OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION:
            raw_parents = document["landing_parent_revisions"]
            if not isinstance(raw_parents, list) or not 1 <= len(raw_parents) <= 16:
                raise ValidationError(
                    "landing readback parent revisions must contain 1 to 16 entries"
                )
            landing_parent_revisions = tuple(
                _revision(item, field="landing parent revision") for item in raw_parents
            )
            if len(set(landing_parent_revisions)) != len(landing_parent_revisions):
                raise ValidationError(
                    "landing readback parent revisions must be unique"
                )
            landing_commit_message = require_string(
                document["landing_commit_message"],
                field="landing commit message",
                max_bytes=65536,
            )
            first_parent_status = document["first_parent_status"]
            if first_parent_status not in {"contains", "absent", "truncated"}:
                raise ValidationError("landing readback first_parent_status is invalid")
            if (
                landing_revision == primary_revision
                and first_parent_status != "contains"
            ):
                raise ValidationError(
                    "landing readback primary revision contradicts first-parent status"
                )
            closure_references = _closure_references(document["closure_references"])
        else:
            landing_parent_revisions = ()
            landing_commit_message = ""
            first_parent_status = "contains" if legacy_reachable else "absent"
            closure_references = ()
        secondary_effects = _secondary_effects(document["secondary_effects"])
        if schema_version == OBSERVED_PROVIDER_EFFECT_READBACK_SCHEMA_VERSION:
            _validate_closure_effects(closure_references, secondary_effects)
        return cls(
            schema_version=schema_version,
            change_id=_identifier(
                document["change_id"], field="landing change_id", pattern=_CHANGE_ID_RE
            ),
            base_ref=_branch_ref(document["base_ref"], field="landing base_ref"),
            head_ref=_branch_ref(document["head_ref"], field="landing head_ref"),
            candidate_revision=_revision(
                document["candidate_revision"],
                field="landing candidate_revision",
            ),
            landing_revision=landing_revision,
            landing_tree=_revision(
                document["landing_tree"], field="landing landing_tree"
            ),
            landing_parent_revisions=landing_parent_revisions,
            landing_commit_message=landing_commit_message,
            primary_ref=_branch_ref(
                document["primary_ref"], field="landing primary_ref"
            ),
            primary_revision=primary_revision,
            first_parent_status=first_parent_status,
            title=require_string(
                document["title"], field="landing title", max_bytes=512
            ),
            body=require_string(
                document["body"],
                field="landing body",
                max_bytes=32768,
                allow_empty=True,
            ),
            draft=draft,
            state=state,
            closure_references=closure_references,
            secondary_effects=secondary_effects,
            _legacy_landing_revision_reachable=legacy_reachable,
            _legacy_merge_method=legacy_merge_method,
        )

    def to_dict(self) -> dict[str, Any]:
        result = {
            "schema_version": self.schema_version,
            "change_id": self.change_id,
            "base_ref": self.base_ref,
            "head_ref": self.head_ref,
            "candidate_revision": self.candidate_revision,
            "landing_revision": self.landing_revision,
            "landing_tree": self.landing_tree,
            "primary_ref": self.primary_ref,
            "primary_revision": self.primary_revision,
            "title": self.title,
            "body": self.body,
            "draft": self.draft,
            "state": self.state,
            "secondary_effects": list(self.secondary_effects),
        }
        if self.schema_version == PROVIDER_EFFECT_READBACK_SCHEMA_VERSION:
            result.update(
                {
                    "landing_revision_reachable": (
                        self._legacy_landing_revision_reachable
                    ),
                    "merge_method": self._legacy_merge_method,
                }
            )
        else:
            result.update(
                {
                    "landing_parent_revisions": list(self.landing_parent_revisions),
                    "landing_commit_message": self.landing_commit_message,
                    "first_parent_status": self.first_parent_status,
                    "closure_references": list(self.closure_references),
                }
            )
        return result
