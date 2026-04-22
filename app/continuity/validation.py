"""Capsule validation, timestamp checking, and write-path normalization."""

from __future__ import annotations

import copy
import posixpath
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from pydantic import ValidationError

from app.continuity.constants import (
    CAPSULE_SIZE_LIMIT_ERROR_DETAIL,
    CAPSULE_SIZE_LIMIT_BYTES,
    CONTINUITY_CAPSULE_SCHEMA_VERSION,
    CONTINUITY_SUPPORTED_CAPSULE_SCHEMA_VERSIONS,
    CONTINUITY_INTERACTION_BOUNDARY_KINDS,
    CONTINUITY_PATH_RE,
    THREAD_DESCRIPTOR_ANCHOR_KIND_RE,
    THREAD_DESCRIPTOR_SCOPE_ANCHOR_RE,
)
from app.continuity.paths import _normalize_subject_id
from app.models import (
    ContinuityCapsule,
    ContinuityUpsertRequest,
    ContinuityVerificationSignal,
)
from app.storage import StorageError, canonical_json, safe_path
from app.timestamps import format_iso, parse_iso as _parse_iso


_LEGACY_TIMESTAMP_FLOOR = "1970-01-01T00:00:00Z"
_RELATED_DOCUMENTS_ALLOWED_KEYS = frozenset({"path", "kind", "label", "relevance"})
_RELATED_DOCUMENTS_REQUIRED_KEYS = ("path", "kind", "label")
_RELATED_DOCUMENTS_KEY_ORDER = ("path", "kind", "label", "relevance")
_RELATED_DOCUMENTS_RESERVED_KEYS = frozenset({"content", "body", "text", "excerpt", "markdown", "html", "payload"})
_RELATED_DOCUMENTS_RELEVANCE = frozenset({"primary", "supporting", "background"})
_RELATED_DOCUMENTS_PATH_RE = re.compile(r"^[A-Za-z0-9._/-]+$")
_RELATED_DOCUMENTS_KIND_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_RELATED_DOCUMENTS_PATH_MAX = 240
_RELATED_DOCUMENTS_KIND_MAX = 32
_RELATED_DOCUMENTS_LABEL_MAX = 120
_RELATED_DOCUMENTS_RELEVANCE_MAX = 32
_RELATED_DOCUMENTS_MAX_ENTRIES = 8
_MISSING = object()


def _upgrade_legacy_structured_entry_timestamps(payload: dict[str, Any]) -> dict[str, Any]:
    """Upgrade stored legacy ``set_at`` fields into the current timestamp shape.

    This helper is for persisted on-disk payloads only. Public write paths
    should use the current schema and the service-layer stamping pass.

    It also repairs legacy top-level capsule timestamps when they are
    missing or unusable so persisted old capsules can still load through
    modern read, patch, and restore paths deterministically.
    """
    upgraded = copy.deepcopy(payload)
    schema_version = str(upgraded.get("schema_version") or "").strip()
    if not schema_version or schema_version in CONTINUITY_SUPPORTED_CAPSULE_SCHEMA_VERSIONS:
        upgraded["schema_version"] = CONTINUITY_CAPSULE_SCHEMA_VERSION

    def _timestamp_state(field_name: str) -> tuple[str, str | None]:
        raw_value = upgraded.get(field_name)
        if raw_value is None:
            return "missing", None
        text = str(raw_value).strip()
        if not text:
            return "missing", None
        parsed = _parse_iso(text)
        if parsed is None:
            return "invalid", None
        return "valid", format_iso(parsed)

    updated_state, normalized_updated_at = _timestamp_state("updated_at")
    verified_state, normalized_verified_at = _timestamp_state("verified_at")

    if normalized_updated_at is not None:
        resolved_updated_at = normalized_updated_at
    elif updated_state == "missing" and normalized_verified_at is not None:
        resolved_updated_at = normalized_verified_at
    else:
        resolved_updated_at = _LEGACY_TIMESTAMP_FLOOR

    if normalized_verified_at is not None:
        resolved_verified_at = normalized_verified_at
    elif verified_state == "missing" and normalized_updated_at is not None:
        resolved_verified_at = normalized_updated_at
    else:
        resolved_verified_at = _LEGACY_TIMESTAMP_FLOOR

    upgraded["updated_at"] = resolved_updated_at
    upgraded["verified_at"] = resolved_verified_at

    def _upgrade_entry(entry: Any) -> Any:
        if not isinstance(entry, dict):
            return entry
        legacy_set_at = entry.pop("set_at", None)
        if legacy_set_at is not None:
            entry.setdefault("created_at", legacy_set_at)
            entry.setdefault("updated_at", legacy_set_at)
        if normalized_updated_at is not None:
            entry.setdefault("created_at", normalized_updated_at)
            entry.setdefault("updated_at", normalized_updated_at)
        return entry

    continuity = upgraded.get("continuity")
    if isinstance(continuity, dict):
        for field_name in ("negative_decisions", "rationale_entries"):
            items = continuity.get(field_name)
            if isinstance(items, list):
                continuity[field_name] = [_upgrade_entry(item) for item in items]
    stable_preferences = upgraded.get("stable_preferences")
    if isinstance(stable_preferences, list):
        upgraded["stable_preferences"] = [_upgrade_entry(item) for item in stable_preferences]
    return upgraded


def _require_utc_timestamp(value: str, field_name: str) -> datetime:
    """Require a valid UTC timestamp or raise an HTTP 400 error."""
    dt = _parse_iso(value)
    if dt is None:
        raise HTTPException(status_code=400, detail=f"Invalid UTC timestamp for {field_name}")
    if not (value.endswith("Z") or value.endswith("+00:00")):
        raise HTTPException(status_code=400, detail=f"Timestamp must be UTC for {field_name}")
    return dt


def _validate_repo_relative_paths(repo_root: Path, paths: list[str], field_name: str) -> None:
    """Validate that repo-relative paths stay within the repository root."""
    for rel in paths:
        if not rel or not CONTINUITY_PATH_RE.match(rel):
            raise HTTPException(status_code=400, detail=f"Invalid repo-relative path in {field_name}")
        try:
            safe_path(repo_root, rel)
        except StorageError as e:
            raise HTTPException(status_code=400, detail=f"Invalid repo-relative path in {field_name}: {e}") from e


def _contains_reserved_related_documents_key(value: Any) -> bool:
    """Return True when any reserved embedded-content key exists recursively."""
    if isinstance(value, dict):
        for key, nested in value.items():
            if key in _RELATED_DOCUMENTS_RESERVED_KEYS:
                return True
            if _contains_reserved_related_documents_key(nested):
                return True
        return False
    if isinstance(value, list):
        return any(_contains_reserved_related_documents_key(item) for item in value)
    return False


def _is_valid_related_document_path(value: str) -> bool:
    """Apply the lexical-only #217 path rules for related_documents[].path."""
    if not _RELATED_DOCUMENTS_PATH_RE.match(value):
        return False
    if value.startswith("/") or ".." in value:
        return False
    normalized = posixpath.normpath(value)
    if normalized == ".." or normalized.startswith("../") or normalized.startswith("/"):
        return False
    return True


def _related_documents_failure_key(category: int, *parts: int) -> tuple[int, ...]:
    """Build a sortable precedence key for one related_documents failure."""
    return (category, *parts)


def _iter_related_documents_failures(value: Any) -> list[tuple[tuple[int, ...], str]]:
    """Return all deterministic write-time validation failures for related_documents."""
    failures: list[tuple[tuple[int, ...], str]] = []

    if _contains_reserved_related_documents_key(value):
        failures.append(
            (
                _related_documents_failure_key(1),
                "Embedded content is not allowed in continuity.related_documents[]",
            )
        )

    if not isinstance(value, list):
        failures.append(
            (
                _related_documents_failure_key(3, -1),
                "Invalid value type in continuity.related_documents[]",
            )
        )
        return failures

    if len(value) > _RELATED_DOCUMENTS_MAX_ENTRIES:
        failures.append(
            (
                _related_documents_failure_key(11),
                "Too many entries in continuity.related_documents",
            )
        )

    seen_duplicates: dict[tuple[str, str, str, object], int] = {}

    for entry_index, entry in enumerate(value):
        if not isinstance(entry, dict):
            failures.append(
                (
                    _related_documents_failure_key(3, entry_index, len(_RELATED_DOCUMENTS_KEY_ORDER)),
                    "Invalid value type in continuity.related_documents[]",
                )
            )
            continue

        unknown_keys = sorted(key for key in entry if key not in _RELATED_DOCUMENTS_ALLOWED_KEYS and key not in _RELATED_DOCUMENTS_RESERVED_KEYS)
        if unknown_keys:
            failures.append(
                (
                    _related_documents_failure_key(2, entry_index, 0),
                    "Unknown key in continuity.related_documents[]",
                )
            )

        for key_rank, key_name in enumerate(_RELATED_DOCUMENTS_REQUIRED_KEYS):
            if key_name not in entry:
                failures.append(
                    (
                        _related_documents_failure_key(4, entry_index, key_rank),
                        "Missing required key in continuity.related_documents[]",
                    )
                )

        candidate_for_duplicate = True
        duplicate_parts: list[str] = []

        for key_rank, key_name in enumerate(_RELATED_DOCUMENTS_KEY_ORDER):
            raw = entry.get(key_name, _MISSING)
            if raw is _MISSING:
                if key_name == "relevance":
                    duplicate_parts.append("")
                else:
                    candidate_for_duplicate = False
                continue

            if not isinstance(raw, str):
                failures.append(
                    (
                        _related_documents_failure_key(3, entry_index, key_rank),
                        "Invalid value type in continuity.related_documents[]",
                    )
                )
                candidate_for_duplicate = False
                continue

            if raw == "":
                failures.append(
                    (
                        _related_documents_failure_key(7, entry_index, key_rank),
                        "Value too short in continuity.related_documents[]",
                    )
                )
                candidate_for_duplicate = False
                continue

            if raw != raw.strip() or raw.strip() == "":
                failures.append(
                    (
                        _related_documents_failure_key(8, entry_index, key_rank),
                        "Leading or trailing whitespace is not allowed in continuity.related_documents[]",
                    )
                )
                candidate_for_duplicate = False
                continue

            if key_name == "path":
                if len(raw) > _RELATED_DOCUMENTS_PATH_MAX:
                    failures.append(
                        (
                            _related_documents_failure_key(9, entry_index, key_rank),
                            "Value too long in continuity.related_documents[].path",
                        )
                    )
                    candidate_for_duplicate = False
                elif not _is_valid_related_document_path(raw):
                    failures.append(
                        (
                            _related_documents_failure_key(5, entry_index, key_rank),
                            "Invalid path in continuity.related_documents[].path",
                        )
                    )
                    candidate_for_duplicate = False
            elif key_name == "kind":
                if len(raw) > _RELATED_DOCUMENTS_KIND_MAX:
                    failures.append(
                        (
                            _related_documents_failure_key(9, entry_index, key_rank),
                            "Value too long in continuity.related_documents[].kind",
                        )
                    )
                    candidate_for_duplicate = False
                elif not _RELATED_DOCUMENTS_KIND_RE.match(raw):
                    failures.append(
                        (
                            _related_documents_failure_key(6, entry_index, key_rank),
                            "Invalid kind format in continuity.related_documents[]",
                        )
                    )
                    candidate_for_duplicate = False
            elif key_name == "label":
                if len(raw) > _RELATED_DOCUMENTS_LABEL_MAX:
                    failures.append(
                        (
                            _related_documents_failure_key(9, entry_index, key_rank),
                            "Value too long in continuity.related_documents[].label",
                        )
                    )
                    candidate_for_duplicate = False
            elif key_name == "relevance":
                if len(raw) > _RELATED_DOCUMENTS_RELEVANCE_MAX:
                    failures.append(
                        (
                            _related_documents_failure_key(9, entry_index, key_rank),
                            "Value too long in continuity.related_documents[].relevance",
                        )
                    )
                    candidate_for_duplicate = False
                elif raw not in _RELATED_DOCUMENTS_RELEVANCE:
                    failures.append(
                        (
                            _related_documents_failure_key(6, entry_index, key_rank),
                            "Invalid relevance in continuity.related_documents[]",
                        )
                    )
                    candidate_for_duplicate = False

            duplicate_parts.append(raw)

        if candidate_for_duplicate:
            duplicate_key = (
                duplicate_parts[0],
                duplicate_parts[1],
                duplicate_parts[2],
                _MISSING if "relevance" not in entry else duplicate_parts[3],
            )
            first_index = seen_duplicates.get(duplicate_key)
            if first_index is None:
                seen_duplicates[duplicate_key] = entry_index
            else:
                failures.append(
                    (
                        _related_documents_failure_key(10, entry_index, first_index),
                        "Duplicate related_documents entry",
                    )
                )

    return failures


def _validate_related_documents_on_write(value: Any, *, field_present: bool) -> Any:
    """Validate raw related_documents input and return the original value on success."""
    if not field_present:
        return None
    failures = _iter_related_documents_failures(value)
    if failures:
        _sort_key, detail = min(failures, key=lambda item: item[0])
        raise HTTPException(status_code=400, detail=detail)
    if value is None:
        raise HTTPException(status_code=400, detail="Invalid value type in continuity.related_documents[]")
    return value


def _sanitize_related_documents_on_read(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Omit malformed stored related_documents and return any degraded-read warnings."""
    continuity = payload.get("continuity")
    if not isinstance(continuity, dict) or "related_documents" not in continuity:
        return payload, []
    value = continuity.get("related_documents")
    if not _iter_related_documents_failures(value):
        return payload, []

    warning = (
        "related_documents_omitted_non_metadata"
        if _contains_reserved_related_documents_key(value)
        else "related_documents_omitted_invalid"
    )
    sanitized = copy.deepcopy(payload)
    sanitized_continuity = sanitized.get("continuity")
    if isinstance(sanitized_continuity, dict):
        sanitized_continuity.pop("related_documents", None)
    return sanitized, [warning]


def _validate_low_commitment_fields(capsule: ContinuityCapsule) -> None:
    """Validate low-commitment continuity fields using service-layer HTTP 400 semantics."""
    # trailing_notes, curiosity_queue, and negative_decisions intentionally add per-item
    # minimum-length checks; older continuity list fields remain max-only.
    for value in list(capsule.continuity.trailing_notes):
        if len(value) < 1:
            raise HTTPException(status_code=400, detail="Value too short in continuity.trailing_notes")
        if len(value) > 160:
            raise HTTPException(status_code=400, detail="Value too long in continuity.trailing_notes")
    for value in list(capsule.continuity.curiosity_queue):
        if len(value) < 1:
            raise HTTPException(status_code=400, detail="Value too short in continuity.curiosity_queue")
        if len(value) > 120:
            raise HTTPException(status_code=400, detail="Value too long in continuity.curiosity_queue")
    seen_negative_decisions: set[str] = set()
    for decision in list(capsule.continuity.negative_decisions):
        if len(decision.decision) < 1:
            raise HTTPException(status_code=400, detail="Value too short in continuity.negative_decisions.decision")
        if len(decision.decision) > 160:
            raise HTTPException(status_code=400, detail="Value too long in continuity.negative_decisions.decision")
        if len(decision.rationale) < 1:
            raise HTTPException(status_code=400, detail="Value too short in continuity.negative_decisions.rationale")
        if len(decision.rationale) > 240:
            raise HTTPException(status_code=400, detail="Value too long in continuity.negative_decisions.rationale")
        if decision.created_at is not None:
            _require_utc_timestamp(decision.created_at, "continuity.negative_decisions[].created_at")
        if decision.updated_at is not None:
            _require_utc_timestamp(decision.updated_at, "continuity.negative_decisions[].updated_at")
        if decision.last_confirmed_at is not None:
            _require_utc_timestamp(decision.last_confirmed_at, "continuity.negative_decisions[].last_confirmed_at")
        if decision.decision in seen_negative_decisions:
            raise HTTPException(
                status_code=400,
                detail=f"Duplicate negative_decisions decision: {decision.decision}",
            )
        seen_negative_decisions.add(decision.decision)
    for entry in list(capsule.continuity.rationale_entries):
        if len(entry.tag) < 1:
            raise HTTPException(status_code=400, detail="Value too short in continuity.rationale_entries[].tag")
        if len(entry.tag) > 80:
            raise HTTPException(status_code=400, detail="Value too long in continuity.rationale_entries[].tag")
        if len(entry.summary) < 1:
            raise HTTPException(status_code=400, detail="Value too short in continuity.rationale_entries[].summary")
        if len(entry.summary) > 320:
            raise HTTPException(status_code=400, detail="Value too long in continuity.rationale_entries[].summary")
        if len(entry.reasoning) < 1:
            raise HTTPException(status_code=400, detail="Value too short in continuity.rationale_entries[].reasoning")
        if len(entry.reasoning) > 560:
            raise HTTPException(status_code=400, detail="Value too long in continuity.rationale_entries[].reasoning")
        for alt in list(entry.alternatives_considered):
            if len(alt) < 1:
                raise HTTPException(status_code=400, detail="Value too short in continuity.rationale_entries[].alternatives_considered")
            if len(alt) > 160:
                raise HTTPException(status_code=400, detail="Value too long in continuity.rationale_entries[].alternatives_considered")
        for dep in list(entry.depends_on):
            if len(dep) < 1:
                raise HTTPException(status_code=400, detail="Value too short in continuity.rationale_entries[].depends_on")
            if len(dep) > 120:
                raise HTTPException(status_code=400, detail="Value too long in continuity.rationale_entries[].depends_on")
        if entry.created_at is not None:
            _require_utc_timestamp(entry.created_at, "continuity.rationale_entries[].created_at")
        if entry.updated_at is not None:
            _require_utc_timestamp(entry.updated_at, "continuity.rationale_entries[].updated_at")
        if entry.last_confirmed_at is not None:
            _require_utc_timestamp(entry.last_confirmed_at, "continuity.rationale_entries[].last_confirmed_at")


def _strip_service_managed_descriptor_fields(capsule: ContinuityCapsule) -> None:
    """Silently discard caller-supplied lifecycle and superseded_by on thread_descriptor."""
    if capsule.thread_descriptor is not None:
        capsule.thread_descriptor.lifecycle = None
        capsule.thread_descriptor.superseded_by = None


def _validate_thread_descriptor(capsule: ContinuityCapsule) -> None:
    """Validate thread_descriptor constraints when present."""
    td = capsule.thread_descriptor
    if td is None:
        return
    if capsule.subject_kind not in ("thread", "task"):
        raise HTTPException(
            status_code=400,
            detail="thread_descriptor is only allowed for thread and task capsules",
        )
    # Normalize keywords: lowercase, strip, filter empty, deduplicate preserving order
    normalized: list[str] = []
    seen: set[str] = set()
    for kw in td.keywords:
        kw = kw.lower().strip()
        if not kw:
            continue
        if kw not in seen:
            normalized.append(kw)
            seen.add(kw)
    td.keywords = normalized
    for kw in td.keywords:
        if len(kw) < 1 or len(kw) > 40:
            raise HTTPException(status_code=400, detail="Keyword must be 1-40 characters")
    for anchor in td.scope_anchors:
        if not THREAD_DESCRIPTOR_SCOPE_ANCHOR_RE.match(anchor):
            raise HTTPException(status_code=400, detail=f"Invalid scope_anchor format: {anchor}")
    seen_anchors: set[tuple[str, str]] = set()
    for ia in td.identity_anchors:
        ia.kind = ia.kind.lower().strip()
        ia.value = ia.value.strip()
        if not THREAD_DESCRIPTOR_ANCHOR_KIND_RE.match(ia.kind):
            raise HTTPException(status_code=400, detail=f"Invalid identity_anchor kind: {ia.kind}")
        if len(ia.value) < 1 or len(ia.value) > 200:
            raise HTTPException(status_code=400, detail="identity_anchor value must be 1-200 characters")
        key = (ia.kind, ia.value)
        if key in seen_anchors:
            raise HTTPException(status_code=400, detail=f"Duplicate identity_anchor: ({ia.kind}, {ia.value})")
        seen_anchors.add(key)


def _validate_lifecycle_transition_request(req: ContinuityUpsertRequest) -> None:
    """Validate lifecycle_transition + superseded_by consistency on the request."""
    if req.lifecycle_transition is None and req.superseded_by is None:
        return
    if req.superseded_by is not None and req.lifecycle_transition != "supersede":
        raise HTTPException(status_code=400, detail="superseded_by is only allowed when lifecycle_transition is 'supersede'")
    if req.subject_kind not in ("thread", "task"):
        if req.lifecycle_transition is not None:
            raise HTTPException(
                status_code=400,
                detail="lifecycle_transition is only allowed for thread and task capsules",
            )
        raise HTTPException(
            status_code=400,
            detail="superseded_by is only allowed for thread and task capsules",
        )
    if req.lifecycle_transition == "supersede" and req.superseded_by is None:
        raise HTTPException(status_code=400, detail="superseded_by is required when lifecycle_transition is 'supersede'")


def _validate_capsule(repo_root: Path, capsule: ContinuityCapsule) -> tuple[dict[str, Any], str]:
    """Validate write-path continuity bounds and return normalized payload plus canonical JSON."""
    capsule.schema_version = CONTINUITY_CAPSULE_SCHEMA_VERSION
    _require_utc_timestamp(capsule.updated_at, "updated_at")
    _require_utc_timestamp(capsule.verified_at, "verified_at")
    if capsule.freshness and capsule.freshness.expires_at:
        _require_utc_timestamp(capsule.freshness.expires_at, "freshness.expires_at")
    for source_input in list(capsule.source.inputs):
        if len(source_input) > 200:
            raise HTTPException(status_code=400, detail="Value too long in source.inputs")
    for field_name in (
        "top_priorities",
        "active_concerns",
        "active_constraints",
        "open_loops",
        "drift_signals",
        "working_hypotheses",
        "long_horizon_commitments",
    ):
        for value in list(getattr(capsule.continuity, field_name)):
            if len(value) > 160:
                raise HTTPException(status_code=400, detail=f"Value too long in {field_name}")
    for value in list(capsule.continuity.session_trajectory):
        if len(value) > 80:
            raise HTTPException(status_code=400, detail="Value too long in continuity.session_trajectory")
    _validate_low_commitment_fields(capsule)
    if len(capsule.continuity.stance_summary) > 240:
        raise HTTPException(status_code=400, detail="Value too long in continuity.stance_summary")
    if capsule.continuity.relationship_model:
        for value in capsule.continuity.relationship_model.preferred_style:
            if len(value) > 80:
                raise HTTPException(status_code=400, detail="Value too long in relationship_model.preferred_style")
        for value in capsule.continuity.relationship_model.sensitivity_notes:
            if len(value) > 120:
                raise HTTPException(status_code=400, detail="Value too long in relationship_model.sensitivity_notes")
    if capsule.attention_policy:
        for value in capsule.attention_policy.presence_bias_overrides:
            if len(value) > 160:
                raise HTTPException(status_code=400, detail="Value too long in attention_policy.presence_bias_overrides")
    if capsule.continuity.retrieval_hints:
        for field_name in ("must_include", "avoid"):
            for value in list(getattr(capsule.continuity.retrieval_hints, field_name)):
                if len(value) > 160:
                    raise HTTPException(status_code=400, detail=f"Value too long in retrieval_hints.{field_name}")
        _validate_repo_relative_paths(repo_root, list(capsule.continuity.retrieval_hints.load_next), "retrieval_hints.load_next")
    if capsule.canonical_sources:
        _validate_repo_relative_paths(repo_root, list(capsule.canonical_sources), "canonical_sources")
    if capsule.metadata and len(capsule.metadata) > 12:
        raise HTTPException(status_code=400, detail="Too many metadata keys")
    for key, value in capsule.metadata.items():
        if not isinstance(key, str):
            raise HTTPException(status_code=400, detail="Invalid metadata key")
        if isinstance(value, (dict, list)):
            raise HTTPException(status_code=400, detail="Metadata values must be scalar")
    boundary_kind = capsule.metadata.get("interaction_boundary_kind")
    if boundary_kind is not None:
        if capsule.source.update_reason != "interaction_boundary":
            raise HTTPException(status_code=400, detail="metadata.interaction_boundary_kind requires source.update_reason=interaction_boundary")
        if boundary_kind not in CONTINUITY_INTERACTION_BOUNDARY_KINDS:
            raise HTTPException(status_code=400, detail="Invalid metadata.interaction_boundary_kind")
    elif capsule.source.update_reason == "interaction_boundary":
        raise HTTPException(status_code=400, detail="metadata.interaction_boundary_kind is required when source.update_reason=interaction_boundary")
    if capsule.stable_preferences:
        if capsule.subject_kind not in ("user", "peer"):
            raise HTTPException(
                status_code=400,
                detail="stable_preferences is only allowed for user and peer capsules",
            )
        seen_tags: set[str] = set()
        for pref in capsule.stable_preferences:
            if pref.created_at is not None:
                _require_utc_timestamp(pref.created_at, "stable_preferences[].created_at")
            if pref.updated_at is not None:
                _require_utc_timestamp(pref.updated_at, "stable_preferences[].updated_at")
            if pref.last_confirmed_at is not None:
                _require_utc_timestamp(pref.last_confirmed_at, "stable_preferences[].last_confirmed_at")
            if pref.tag in seen_tags:
                raise HTTPException(
                    status_code=400,
                    detail=f"Duplicate stable_preferences tag: {pref.tag}",
                )
            seen_tags.add(pref.tag)
    if capsule.continuity.rationale_entries:
        seen_re_tags: set[str] = set()
        for entry in capsule.continuity.rationale_entries:
            if entry.tag in seen_re_tags:
                raise HTTPException(
                    status_code=400,
                    detail=f"Duplicate rationale_entries tag: {entry.tag}",
                )
            seen_re_tags.add(entry.tag)
        all_re_tags = {e.tag: e for e in capsule.continuity.rationale_entries}
        for entry in capsule.continuity.rationale_entries:
            if entry.supersedes is not None:
                if entry.supersedes == entry.tag:
                    raise HTTPException(
                        status_code=400,
                        detail=f"rationale_entries[].supersedes must not reference its own tag '{entry.tag}'",
                    )
                target = all_re_tags.get(entry.supersedes)
                if target is None or target.status != "superseded":
                    raise HTTPException(
                        status_code=400,
                        detail=f"rationale_entries[].supersedes references tag '{entry.supersedes}' which does not exist with status 'superseded'",
                    )
    if capsule.continuity.related_documents is not None:
        capsule.continuity.related_documents = _validate_related_documents_on_write(
            capsule.continuity.related_documents,
            field_present=True,
        )
    _validate_thread_descriptor(capsule)
    payload = capsule.model_dump(mode="json", exclude_none=True)
    canonical = canonical_json(payload)
    if len(canonical.encode("utf-8")) > CAPSULE_SIZE_LIMIT_BYTES:
        raise HTTPException(status_code=400, detail=CAPSULE_SIZE_LIMIT_ERROR_DETAIL)
    return payload, canonical


def _validate_verification_state_and_health(capsule: ContinuityCapsule) -> None:
    """Validate verification_state and capsule_health when present on a capsule."""
    if capsule.verification_state is not None:
        _require_utc_timestamp(capsule.verification_state.last_revalidated_at, "verification_state.last_revalidated_at")
        for ref in capsule.verification_state.evidence_refs:
            if len(ref) > 200:
                raise HTTPException(status_code=400, detail="Value too long in verification_state.evidence_refs")
        if capsule.verification_state.status == "conflicted" and not capsule.verification_state.conflict_summary:
            raise HTTPException(status_code=400, detail="verification_state.conflict_summary is required when status=conflicted")
    if capsule.capsule_health is not None:
        _require_utc_timestamp(capsule.capsule_health.last_checked_at, "capsule_health.last_checked_at")
        for reason in capsule.capsule_health.reasons:
            if len(reason) > 120:
                raise HTTPException(status_code=400, detail="Value too long in capsule_health.reasons")
        if capsule.capsule_health.status in {"degraded", "conflicted"} and not capsule.capsule_health.reasons:
            raise HTTPException(status_code=400, detail="capsule_health.reasons is required when status is degraded or conflicted")


def _strip_verification_fields_for_upsert(capsule: ContinuityCapsule) -> ContinuityCapsule:
    """Return a capsule copy with verification-derived fields removed for upsert."""
    payload = capsule.model_dump(mode="json", exclude_none=True, exclude={"verification_state", "capsule_health"})
    try:
        return ContinuityCapsule.model_validate(payload)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Capsule invalid after verification-field stripping: {e}") from e


def _validate_verification_signals(signals: list[ContinuityVerificationSignal]) -> None:
    """Validate verification signals using the continuity timestamp rules."""
    for signal in signals:
        _require_utc_timestamp(signal.observed_at, "signals.observed_at")


def _validate_candidate_selector_match(subject_kind: str, subject_id: str, candidate_capsule: ContinuityCapsule) -> None:
    """Require a candidate capsule to match the exact request selector after normalization."""
    if candidate_capsule.subject_kind != subject_kind:
        raise HTTPException(status_code=400, detail="Candidate capsule subject does not match request subject")
    if _normalize_subject_id(candidate_capsule.subject_id) != _normalize_subject_id(subject_id):
        raise HTTPException(status_code=400, detail="Candidate capsule subject does not match request subject")


def _final_capsule_payload(repo_root: Path, capsule: ContinuityCapsule) -> tuple[dict[str, Any], str]:
    """Validate a final assembled capsule including verification-derived fields and return canonical JSON."""
    payload, canonical = _validate_capsule(repo_root, capsule)
    _validate_verification_state_and_health(capsule)
    return payload, canonical


def _normalize_compare_payload(repo_root: Path, capsule: ContinuityCapsule) -> dict[str, Any]:
    """Validate and normalize a capsule payload for compare and revalidate semantics."""
    _validate_capsule(repo_root, capsule)
    _validate_verification_state_and_health(capsule)
    return capsule.model_dump(mode="json", exclude_none=True)


# ---------------------------------------------------------------------------
# Write-path normalization expansion (#176 Move D)
# ---------------------------------------------------------------------------

# ContinuityState string-list field names eligible for strip/dedup normalization.
_NORMALIZABLE_STRING_LIST_FIELDS: tuple[str, ...] = (
    "top_priorities",
    "active_concerns",
    "active_constraints",
    "open_loops",
    "drift_signals",
    "working_hypotheses",
    "long_horizon_commitments",
    "session_trajectory",
    "trailing_notes",
    "curiosity_queue",
)


def _dedup_first_wins(items: list[str]) -> tuple[list[str], bool]:
    """Deduplicate a string list preserving the first occurrence of each value."""
    seen: set[str] = set()
    result: list[str] = []
    changed = False
    for item in items:
        if item in seen:
            changed = True
            continue
        seen.add(item)
        result.append(item)
    return result, changed


def _normalize_capsule_fields(capsule: ContinuityCapsule) -> list[str]:
    """Apply write-path normalizations to capsule fields in place.

    Returns a list of normalization action strings (e.g.
    ``"strip:continuity.open_loops"``) describing what changed.  Empty list
    when no normalizations fired.  Idempotent on already-clean data.
    """
    applied: list[str] = []
    cont = capsule.continuity

    # --- ContinuityState string-list fields: strip, drop empty, dedup first-wins ---
    for field_name in _NORMALIZABLE_STRING_LIST_FIELDS:
        items: list[str] = getattr(cont, field_name)
        target = f"continuity.{field_name}"

        # Strip whitespace
        stripped = [s.strip() for s in items]
        if stripped != items:
            applied.append(f"strip:{target}")
            items = stripped
            setattr(cont, field_name, items)

        # Drop empty strings
        filtered = [s for s in items if s]
        if len(filtered) != len(items):
            applied.append(f"drop_empty:{target}")
            items = filtered
            setattr(cont, field_name, items)

        # Deduplicate (first-wins)
        deduped, did_dedup = _dedup_first_wins(items)
        if did_dedup:
            applied.append(f"dedup:{target}")
            setattr(cont, field_name, deduped)

    # --- canonical_sources: strip, drop empty, dedup first-wins ---
    if capsule.canonical_sources:
        cs = capsule.canonical_sources
        stripped_cs = [s.strip() for s in cs]
        if stripped_cs != cs:
            applied.append("strip:canonical_sources")
            cs = stripped_cs
            capsule.canonical_sources = cs
        filtered_cs = [s for s in cs if s]
        if len(filtered_cs) != len(cs):
            applied.append("drop_empty:canonical_sources")
            cs = filtered_cs
            capsule.canonical_sources = cs
        deduped_cs, did_dedup_cs = _dedup_first_wins(cs)
        if did_dedup_cs:
            applied.append("dedup:canonical_sources")
            capsule.canonical_sources = deduped_cs

    # --- stable_preferences: strip tag/content, dedup by tag (last-wins) ---
    if capsule.stable_preferences:
        sp = capsule.stable_preferences
        sp_stripped = False
        for pref in sp:
            new_tag = pref.tag.strip()
            new_content = pref.content.strip()
            if new_tag != pref.tag or new_content != pref.content:
                pref.tag = new_tag
                pref.content = new_content
                sp_stripped = True
        if sp_stripped:
            applied.append("strip:stable_preferences")
        # Dedup by tag, last-wins
        seen_tags: dict[str, int] = {}
        for i, pref in enumerate(sp):
            seen_tags[pref.tag] = i
        if len(seen_tags) < len(sp):
            kept_indices = sorted(seen_tags.values())
            capsule.stable_preferences = [sp[i] for i in kept_indices]
            applied.append("dedup:stable_preferences")

    # --- rationale_entries: strip tag, dedup by tag (last-wins) ---
    if cont.rationale_entries:
        re_list = cont.rationale_entries
        re_stripped = False
        for entry in re_list:
            new_tag = entry.tag.strip()
            if new_tag != entry.tag:
                entry.tag = new_tag
                re_stripped = True
        if re_stripped:
            applied.append("strip:rationale_entries.tag")
        # Dedup by tag, last-wins
        seen_re: dict[str, int] = {}
        for i, entry in enumerate(re_list):
            seen_re[entry.tag] = i
        if len(seen_re) < len(re_list):
            kept = sorted(seen_re.values())
            cont.rationale_entries = [re_list[i] for i in kept]
            applied.append("dedup:rationale_entries")

    # --- negative_decisions: strip decision/rationale, dedup by decision (last-wins) ---
    if cont.negative_decisions:
        nd_list = cont.negative_decisions
        nd_stripped = False
        for nd in nd_list:
            new_d = nd.decision.strip()
            new_r = nd.rationale.strip()
            if new_d != nd.decision or new_r != nd.rationale:
                nd.decision = new_d
                nd.rationale = new_r
                nd_stripped = True
        if nd_stripped:
            applied.append("strip:negative_decisions")
        # Dedup by decision text, last-wins
        seen_nd: dict[str, int] = {}
        for i, nd in enumerate(nd_list):
            seen_nd[nd.decision] = i
        if len(seen_nd) < len(nd_list):
            kept_nd = sorted(seen_nd.values())
            cont.negative_decisions = [nd_list[i] for i in kept_nd]
            applied.append("dedup:negative_decisions")

    return applied
