"""Capsule validation, timestamp checking, and write-path normalization."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from pydantic import ValidationError

from app.continuity.constants import (
    CAPSULE_SIZE_LIMIT_BYTES,
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
from app.timestamps import parse_iso as _parse_iso


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
    for decision in list(capsule.continuity.negative_decisions):
        if len(decision.decision) < 1:
            raise HTTPException(status_code=400, detail="Value too short in continuity.negative_decisions.decision")
        if len(decision.decision) > 160:
            raise HTTPException(status_code=400, detail="Value too long in continuity.negative_decisions.decision")
        if len(decision.rationale) < 1:
            raise HTTPException(status_code=400, detail="Value too short in continuity.negative_decisions.rationale")
        if len(decision.rationale) > 240:
            raise HTTPException(status_code=400, detail="Value too long in continuity.negative_decisions.rationale")
    for entry in list(capsule.continuity.rationale_entries):
        if len(entry.tag) < 1:
            raise HTTPException(status_code=400, detail="Value too short in continuity.rationale_entries[].tag")
        if len(entry.tag) > 80:
            raise HTTPException(status_code=400, detail="Value too long in continuity.rationale_entries[].tag")
        if len(entry.summary) < 1:
            raise HTTPException(status_code=400, detail="Value too short in continuity.rationale_entries[].summary")
        if len(entry.summary) > 240:
            raise HTTPException(status_code=400, detail="Value too long in continuity.rationale_entries[].summary")
        if len(entry.reasoning) < 1:
            raise HTTPException(status_code=400, detail="Value too short in continuity.rationale_entries[].reasoning")
        if len(entry.reasoning) > 400:
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
            _require_utc_timestamp(pref.set_at, "stable_preferences[].set_at")
            if pref.tag in seen_tags:
                raise HTTPException(
                    status_code=400,
                    detail=f"Duplicate stable_preferences tag: {pref.tag}",
                )
            seen_tags.add(pref.tag)
    if capsule.continuity.rationale_entries:
        seen_re_tags: set[str] = set()
        for entry in capsule.continuity.rationale_entries:
            _require_utc_timestamp(entry.set_at, "rationale_entries[].set_at")
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
    _validate_thread_descriptor(capsule)
    payload = capsule.model_dump(mode="json", exclude_none=True)
    canonical = canonical_json(payload)
    if len(canonical.encode("utf-8")) > CAPSULE_SIZE_LIMIT_BYTES:
        raise HTTPException(status_code=400, detail="Continuity capsule exceeds 12 KB serialized UTF-8")
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
