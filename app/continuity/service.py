"""Continuity capsule validation, storage, and retrieval shaping."""

from __future__ import annotations

import hashlib
import gzip
import json
import logging
import math
import re
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from fastapi import HTTPException
from pydantic import ValidationError

from app.auth import AuthContext
from app.timestamps import parse_iso as _parse_iso, format_iso, format_compact, iso_now
from app.lifecycle_warnings import make_error_detail, make_warning
from app.storage import build_cold_gzip_bytes
from app.git_locking import repository_mutation_lock
from app.git_manager import GitManager
from app.git_safety import try_commit_paths, try_unstage_paths
from app.models import (
    ContinuityArchiveRequest,
    ContinuityColdRehydrateRequest,
    ContinuityColdStoreRequest,
    ContinuityCapsuleHealth,
    ContinuityCapsule,
    ContinuityCompareRequest,
    ContinuityDeleteRequest,
    ContinuityListRequest,
    ContinuityReadRequest,
    ContinuityRetentionApplyRequest,
    ContinuityRetentionPlanRequest,
    ContinuityRefreshPlanRequest,
    ContinuityRevalidateRequest,
    ContinuityUpsertRequest,
    ContinuityVerificationState,
    ContinuityVerificationSignal,
    ContextRetrieveRequest,
)
from app.storage import StorageError, canonical_json, safe_path, write_bytes_file, write_text_file

_logger = logging.getLogger(__name__)

CONTINUITY_DIR_REL = "memory/continuity"
CONTINUITY_SUBJECT_RE = re.compile(r"^(task|thread):(.+)$")
CONTINUITY_PATH_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")
CONTINUITY_DEFAULT_STALE: dict[str, int | None] = {
    "persistent": None,
    "durable": 15552000,
    "situational": 2592000,
    "ephemeral": 259200,
}
CONTINUITY_WARNING_STALE_SOFT = "continuity_stale_soft"
CONTINUITY_WARNING_STALE_HARD = "continuity_stale_hard"
CONTINUITY_WARNING_EXPIRED = "continuity_expired"
CONTINUITY_WARNING_TRUNCATED = "continuity_truncated_to_zero"
CONTINUITY_WARNING_TRUNCATED_MULTI = "continuity_capsule_truncated_to_zero"
CONTINUITY_WARNING_DEGRADED = "continuity_degraded"
CONTINUITY_WARNING_CONFLICTED = "continuity_conflicted"
CONTINUITY_WARNING_INVALID = "continuity_invalid_capsule"
CONTINUITY_WARNING_ACTIVE_MISSING = "continuity_active_missing"
CONTINUITY_WARNING_ACTIVE_INVALID = "continuity_active_invalid"
CONTINUITY_WARNING_FALLBACK_WRITE_FAILED = "continuity_fallback_write_failed"
CONTINUITY_WARNING_FALLBACK_USED = "continuity_fallback_used"
CONTINUITY_WARNING_FALLBACK_MISSING = "continuity_fallback_missing"
CONTINUITY_WARNING_STARTUP_SUMMARY_BUILD_FAILED = "startup_summary_build_failed"
CONTINUITY_FALLBACK_SCHEMA_TYPE = "continuity_fallback_snapshot"
CONTINUITY_FALLBACK_SCHEMA_VERSION = "1.0"
CONTINUITY_ARCHIVE_SCHEMA_TYPE = "continuity_archive_envelope"
CONTINUITY_ARCHIVE_SCHEMA_VERSION = "1.0"
CONTINUITY_COLD_STUB_SCHEMA_TYPE = "continuity_cold_stub"
CONTINUITY_COLD_STUB_SCHEMA_VERSION = "1.0"
CONTINUITY_REFRESH_STATE_SCHEMA_VERSION = "1.0"
CONTINUITY_INTERACTION_BOUNDARY_KINDS = {
    "person_switch",
    "thread_switch",
    "task_switch",
    "public_reply",
    "manual_checkpoint",
}
CONTINUITY_SIGNAL_RANK = {
    "self_review": 0,
    "external_observation": 1,
    "peer_confirmation": 2,
    "user_confirmation": 3,
    "system_check": 4,
}
CONTINUITY_COMPARE_TOP_LEVEL_ORDER = [
    "subject_kind",
    "subject_id",
    "schema_version",
    "updated_at",
    "source",
    "continuity",
    "confidence",
    "attention_policy",
    "freshness",
    "canonical_sources",
    "metadata",
]
CONTINUITY_COMPARE_NESTED_ORDERS: dict[str, list[str]] = {
    "source": ["producer", "update_reason", "inputs"],
    "confidence": ["continuity", "relationship_model"],
    "freshness": ["freshness_class", "expires_at", "stale_after_seconds"],
    "attention_policy": ["early_load", "presence_bias_overrides"],
    "continuity": [
        "top_priorities",
        "active_concerns",
        "active_constraints",
        "open_loops",
        "stance_summary",
        "drift_signals",
        "working_hypotheses",
        "long_horizon_commitments",
        "session_trajectory",
        "negative_decisions",
        "trailing_notes",
        "curiosity_queue",
        "relationship_model",
        "retrieval_hints",
    ],
    "relationship_model": ["trust_level", "preferred_style", "sensitivity_notes"],
    "retrieval_hints": ["must_include", "avoid", "load_next"],
}
CONTINUITY_COMPARE_IGNORED_FIELDS = {"verified_at", "verification_kind", "verification_state", "capsule_health"}
CONTINUITY_SIGNAL_STATUS = {
    "self_review": "self_attested",
    "external_observation": "externally_supported",
    "peer_confirmation": "peer_confirmed",
    "user_confirmation": "user_confirmed",
    "system_check": "system_confirmed",
}
CONTINUITY_HEALTH_ORDER = {"healthy": 0, "degraded": 1, "conflicted": 2}
CONTINUITY_REFRESH_STATE_REL = f"{CONTINUITY_DIR_REL}/refresh_state.json"
CONTINUITY_RETENTION_ARCHIVE_DAYS = 90
CONTINUITY_RETENTION_STATE_REL = f"{CONTINUITY_DIR_REL}/retention_state.json"
CONTINUITY_RETENTION_PLAN_SCHEMA_TYPE = "continuity_retention_plan"
CONTINUITY_RETENTION_PLAN_SCHEMA_VERSION = "1.0"
CONTINUITY_STATE_METADATA_FILES = {
    Path(CONTINUITY_REFRESH_STATE_REL).name,
    Path(CONTINUITY_RETENTION_STATE_REL).name,
}
CONTINUITY_COLD_DIR_REL = f"{CONTINUITY_DIR_REL}/cold"
CONTINUITY_COLD_INDEX_DIR_REL = f"{CONTINUITY_COLD_DIR_REL}/index"
CONTINUITY_COLD_STUB_SECTION_ORDER = [
    "top_priorities",
    "active_constraints",
    "active_concerns",
    "open_loops",
    "stance_summary",
    "drift_signals",
    "session_trajectory",
    "trailing_notes",
    "curiosity_queue",
    "negative_decisions",
]
CONTINUITY_COLD_STUB_FRONTMATTER_ORDER = [
    "type",
    "schema_version",
    "artifact_state",
    "subject_kind",
    "subject_id",
    "source_archive_path",
    "cold_storage_path",
    "archived_at",
    "cold_stored_at",
    "verification_kind",
    "verification_status",
    "health_status",
    "freshness_class",
    "phase",
    "update_reason",
]


def continuity_cold_storage_rel_path(source_archive_path: str) -> str:
    """Map an archive envelope path to its cold gzip payload path."""
    archive_rel = _validate_archive_rel_path(source_archive_path)
    return f"{CONTINUITY_COLD_DIR_REL}/{Path(archive_rel).name}.gz"


def continuity_cold_stub_rel_path(source_archive_path: str) -> str:
    """Map an archive envelope path to its hot cold-stub path."""
    archive_rel = _validate_archive_rel_path(source_archive_path)
    return f"{CONTINUITY_COLD_INDEX_DIR_REL}/{Path(archive_rel).stem}.md"


def continuity_archive_rel_path_from_cold_artifact(cold_artifact_path: str) -> str:
    """Derive the archive envelope path from a cold payload or stub path."""
    rel = str(cold_artifact_path or "").strip()
    if rel.startswith(f"{CONTINUITY_COLD_INDEX_DIR_REL}/") and rel.endswith(".md"):
        basename = Path(rel).stem + ".json"
        return f"{CONTINUITY_DIR_REL}/archive/{basename}"
    if rel.startswith(f"{CONTINUITY_COLD_DIR_REL}/") and rel.endswith(".json.gz"):
        basename = Path(rel).name[:-3]
        return f"{CONTINUITY_DIR_REL}/archive/{basename}"
    raise HTTPException(status_code=400, detail="Invalid continuity cold artifact path")


def _require_utc_timestamp(value: str, field_name: str) -> datetime:
    """Require a valid UTC timestamp or raise an HTTP 400 error."""
    dt = _parse_iso(value)
    if dt is None:
        raise HTTPException(status_code=400, detail=f"Invalid UTC timestamp for {field_name}")
    if not (value.endswith("Z") or value.endswith("+00:00")):
        raise HTTPException(status_code=400, detail=f"Timestamp must be UTC for {field_name}")
    return dt


def _normalize_subject_id(subject_id: str) -> str:
    """Normalize a subject id into a filesystem-safe continuity key."""
    raw = subject_id.strip().lower()
    normalized = re.sub(r"[^a-z0-9._-]+", "-", raw)
    normalized = normalized.strip("-")
    if not normalized:
        raise HTTPException(status_code=400, detail="Normalized subject_id is empty")
    normalized = normalized[:120].strip("-")
    if not normalized:
        raise HTTPException(status_code=400, detail="Normalized subject_id is empty")
    return normalized


def continuity_rel_path(subject_kind: str, subject_id: str) -> str:
    """Return the repository-relative path for a continuity capsule."""
    normalized = _normalize_subject_id(subject_id)
    return f"{CONTINUITY_DIR_REL}/{subject_kind}-{normalized}.json"


def continuity_fallback_rel_path(subject_kind: str, subject_id: str) -> str:
    """Return the repository-relative path for a continuity fallback snapshot."""
    normalized = _normalize_subject_id(subject_id)
    return f"{CONTINUITY_DIR_REL}/fallback/{subject_kind}-{normalized}.json"


def _validate_archive_rel_path(rel: str) -> str:
    """Require a repo-relative continuity archive-envelope path."""
    normalized = str(rel or "").strip()
    if not normalized.startswith(f"{CONTINUITY_DIR_REL}/archive/") or not normalized.endswith(".json"):
        raise HTTPException(status_code=400, detail="source_archive_path must be under memory/continuity/archive/")
    return normalized


def _continuity_subject_lock_id(subject_kind: str, subject_id: str) -> str:
    """Return a safe per-subject lock id shared by all continuity mutation endpoints."""
    normalized = _normalize_subject_id(subject_id)
    digest = hashlib.sha256(f"{subject_kind}:{normalized}".encode("utf-8")).hexdigest()
    return f"continuity_{digest}"


@contextmanager
def _continuity_subject_lock(*, repo_root: Path, subject_kind: str, subject_id: str):
    """Acquire the per-subject mutation lock, translating domain exceptions.

    Converts ``ArtifactLockTimeout`` → HTTP 409 and
    ``ArtifactLockInfrastructureError`` → HTTP 503 via the shared
    ``make_lock_error`` helper so that continuity lock errors use the
    same status codes as all other lifecycle modules.
    """
    from app.coordination.locking import (
        ArtifactLockInfrastructureError,
        ArtifactLockTimeout,
        artifact_lock,
    )
    from app.lifecycle_warnings import make_lock_error

    try:
        with artifact_lock(
            _continuity_subject_lock_id(subject_kind, subject_id),
            lock_dir=repo_root / ".locks",
        ):
            yield
    except ArtifactLockTimeout as exc:
        raise make_lock_error("continuity", None, exc, is_timeout=True) from exc
    except ArtifactLockInfrastructureError as exc:
        raise make_lock_error("continuity", None, exc, is_timeout=False) from exc


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
    payload = capsule.model_dump(mode="json", exclude_none=True)
    canonical = canonical_json(payload)
    if len(canonical.encode("utf-8")) > 12 * 1024:
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


def _strongest_signal_kind(signals: list[ContinuityVerificationSignal]) -> str:
    """Return the strongest verification signal kind preserving request order on ties."""
    strongest = signals[0].kind
    for signal in signals[1:]:
        if CONTINUITY_SIGNAL_RANK[signal.kind] > CONTINUITY_SIGNAL_RANK[strongest]:
            strongest = signal.kind
    return strongest


def _normalize_compare_payload(repo_root: Path, capsule: ContinuityCapsule) -> dict[str, Any]:
    """Validate and normalize a capsule payload for compare and revalidate semantics."""
    _validate_capsule(repo_root, capsule)
    _validate_verification_state_and_health(capsule)
    return capsule.model_dump(mode="json", exclude_none=True)


def _validate_candidate_selector_match(subject_kind: str, subject_id: str, candidate_capsule: ContinuityCapsule) -> None:
    """Require a candidate capsule to match the exact request selector after normalization."""
    if candidate_capsule.subject_kind != subject_kind:
        raise HTTPException(status_code=400, detail="Candidate capsule subject does not match request subject")
    if _normalize_subject_id(candidate_capsule.subject_id) != _normalize_subject_id(subject_id):
        raise HTTPException(status_code=400, detail="Candidate capsule subject does not match request subject")


def _compare_values(left: Any, right: Any, *, path: str = "", order_name: str | None = None) -> list[str]:
    """Compare two normalized capsule values and return shallowest changed paths."""
    if left == right:
        return []
    if left is None and right is None:
        return []
    if isinstance(left, list) and isinstance(right, list):
        return [path] if left != right else []
    if isinstance(left, dict) and isinstance(right, dict):
        if order_name == "metadata":
            keys = sorted(set(left) | set(right))
        else:
            explicit = CONTINUITY_COMPARE_NESTED_ORDERS.get(order_name or "", [])
            keys = list(explicit)
            for key in sorted(set(left) | set(right)):
                if key not in keys and key not in CONTINUITY_COMPARE_IGNORED_FIELDS:
                    keys.append(key)
        changes: list[str] = []
        for key in keys:
            if key in CONTINUITY_COMPARE_IGNORED_FIELDS:
                continue
            l_has = key in left
            r_has = key in right
            l_val = left.get(key) if l_has else None
            r_val = right.get(key) if r_has else None
            if l_val is None and r_val is None and (l_has or r_has):
                continue
            child_path = f"{path}.{key}" if path else key
            next_order = key if key in CONTINUITY_COMPARE_NESTED_ORDERS else ("metadata" if key == "metadata" else None)
            child_changes = _compare_values(l_val, r_val, path=child_path, order_name=next_order)
            if child_changes:
                changes.extend(child_changes)
        return changes
    return [path]


def _compare_capsules(active: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    """Compare two normalized capsules using the canonical traversal order."""
    changes: list[str] = []
    for key in CONTINUITY_COMPARE_TOP_LEVEL_ORDER:
        if key in CONTINUITY_COMPARE_IGNORED_FIELDS:
            continue
        active_has = key in active
        candidate_has = key in candidate
        active_value = active.get(key) if active_has else None
        candidate_value = candidate.get(key) if candidate_has else None
        if active_value is None and candidate_value is None and (active_has or candidate_has):
            continue
        order_name = key if key in CONTINUITY_COMPARE_NESTED_ORDERS else ("metadata" if key == "metadata" else None)
        changes.extend(_compare_values(active_value, candidate_value, path=key, order_name=order_name))
    return changes


def _signals_to_evidence_refs(signals: list[ContinuityVerificationSignal]) -> list[str]:
    """Derive bounded evidence refs from ordered verification signals."""
    return [signal.source_ref for signal in signals[:4]]


def _final_capsule_payload(repo_root: Path, capsule: ContinuityCapsule) -> tuple[dict[str, Any], str]:
    """Validate a final assembled capsule including verification-derived fields and return canonical JSON."""
    payload, canonical = _validate_capsule(repo_root, capsule)
    _validate_verification_state_and_health(capsule)
    return payload, canonical


def _persist_active_capsule(
    *,
    repo_root: Path,
    gm: GitManager,
    path: Path,
    canonical: str,
    commit_message: str,
) -> None:
    """Persist an active capsule safely, restoring the prior durable file on commit failure."""
    old_bytes = path.read_bytes() if path.exists() else None
    with repository_mutation_lock(repo_root):
        write_text_file(path, canonical)
        try:
            gm.commit_file(path, commit_message)
        except Exception as exc:
            _logger.error("Continuity capsule persist failed: %s", exc, exc_info=True)
            restore_error: Exception | None = None
            try_unstage_paths(gm, [path])
            try:
                if old_bytes is None:
                    path.unlink(missing_ok=True)
                else:
                    write_bytes_file(path, old_bytes)
            except Exception as restore_exc:
                restore_error = restore_exc
                _logger.exception("Rollback also failed after continuity persist error")
            error_detail = f"Failed to persist continuity capsule: {exc}"
            if restore_error is not None:
                error_detail = f"{error_detail}; rollback failed: {restore_error}"
            raise HTTPException(
                status_code=500,
                detail=make_error_detail(
                    operation="continuity_persist",
                    error_code="continuity_persist_commit_failed" if restore_error is None else "continuity_persist_rollback_failed",
                    error_detail=error_detail,
                ),
            ) from exc


def _fallback_snapshot_payload(*, capsule: dict[str, Any], active_rel: str, captured_at: str) -> dict[str, Any]:
    """Build one fallback snapshot envelope from a validated active capsule."""
    health_status, _health_reasons = _capsule_health_summary(capsule)
    return {
        "schema_type": CONTINUITY_FALLBACK_SCHEMA_TYPE,
        "schema_version": CONTINUITY_FALLBACK_SCHEMA_VERSION,
        "captured_at": captured_at,
        "source_path": active_rel,
        "verification_status": _verification_status(capsule),
        "health_status": health_status,
        "capsule": capsule,
    }


def _restore_failed_fallback_snapshot(path: Path, old_bytes: bytes | None, exc: Exception) -> str:
    """Restore the prior fallback snapshot bytes after a failed commit and return audit detail."""
    restore_error: Exception | None = None
    try:
        if old_bytes is None:
            path.unlink(missing_ok=True)
        else:
            write_bytes_file(path, old_bytes)
    except Exception as restore_exc:
        restore_error = restore_exc
    detail = f"Failed to persist continuity fallback snapshot: {exc}"
    if restore_error is not None:
        detail = f"{detail}; rollback failed: {restore_error}"
    return detail


def _restore_failed_refresh_state(path: Path, old_bytes: bytes | None, exc: Exception) -> HTTPException:
    """Return a refresh-state persistence error after restoring the prior durable bytes."""
    restore_error: Exception | None = None
    try:
        if old_bytes is None:
            path.unlink(missing_ok=True)
        else:
            write_bytes_file(path, old_bytes)
    except Exception as restore_exc:
        restore_error = restore_exc
    detail = f"Failed to persist continuity refresh state: {exc}"
    if restore_error is not None:
        detail = f"{detail}; rollback failed: {restore_error}"
    return HTTPException(status_code=500, detail=detail)


def _restore_failed_retention_state(path: Path, old_bytes: bytes | None, exc: Exception) -> HTTPException:
    """Return a retention-state persistence error after restoring the prior durable bytes."""
    restore_error: Exception | None = None
    try:
        if old_bytes is None:
            path.unlink(missing_ok=True)
        else:
            write_bytes_file(path, old_bytes)
    except Exception as restore_exc:
        restore_error = restore_exc
    detail = f"Failed to persist continuity retention state: {exc}; prior durable plan was restored"
    if restore_error is not None:
        detail = f"Failed to persist continuity retention state: {exc}; rollback failed: {restore_error}"
    return HTTPException(status_code=500, detail=detail)


def _is_archive_stale(*, archived_at: datetime | None, now: datetime, retention_archive_days: int) -> bool:
    """Return whether an archive timestamp is older than the configured stale threshold."""
    if archived_at is None:
        return False
    return (now - archived_at).total_seconds() > (retention_archive_days * 86400)


def _retention_age_days(*, archived_at: datetime, generated_at: datetime) -> int:
    """Return the floor age in UTC days for one archived artifact."""
    return max(0, math.floor((generated_at - archived_at).total_seconds() / 86400))


def _retention_warning_sort_key(warning: str) -> tuple[int, str]:
    """Return the deterministic sort key for retention warnings."""
    prefix_order = {
        "continuity_retention_partial_cold_conflict:": 0,
        "continuity_retention_skipped_invalid_archive:": 1,
        "duplicate_source_archive_path:": 3,
    }
    for prefix, rank in prefix_order.items():
        if warning.startswith(prefix):
            return rank, warning[len(prefix):]
    return 99, warning


def _retention_cold_state(repo_root: Path, source_archive_path: str) -> tuple[str, str, str]:
    """Return the cold-artifact state plus expected payload and stub paths for one archive."""
    cold_storage_path = continuity_cold_storage_rel_path(source_archive_path)
    cold_stub_path = continuity_cold_stub_rel_path(source_archive_path)
    cold_payload_file = safe_path(repo_root, cold_storage_path)
    cold_stub_file = safe_path(repo_root, cold_stub_path)
    payload_exists = cold_payload_file.exists()
    stub_exists = cold_stub_file.exists()
    if payload_exists and not cold_payload_file.is_file():
        return "conflict", cold_storage_path, cold_stub_path
    if stub_exists and not cold_stub_file.is_file():
        return "conflict", cold_storage_path, cold_stub_path
    if payload_exists != stub_exists:
        return "partial", cold_storage_path, cold_stub_path
    if not payload_exists:
        return "none", cold_storage_path, cold_stub_path
    try:
        frontmatter = _load_cold_stub(repo_root, cold_stub_path)
    except HTTPException:
        return "conflict", cold_storage_path, cold_stub_path
    if frontmatter.get("source_archive_path") != source_archive_path:
        return "conflict", cold_storage_path, cold_stub_path
    return "full", cold_storage_path, cold_stub_path


def _retention_candidate_sort_key(candidate: dict[str, Any]) -> tuple[str, str, str, str]:
    """Return the deterministic candidate ordering tuple."""
    return (
        str(candidate["archived_at"]),
        str(candidate["subject_kind"]),
        _normalize_subject_id(str(candidate["subject_id"])),
        str(candidate["source_archive_path"]),
    )


def _retention_candidate_from_envelope(
    *,
    envelope: dict[str, Any],
    source_archive_path: str,
    generated_at: datetime,
) -> dict[str, Any]:
    """Build one retention-plan candidate row from a validated archive envelope."""
    capsule = envelope["capsule"]
    archived_at = _require_utc_timestamp(str(envelope.get("archived_at") or ""), "archived_at")
    return {
        "subject_kind": capsule["subject_kind"],
        "subject_id": capsule["subject_id"],
        "source_archive_path": source_archive_path,
        "artifact_state": "archived",
        "retention_class": "archive_stale",
        "policy_action": "cold_store",
        "archived_at": format_iso(archived_at),
        "age_days": _retention_age_days(archived_at=archived_at, generated_at=generated_at),
        "cold_storage_path": continuity_cold_storage_rel_path(source_archive_path),
        "cold_stub_path": continuity_cold_stub_rel_path(source_archive_path),
        "reason_codes": ["archive_stale"],
    }


def _retention_plan_payload(
    *,
    generated_at: datetime,
    req: ContinuityRetentionPlanRequest,
    candidates: list[dict[str, Any]],
    warnings: list[str],
    total_candidates: int,
) -> dict[str, Any]:
    """Build the persisted operator-visible continuity retention plan payload."""
    return {
        "schema_type": CONTINUITY_RETENTION_PLAN_SCHEMA_TYPE,
        "schema_version": CONTINUITY_RETENTION_PLAN_SCHEMA_VERSION,
        "generated_at": format_iso(generated_at),
        "path": CONTINUITY_RETENTION_STATE_REL,
        "filters": {"subject_kind": req.subject_kind, "limit": req.limit},
        "count": len(candidates),
        "total_candidates": total_candidates,
        "has_more": total_candidates > len(candidates),
        "warnings": warnings,
        "candidates": candidates,
    }


def _load_fallback_envelope_payload(repo_root: Path, rel: str) -> dict[str, Any]:
    """Load and validate a fallback snapshot envelope payload."""
    path = safe_path(repo_root, rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Continuity fallback snapshot not found")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity fallback snapshot JSON: {e}") from e
    if payload.get("schema_type") != CONTINUITY_FALLBACK_SCHEMA_TYPE:
        raise HTTPException(status_code=400, detail="Invalid continuity fallback snapshot schema_type")
    if payload.get("schema_version") != CONTINUITY_FALLBACK_SCHEMA_VERSION:
        raise HTTPException(status_code=400, detail="Invalid continuity fallback snapshot schema_version")
    nested = payload.get("capsule")
    if not isinstance(nested, dict):
        raise HTTPException(status_code=400, detail="Invalid continuity fallback snapshot capsule")
    try:
        payload["capsule"] = ContinuityCapsule.model_validate(nested).model_dump(mode="json", exclude_none=True)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity fallback snapshot capsule: {e}") from e
    return payload


def _persist_fallback_snapshot(
    *,
    repo_root: Path,
    gm: GitManager,
    subject_kind: str,
    subject_id: str,
    capsule: dict[str, Any],
 ) -> tuple[str, str, str | None]:
    """Persist the fallback snapshot and restore prior fallback bytes if this write fails."""
    fallback_rel = continuity_fallback_rel_path(subject_kind, subject_id)
    path: Path | None = None
    old_bytes: bytes | None = None
    try:
        path = safe_path(repo_root, fallback_rel)
        old_bytes = path.read_bytes() if path.exists() else None
        payload = _fallback_snapshot_payload(
            capsule=capsule,
            active_rel=continuity_rel_path(subject_kind, subject_id),
            captured_at=str(capsule.get("updated_at") or capsule.get("verified_at") or format_iso(iso_now())),
        )
        canonical = canonical_json(payload)
        new_bytes = canonical.encode("utf-8")
        if old_bytes == new_bytes:
            return fallback_rel, "unchanged", None
        write_text_file(path, canonical)
        # Fallback snapshots are defense-in-depth; use a shorter git
        # lock timeout (15 s) to reduce thread-pool exhaustion risk when
        # the primary capsule write already consumed the full 60 s budget.
        with repository_mutation_lock(repo_root, timeout=15.0):
            committed = gm.commit_file(path, f"continuity: update fallback {subject_kind} {subject_id}")
            if not committed:
                raise RuntimeError("git commit produced no changes")
    except Exception as exc:
        if path is not None:
            try_unstage_paths(gm, [path])
            return fallback_rel, "failed", _restore_failed_fallback_snapshot(path, old_bytes, exc)
        return fallback_rel, "failed", f"Failed to persist continuity fallback snapshot: {exc}"
    return fallback_rel, "committed", None


def _delete_commit_message(subject_kind: str, subject_id: str, reason: str) -> str:
    """Build a bounded delete commit subject while keeping the full reason in audit detail."""
    message = f"continuity: delete {subject_kind} {subject_id} - {reason}"
    if len(message) <= 120:
        return message
    return message[:117] + "..."


def _load_fallback_snapshot(repo_root: Path, rel: str, *, expected_subject: tuple[str, str]) -> dict[str, Any]:
    """Load and validate one fallback snapshot envelope, returning the nested capsule."""
    payload = _load_fallback_envelope_payload(repo_root, rel)
    capsule = payload["capsule"]
    expected_kind, expected_id = expected_subject
    capsule_kind = str(capsule.get("subject_kind") or "")
    capsule_subject_id = str(capsule.get("subject_id") or "")
    if capsule_kind != expected_kind or _normalize_subject_id(capsule_subject_id) != _normalize_subject_id(expected_id):
        raise HTTPException(status_code=400, detail="Continuity fallback snapshot subject does not match requested subject")
    return capsule


def _load_archive_envelope(repo_root: Path, rel: str) -> dict[str, Any]:
    """Load and validate an archive envelope."""
    path = safe_path(repo_root, rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Continuity archive envelope not found")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="Continuity archive envelope not found") from e
    except UnicodeDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity archive envelope text: {e}") from e
    except OSError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity archive envelope text: {e}") from e
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity archive envelope JSON: {e}") from e
    if payload.get("schema_type") != CONTINUITY_ARCHIVE_SCHEMA_TYPE:
        raise HTTPException(status_code=400, detail="Invalid continuity archive envelope schema_type")
    if payload.get("schema_version") != CONTINUITY_ARCHIVE_SCHEMA_VERSION:
        raise HTTPException(status_code=400, detail="Invalid continuity archive envelope schema_version")
    capsule = payload.get("capsule")
    if not isinstance(capsule, dict):
        raise HTTPException(status_code=400, detail="Invalid continuity archive envelope capsule")
    try:
        payload["capsule"] = ContinuityCapsule.model_validate(capsule).model_dump(mode="json", exclude_none=True)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity archive envelope capsule: {e}") from e
    return payload


def _normalize_stub_scalar(value: Any) -> str:
    """Normalize a stub scalar to one trimmed line with newlines replaced."""
    return str(value or "").replace("\r\n", "\n").replace("\r", "\n").replace("\n", " ").strip()


def _truncate_stub_text(value: Any, limit: int) -> str:
    """Apply the cold-stub scalar normalization and code-point truncation."""
    return _normalize_stub_scalar(value)[:limit]


def _render_cold_stub_list(items: Any, *, count: int, limit: int) -> list[str]:
    """Project a list-valued continuity field into bounded cold-stub bullets."""
    if not isinstance(items, list):
        return []
    return [_truncate_stub_text(item, limit) for item in items[:count]]


def _render_cold_negative_decisions(items: Any) -> list[str]:
    """Project negative decisions into deterministic stub bullets."""
    if not isinstance(items, list):
        return []
    lines: list[str] = []
    for item in items[:2]:
        if not isinstance(item, dict):
            continue
        decision = _truncate_stub_text(item.get("decision"), 160)
        rationale = _truncate_stub_text(item.get("rationale"), 240)
        lines.append(f"decision: {decision} | rationale: {rationale}")
    return lines


def _build_cold_stub_text(*, envelope: dict[str, Any], source_archive_path: str, cold_storage_path: str, cold_stored_at: str, now: datetime) -> str:
    """Build the deterministic searchable stub for one cold-stored archive envelope."""
    capsule = envelope["capsule"]
    continuity = capsule.get("continuity") if isinstance(capsule.get("continuity"), dict) else {}
    freshness = capsule.get("freshness") if isinstance(capsule.get("freshness"), dict) else {}
    verification_status = _verification_status(capsule)
    health_status, _ = _capsule_health_summary(capsule)
    phase, _ = _continuity_phase(capsule, now)
    frontmatter = {
        "type": CONTINUITY_COLD_STUB_SCHEMA_TYPE,
        "schema_version": '"1.0"',
        "artifact_state": "cold",
        "subject_kind": _normalize_stub_scalar(capsule.get("subject_kind")),
        "subject_id": _normalize_stub_scalar(capsule.get("subject_id")),
        "source_archive_path": _normalize_stub_scalar(source_archive_path),
        "cold_storage_path": _normalize_stub_scalar(cold_storage_path),
        "archived_at": _normalize_stub_scalar(envelope.get("archived_at")),
        "cold_stored_at": _normalize_stub_scalar(cold_stored_at),
        "verification_kind": _normalize_stub_scalar(capsule.get("verification_kind")),
        "verification_status": _normalize_stub_scalar(verification_status),
        "health_status": _normalize_stub_scalar(health_status),
        "freshness_class": _normalize_stub_scalar(freshness.get("freshness_class")),
        "phase": _normalize_stub_scalar(phase),
        "update_reason": _normalize_stub_scalar((capsule.get("source") or {}).get("update_reason") if isinstance(capsule.get("source"), dict) else ""),
    }
    sections = {
        "top_priorities": _render_cold_stub_list(continuity.get("top_priorities"), count=3, limit=160),
        "active_constraints": _render_cold_stub_list(continuity.get("active_constraints"), count=3, limit=160),
        "active_concerns": _render_cold_stub_list(continuity.get("active_concerns"), count=3, limit=160),
        "open_loops": _render_cold_stub_list(continuity.get("open_loops"), count=3, limit=160),
        "stance_summary": _truncate_stub_text(continuity.get("stance_summary"), 240),
        "drift_signals": _render_cold_stub_list(continuity.get("drift_signals"), count=5, limit=160),
        "session_trajectory": _render_cold_stub_list(continuity.get("session_trajectory"), count=3, limit=80),
        "trailing_notes": _render_cold_stub_list(continuity.get("trailing_notes"), count=3, limit=160),
        "curiosity_queue": _render_cold_stub_list(continuity.get("curiosity_queue"), count=3, limit=120),
        "negative_decisions": _render_cold_negative_decisions(continuity.get("negative_decisions")),
    }
    lines = ["---"]
    for key in CONTINUITY_COLD_STUB_FRONTMATTER_ORDER:
        lines.append(f"{key}: {frontmatter[key]}")
    lines.append("---")
    for section in CONTINUITY_COLD_STUB_SECTION_ORDER:
        lines.append(f"## {section}")
        if section == "stance_summary":
            lines.append(str(sections[section]))
            continue
        for item in sections[section]:
            lines.append(f"- {item}")
        if not sections[section]:
            lines.append("")
    return "\n".join(lines).rstrip("\n") + "\n"


def _parse_cold_stub_text(text: str) -> tuple[list[tuple[str, str]], str]:
    """Parse a cold-stub frontmatter block and return ordered fields plus the body."""
    if not text.startswith("---\n"):
        raise HTTPException(status_code=400, detail="Invalid continuity cold stub frontmatter")
    parts = text.split("\n---\n", 1)
    if len(parts) != 2:
        raise HTTPException(status_code=400, detail="Invalid continuity cold stub frontmatter")
    frontmatter_raw = parts[0][4:]
    body = parts[1]
    values: list[tuple[str, str]] = []
    for line in frontmatter_raw.splitlines():
        if ":" not in line:
            raise HTTPException(status_code=400, detail="Invalid continuity cold stub frontmatter")
        key, value = line.split(":", 1)
        values.append((key.strip(), value.strip()))
    return values, body


def _archive_rel_path_from_envelope(payload: dict[str, Any]) -> str:
    """Derive the canonical archive-envelope path from a validated envelope payload."""
    capsule = payload.get("capsule")
    if not isinstance(capsule, dict):
        raise HTTPException(status_code=400, detail="Invalid continuity archive envelope capsule")
    subject_kind = str(capsule.get("subject_kind") or "")
    subject_id = str(capsule.get("subject_id") or "")
    archived_at = _require_utc_timestamp(str(payload.get("archived_at") or ""), "archived_at")
    timestamp = format_compact(archived_at)
    return f"{CONTINUITY_DIR_REL}/archive/{subject_kind}-{_normalize_subject_id(subject_id)}-{timestamp}.json"


def _load_cold_stub(repo_root: Path, rel: str) -> dict[str, str]:
    """Load and validate one continuity cold stub against shared path helpers."""
    if not rel.startswith(f"{CONTINUITY_COLD_INDEX_DIR_REL}/") or not rel.endswith(".md"):
        raise HTTPException(status_code=400, detail="cold_stub_path must be under memory/continuity/cold/index/")
    path = safe_path(repo_root, rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Continuity cold stub not found")
    try:
        ordered_fields, _body = _parse_cold_stub_text(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid continuity cold stub text: {exc}") from exc
    field_order = [key for key, _value in ordered_fields]
    if field_order != CONTINUITY_COLD_STUB_FRONTMATTER_ORDER:
        raise HTTPException(status_code=400, detail="Invalid continuity cold stub frontmatter order")
    frontmatter = dict(ordered_fields)
    if len(frontmatter) != len(CONTINUITY_COLD_STUB_FRONTMATTER_ORDER):
        raise HTTPException(status_code=400, detail="Invalid continuity cold stub frontmatter fields")
    if frontmatter.get("type") != CONTINUITY_COLD_STUB_SCHEMA_TYPE:
        raise HTTPException(status_code=400, detail="Invalid continuity cold stub type")
    if frontmatter.get("schema_version") != '"1.0"':
        raise HTTPException(status_code=400, detail="Invalid continuity cold stub schema_version")
    source_archive_path = frontmatter["source_archive_path"]
    expected_stub_path = continuity_cold_stub_rel_path(source_archive_path)
    expected_payload_path = continuity_cold_storage_rel_path(source_archive_path)
    if rel != expected_stub_path:
        raise HTTPException(status_code=400, detail="Continuity cold stub path does not match source archive identity")
    if frontmatter.get("cold_storage_path") != expected_payload_path:
        raise HTTPException(status_code=400, detail="Continuity cold stub payload path does not match source archive identity")
    if continuity_archive_rel_path_from_cold_artifact(rel) != source_archive_path:
        raise HTTPException(status_code=400, detail="Continuity cold stub archive identity does not match path")
    return frontmatter


def _resolve_selector(req: ContextRetrieveRequest) -> tuple[str, str, str] | None:
    """Resolve an explicit or inferred continuity selector from a request."""
    if bool(req.subject_kind) != bool(req.subject_id):
        raise HTTPException(status_code=400, detail="subject_kind and subject_id must be provided together")
    if req.subject_kind and req.subject_id:
        return req.subject_kind, req.subject_id, "explicit"
    m = CONTINUITY_SUBJECT_RE.match(req.task.strip())
    if not m:
        return None
    kind, value = m.group(1), m.group(2).strip()
    if kind not in {"task", "thread"} or not value:
        return None
    return kind, value, "inferred"


def _warning_mode_is_multi(req: ContextRetrieveRequest) -> bool:
    """Return whether retrieval should use selector-qualified multi-capsule warning strings."""
    return "continuity_selectors" in req.model_fields_set and bool(req.continuity_selectors)


def _selector_key(subject_kind: str, subject_id: str) -> tuple[str, str]:
    """Return the normalized selector identity key used for deduplication."""
    return subject_kind, _normalize_subject_id(subject_id)


def _format_selector(subject_kind: str, subject_id: str) -> str:
    """Format a selector string using the original subject identifier."""
    return f"{subject_kind}:{subject_id}"


def _qualify_warning(warning: str, subject_kind: str, subject_id: str, *, multi_mode: bool) -> str:
    """Return a warning string in either single-capsule or selector-qualified retrieval format."""
    if warning == CONTINUITY_WARNING_TRUNCATED_MULTI and not multi_mode:
        return CONTINUITY_WARNING_TRUNCATED
    if not multi_mode:
        return warning
    return f"{warning}:{subject_kind}:{subject_id}"


def _restore_failed_archive(active_path: Path, archive_path: Path, active_bytes: bytes) -> None:
    """Restore the active capsule and discard the archive envelope after a failed archive commit."""
    errors: list[str] = []
    first_exc: Exception | None = None
    try:
        write_bytes_file(active_path, active_bytes)
    except Exception as exc:
        first_exc = exc
        errors.append(f"restore active: {exc}")
    try:
        archive_path.unlink(missing_ok=True)
    except Exception as exc:
        if first_exc is None:
            first_exc = exc
        errors.append(f"remove archive: {exc}")
    if errors:
        raise RuntimeError(
            f"Failed to restore archived continuity capsule: {'; '.join(errors)}"
        ) from first_exc


def _effective_selectors(req: ContextRetrieveRequest) -> tuple[list[dict[str, str]], list[str], list[str]]:
    """Build selected selectors, requested selectors, and selector-limit omissions for retrieval."""
    selectors: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    if req.subject_kind and req.subject_id:
        key = _selector_key(req.subject_kind, req.subject_id)
        selectors.append(
            {
                "subject_kind": req.subject_kind,
                "subject_id": req.subject_id,
                "resolution": "explicit",
            }
        )
        seen.add(key)

    for selector in req.continuity_selectors:
        key = _selector_key(selector.subject_kind, selector.subject_id)
        if key in seen:
            continue
        selectors.append(
            {
                "subject_kind": selector.subject_kind,
                "subject_id": selector.subject_id,
                "resolution": "explicit",
            }
        )
        seen.add(key)

    omitted: list[str] = []
    if selectors:
        requested = [_format_selector(item["subject_kind"], item["subject_id"]) for item in selectors]
        if len(selectors) > req.continuity_max_capsules:
            omitted = [_format_selector(item["subject_kind"], item["subject_id"]) for item in selectors[req.continuity_max_capsules :]]
            selectors = selectors[: req.continuity_max_capsules]
        return selectors, requested, omitted

    inferred = _resolve_selector(req)
    if inferred is None:
        return [], [], omitted
    kind, subject_id, resolution = inferred
    requested = [_format_selector(kind, subject_id)]
    return [{"subject_kind": kind, "subject_id": subject_id, "resolution": resolution}], requested, omitted


def _load_capsule(repo_root: Path, rel: str, *, expected_subject: tuple[str, str] | None = None) -> dict[str, Any]:
    """Load one capsule from disk and enforce optional subject matching."""
    path = safe_path(repo_root, rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Continuity capsule not found")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        capsule = ContinuityCapsule.model_validate(payload).model_dump(mode="json", exclude_none=True)
        if expected_subject is not None:
            expected_kind, expected_id = expected_subject
            capsule_kind = str(capsule.get("subject_kind") or "")
            capsule_subject_id = str(capsule.get("subject_id") or "")
            if capsule_kind != expected_kind or _normalize_subject_id(capsule_subject_id) != _normalize_subject_id(expected_id):
                raise HTTPException(status_code=400, detail="Continuity capsule subject does not match requested subject")
        return capsule
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity capsule JSON: {e}") from e
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity capsule: {e}") from e


def _effective_stale_seconds(capsule: dict[str, Any]) -> int | None:
    """Compute the effective stale threshold for a capsule."""
    freshness = capsule.get("freshness") if isinstance(capsule.get("freshness"), dict) else {}
    explicit = freshness.get("stale_after_seconds")
    if explicit is not None:
        return int(explicit)
    freshness_class = str(freshness.get("freshness_class") or "situational")
    return CONTINUITY_DEFAULT_STALE.get(freshness_class)


def _verification_status(capsule: dict[str, Any]) -> str:
    """Return the persisted or implicit verification status for one capsule."""
    verification_state = capsule.get("verification_state")
    if isinstance(verification_state, dict):
        status = verification_state.get("status")
        if isinstance(status, str) and status:
            return status
    return "unverified"


def _capsule_health_summary(capsule: dict[str, Any]) -> tuple[str, list[str]]:
    """Return the persisted or implicit capsule health summary."""
    capsule_health = capsule.get("capsule_health")
    if isinstance(capsule_health, dict):
        status = capsule_health.get("status")
        reasons = capsule_health.get("reasons")
        if isinstance(status, str) and status:
            return status, list(reasons or [])
        return "degraded", ["invalid capsule_health payload"]
    return "healthy", []


def _audit_recent_selectors(repo_root: Path, now: datetime) -> set[tuple[str, str]]:
    """Return selectors recently used by continuity reads or retrievals."""
    path = repo_root / "logs" / "api_audit.jsonl"
    if not path.exists() or not path.is_file():
        return set()
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            rows = list(deque(handle, maxlen=10000))
    except Exception:  # noqa: BLE001 — mission-critical degradation
        _logger.warning("Failed to read audit log %s for selector scan", path, exc_info=True)
        return set()
    if any("\ufffd" in line for line in rows):
        _logger.warning("file %s contains invalid UTF-8 bytes (replaced with U+FFFD)", path)
    cutoff = now.timestamp() - (7 * 86400)
    recent: set[tuple[str, str]] = set()
    for line in rows:
        try:
            row = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        ts = _parse_iso(str(row.get("ts") or ""))
        if ts is None or ts.timestamp() < cutoff:
            continue
        detail = row.get("detail")
        if not isinstance(detail, dict):
            continue
        if row.get("event") == "continuity_read":
            kind = detail.get("subject_kind")
            subject_id = detail.get("subject_id")
            if isinstance(kind, str) and isinstance(subject_id, str):
                try:
                    recent.add((kind, _normalize_subject_id(subject_id)))
                except HTTPException:
                    continue
            continue
        if row.get("event") != "context_retrieve":
            continue
        selectors = detail.get("continuity_selectors")
        if not isinstance(selectors, list):
            continue
        for item in selectors:
            if not isinstance(item, dict):
                continue
            kind = item.get("subject_kind")
            subject_id = item.get("subject_id")
            if isinstance(kind, str) and isinstance(subject_id, str):
                try:
                    recent.add((kind, _normalize_subject_id(subject_id)))
                except HTTPException:
                    continue
    return recent


def _refresh_reason_codes(
    *,
    capsule: dict[str, Any],
    fallback_only: bool,
    recently_used: bool,
    now: datetime,
) -> list[str]:
    """Derive deterministic refresh reason codes for one capsule payload."""
    codes: list[str] = []
    health_status, _health_reasons = _capsule_health_summary(capsule)
    if health_status == "degraded":
        codes.append("health_degraded")
    elif health_status == "conflicted":
        codes.append("health_conflicted")

    verification_status = _verification_status(capsule)
    if verification_status == "unverified":
        codes.append("verification_unverified")
    elif verification_status == "self_attested":
        codes.append("verification_self_attested")

    verified_at = _parse_iso(str(capsule.get("verified_at") or ""))
    if verified_at is not None and (now - verified_at).total_seconds() > 30 * 86400:
        codes.append("stale_verified_at")
    if recently_used:
        codes.append("recently_used")
    if fallback_only:
        codes.append("fallback_only")
    return codes


def _refresh_priority(reason_codes: list[str], *, health_status: str, verification_status: str) -> str:
    """Map deterministic refresh reason codes to high, medium, or low priority."""
    if health_status in {"degraded", "conflicted"} or "fallback_only" in reason_codes:
        return "high"
    if verification_status in {"unverified", "self_attested"} or "stale_verified_at" in reason_codes:
        return "medium"
    return "low"


def _refresh_state_payload(now: datetime, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the persisted refresh-state payload from one refresh-plan result."""
    return {
        "schema_version": CONTINUITY_REFRESH_STATE_SCHEMA_VERSION,
        "last_planned_at": format_iso(now),
        "last_run_at": None,
        "last_run_count": 0,
        "entries": candidates,
    }


def _continuity_phase(capsule: dict[str, Any], now: datetime) -> tuple[str, list[str]]:
    """Determine freshness phase and warnings for the given capsule."""
    warnings: list[str] = []
    freshness = capsule.get("freshness") if isinstance(capsule.get("freshness"), dict) else {}
    verified_at = _require_utc_timestamp(str(capsule.get("verified_at", "")), "verified_at")
    expires_at = freshness.get("expires_at")
    expires_dt = _parse_iso(str(expires_at)) if expires_at else None
    if expires_dt is not None and now > expires_dt:
        warnings.append(CONTINUITY_WARNING_EXPIRED)
        return "expired", warnings
    stale_after = _effective_stale_seconds(capsule)
    if stale_after is None:
        return "fresh", warnings
    age_seconds = max(0.0, (now - verified_at).total_seconds())
    if age_seconds <= stale_after:
        return "fresh", warnings
    if age_seconds <= stale_after * 1.5:
        warnings.append(CONTINUITY_WARNING_STALE_SOFT)
        return "stale_soft", warnings
    if age_seconds <= stale_after * 2.0:
        warnings.append(CONTINUITY_WARNING_STALE_HARD)
        return "stale_hard", warnings
    warnings.append(CONTINUITY_WARNING_EXPIRED)
    return "expired_by_age", warnings


def _estimated_tokens(text: str) -> int:
    """Estimate token usage with the repository four-characters-per-token heuristic."""
    return int(math.ceil(len(text) / 4.0))


def _render_value(value: Any) -> str:
    """Render a JSON-like value into the internal token-accounting form."""
    if isinstance(value, list):
        return "\n".join(f"- {item}" for item in value)
    if isinstance(value, dict):
        return "\n".join(f"{key}: {_render_value(value[key])}" for key in value)
    return str(value)


def _truncate_string(value: str, max_tokens: int) -> str:
    """Truncate a string to fit a token budget using a character heuristic."""
    if max_tokens <= 0:
        return ""
    max_chars = max_tokens * 4
    if len(value) <= max_chars:
        return value
    if max_chars <= 3:
        return "." * max(0, max_chars)
    return value[: max_chars - 3] + "..."


def _truncate_list(items: list[str], max_tokens: int) -> list[str]:
    """Trim list entries until the rendered list fits the token budget."""
    if max_tokens <= 0:
        return []
    out = list(items)
    while out and _estimated_tokens(_render_value(out)) > max_tokens:
        out.pop()
    while out and _estimated_tokens(_render_value(out)) > max_tokens:
        trimmed = _truncate_string(out[-1], max_tokens)
        if not trimmed or trimmed == out[-1]:
            out.pop()
        else:
            out[-1] = trimmed
    return out


def _drop_nested(payload: dict[str, Any], dotted: str) -> None:
    """Drop a dotted nested key from a JSON-like payload when present."""
    parts = dotted.split(".")
    cur: Any = payload
    for key in parts[:-1]:
        if not isinstance(cur, dict):
            return
        cur = cur.get(key)
    if isinstance(cur, dict):
        cur.pop(parts[-1], None)


def _trim_capsule(capsule: dict[str, Any], max_tokens: int) -> dict[str, Any] | None:
    """Trim a capsule deterministically to fit the reserved continuity budget."""
    payload = json.loads(json.dumps(capsule, ensure_ascii=False))
    # Lower-commitment fields (trailing_notes, curiosity_queue, negative_decisions) trim before
    # working_hypotheses so deliberate non-action survives longer than residual notes/curiosity,
    # while hypotheses still outlive all three.
    for dotted in (
        "metadata",
        "canonical_sources",
        "freshness",
        "attention_policy.presence_bias_overrides",
        "continuity.relationship_model.sensitivity_notes",
        "continuity.relationship_model.preferred_style",
        "continuity.retrieval_hints.avoid",
        "continuity.retrieval_hints.load_next",
        "continuity.trailing_notes",
        "continuity.curiosity_queue",
        "continuity.negative_decisions",
        "continuity.working_hypotheses",
    ):
        if _estimated_tokens(_render_value(payload)) <= max_tokens:
            break
        _drop_nested(payload, dotted)

    continuity = payload.get("continuity")
    if not isinstance(continuity, dict):
        return None
    for field in (
        "retrieval_hints.must_include",
        "relationship_model",
        "long_horizon_commitments",
        "stance_summary",
        "drift_signals",
        "open_loops",
        "active_constraints",
        "active_concerns",
        "top_priorities",
    ):
        if _estimated_tokens(_render_value(payload)) <= max_tokens:
            break
        if field == "retrieval_hints.must_include":
            hints = continuity.get("retrieval_hints")
            if isinstance(hints, dict):
                hints["must_include"] = _truncate_list(list(hints.get("must_include") or []), max(1, max_tokens // 4))
                if not hints["must_include"]:
                    hints.pop("must_include", None)
        elif field == "relationship_model":
            model = continuity.get("relationship_model")
            if isinstance(model, dict):
                if model.get("trust_level") is not None:
                    model.pop("trust_level", None)
                elif model:
                    model.pop(sorted(model)[0], None)
                if not model:
                    continuity.pop("relationship_model", None)
        elif field == "stance_summary":
            continuity["stance_summary"] = _truncate_string(str(continuity.get("stance_summary", "")), max(1, max_tokens // 4))
        else:
            current = continuity.get(field)
            if isinstance(current, list):
                continuity[field] = _truncate_list(list(current), max(1, max_tokens // 4))
        if field in {"drift_signals", "open_loops", "active_constraints", "active_concerns", "top_priorities"} and not continuity.get(field):
            continuity[field] = []

    min_required = any(
        continuity.get(name)
        for name in ("top_priorities", "active_concerns", "active_constraints", "open_loops", "drift_signals", "stance_summary")
    )
    if not min_required or _estimated_tokens(_render_value(payload)) > max_tokens:
        return None
    return payload


def _budget(requested_max_tokens: int) -> dict[str, int]:
    """Compute the continuity token reservation from the requested budget."""
    token_budget_hint = min(requested_max_tokens, 4000)
    if token_budget_hint < 1000:
        reserved = min(150, max(0, int(token_budget_hint * 0.2)))
    else:
        reserved = min(800, max(200, int(token_budget_hint * 0.2)))
    return {
        "requested_max_tokens_estimate": requested_max_tokens,
        "token_budget_hint": token_budget_hint,
        "continuity_tokens_reserved": reserved,
        "continuity_tokens_used": 0,
    }


def _reject_stale_or_conflicting_write(path: Path, req: ContinuityUpsertRequest) -> None:
    """Reject older or equal-timestamp conflicting writes against the stored capsule."""
    if not path.exists() or not path.is_file():
        return
    try:
        current = ContinuityCapsule.model_validate(json.loads(path.read_text(encoding="utf-8")))
    except (ValidationError, json.JSONDecodeError):
        return
    incoming_updated = _require_utc_timestamp(req.capsule.updated_at, "updated_at")
    current_updated = _require_utc_timestamp(current.updated_at, "updated_at")
    if incoming_updated < current_updated:
        raise HTTPException(status_code=409, detail="Incoming continuity capsule is older than the current stored capsule")
    if incoming_updated == current_updated:
        raise HTTPException(status_code=409, detail="Incoming continuity capsule conflicts with the current stored capsule timestamp")


def continuity_upsert_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityUpsertRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Validate and persist one continuity capsule with commit-on-change behavior."""
    auth.require("write:projects")
    capsule = _strip_verification_fields_for_upsert(req.capsule)
    if capsule.subject_kind != req.subject_kind or capsule.subject_id != req.subject_id:
        raise HTTPException(status_code=400, detail="Capsule subject does not match request subject")
    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    auth.require_write_path(rel)
    _validate_capsule(repo_root, capsule)
    path = safe_path(repo_root, rel)
    with _continuity_subject_lock(repo_root=repo_root, subject_kind=req.subject_kind, subject_id=req.subject_id):
        canonical = canonical_json(capsule.model_dump(mode="json", exclude_none=True))
        new_bytes = canonical.encode("utf-8")
        old_bytes = path.read_bytes() if path.exists() else None
        if old_bytes != new_bytes:
            stripped_req = ContinuityUpsertRequest(
                subject_kind=req.subject_kind,
                subject_id=req.subject_id,
                capsule=capsule,
                commit_message=req.commit_message,
                idempotency_key=req.idempotency_key,
            )
            _reject_stale_or_conflicting_write(path, stripped_req)
        capsule_sha256 = hashlib.sha256(new_bytes).hexdigest()
        created = not path.exists()
        changed = old_bytes != new_bytes
        committed = False
        fallback_warning: str | None = None
        fallback_warning_detail: str | None = None
        if changed:
            _persist_active_capsule(
                repo_root=repo_root,
                gm=gm,
                path=path,
                canonical=canonical,
                commit_message=req.commit_message or f"continuity: upsert {req.subject_kind} {req.subject_id}",
            )
            committed = True
            fallback_rel, fallback_status, fallback_warning_detail = _persist_fallback_snapshot(
                repo_root=repo_root,
                gm=gm,
                subject_kind=req.subject_kind,
                subject_id=req.subject_id,
                capsule=capsule.model_dump(mode="json", exclude_none=True),
            )
            if fallback_status == "failed":
                fallback_warning = CONTINUITY_WARNING_FALLBACK_WRITE_FAILED
        else:
            fallback_rel = continuity_fallback_rel_path(req.subject_kind, req.subject_id)
    audit(
        auth,
        "continuity_upsert",
        {
            "subject_kind": req.subject_kind,
            "subject_id": req.subject_id,
            "path": rel,
            "created": created,
            "updated": bool(changed and not created),
            "capsule_sha256": capsule_sha256,
            "idempotency_key": req.idempotency_key,
            "committed": committed,
            "fallback_path": fallback_rel,
            "fallback_warning": fallback_warning,
            "fallback_warning_detail": fallback_warning_detail,
        },
    )
    _warnings: list[dict[str, Any]] = []
    if fallback_warning:
        _warnings.append(make_warning(
            fallback_warning,
            fallback_warning_detail or "Fallback snapshot write failed",
            path=rel,
        ))
    return {
        "ok": True,
        "path": rel,
        "created": created,
        "updated": bool(changed and not created),
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "capsule_sha256": capsule_sha256,
        "warnings": _warnings,
        "recovery_warnings": [fallback_warning] if fallback_warning else [],
        "fallback_warning_detail": fallback_warning_detail,
    }


def _build_startup_summary(out: dict[str, Any]) -> dict[str, Any]:
    """Build a startup-oriented summary from an assembled continuity read result.

    Pure function: no I/O, no side effects.  Same input always produces
    identical output with identical key order.  All list values are shallow
    copies so mutations to the returned summary cannot affect the source
    capsule dict.
    """
    capsule = out.get("capsule")
    source_state = out.get("source_state", "missing")
    recovery_warnings = list(out.get("recovery_warnings", []))

    # --- Tier 1: Recovery (always present, never null) ---
    if capsule is not None:
        health = capsule.get("capsule_health", {})
        capsule_health_status = health.get("status")  # None when absent
        capsule_health_reasons = list(health.get("reasons", []))
    else:
        capsule_health_status = None
        capsule_health_reasons: list[str] = []

    recovery = {
        "source_state": source_state,
        "recovery_warnings": recovery_warnings,
        "capsule_health_status": capsule_health_status,
        "capsule_health_reasons": capsule_health_reasons,
    }

    # --- Tier 2 & 3: Orientation / Context (null when missing) ---
    if source_state == "missing" or capsule is None:
        orientation = None
        context = None
        updated_at = None
    else:
        cont = capsule["continuity"]
        orientation = {
            "top_priorities": list(cont.get("top_priorities", [])),
            "active_constraints": list(cont.get("active_constraints", [])),
            "open_loops": list(cont.get("open_loops", [])),
            # One-level shallow copy; NegativeDecision has only scalar (str) fields.
            "negative_decisions": [dict(d) for d in cont.get("negative_decisions", [])],
        }
        context = {
            "session_trajectory": list(cont.get("session_trajectory", [])),
            "stance_summary": cont.get("stance_summary", ""),
            "active_concerns": list(cont.get("active_concerns", [])),
        }
        updated_at = capsule.get("updated_at")

    return {
        "recovery": recovery,
        "orientation": orientation,
        "context": context,
        "updated_at": updated_at,
    }


def continuity_read_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContinuityReadRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Read one active continuity capsule by exact selector with fallback degradation."""
    auth.require("read:files")
    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    auth.require_read_path(rel)
    fallback_rel = continuity_fallback_rel_path(req.subject_kind, req.subject_id)
    recovery_warnings: list[str] = []
    try:
        capsule = _load_capsule(repo_root, rel, expected_subject=(req.subject_kind, req.subject_id))
        out = {
            "ok": True,
            "path": rel,
            "capsule": capsule,
            "archived": False,
            "source_state": "active",
            "recovery_warnings": recovery_warnings,
        }
    except HTTPException as exc:
        if exc.status_code == 404:
            recovery_warnings.append(CONTINUITY_WARNING_ACTIVE_MISSING)
        elif exc.status_code == 400:
            if "subject does not match" in str(exc.detail):
                raise
            recovery_warnings.append(CONTINUITY_WARNING_ACTIVE_INVALID)
        else:
            raise
        if not req.allow_fallback:
            raise
        auth.require_read_path(fallback_rel)
        try:
            capsule = _load_fallback_snapshot(repo_root, fallback_rel, expected_subject=(req.subject_kind, req.subject_id))
            recovery_warnings.append(CONTINUITY_WARNING_FALLBACK_USED)
            out = {
                "ok": True,
                "path": rel,
                "capsule": capsule,
                "archived": False,
                "source_state": "fallback",
                "recovery_warnings": recovery_warnings,
            }
        except HTTPException as fallback_exc:
            if fallback_exc.status_code in {400, 404}:
                recovery_warnings.append(CONTINUITY_WARNING_FALLBACK_MISSING)
                out = {
                    "ok": True,
                    "path": rel,
                    "capsule": None,
                    "archived": False,
                    "source_state": "missing",
                    "recovery_warnings": recovery_warnings,
                }
            else:
                raise
    audit(
        auth,
        "continuity_read",
        {
            "subject_kind": req.subject_kind,
            "subject_id": req.subject_id,
            "path": rel,
            "source_state": out["source_state"],
            "recovery_warnings": out["recovery_warnings"],
        },
    )
    if req.view == "startup":
        try:
            out["startup_summary"] = _build_startup_summary(out)
        except Exception:
            _logger.warning("startup_summary build failed; degrading to null summary", exc_info=True)
            out["startup_summary"] = None
            # out["recovery_warnings"] is the same list object populated earlier in this function.
            out["recovery_warnings"].append(CONTINUITY_WARNING_STARTUP_SUMMARY_BUILD_FAILED)
    return out


def continuity_list_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContinuityListRequest,
    now: datetime,
    retention_archive_days: int = CONTINUITY_RETENTION_ARCHIVE_DAYS,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """List active, fallback, and archive continuity summaries under the repository namespace."""
    auth.require("read:files")
    base = repo_root / CONTINUITY_DIR_REL
    summaries: list[dict[str, Any]] = []
    if base.exists() and base.is_dir():
        for path in sorted(base.iterdir(), key=lambda item: item.name):
            if path.is_dir() or path.suffix.lower() != ".json":
                continue
            if path.name in CONTINUITY_STATE_METADATA_FILES:
                continue
            if req.subject_kind and not path.name.startswith(f"{req.subject_kind}-"):
                continue
            rel = str(path.relative_to(repo_root))
            try:
                auth.require_read_path(rel)
            except HTTPException:
                continue
            try:
                capsule = _load_capsule(repo_root, rel)
            except HTTPException as exc:
                if exc.status_code in {400, 404}:
                    continue
                raise
            phase, _ = _continuity_phase(capsule, now)
            freshness = capsule.get("freshness") if isinstance(capsule.get("freshness"), dict) else {}
            verification_status = _verification_status(capsule)
            health_status, health_reasons = _capsule_health_summary(capsule)
            summaries.append(
                {
                    "subject_kind": capsule["subject_kind"],
                    "subject_id": capsule["subject_id"],
                    "path": rel,
                    "updated_at": capsule["updated_at"],
                    "verified_at": capsule["verified_at"],
                    "verification_kind": capsule.get("verification_kind"),
                    "freshness_class": freshness.get("freshness_class"),
                    "phase": phase,
                    "verification_status": verification_status,
                    "health_status": health_status,
                    "health_reasons": health_reasons,
                    "artifact_state": "active",
                    "retention_class": "active",
                }
            )
    if req.include_fallback:
        fallback_base = repo_root / CONTINUITY_DIR_REL / "fallback"
        if fallback_base.exists() and fallback_base.is_dir():
            for path in sorted(fallback_base.iterdir(), key=lambda item: item.name):
                if path.is_dir() or path.suffix.lower() != ".json":
                    continue
                rel = str(path.relative_to(repo_root))
                try:
                    auth.require_read_path(rel)
                except HTTPException:
                    continue
                try:
                    envelope = _load_fallback_envelope_payload(repo_root, rel)
                except HTTPException as exc:
                    if exc.status_code in {400, 404}:
                        continue
                    raise
                capsule = envelope["capsule"]
                if req.subject_kind and capsule["subject_kind"] != req.subject_kind:
                    continue
                phase, _ = _continuity_phase(capsule, now)
                freshness = capsule.get("freshness") if isinstance(capsule.get("freshness"), dict) else {}
                verification_status = _verification_status(capsule)
                health_status, health_reasons = _capsule_health_summary(capsule)
                summaries.append(
                    {
                        "subject_kind": capsule["subject_kind"],
                        "subject_id": capsule["subject_id"],
                        "path": rel,
                        "updated_at": capsule["updated_at"],
                        "verified_at": capsule["verified_at"],
                        "verification_kind": capsule.get("verification_kind"),
                        "freshness_class": freshness.get("freshness_class"),
                        "phase": phase,
                        "verification_status": verification_status,
                        "health_status": health_status,
                        "health_reasons": health_reasons,
                        "artifact_state": "fallback",
                        "retention_class": "fallback",
                    }
                )
    if req.include_archived:
        archive_base = repo_root / CONTINUITY_DIR_REL / "archive"
        if archive_base.exists() and archive_base.is_dir():
            for path in sorted(archive_base.iterdir(), key=lambda item: item.name):
                if path.is_dir() or path.suffix.lower() != ".json":
                    continue
                rel = str(path.relative_to(repo_root))
                try:
                    auth.require_read_path(rel)
                except HTTPException:
                    continue
                try:
                    envelope = _load_archive_envelope(repo_root, rel)
                except HTTPException as exc:
                    if exc.status_code in {400, 404}:
                        continue
                    raise
                capsule = envelope["capsule"]
                if req.subject_kind and capsule["subject_kind"] != req.subject_kind:
                    continue
                phase, _ = _continuity_phase(capsule, now)
                freshness = capsule.get("freshness") if isinstance(capsule.get("freshness"), dict) else {}
                verification_status = _verification_status(capsule)
                health_status, health_reasons = _capsule_health_summary(capsule)
                archived_at = _parse_iso(str(envelope.get("archived_at") or ""))
                retention_class = "archive_recent"
                if _is_archive_stale(archived_at=archived_at, now=now, retention_archive_days=retention_archive_days):
                    retention_class = "archive_stale"
                summaries.append(
                    {
                        "subject_kind": capsule["subject_kind"],
                        "subject_id": capsule["subject_id"],
                        "path": str(envelope.get("active_path") or rel),
                        "updated_at": capsule["updated_at"],
                        "verified_at": capsule["verified_at"],
                        "verification_kind": capsule.get("verification_kind"),
                        "freshness_class": freshness.get("freshness_class"),
                        "phase": phase,
                        "verification_status": verification_status,
                        "health_status": health_status,
                        "health_reasons": health_reasons,
                        "artifact_state": "archived",
                        "retention_class": retention_class,
                    }
                )
    if req.include_cold:
        cold_stub_base = repo_root / CONTINUITY_COLD_INDEX_DIR_REL
        if cold_stub_base.exists() and cold_stub_base.is_dir():
            for path in sorted(cold_stub_base.iterdir(), key=lambda item: item.name):
                if path.is_dir() or path.suffix.lower() != ".md":
                    continue
                rel = str(path.relative_to(repo_root))
                try:
                    auth.require_read_path(rel)
                except HTTPException:
                    continue
                try:
                    frontmatter = _load_cold_stub(repo_root, rel)
                except HTTPException as exc:
                    if exc.status_code in {400, 404}:
                        continue
                    raise
                source_archive_path = frontmatter["source_archive_path"]
                try:
                    auth.require_read_path(source_archive_path)
                except HTTPException:
                    continue
                if req.subject_kind and frontmatter["subject_kind"] != req.subject_kind:
                    continue
                summaries.append(
                    {
                        "subject_kind": frontmatter["subject_kind"],
                        "subject_id": frontmatter["subject_id"],
                        "path": rel,
                        "source_archive_path": source_archive_path,
                        "updated_at": None,
                        "verified_at": None,
                        "verification_kind": frontmatter["verification_kind"] or None,
                        "freshness_class": frontmatter["freshness_class"] or None,
                        "phase": frontmatter["phase"],
                        "verification_status": frontmatter["verification_status"],
                        "health_status": frontmatter["health_status"],
                        "health_reasons": [],
                        "artifact_state": "cold",
                        "retention_class": "cold",
                        "cold_stub_path": rel,
                        "cold_storage_path": frontmatter["cold_storage_path"],
                        "archived_at": frontmatter["archived_at"],
                        "cold_stored_at": frontmatter["cold_stored_at"],
                    }
                )
    artifact_order = {"active": 0, "fallback": 1, "archived": 2, "cold": 3}
    summaries.sort(key=lambda row: (str(row["subject_kind"]), str(row["subject_id"]), artifact_order.get(str(row.get("artifact_state")), 99), str(row["path"])))
    summaries = summaries[: req.limit]
    audit(
        auth,
        "continuity_list",
        {
            "subject_kind": req.subject_kind,
            "count": len(summaries),
        },
    )
    return {"ok": True, "count": len(summaries), "capsules": summaries}


def continuity_delete_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityDeleteRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Delete selected continuity artifacts for one exact selector."""
    auth.require("write:projects")
    active_rel = continuity_rel_path(req.subject_kind, req.subject_id)
    fallback_rel = continuity_fallback_rel_path(req.subject_kind, req.subject_id)
    archive_prefix = f"{req.subject_kind}-{_normalize_subject_id(req.subject_id)}-"
    archive_base = repo_root / CONTINUITY_DIR_REL / "archive"
    with _continuity_subject_lock(repo_root=repo_root, subject_kind=req.subject_kind, subject_id=req.subject_id):
        selected_rels: list[str] = []
        missing_paths: list[str] = []
        paths_to_stage: list[Path] = []
        original_bytes: dict[Path, bytes] = {}

        if req.delete_active:
            auth.require_write_path(active_rel)
            active_path = safe_path(repo_root, active_rel)
            if active_path.exists():
                selected_rels.append(active_rel)
                paths_to_stage.append(active_path)
                original_bytes[active_path] = active_path.read_bytes()
            else:
                missing_paths.append(active_rel)

        if req.delete_fallback:
            auth.require_write_path(fallback_rel)
            fallback_path = safe_path(repo_root, fallback_rel)
            if fallback_path.exists():
                selected_rels.append(fallback_rel)
                paths_to_stage.append(fallback_path)
                original_bytes[fallback_path] = fallback_path.read_bytes()
            else:
                missing_paths.append(fallback_rel)

        if req.delete_archive and archive_base.exists() and archive_base.is_dir():
            archive_paths: list[tuple[str, Path]] = []
            for path in sorted(archive_base.iterdir(), key=lambda item: item.name):
                stem = path.stem
                if path.is_dir() or path.suffix.lower() != ".json" or not stem.startswith(archive_prefix):
                    continue
                archive_suffix = stem[len(archive_prefix):]
                if re.fullmatch(r"\d{8}T\d{6}Z", archive_suffix) is None:
                    continue
                rel = str(path.relative_to(repo_root))
                auth.require_write_path(rel)
                archive_paths.append((rel, path))
            for rel, path in archive_paths:
                selected_rels.append(rel)
                paths_to_stage.append(path)
                original_bytes[path] = path.read_bytes()

        if not paths_to_stage:
            audit(
                auth,
                "continuity_delete",
                {
                    "subject_kind": req.subject_kind,
                    "subject_id": req.subject_id,
                    "deleted_paths": [],
                    "missing_paths": missing_paths,
                    "reason": req.reason,
                },
            )
            return {
                "ok": True,
                "deleted_paths": [],
                "missing_paths": missing_paths,
                "durable": True,
                "latest_commit": gm.latest_commit(),
                "warnings": [],
            }

        with repository_mutation_lock(repo_root):
            try:
                for path in paths_to_stage:
                    path.unlink()
                committed = gm.commit_paths(
                    paths_to_stage,
                    _delete_commit_message(req.subject_kind, req.subject_id, req.reason),
                )
                if not committed:
                    raise RuntimeError("Continuity delete commit produced no changes")
            except Exception as exc:
                try_unstage_paths(gm, paths_to_stage)
                restore_errors: list[str] = []
                for path, data in original_bytes.items():
                    try:
                        write_bytes_file(path, data)
                    except Exception as restore_exc:
                        restore_errors.append(f"{path}: {restore_exc}")
                if restore_errors:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Continuity delete failed: {exc}; rollback failed for {', '.join(restore_errors)}",
                    ) from exc
                raise HTTPException(status_code=500, detail=f"Continuity delete failed: {exc}") from exc
    audit(
        auth,
        "continuity_delete",
        {
            "subject_kind": req.subject_kind,
            "subject_id": req.subject_id,
            "deleted_paths": selected_rels,
            "missing_paths": missing_paths,
            "reason": req.reason,
        },
    )
    return {
        "ok": True,
        "deleted_paths": selected_rels,
        "missing_paths": missing_paths,
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "warnings": [],
    }


def continuity_refresh_plan_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityRefreshPlanRequest,
    now: datetime,
    retention_archive_days: int | None = None,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Build and persist a deterministic continuity refresh plan."""
    auth.require("read:files")
    auth.require("write:projects")
    refresh_rel = CONTINUITY_REFRESH_STATE_REL
    auth.require_write_path(refresh_rel)
    recent_selectors = _audit_recent_selectors(repo_root, now)
    base = repo_root / CONTINUITY_DIR_REL
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    if base.exists() and base.is_dir():
        for path in sorted(base.iterdir(), key=lambda item: item.name):
            if path.is_dir() or path.suffix.lower() != ".json":
                continue
            if path.name in CONTINUITY_STATE_METADATA_FILES:
                continue
            rel = str(path.relative_to(repo_root))
            if req.subject_kind and not path.name.startswith(f"{req.subject_kind}-"):
                continue
            try:
                auth.require_read_path(rel)
            except HTTPException:
                continue
            try:
                capsule = _load_capsule(repo_root, rel)
            except HTTPException as exc:
                if exc.status_code in {400, 404}:
                    continue
                raise
            selector = (str(capsule["subject_kind"]), str(capsule["subject_id"]))
            selector_key = _selector_key(selector[0], selector[1])
            seen.add(selector_key)
            codes = _refresh_reason_codes(
                capsule=capsule,
                fallback_only=False,
                recently_used=selector_key in recent_selectors,
                now=now,
            )
            if not req.include_healthy and (not codes or codes == ["recently_used"]):
                continue
            verification_status = _verification_status(capsule)
            health_status, _health_reasons = _capsule_health_summary(capsule)
            candidates.append(
                {
                    "subject_kind": selector[0],
                    "subject_id": selector[1],
                    "path": rel,
                    "health_status": health_status,
                    "verification_status": verification_status,
                    "last_revalidated_at": (
                        str(capsule.get("verification_state", {}).get("last_revalidated_at"))
                        if isinstance(capsule.get("verification_state"), dict)
                        and capsule.get("verification_state", {}).get("last_revalidated_at")
                        else None
                    ),
                    "updated_at": capsule["updated_at"],
                    "reason_codes": codes,
                    "recommended_priority": _refresh_priority(
                        codes,
                        health_status=health_status,
                        verification_status=verification_status,
                    ),
                }
            )

    fallback_dir = repo_root / CONTINUITY_DIR_REL / "fallback"
    if fallback_dir.exists() and fallback_dir.is_dir():
        for path in sorted(fallback_dir.iterdir(), key=lambda item: item.name):
            if path.is_dir() or path.suffix.lower() != ".json":
                continue
            if req.subject_kind and not path.name.startswith(f"{req.subject_kind}-"):
                continue
            rel = str(path.relative_to(repo_root))
            try:
                auth.require_read_path(rel)
            except HTTPException:
                continue
            try:
                envelope = _load_fallback_envelope_payload(repo_root, rel)
            except Exception:
                continue
            capsule = envelope["capsule"]
            selector = (str(capsule["subject_kind"]), str(capsule["subject_id"]))
            selector_key = _selector_key(selector[0], selector[1])
            if selector_key in seen:
                continue
            active_rel = continuity_rel_path(selector[0], selector[1])
            try:
                auth.require_read_path(active_rel)
            except HTTPException:
                continue
            codes = _refresh_reason_codes(
                capsule=capsule,
                fallback_only=True,
                recently_used=selector_key in recent_selectors,
                now=now,
            )
            if not req.include_healthy and (not codes or codes == ["recently_used"]):
                continue
            verification_status = _verification_status(capsule)
            health_status, _health_reasons = _capsule_health_summary(capsule)
            candidates.append(
                {
                    "subject_kind": selector[0],
                    "subject_id": selector[1],
                    "path": active_rel,
                    "health_status": health_status,
                    "verification_status": verification_status,
                    "last_revalidated_at": (
                        str(capsule.get("verification_state", {}).get("last_revalidated_at"))
                        if isinstance(capsule.get("verification_state"), dict)
                        and capsule.get("verification_state", {}).get("last_revalidated_at")
                        else None
                    ),
                    "updated_at": capsule["updated_at"],
                    "reason_codes": codes,
                    "recommended_priority": _refresh_priority(
                        codes,
                        health_status=health_status,
                        verification_status=verification_status,
                    ),
                }
            )

    priority_order = {"high": 0, "medium": 1, "low": 2}
    candidates.sort(key=lambda row: (priority_order.get(str(row["recommended_priority"]), 3), str(row["subject_kind"]), str(row["subject_id"])))
    candidates = candidates[: req.limit]

    refresh_path = safe_path(repo_root, refresh_rel)
    payload = _refresh_state_payload(now, candidates)
    canonical = canonical_json(payload)
    new_bytes = canonical.encode("utf-8")
    latest_commit: str
    with repository_mutation_lock(repo_root):
        old_bytes = refresh_path.read_bytes() if refresh_path.exists() else None
        latest_commit = gm.latest_commit()
        if old_bytes != new_bytes:
            try:
                write_text_file(refresh_path, canonical)
                committed = gm.commit_file(refresh_path, "continuity: refresh plan")
                if not committed:
                    raise RuntimeError("git commit produced no changes")
                latest_commit = gm.latest_commit()
            except Exception as exc:
                try_unstage_paths(gm, [refresh_path])
                raise _restore_failed_refresh_state(refresh_path, old_bytes, exc) from exc

    audit(
        auth,
        "continuity_refresh_plan",
        {
            "subject_kind": req.subject_kind,
            "count": len(candidates),
            "path": refresh_rel,
            "latest_commit": latest_commit,
        },
    )
    return {
        "ok": True,
        "count": len(candidates),
        "generated_at": payload["last_planned_at"],
        "candidates": candidates,
        "durable": True,
        "latest_commit": latest_commit,
        "warnings": [],
    }


def _scan_retention_candidates(
    *,
    repo_root: Path,
    auth: AuthContext,
    subject_kind: str | None,
    generated_at: datetime,
    retention_archive_days: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Scan archive envelopes and return eligible retention candidates plus warnings."""
    archive_base = repo_root / CONTINUITY_DIR_REL / "archive"
    candidates: list[dict[str, Any]] = []
    warnings: list[str] = []
    if not archive_base.exists() or not archive_base.is_dir():
        return candidates, warnings

    for path in sorted(archive_base.iterdir(), key=lambda item: item.name):
        if path.is_dir() or path.suffix.lower() != ".json":
            continue
        rel = str(path.relative_to(repo_root))
        try:
            auth.require_read_path(rel)
        except HTTPException:
            continue
        try:
            envelope = _load_archive_envelope(repo_root, rel)
            if _archive_rel_path_from_envelope(envelope) != rel:
                raise HTTPException(status_code=400, detail="archive identity mismatch")
        except HTTPException as exc:
            if exc.status_code == 400:
                warnings.append(f"continuity_retention_skipped_invalid_archive:{rel}")
                continue
            if exc.status_code == 404:
                continue
            raise

        capsule = envelope["capsule"]
        if subject_kind and capsule["subject_kind"] != subject_kind:
            continue
        archived_at = _parse_iso(str(envelope.get("archived_at") or ""))
        if not _is_archive_stale(archived_at=archived_at, now=generated_at, retention_archive_days=retention_archive_days):
            continue
        cold_state, _cold_storage_path, _cold_stub_path = _retention_cold_state(repo_root, rel)
        if cold_state == "none":
            candidates.append(
                _retention_candidate_from_envelope(
                    envelope=envelope,
                    source_archive_path=rel,
                    generated_at=generated_at,
                )
            )
            continue
        if cold_state in {"partial", "conflict"}:
            warnings.append(f"continuity_retention_partial_cold_conflict:{rel}")

    candidates.sort(key=_retention_candidate_sort_key)
    warnings.sort(key=_retention_warning_sort_key)
    return candidates, warnings


def continuity_retention_plan_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityRetentionPlanRequest,
    now: datetime,
    retention_archive_days: int,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Build and persist a deterministic continuity retention plan."""
    auth.require("read:files")
    auth.require("write:projects")
    auth.require_write_path(CONTINUITY_RETENTION_STATE_REL)

    all_candidates, warnings = _scan_retention_candidates(
        repo_root=repo_root,
        auth=auth,
        subject_kind=req.subject_kind,
        generated_at=now,
        retention_archive_days=retention_archive_days,
    )
    candidates = all_candidates[: req.limit]

    retention_path = safe_path(repo_root, CONTINUITY_RETENTION_STATE_REL)
    payload = _retention_plan_payload(
        generated_at=now,
        req=req,
        candidates=candidates,
        warnings=warnings,
        total_candidates=len(all_candidates),
    )
    canonical = canonical_json(payload)
    new_bytes = canonical.encode("utf-8")
    latest_commit: str
    with repository_mutation_lock(repo_root):
        old_bytes = retention_path.read_bytes() if retention_path.exists() else None
        latest_commit = gm.latest_commit()
        if old_bytes != new_bytes:
            try:
                write_text_file(retention_path, canonical)
                committed = gm.commit_file(retention_path, "continuity: retention plan")
                if not committed:
                    raise RuntimeError("git commit produced no changes")
                latest_commit = gm.latest_commit()
            except Exception as exc:
                try_unstage_paths(gm, [retention_path])
                raise _restore_failed_retention_state(retention_path, old_bytes, exc) from exc

    audit(
        auth,
        "continuity_retention_plan",
        {
            "subject_kind": req.subject_kind,
            "count": len(candidates),
            "total_candidates": len(all_candidates),
            "has_more": len(all_candidates) > len(candidates),
            "path": CONTINUITY_RETENTION_STATE_REL,
            "warnings_count": len(warnings),
            "latest_commit": latest_commit,
        },
    )
    return {
        "ok": True,
        "count": len(candidates),
        "generated_at": payload["generated_at"],
        "path": CONTINUITY_RETENTION_STATE_REL,
        "durable": True,
        "latest_commit": latest_commit,
        "warnings": warnings,
        "candidates": candidates,
        "total_candidates": len(all_candidates),
        "has_more": len(all_candidates) > len(candidates),
    }


def continuity_retention_apply_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityRetentionApplyRequest,
    now: datetime,
    retention_archive_days: int,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Batch-apply continuity retention policy against exact archive paths."""
    auth.require("admin:peers")

    warnings: list[str] = []
    unique_paths: list[str] = []
    seen_paths: set[str] = set()
    for raw_path in list(req.source_archive_paths):
        path = str(raw_path)
        if path in seen_paths:
            warnings.append(f"duplicate_source_archive_path:{path}")
            continue
        seen_paths.add(path)
        unique_paths.append(path)
    warnings.sort(key=_retention_warning_sort_key)

    results: list[dict[str, Any]] = []
    for raw_path in unique_paths:
        result: dict[str, Any] = {
            "source_archive_path": raw_path,
            "ok": False,
            "status": "failed_internal",
            "detail": "",
            "cold_storage_path": None,
            "cold_stub_path": None,
        }
        try:
            source_archive_path = _validate_archive_rel_path(raw_path)
            cold_state, cold_storage_path, cold_stub_path = _retention_cold_state(repo_root, source_archive_path)
            result["cold_storage_path"] = cold_storage_path
            result["cold_stub_path"] = cold_stub_path

            try:
                auth.require_read_path(source_archive_path)
                auth.require_write_path(source_archive_path)
                auth.require_write_path(cold_storage_path)
                auth.require_write_path(cold_stub_path)
            except HTTPException as exc:
                result["status"] = "failed_authorization"
                result["detail"] = str(exc.detail)
                results.append(result)
                continue

            archive_path = safe_path(repo_root, source_archive_path)
            if cold_state in {"partial", "conflict"}:
                result["status"] = "failed_conflict"
                result["detail"] = "Partial or conflicting cold artifact state"
                results.append(result)
                continue
            if cold_state == "full":
                result["ok"] = True
                result["status"] = "skipped_already_cold"
                result["detail"] = "Matching cold payload and stub already exist"
                results.append(result)
                continue
            if not archive_path.exists() or not archive_path.is_file():
                result["ok"] = True
                result["status"] = "skipped_missing"
                result["detail"] = "Continuity archive envelope not found"
                results.append(result)
                continue

            envelope = _load_archive_envelope(repo_root, source_archive_path)
            if _archive_rel_path_from_envelope(envelope) != source_archive_path:
                raise HTTPException(status_code=400, detail="archive identity mismatch")
            archived_at = _parse_iso(str(envelope.get("archived_at") or ""))
            if not _is_archive_stale(archived_at=archived_at, now=now, retention_archive_days=retention_archive_days):
                result["ok"] = True
                result["status"] = "skipped_not_stale"
                result["detail"] = "Archive is not stale under current retention threshold"
                results.append(result)
                continue

            cold_result = continuity_cold_store_service(
                repo_root=repo_root,
                gm=gm,
                auth=auth,
                req=ContinuityColdStoreRequest(source_archive_path=source_archive_path),
                audit=audit,
            )
            result["ok"] = True
            result["status"] = "cold_stored"
            result["detail"] = "Cold-stored archived continuity envelope"
            result["cold_storage_path"] = cold_result["cold_storage_path"]
            result["cold_stub_path"] = cold_result["cold_stub_path"]
            results.append(result)
        except HTTPException as exc:
            if exc.status_code == 400:
                result["status"] = "failed_invalid_archive"
            elif exc.status_code == 403:
                result["status"] = "failed_authorization"
            elif exc.status_code == 404:
                result["ok"] = True
                result["status"] = "skipped_missing"
            elif exc.status_code == 409:
                current_source = str(result["source_archive_path"])
                if current_source.startswith(f"{CONTINUITY_DIR_REL}/archive/"):
                    cold_state, _cold_storage_path, _cold_stub_path = _retention_cold_state(repo_root, current_source)
                    if cold_state == "full":
                        result["ok"] = True
                        result["status"] = "skipped_already_cold"
                    else:
                        result["status"] = "failed_conflict"
                else:
                    result["status"] = "failed_conflict"
            else:
                result["status"] = "failed_internal"
            result["detail"] = str(exc.detail)
            results.append(result)
        except Exception as exc:
            result["status"] = "failed_internal"
            result["detail"] = str(exc)
            results.append(result)

    failed = sum(1 for row in results if str(row["status"]).startswith("failed_"))
    cold_stored = sum(1 for row in results if row["status"] == "cold_stored")
    audit(
        auth,
        "continuity_retention_apply",
        {
            "requested": len(req.source_archive_paths),
            "unique_requested": len(unique_paths),
            "processed": len(results),
            "cold_stored": cold_stored,
            "failed": failed,
        },
    )
    return {
        "ok": failed == 0,
        "requested": len(req.source_archive_paths),
        "unique_requested": len(unique_paths),
        "processed": len(results),
        "cold_stored": cold_stored,
        "failed": failed,
        "durable": True,
        "results": results,
        "warnings": warnings,
    }


def continuity_compare_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContinuityCompareRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Compare one active continuity capsule to a candidate without mutating storage."""
    auth.require("read:files")
    _validate_verification_signals(req.signals)
    _validate_candidate_selector_match(req.subject_kind, req.subject_id, req.candidate_capsule)
    candidate = _normalize_compare_payload(repo_root, req.candidate_capsule)
    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    auth.require_read_path(rel)
    active = _load_capsule(repo_root, rel, expected_subject=(req.subject_kind, req.subject_id))
    changed_fields = _compare_capsules(active, candidate)
    identical = not changed_fields
    strongest_signal = _strongest_signal_kind(req.signals)
    recommended_outcome = "confirm" if identical else ("correct" if strongest_signal != "self_review" else "conflict")
    audit(
        auth,
        "continuity_compare",
        {
            "subject_kind": req.subject_kind,
            "subject_id": req.subject_id,
            "path": rel,
            "strongest_signal": strongest_signal,
            "identical": identical,
            "recommended_outcome": recommended_outcome,
        },
    )
    return {
        "ok": True,
        "path": rel,
        "identical": identical,
        "changed_fields": changed_fields,
        "strongest_signal": strongest_signal,
        "recommended_outcome": recommended_outcome,
    }


def continuity_revalidate_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityRevalidateRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Confirm, correct, degrade, or conflict-mark one active continuity capsule."""
    auth.require("write:projects")
    _validate_verification_signals(req.signals)
    if req.outcome == "correct":
        if req.candidate_capsule is None:
            raise HTTPException(status_code=400, detail="candidate_capsule is required for outcome=correct")
        _validate_candidate_selector_match(req.subject_kind, req.subject_id, req.candidate_capsule)
        _normalize_compare_payload(repo_root, req.candidate_capsule)
    elif req.candidate_capsule is not None:
        raise HTTPException(status_code=400, detail=f"candidate_capsule is not allowed for outcome={req.outcome}")
    if req.outcome == "confirm" and req.reason is not None:
        raise HTTPException(status_code=400, detail="reason is not allowed for outcome=confirm")
    if req.outcome in {"degrade", "conflict"} and not req.reason:
        raise HTTPException(status_code=400, detail=f"reason is required for outcome={req.outcome}")

    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    auth.require_read_path(rel)
    auth.require_write_path(rel)
    active_path = safe_path(repo_root, rel)
    with _continuity_subject_lock(repo_root=repo_root, subject_kind=req.subject_kind, subject_id=req.subject_id):
        active = ContinuityCapsule.model_validate(
            _load_capsule(repo_root, rel, expected_subject=(req.subject_kind, req.subject_id))
        )
        now = datetime.now(timezone.utc).replace(microsecond=0)
        now_iso = format_iso(now)
        strongest_signal = _strongest_signal_kind(req.signals)
        derived_status = CONTINUITY_SIGNAL_STATUS[strongest_signal]
        evidence_refs = _signals_to_evidence_refs(req.signals)
        compare_changed_fields: list[str] = []
        result_outcome = req.outcome
        updated = False

        if req.outcome == "correct":
            candidate = req.candidate_capsule
            if candidate is None:
                raise HTTPException(status_code=400, detail="candidate_capsule is required for outcome=correct")
            compare_changed_fields = _compare_capsules(
                _normalize_compare_payload(repo_root, active),
                _normalize_compare_payload(repo_root, candidate),
            )
            if not compare_changed_fields:
                result_outcome = "confirm"
                final_capsule = active.model_copy(deep=True)
            else:
                updated = True
                final_capsule = candidate.model_copy(deep=True)
        else:
            final_capsule = active.model_copy(deep=True)

        final_capsule.verified_at = now_iso
        final_capsule.verification_kind = strongest_signal  # type: ignore[assignment]

        if result_outcome == "conflict":
            final_capsule.verification_state = ContinuityVerificationState.model_validate({
                "status": "conflicted",
                "last_revalidated_at": now_iso,
                "strongest_signal": strongest_signal,
                "evidence_refs": evidence_refs,
                "conflict_summary": req.reason,
            })
            final_capsule.capsule_health = ContinuityCapsuleHealth.model_validate({
                "status": "conflicted",
                "reasons": [req.reason],
                "last_checked_at": now_iso,
            })
        elif result_outcome == "degrade":
            final_capsule.verification_state = ContinuityVerificationState.model_validate({
                "status": derived_status,
                "last_revalidated_at": now_iso,
                "strongest_signal": strongest_signal,
                "evidence_refs": evidence_refs,
            })
            final_capsule.capsule_health = ContinuityCapsuleHealth.model_validate({
                "status": "degraded",
                "reasons": [req.reason],
                "last_checked_at": now_iso,
            })
        else:
            final_capsule.verification_state = ContinuityVerificationState.model_validate({
                "status": derived_status,
                "last_revalidated_at": now_iso,
                "strongest_signal": strongest_signal,
                "evidence_refs": evidence_refs,
            })
            final_capsule.capsule_health = ContinuityCapsuleHealth.model_validate({
                "status": "healthy",
                "reasons": [],
                "last_checked_at": now_iso,
            })

        _, canonical = _final_capsule_payload(repo_root, final_capsule)
        _persist_active_capsule(
            repo_root=repo_root,
            gm=gm,
            path=active_path,
            canonical=canonical,
            commit_message=f"continuity: revalidate {req.subject_kind} {req.subject_id}",
        )
        fallback_rel, fallback_status, fallback_warning_detail = _persist_fallback_snapshot(
            repo_root=repo_root,
            gm=gm,
            subject_kind=req.subject_kind,
            subject_id=req.subject_id,
            capsule=final_capsule.model_dump(mode="json", exclude_none=True),
        )
    audit(
        auth,
        "continuity_revalidate",
        {
            "subject_kind": req.subject_kind,
            "subject_id": req.subject_id,
            "path": rel,
            "outcome": result_outcome,
            "strongest_signal": strongest_signal,
            "updated": updated,
            "fallback_path": fallback_rel,
            "fallback_warning": CONTINUITY_WARNING_FALLBACK_WRITE_FAILED if fallback_status == "failed" else None,
            "fallback_warning_detail": fallback_warning_detail,
        },
    )
    _rev_warnings: list[dict[str, Any]] = []
    if fallback_status == "failed":
        _rev_warnings.append(make_warning(
            CONTINUITY_WARNING_FALLBACK_WRITE_FAILED,
            fallback_warning_detail or "Fallback snapshot write failed",
            path=rel,
        ))
    return {
        "ok": True,
        "path": rel,
        "outcome": result_outcome,
        "updated": updated,
        "verification_state": final_capsule.verification_state.model_dump(mode="json", exclude_none=True),
        "capsule_health": final_capsule.capsule_health.model_dump(mode="json", exclude_none=True),
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "warnings": _rev_warnings,
        "recovery_warnings": [CONTINUITY_WARNING_FALLBACK_WRITE_FAILED] if fallback_status == "failed" else [],
        "fallback_warning_detail": fallback_warning_detail,
    }


def continuity_archive_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityArchiveRequest,
    now: datetime,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Archive one active continuity capsule and remove the active file in one commit."""
    auth.require("write:projects")
    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    timestamp = format_compact(now)
    archive_rel = f"{CONTINUITY_DIR_REL}/archive/{req.subject_kind}-{_normalize_subject_id(req.subject_id)}-{timestamp}.json"
    auth.require_read_path(rel)
    auth.require_write_path(rel)
    auth.require_write_path(archive_rel)
    with _continuity_subject_lock(repo_root=repo_root, subject_kind=req.subject_kind, subject_id=req.subject_id):
        capsule = _load_capsule(repo_root, rel, expected_subject=(req.subject_kind, req.subject_id))
        archive_payload = {
            "schema_type": CONTINUITY_ARCHIVE_SCHEMA_TYPE,
            "schema_version": CONTINUITY_ARCHIVE_SCHEMA_VERSION,
            "archived_at": format_iso(now),
            "archived_by": auth.peer_id,
            "reason": req.reason,
            "active_path": rel,
            "capsule": capsule,
        }

        archive_path = safe_path(repo_root, archive_rel)
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        active_path = safe_path(repo_root, rel)
        active_bytes = active_path.read_bytes()
        # Both the archive write and the active-file deletion are performed
        # inside the git lock so that a process crash before commit cannot
        # leave the active capsule deleted without a durable archive.
        with repository_mutation_lock(repo_root):
            write_text_file(archive_path, canonical_json(archive_payload))
            try:
                active_path.unlink()
                committed = gm.commit_paths(
                    [archive_path, active_path],
                    f"continuity: archive {req.subject_kind} {req.subject_id}",
                )
            except Exception as exc:
                _logger.error("Continuity archive commit failed: %s", exc, exc_info=True)
                try_unstage_paths(gm, [archive_path, active_path])
                try:
                    _restore_failed_archive(active_path, archive_path, active_bytes)
                except Exception as restore_exc:
                    _logger.exception("Rollback also failed after archive commit error")
                    raise HTTPException(
                        status_code=500,
                        detail=make_error_detail(
                            operation="continuity_archive",
                            error_code="continuity_archive_rollback_failed",
                            error_detail=f"Continuity archive commit failed: {exc}; rollback failed: {restore_exc}",
                        ),
                    ) from exc
                raise HTTPException(
                    status_code=500,
                    detail=make_error_detail(
                        operation="continuity_archive",
                        error_code="continuity_archive_commit_failed",
                        error_detail=f"Continuity archive commit failed: {exc}",
                    ),
                ) from exc
            if not committed:
                _logger.error("Continuity archive commit produced no changes")
                try_unstage_paths(gm, [archive_path, active_path])
                try:
                    _restore_failed_archive(active_path, archive_path, active_bytes)
                except Exception as restore_exc:
                    _logger.exception("Rollback failed after archive no-changes error")
                    raise HTTPException(
                        status_code=500,
                        detail=make_error_detail(
                            operation="continuity_archive",
                            error_code="continuity_archive_rollback_failed",
                            error_detail=f"Continuity archive commit produced no changes; rollback failed: {restore_exc}",
                        ),
                    ) from restore_exc
                raise HTTPException(
                    status_code=500,
                    detail=make_error_detail(
                        operation="continuity_archive",
                        error_code="continuity_archive_commit_no_changes",
                        error_detail="Continuity archive commit produced no changes",
                    ),
                )

    audit(
        auth,
        "continuity_archive",
        {
            "subject_kind": req.subject_kind,
            "subject_id": req.subject_id,
            "archived_path": archive_rel,
            "removed_active_path": rel,
            "reason": req.reason,
        },
    )
    return {
        "ok": True,
        "archived_path": archive_rel,
        "removed_active_path": rel,
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "warnings": [],
    }


def _restore_failed_cold_store(*, archive_path: Path, archive_bytes: bytes, cold_payload_path: Path, cold_stub_path: Path) -> list[str]:
    """Restore the source archive and remove partial cold files after a failed cold-store commit."""
    errors: list[str] = []
    try:
        write_bytes_file(archive_path, archive_bytes)
    except Exception as exc:
        errors.append(f"restore archive: {exc}")
    for path in (cold_payload_path, cold_stub_path):
        try:
            path.unlink(missing_ok=True)
        except Exception as exc:
            errors.append(f"remove {path}: {exc}")
    return errors


def continuity_cold_store_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityColdStoreRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Transform one archive envelope into a cold gzip plus searchable stub."""
    auth.require("admin:peers")
    source_archive_path = _validate_archive_rel_path(req.source_archive_path)
    auth.require_read_path(source_archive_path)
    auth.require_write_path(source_archive_path)
    cold_storage_path = continuity_cold_storage_rel_path(source_archive_path)
    cold_stub_path = continuity_cold_stub_rel_path(source_archive_path)
    auth.require_write_path(cold_storage_path)
    auth.require_write_path(cold_stub_path)

    archive_path = safe_path(repo_root, source_archive_path)
    cold_payload_file = safe_path(repo_root, cold_storage_path)
    cold_stub_file = safe_path(repo_root, cold_stub_path)

    # --- Crash recovery: cold-store completed but uncommitted ---
    # If archive is missing but both cold files exist with valid stub identity,
    # the prior cold-store deleted the archive inside the git lock but crashed
    # before the commit completed.  Recover by committing the existing state.
    if not archive_path.exists() and cold_payload_file.exists() and cold_stub_file.exists():
        try:
            _cold_fm = _load_cold_stub(repo_root, cold_stub_path)
            if _cold_fm.get("source_archive_path") == source_archive_path:
                with _continuity_subject_lock(
                    repo_root=repo_root,
                    subject_kind=_cold_fm["subject_kind"],
                    subject_id=_cold_fm["subject_id"],
                ):
                    # Re-verify under lock (TOCTOU prevention)
                    if not archive_path.exists() and cold_payload_file.exists() and cold_stub_file.exists():
                        _recovery_committed = bool(try_commit_paths(
                            paths=[cold_payload_file, cold_stub_file, archive_path],
                            gm=gm,
                            commit_message=(
                                f"continuity: cold-store recovery "
                                f"{_cold_fm['subject_kind']} {_cold_fm['subject_id']}"
                            ),
                        ))
                        audit(
                            auth,
                            "continuity_cold_store",
                            {
                                "source_archive_path": source_archive_path,
                                "cold_storage_path": cold_storage_path,
                                "cold_stub_path": cold_stub_path,
                                "crash_recovery": True,
                            },
                        )
                        _cs_recovery_warnings: list[dict[str, Any]] = [
                            make_warning(
                                "continuity_cold_store_crash_recovery",
                                "Completed cold-store via crash recovery: "
                                "archive was already deleted, cold files committed",
                            ),
                        ]
                        if not _recovery_committed:
                            _cs_recovery_warnings.append(
                                make_warning(
                                    "continuity_cold_store_recovery_not_durable",
                                    "Crash recovery completed on disk but git commit "
                                    "failed; state is not yet durable",
                                ),
                            )
                        return {
                            "ok": True,
                            "artifact_state": "cold",
                            "source_archive_path": source_archive_path,
                            "cold_storage_path": cold_storage_path,
                            "cold_stub_path": cold_stub_path,
                            "cold_stored_at": _cold_fm.get("cold_stored_at", ""),
                            "committed_files": [cold_storage_path, cold_stub_path, source_archive_path],
                            "durable": _recovery_committed,
                            "latest_commit": gm.latest_commit(),
                            "warnings": _cs_recovery_warnings,
                            "recovery_warnings": _cs_recovery_warnings,
                        }
        except Exception:
            _logger.warning(
                "Continuity cold-store crash recovery: cold stub validation "
                "failed; falling through to normal flow",
                exc_info=True,
            )

    if not archive_path.exists() or not archive_path.is_file():
        raise HTTPException(status_code=404, detail="Continuity archive envelope not found")
    envelope = _load_archive_envelope(repo_root, source_archive_path)
    capsule = envelope["capsule"]
    with _continuity_subject_lock(
        repo_root=repo_root,
        subject_kind=str(capsule["subject_kind"]),
        subject_id=str(capsule["subject_id"]),
    ):
        try:
            if not archive_path.exists() or not archive_path.is_file():
                raise HTTPException(status_code=404, detail="Continuity archive envelope not found")
            source_bytes = archive_path.read_bytes()
        except HTTPException:
            raise
        except FileNotFoundError as exc:
            raise HTTPException(status_code=409, detail=f"Continuity archive envelope changed during cold-store: {exc.filename or exc}") from exc

        try:
            envelope = _load_archive_envelope(repo_root, source_archive_path)
            if _archive_rel_path_from_envelope(envelope) != source_archive_path:
                raise HTTPException(status_code=400, detail="Continuity archive envelope identity does not match source_archive_path")
            # Crash recovery: if both archive and cold artifacts exist,
            # a prior cold-store wrote cold but crashed before committing
            # the archive deletion. Remove orphaned cold artifacts.
            if (cold_payload_file.exists() or cold_stub_file.exists()) and archive_path.exists():
                _logger.warning(
                    "Continuity cold-store crash recovery: removing orphaned cold artifacts for %s",
                    source_archive_path,
                )
                cold_payload_file.unlink(missing_ok=True)
                cold_stub_file.unlink(missing_ok=True)
            elif cold_payload_file.exists() or cold_stub_file.exists():
                raise HTTPException(status_code=409, detail="Continuity cold artifact already exists for source archive")

            now = datetime.now(timezone.utc).replace(microsecond=0)
            cold_stored_at = format_iso(now)
            gzip_bytes = build_cold_gzip_bytes(source_bytes)
            stub_text = _build_cold_stub_text(
                envelope=envelope,
                source_archive_path=source_archive_path,
                cold_storage_path=cold_storage_path,
                cold_stored_at=cold_stored_at,
                now=now,
            )
            write_bytes_file(cold_payload_file, gzip_bytes)
            write_text_file(cold_stub_file, stub_text)
            # Archive deletion and commit are inside the git lock so that a
            # process crash before commit cannot leave the archive deleted
            # without durable cold files.
            with repository_mutation_lock(repo_root):
                archive_path.unlink()
                committed = gm.commit_paths(
                    [cold_payload_file, cold_stub_file, archive_path],
                    f"continuity: cold-store {envelope['capsule']['subject_kind']} {envelope['capsule']['subject_id']}",
                )
                if not committed:
                    raise RuntimeError("Continuity cold-store commit produced no changes")
        except HTTPException:
            raise
        except Exception as exc:
            try_unstage_paths(gm, [cold_payload_file, cold_stub_file, archive_path])
            cleanup_errors = _restore_failed_cold_store(
                archive_path=archive_path,
                archive_bytes=source_bytes,
                cold_payload_path=cold_payload_file,
                cold_stub_path=cold_stub_file,
            )
            raise HTTPException(
                status_code=500,
                detail=make_error_detail(
                    operation="continuity_cold_store",
                    error_code="continuity_cold_store_failed",
                    error_detail=str(exc),
                    rollback_errors=cleanup_errors,
                ),
            ) from exc

    audit(
        auth,
        "continuity_cold_store",
        {
            "source_archive_path": source_archive_path,
            "cold_storage_path": cold_storage_path,
            "cold_stub_path": cold_stub_path,
        },
    )
    return {
        "ok": True,
        "artifact_state": "cold",
        "source_archive_path": source_archive_path,
        "cold_storage_path": cold_storage_path,
        "cold_stub_path": cold_stub_path,
        "cold_stored_at": cold_stored_at,
        "committed_files": [cold_storage_path, cold_stub_path, source_archive_path],
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "warnings": [],
        "recovery_warnings": [],
    }


def continuity_cold_rehydrate_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityColdRehydrateRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Restore one cold-stored archive envelope back into the hot archive namespace."""
    auth.require("admin:peers")
    if req.cold_stub_path:
        cold_stub_path = str(req.cold_stub_path)
        frontmatter = _load_cold_stub(repo_root, cold_stub_path)
        source_archive_path = frontmatter["source_archive_path"]
    else:
        source_archive_path = _validate_archive_rel_path(str(req.source_archive_path))
        cold_stub_path = continuity_cold_stub_rel_path(source_archive_path)
        frontmatter = _load_cold_stub(repo_root, cold_stub_path)
        if frontmatter["source_archive_path"] != source_archive_path:
            raise HTTPException(status_code=400, detail="Continuity cold stub identity does not match requested source archive")
    cold_storage_path = continuity_cold_storage_rel_path(source_archive_path)
    auth.require_read_path(cold_stub_path)
    auth.require_read_path(cold_storage_path)
    auth.require_write_path(source_archive_path)
    auth.require_write_path(cold_stub_path)
    auth.require_write_path(cold_storage_path)

    archive_path = safe_path(repo_root, source_archive_path)
    cold_payload_file = safe_path(repo_root, cold_storage_path)
    cold_stub_file = safe_path(repo_root, cold_stub_path)
    with _continuity_subject_lock(
        repo_root=repo_root,
        subject_kind=frontmatter["subject_kind"],
        subject_id=frontmatter["subject_id"],
    ):
        try:
            frontmatter = _load_cold_stub(repo_root, cold_stub_path)
            if frontmatter["source_archive_path"] != source_archive_path:
                raise HTTPException(status_code=400, detail="Continuity cold stub identity does not match requested source archive")
            if archive_path.exists():
                # --- Crash recovery: rehydrate wrote archive but crashed
                # before deleting cold files and committing. ---
                if cold_payload_file.exists() or cold_stub_file.exists():
                    try:
                        _check_envelope = _load_archive_envelope(repo_root, source_archive_path)
                        if _archive_rel_path_from_envelope(_check_envelope) == source_archive_path:
                            cold_payload_file.unlink(missing_ok=True)
                            cold_stub_file.unlink(missing_ok=True)
                            _rh_recovery_committed = bool(try_commit_paths(
                                paths=[archive_path, cold_payload_file, cold_stub_file],
                                gm=gm,
                                commit_message=(
                                    f"continuity: cold-rehydrate recovery "
                                    f"{frontmatter['subject_kind']} {frontmatter['subject_id']}"
                                ),
                            ))
                            audit(
                                auth,
                                "continuity_cold_rehydrate",
                                {
                                    "source_archive_path": source_archive_path,
                                    "cold_storage_path": cold_storage_path,
                                    "cold_stub_path": cold_stub_path,
                                    "crash_recovery": True,
                                },
                            )
                            _rh_warnings: list[dict[str, Any]] = [
                                make_warning(
                                    "continuity_cold_rehydrate_crash_recovery",
                                    "Completed rehydrate via crash recovery: "
                                    "archive already restored, removed orphaned cold files",
                                ),
                            ]
                            if not _rh_recovery_committed:
                                _rh_warnings.append(
                                    make_warning(
                                        "continuity_cold_rehydrate_recovery_not_durable",
                                        "Crash recovery completed on disk but git commit "
                                        "failed; state is not yet durable",
                                    ),
                                )
                            return {
                                "ok": True,
                                "artifact_state": "archived",
                                "source_archive_path": source_archive_path,
                                "restored_archive_path": source_archive_path,
                                "cold_storage_path": cold_storage_path,
                                "cold_stub_path": cold_stub_path,
                                "rehydrated_at": format_iso(iso_now()),
                                "committed_files": [source_archive_path, cold_storage_path, cold_stub_path],
                                "durable": _rh_recovery_committed,
                                "latest_commit": gm.latest_commit(),
                                "warnings": _rh_warnings,
                                "recovery_warnings": _rh_warnings,
                            }
                    except Exception:
                        _logger.warning(
                            "Continuity cold-rehydrate crash recovery: "
                            "archive validation failed; falling through to 409",
                            exc_info=True,
                        )
                raise HTTPException(status_code=409, detail="Continuity archive envelope already exists")
            if not cold_payload_file.exists() or not cold_payload_file.is_file():
                raise HTTPException(status_code=404, detail="Continuity cold payload not found")
            cold_payload_bytes = cold_payload_file.read_bytes()
            cold_stub_bytes = cold_stub_file.read_bytes()
            archive_bytes = gzip.decompress(cold_payload_bytes)
            payload = json.loads(archive_bytes.decode("utf-8"))
            if payload.get("schema_type") != CONTINUITY_ARCHIVE_SCHEMA_TYPE:
                raise ValueError("wrong schema_type")
            if payload.get("schema_version") != CONTINUITY_ARCHIVE_SCHEMA_VERSION:
                raise ValueError("wrong schema_version")
            capsule = ContinuityCapsule.model_validate(payload.get("capsule")).model_dump(mode="json", exclude_none=True)
            expected_archive_path = _archive_rel_path_from_envelope(
                {**payload, "capsule": capsule}
            )
            if expected_archive_path != source_archive_path:
                raise HTTPException(status_code=400, detail="Continuity cold payload identity does not match requested source archive")
            if str(payload.get("active_path") or "") != continuity_rel_path(str(capsule["subject_kind"]), str(capsule["subject_id"])):
                raise HTTPException(status_code=400, detail="Invalid continuity archive envelope in cold payload: active_path mismatch")
        except HTTPException:
            raise
        except FileNotFoundError as exc:
            raise HTTPException(status_code=409, detail=f"Continuity cold artifact changed during rehydrate: {exc.filename or exc}") from exc
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid continuity cold payload: {exc}") from exc

        rehydrated_at = format_iso(iso_now())
        try:
            write_bytes_file(archive_path, archive_bytes)
            # Cold file deletions and commit are inside the git lock so
            # that a process crash before commit cannot leave cold files
            # deleted without a durable rehydrated archive.
            with repository_mutation_lock(repo_root):
                cold_payload_file.unlink()
                cold_stub_file.unlink()
                committed = gm.commit_paths(
                    [archive_path, cold_payload_file, cold_stub_file],
                    f"continuity: cold-rehydrate {capsule['subject_kind']} {capsule['subject_id']}",
                )
                if not committed:
                    raise RuntimeError("Continuity cold rehydrate commit produced no changes")
        except Exception as exc:
            try_unstage_paths(gm, [archive_path, cold_payload_file, cold_stub_file])
            rollback_errors: list[str] = []
            try:
                archive_path.unlink(missing_ok=True)
            except Exception as rollback_exc:
                rollback_errors.append(f"remove restored archive: {rollback_exc}")
            try:
                write_bytes_file(cold_payload_file, cold_payload_bytes)
            except Exception as rollback_exc:
                rollback_errors.append(f"restore cold payload: {rollback_exc}")
            try:
                write_bytes_file(cold_stub_file, cold_stub_bytes)
            except Exception as rollback_exc:
                rollback_errors.append(f"restore cold stub: {rollback_exc}")
            raise HTTPException(
                status_code=500,
                detail=make_error_detail(
                    operation="continuity_cold_rehydrate",
                    error_code="continuity_cold_rehydrate_failed",
                    error_detail=str(exc),
                    rollback_errors=rollback_errors,
                ),
            ) from exc

    audit(
        auth,
        "continuity_cold_rehydrate",
        {
            "source_archive_path": source_archive_path,
            "cold_storage_path": cold_storage_path,
            "cold_stub_path": cold_stub_path,
        },
    )
    return {
        "ok": True,
        "artifact_state": "archived",
        "source_archive_path": source_archive_path,
        "restored_archive_path": source_archive_path,
        "cold_storage_path": cold_storage_path,
        "cold_stub_path": cold_stub_path,
        "rehydrated_at": rehydrated_at,
        "committed_files": [source_archive_path, cold_storage_path, cold_stub_path],
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "warnings": [],
        "recovery_warnings": [],
    }


def build_continuity_state(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContextRetrieveRequest,
    now: datetime,
) -> dict[str, Any]:
    """Load, trim, and package continuity state for context retrieval."""
    budget = _budget(req.max_tokens_estimate)
    state = {
        "present": False,
        "requested_selectors": [],
        "omitted_selectors": [],
        "capsules": [],
        "selection_order": [],
        "budget": budget,
        "warnings": [],
        "fallback_used": False,
        "recovery_warnings": [],
    }
    if req.continuity_mode == "off":
        return state
    multi_warning_mode = _warning_mode_is_multi(req)
    selectors, requested_selectors, pre_load_omitted = _effective_selectors(req)
    state["requested_selectors"] = requested_selectors
    state["omitted_selectors"] = list(pre_load_omitted)
    if not selectors:
        if req.continuity_mode == "required":
            raise HTTPException(status_code=404, detail="Continuity capsule not found")
        return state

    loaded: list[dict[str, Any]] = []
    warnings: list[str] = []
    recovery_warnings: list[str] = []
    fallback_used = False
    for item in selectors:
        kind = item["subject_kind"]
        subject_id = item["subject_id"]
        resolution = item["resolution"]
        rel = continuity_rel_path(kind, subject_id)
        try:
            auth.require_read_path(rel)
        except HTTPException as auth_exc:
            if auth_exc.status_code == 403:
                selector_label = _format_selector(kind, subject_id)
                recovery_warnings.append(
                    _qualify_warning(CONTINUITY_WARNING_ACTIVE_MISSING, kind, subject_id, multi_mode=multi_warning_mode)
                    + " (owner only)"
                )
                state["omitted_selectors"].append(selector_label)
                continue
            raise
        source_state = "active"
        try:
            capsule = _load_capsule(repo_root, rel, expected_subject=(kind, subject_id))
        except HTTPException as exc:
            selector_label = _format_selector(kind, subject_id)
            if exc.status_code == 404:
                recovery_warnings.append(_qualify_warning(CONTINUITY_WARNING_ACTIVE_MISSING, kind, subject_id, multi_mode=multi_warning_mode))
            elif exc.status_code == 400:
                if "subject does not match" in str(exc.detail):
                    raise
                recovery_warnings.append(_qualify_warning(CONTINUITY_WARNING_ACTIVE_INVALID, kind, subject_id, multi_mode=multi_warning_mode))
            else:
                raise
            resilience_policy = req.continuity_resilience_policy
            if resilience_policy == "require_active":
                state["omitted_selectors"].append(selector_label)
                continue
            if resilience_policy not in {"allow_fallback", "prefer_active"}:
                raise HTTPException(status_code=400, detail="Unsupported continuity_resilience_policy")
            fallback_rel = continuity_fallback_rel_path(kind, subject_id)
            try:
                auth.require_read_path(fallback_rel)
            except HTTPException as fallback_auth_exc:
                if fallback_auth_exc.status_code == 403:
                    recovery_warnings.append(
                        _qualify_warning(CONTINUITY_WARNING_FALLBACK_MISSING, kind, subject_id, multi_mode=multi_warning_mode)
                        + " (owner only)"
                    )
                    state["omitted_selectors"].append(selector_label)
                    continue
                raise
            try:
                capsule = _load_fallback_snapshot(repo_root, fallback_rel, expected_subject=(kind, subject_id))
                recovery_warnings.append(_qualify_warning(CONTINUITY_WARNING_FALLBACK_USED, kind, subject_id, multi_mode=multi_warning_mode))
                fallback_used = True
                source_state = "fallback"
            except HTTPException as fallback_exc:
                if fallback_exc.status_code in {400, 404}:
                    if exc.status_code == 404:
                        recovery_warnings.append(_qualify_warning(CONTINUITY_WARNING_FALLBACK_MISSING, kind, subject_id, multi_mode=multi_warning_mode))
                        state["omitted_selectors"].append(selector_label)
                        continue
                    if len(selectors) > 1:
                        warnings.append(_qualify_warning(CONTINUITY_WARNING_INVALID, kind, subject_id, multi_mode=multi_warning_mode))
                        state["omitted_selectors"].append(selector_label)
                        continue
                    raise exc
                raise
        phase, phase_warnings = _continuity_phase(capsule, now)
        warnings.extend(_qualify_warning(warning, kind, subject_id, multi_mode=multi_warning_mode) for warning in phase_warnings)
        if phase in {"expired", "expired_by_age"}:
            state["omitted_selectors"].append(_format_selector(kind, subject_id))
            continue
        health_status, health_reasons = _capsule_health_summary(capsule)
        loaded.append(
            {
                "selector": item,
                "capsule": capsule,
                "verification_status": _verification_status(capsule),
                "health_status": health_status,
                "health_reasons": health_reasons,
                "source_state": source_state,
            }
        )

    if not loaded:
        if req.continuity_mode == "required":
            raise HTTPException(status_code=404, detail="Continuity capsule not found")
        state["warnings"] = warnings
        state["recovery_warnings"] = recovery_warnings
        state["fallback_used"] = fallback_used
        return state

    if req.continuity_verification_policy == "prefer_healthy":
        loaded = sorted(
            loaded,
            key=lambda row: CONTINUITY_HEALTH_ORDER.get(str(row["health_status"]), CONTINUITY_HEALTH_ORDER["conflicted"]),
        )
    elif req.continuity_verification_policy == "require_healthy":
        filtered: list[dict[str, Any]] = []
        for row in loaded:
            if row["health_status"] == "healthy":
                filtered.append(row)
                continue
            selector = row["selector"]
            state["omitted_selectors"].append(_format_selector(selector["subject_kind"], selector["subject_id"]))
        loaded = filtered

    if not loaded:
        if req.continuity_mode == "required":
            raise HTTPException(status_code=404, detail="Continuity capsule not found")
        state["warnings"] = warnings
        state["recovery_warnings"] = recovery_warnings
        state["fallback_used"] = fallback_used
        return state

    reserve = budget["continuity_tokens_reserved"]
    count = len(loaded)
    base = reserve // count
    remainder = reserve % count

    trimmed_capsules: list[dict[str, Any]] = []
    trimmed_selection_order: list[str] = []
    for idx, row in enumerate(loaded):
        allocation = base + (1 if idx < remainder else 0)
        selector = row["selector"]
        kind = selector["subject_kind"]
        subject_id = selector["subject_id"]
        resolution = selector["resolution"]
        trimmed = _trim_capsule(row["capsule"], allocation)
        if trimmed is None:
            state["omitted_selectors"].append(_format_selector(kind, subject_id))
            warnings.append(_qualify_warning(CONTINUITY_WARNING_TRUNCATED_MULTI, kind, subject_id, multi_mode=multi_warning_mode))
            continue
        trimmed["source_state"] = row["source_state"]
        trimmed_capsules.append(trimmed)
        trimmed_selection_order.append(f"{resolution}:{kind}:{subject_id}")
        if row["health_status"] == "degraded":
            warnings.append(_qualify_warning(CONTINUITY_WARNING_DEGRADED, kind, subject_id, multi_mode=multi_warning_mode))
        elif row["health_status"] == "conflicted":
            warnings.append(_qualify_warning(CONTINUITY_WARNING_CONFLICTED, kind, subject_id, multi_mode=multi_warning_mode))

    if not trimmed_capsules:
        if req.continuity_mode == "required":
            raise HTTPException(status_code=404, detail="Continuity capsule not found")
        state["warnings"] = warnings
        state["recovery_warnings"] = recovery_warnings
        state["fallback_used"] = fallback_used
        return state

    state["present"] = True
    state["capsules"] = trimmed_capsules
    state["selection_order"] = trimmed_selection_order
    state["warnings"] = warnings
    state["recovery_warnings"] = recovery_warnings
    state["fallback_used"] = fallback_used
    state["budget"]["continuity_tokens_used"] = sum(_estimated_tokens(_render_value(item)) for item in trimmed_capsules)
    return state
