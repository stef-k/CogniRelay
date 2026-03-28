"""Retention planning: archive staleness, cold-state inspection, and candidate scanning."""

from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.auth import AuthContext
from app.continuity.cold import _load_cold_stub
from app.continuity.constants import (
    CONTINUITY_DIR_REL,
    CONTINUITY_RETENTION_PLAN_SCHEMA_TYPE,
    CONTINUITY_RETENTION_PLAN_SCHEMA_VERSION,
    CONTINUITY_RETENTION_STATE_REL,
)
from app.continuity.paths import (
    _archive_rel_path_from_envelope,
    _normalize_subject_id,
    continuity_cold_storage_rel_path,
    continuity_cold_stub_rel_path,
)
from app.continuity.persistence import _load_archive_envelope
from app.continuity.validation import _require_utc_timestamp
from app.models import ContinuityRetentionPlanRequest
from app.storage import safe_path
from app.timestamps import parse_iso as _parse_iso, format_iso


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
