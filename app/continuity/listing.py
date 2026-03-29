"""Continuity list-service scanning and summary-row construction."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.auth import AuthContext
from app.models import ContinuityListRequest
from app.timestamps import parse_iso as _parse_iso

from app.continuity.cold import _load_cold_stub
from app.continuity.constants import (
    CONTINUITY_COLD_INDEX_DIR_REL,
    CONTINUITY_DIR_REL,
    CONTINUITY_STATE_METADATA_FILES,
)
from app.continuity.freshness import (
    _capsule_health_summary,
    _continuity_phase,
    _verification_status,
)
from app.continuity.persistence import (
    _load_archive_envelope,
    _load_capsule,
    _load_fallback_envelope_payload,
)
from app.continuity.retention import _is_archive_stale


def _capsule_list_summary(
    capsule: dict[str, Any],
    *,
    rel: str,
    now: datetime,
    artifact_state: str,
    retention_class: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a normalized list-summary row from a loaded capsule dict."""
    phase, _ = _continuity_phase(capsule, now)
    freshness = capsule.get("freshness") if isinstance(capsule.get("freshness"), dict) else {}
    verification_status = _verification_status(capsule)
    health_status, health_reasons = _capsule_health_summary(capsule)
    row: dict[str, Any] = {
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
        "artifact_state": artifact_state,
        "retention_class": retention_class,
        "stable_preference_count": len(capsule.get("stable_preferences", [])),
        "rationale_entry_count": len(capsule.get("continuity", {}).get("rationale_entries", [])),
        "thread_descriptor": capsule.get("thread_descriptor"),
    }
    if extra:
        row.update(extra)
    return row


def _scan_active_summaries(
    repo_root: Path,
    auth: AuthContext,
    subject_kind: str | None,
    now: datetime,
) -> list[dict[str, Any]]:
    """Scan the active continuity directory and return list-summary rows."""
    base = repo_root / CONTINUITY_DIR_REL
    summaries: list[dict[str, Any]] = []
    if not (base.exists() and base.is_dir()):
        return summaries
    for path in sorted(base.iterdir(), key=lambda item: item.name):
        if path.is_dir() or path.suffix.lower() != ".json":
            continue
        if path.name in CONTINUITY_STATE_METADATA_FILES:
            continue
        if subject_kind and not path.name.startswith(f"{subject_kind}-"):
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
        summaries.append(
            _capsule_list_summary(capsule, rel=rel, now=now, artifact_state="active", retention_class="active")
        )
    return summaries


def _scan_fallback_summaries(
    repo_root: Path,
    auth: AuthContext,
    subject_kind: str | None,
    now: datetime,
) -> list[dict[str, Any]]:
    """Scan the fallback continuity directory and return list-summary rows."""
    fallback_base = repo_root / CONTINUITY_DIR_REL / "fallback"
    summaries: list[dict[str, Any]] = []
    if not (fallback_base.exists() and fallback_base.is_dir()):
        return summaries
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
        if subject_kind and capsule["subject_kind"] != subject_kind:
            continue
        summaries.append(
            _capsule_list_summary(capsule, rel=rel, now=now, artifact_state="fallback", retention_class="fallback")
        )
    return summaries


def _scan_archive_summaries(
    repo_root: Path,
    auth: AuthContext,
    subject_kind: str | None,
    now: datetime,
    retention_archive_days: int,
) -> list[dict[str, Any]]:
    """Scan the archive continuity directory and return list-summary rows."""
    archive_base = repo_root / CONTINUITY_DIR_REL / "archive"
    summaries: list[dict[str, Any]] = []
    if not (archive_base.exists() and archive_base.is_dir()):
        return summaries
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
        if subject_kind and capsule["subject_kind"] != subject_kind:
            continue
        archived_at = _parse_iso(str(envelope.get("archived_at") or ""))
        retention_class = "archive_recent"
        if _is_archive_stale(archived_at=archived_at, now=now, retention_archive_days=retention_archive_days):
            retention_class = "archive_stale"
        summaries.append(
            _capsule_list_summary(
                capsule,
                rel=str(envelope.get("active_path") or rel),
                now=now,
                artifact_state="archived",
                retention_class=retention_class,
            )
        )
    return summaries


def _scan_cold_summaries(
    repo_root: Path,
    auth: AuthContext,
    subject_kind: str | None,
) -> list[dict[str, Any]]:
    """Scan the cold-stub index directory and return list-summary rows."""
    cold_stub_base = repo_root / CONTINUITY_COLD_INDEX_DIR_REL
    summaries: list[dict[str, Any]] = []
    if not (cold_stub_base.exists() and cold_stub_base.is_dir()):
        return summaries
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
        if subject_kind and frontmatter["subject_kind"] != subject_kind:
            continue
        summaries.append({
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
            "stable_preference_count": None,
            "rationale_entry_count": None,
        })
    return summaries


def _matches_thread_filters(row: dict[str, Any], req: ContinuityListRequest) -> bool:
    """Check if a summary row matches all thread descriptor filters (conjunctive)."""
    td = row.get("thread_descriptor")
    if td is None:
        return False
    if req.lifecycle is not None and td.get("lifecycle") != req.lifecycle:
        return False
    if req.scope_anchor is not None and req.scope_anchor not in (td.get("scope_anchors") or []):
        return False
    if req.keyword is not None:
        normalized_keyword = req.keyword.lower().strip()
        if normalized_keyword not in [kw.lower().strip() for kw in (td.get("keywords") or [])]:
            return False
    if req.label_exact is not None and td.get("label") != req.label_exact:
        return False
    if req.anchor_kind is not None or req.anchor_value is not None:
        anchors = td.get("identity_anchors") or []
        matched = False
        for anchor in anchors:
            kind_ok = req.anchor_kind is None or anchor.get("kind") == req.anchor_kind
            value_ok = req.anchor_value is None or anchor.get("value") == req.anchor_value
            if kind_ok and value_ok:
                matched = True
                break
        if not matched:
            return False
    return True
