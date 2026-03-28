"""Refresh planning: audit-log selector scanning, reason codes, and state payload."""

from __future__ import annotations

import json
import logging
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.continuity.constants import CONTINUITY_REFRESH_STATE_SCHEMA_VERSION
from app.continuity.freshness import _capsule_health_summary, _verification_status
from app.continuity.paths import _normalize_subject_id
from app.timestamps import parse_iso as _parse_iso, format_iso

_logger = logging.getLogger(__name__)


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
