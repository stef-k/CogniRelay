"""Freshness phase, staleness, health summary, and verification status derivation."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from app.continuity.constants import (
    CONTINUITY_DEFAULT_STALE,
    CONTINUITY_WARNING_EXPIRED,
    CONTINUITY_WARNING_STALE_HARD,
    CONTINUITY_WARNING_STALE_SOFT,
)
from app.timestamps import parse_iso as _parse_iso


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


def _continuity_phase(capsule: dict[str, Any], now: datetime) -> tuple[str, list[str]]:
    """Determine freshness phase and warnings for the given capsule."""
    # Function-level import to avoid circular dependency with validation.py
    from app.continuity.service import _require_utc_timestamp

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
