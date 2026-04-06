"""Trust signals, startup summary, and resume quality diagnostics."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from app.continuity.constants import (
    CONTINUITY_HEALTH_ORDER,
    CONTINUITY_PHASE_SEVERITY,
    RESUME_QUALITY_STANCE_MIN_LEN,
)
from app.continuity.freshness import (
    _capsule_health_summary,
    _continuity_phase,
    _effective_stale_seconds,
    _verification_status,
)
from app.models import ContinuityCapsule
from app.timestamps import parse_iso as _parse_iso


_ORIENTATION_FIELDS = (
    "top_priorities",
    "active_constraints",
    "open_loops",
    "active_concerns",
    "stance_summary",
    "drift_signals",
)
_LEGACY_TIMESTAMP_FLOOR = "1970-01-01T00:00:00Z"



def _compute_resume_quality(capsule: ContinuityCapsule) -> dict[str, Any]:
    """Return a minimal resume-quality diagnostic for the merged capsule.

    adequate is True iff open_loops, top_priorities, and active_constraints
    are each non-empty and stance_summary is at least
    RESUME_QUALITY_STANCE_MIN_LEN characters long.
    """
    cont = capsule.continuity
    adequate = bool(cont.open_loops and cont.top_priorities and cont.active_constraints and len(cont.stance_summary) >= RESUME_QUALITY_STANCE_MIN_LEN)
    return {"adequate": adequate}


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

    if capsule is not None:
        if capsule.get("subject_kind") in ("thread", "task") and not capsule.get("thread_descriptor"):
            recovery_warnings.append(f"continuity_thread_descriptor_missing:thread:{capsule.get('subject_id', 'unknown')}")

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
        stable_preferences = None
    else:
        cont = capsule["continuity"]
        orientation = {
            "top_priorities": list(cont.get("top_priorities", [])),
            "active_constraints": list(cont.get("active_constraints", [])),
            "open_loops": list(cont.get("open_loops", [])),
            # One-level shallow copy; NegativeDecision has only scalar (str) fields.
            "negative_decisions": [dict(d) for d in cont.get("negative_decisions", [])],
            # Deep copy: RationaleEntry has nested lists (alternatives_considered, depends_on).
            "rationale_entries": [
                {**r, "alternatives_considered": list(r.get("alternatives_considered", [])), "depends_on": list(r.get("depends_on", []))}
                for r in cont.get("rationale_entries", [])
                if r.get("status") == "active"
            ],
        }
        context = {
            "session_trajectory": list(cont.get("session_trajectory", [])),
            "stance_summary": cont.get("stance_summary", ""),
            "active_concerns": list(cont.get("active_concerns", [])),
        }
        updated_at = capsule.get("updated_at")
        stable_preferences = [dict(p) for p in capsule.get("stable_preferences", [])]

    return {
        "recovery": recovery,
        "orientation": orientation,
        "context": context,
        "updated_at": updated_at,
        "trust_signals": out.get("trust_signals"),
        "stable_preferences": stable_preferences,
    }


def _build_trust_signals(
    capsule: dict[str, Any],
    now: datetime,
    *,
    source_state: str,
    trimmed: bool = False,
    trimmed_fields: list[str] | None = None,
) -> dict[str, Any]:
    """Build per-capsule trust signals from objective, mechanical checks.

    Deterministic for valid capsules — no I/O, no side effects.  Same
    capsule and *now* always produce an identical result with identical
    key order.

    When ``verified_at`` is missing or malformed, the function does not
    raise — it falls back to ``phase="expired"`` and ``null`` age fields
    so the consumer never sees a misleadingly fresh signal.

    Args:
        capsule: Raw capsule dict (pre-trim for ``build_continuity_state``).
        now: Current UTC time for age computation.
        source_state: ``"active"`` or ``"fallback"``.
        trimmed: Whether token-budget trimming was applied.
        trimmed_fields: Dotted paths of fields removed by trimming.
    """
    # -- recency --
    updated_dt = _parse_iso(str(capsule.get("updated_at", "")))
    verified_dt = _parse_iso(str(capsule.get("verified_at", "")))
    updated_age: int | None = max(0, int((now - updated_dt).total_seconds())) if updated_dt else None
    verified_age: int | None = max(0, int((now - verified_dt).total_seconds())) if verified_dt else None
    try:
        phase, _ = _continuity_phase(capsule, now)
    except Exception:
        phase = "expired"
    if str(capsule.get("verified_at") or "") == _LEGACY_TIMESTAMP_FLOOR:
        phase = "expired"
        verified_age = None
    freshness_raw = capsule.get("freshness")
    freshness_dict = freshness_raw if isinstance(freshness_raw, dict) else {}
    fc = freshness_dict.get("freshness_class")
    freshness_class = fc if isinstance(fc, str) and fc else None
    stale_threshold = _effective_stale_seconds(capsule)

    recency = {
        "updated_age_seconds": updated_age,
        "verified_age_seconds": verified_age,
        "phase": phase,
        "freshness_class": freshness_class,
        "stale_threshold_seconds": stale_threshold,
    }

    # -- completeness --
    cont = capsule.get("continuity")
    cont_dict = cont if isinstance(cont, dict) else {}
    ol = cont_dict.get("open_loops")
    tp = cont_dict.get("top_priorities")
    ac = cont_dict.get("active_constraints")
    ss = str(cont_dict.get("stance_summary", ""))
    adequate = bool(ol and tp and ac and len(ss) >= RESUME_QUALITY_STANCE_MIN_LEN)

    empty_fields: list[str] = []
    for fname in _ORIENTATION_FIELDS:
        val = cont_dict.get(fname)
        if fname == "stance_summary":
            if not isinstance(val, str) or len(val) < RESUME_QUALITY_STANCE_MIN_LEN:
                empty_fields.append(fname)
        elif not val:
            empty_fields.append(fname)

    completeness = {
        "orientation_adequate": adequate,
        "empty_orientation_fields": empty_fields,
        "trimmed": trimmed,
        "trimmed_fields": list(trimmed_fields) if trimmed_fields else [],
    }

    # -- integrity --
    health_status, health_reasons = _capsule_health_summary(capsule)
    verification = _verification_status(capsule)

    integrity = {
        "source_state": source_state,
        "health_status": health_status,
        "health_reasons": list(health_reasons),
        "verification_status": verification,
    }

    # -- scope_match --
    scope_match = {
        "exact": source_state == "active",
    }

    return {
        "recency": recency,
        "completeness": completeness,
        "integrity": integrity,
        "scope_match": scope_match,
    }


def _build_compact_trust_signals(
    capsule: dict[str, Any],
    now: datetime,
    *,
    source_state: str,
    trimmed: bool = False,
) -> dict[str, Any]:
    """Build a reduced trust-signals shape for tight token budgets.

    Contains the minimum subfields needed for trust assessment.  Falls
    back to ``phase="expired"`` on malformed timestamps rather than
    raising.
    """
    try:
        phase, _ = _continuity_phase(capsule, now)
    except Exception:
        phase = "expired"
    if str(capsule.get("verified_at") or "") == _LEGACY_TIMESTAMP_FLOOR:
        phase = "expired"
    cont = capsule.get("continuity")
    cont_dict = cont if isinstance(cont, dict) else {}
    ol = cont_dict.get("open_loops")
    tp = cont_dict.get("top_priorities")
    ac = cont_dict.get("active_constraints")
    ss = str(cont_dict.get("stance_summary", ""))
    adequate = bool(ol and tp and ac and len(ss) >= RESUME_QUALITY_STANCE_MIN_LEN)
    health_status, _ = _capsule_health_summary(capsule)

    return {
        "compact": True,
        "recency": {"phase": phase},
        "completeness": {"orientation_adequate": adequate, "trimmed": trimmed},
        "integrity": {"source_state": source_state, "health_status": health_status},
        "scope_match": {"exact": source_state == "active"},
    }


def _build_aggregate_trust_signals(
    per_capsule_signals: list[dict[str, Any]],
    *,
    selectors_requested: int,
    selectors_returned: int,
    selectors_omitted: int,
) -> dict[str, Any]:
    """Build aggregate trust signals from a list of per-capsule trust signals.

    Pure function: deterministic, no I/O, no side effects.  Same inputs
    always produce an identical result with identical key order.

    Handles a mix of full and compact per-capsule trust shapes.  Compact
    signals omit ``updated_age_seconds`` and ``verified_age_seconds`` —
    these are treated as ``None`` for aggregation purposes and the
    aggregate age fields are ``null`` when no full signal provides them.

    Raises ``ValueError`` if *per_capsule_signals* is empty — callers
    must guard against the empty case before invoking.
    """
    if not per_capsule_signals:
        raise ValueError("per_capsule_signals must be non-empty")
    # -- recency --
    phases = [s["recency"]["phase"] for s in per_capsule_signals]
    worst_phase = max(phases, key=lambda p: CONTINUITY_PHASE_SEVERITY.get(p, CONTINUITY_PHASE_SEVERITY["expired"]))
    # Age fields may be absent (compact signals) or null (malformed timestamps).
    updated_ages = [s["recency"].get("updated_age_seconds") for s in per_capsule_signals]
    verified_ages = [s["recency"].get("verified_age_seconds") for s in per_capsule_signals]
    known_updated = [a for a in updated_ages if a is not None]
    known_verified = [a for a in verified_ages if a is not None]
    oldest_updated: int | None = max(known_updated) if known_updated else None
    oldest_verified: int | None = max(known_verified) if known_verified else None

    recency = {
        "worst_phase": worst_phase,
        "oldest_updated_age_seconds": oldest_updated,
        "oldest_verified_age_seconds": oldest_verified,
    }

    # -- completeness --
    adequate_flags = [s["completeness"]["orientation_adequate"] for s in per_capsule_signals]
    completeness = {
        "all_adequate": all(adequate_flags),
        "adequate_count": sum(1 for f in adequate_flags if f),
        "total_count": len(per_capsule_signals),
        "any_trimmed": any(s["completeness"]["trimmed"] for s in per_capsule_signals),
    }

    # -- integrity --
    health_values = [s["integrity"]["health_status"] for s in per_capsule_signals]
    worst_health = max(
        health_values,
        key=lambda h: CONTINUITY_HEALTH_ORDER.get(h, CONTINUITY_HEALTH_ORDER["conflicted"]),
    )
    integrity = {
        "worst_health": worst_health,
        "any_fallback": any(s["integrity"]["source_state"] == "fallback" for s in per_capsule_signals),
        "any_degraded": any(s["integrity"]["health_status"] == "degraded" for s in per_capsule_signals),
        "any_conflicted": any(s["integrity"]["health_status"] == "conflicted" for s in per_capsule_signals),
    }

    # -- scope_match --
    scope_match = {
        "selectors_requested": selectors_requested,
        "selectors_returned": selectors_returned,
        "selectors_omitted": selectors_omitted,
        "all_returned": selectors_requested == selectors_returned and selectors_requested > 0,
    }

    return {
        "recency": recency,
        "completeness": completeness,
        "integrity": integrity,
        "scope_match": scope_match,
    }
