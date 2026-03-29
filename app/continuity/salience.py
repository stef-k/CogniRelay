"""Deterministic salience ranking for continuity retrieval and list output.

Computes a lexicographic sort key from five mechanical signals already
present in each capsule, plus two deterministic tiebreakers that
guarantee total ordering.  Nothing is stored — salience is computed at
retrieval time from in-memory capsule state.

Spec: #123 (first slice).
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from app.continuity.constants import (
    CONTINUITY_HEALTH_ORDER,
    CONTINUITY_PHASE_SEVERITY,
    CONTINUITY_SIGNAL_RANK,
    RESUME_QUALITY_STANCE_MIN_LEN,
    SALIENCE_LIFECYCLE_NO_DESCRIPTOR,
    SALIENCE_LIFECYCLE_RANK,
)
from app.continuity.freshness import (
    _capsule_health_summary,
    _continuity_phase,
)
from app.timestamps import parse_iso as _parse_iso




# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _lifecycle_rank(capsule: dict[str, Any]) -> int:
    """Return the lifecycle rank for *capsule*.

    Capsules without a ``thread_descriptor`` receive
    ``SALIENCE_LIFECYCLE_NO_DESCRIPTOR`` (sorts after all lifecycle-bearing
    capsules).
    """
    td = capsule.get("thread_descriptor")
    if not isinstance(td, dict):
        return SALIENCE_LIFECYCLE_NO_DESCRIPTOR
    lifecycle = td.get("lifecycle")
    if not isinstance(lifecycle, str):
        return SALIENCE_LIFECYCLE_NO_DESCRIPTOR
    return SALIENCE_LIFECYCLE_RANK.get(lifecycle, SALIENCE_LIFECYCLE_NO_DESCRIPTOR)


def _health_rank(row: dict[str, Any]) -> int:
    """Return the health rank from a loaded-capsule row.

    Falls back to the capsule dict when ``health_status`` is not
    pre-computed on the row (list-summary path).
    """
    status = row.get("health_status")
    if status is None:
        status, _ = _capsule_health_summary(row.get("capsule", row))
    return CONTINUITY_HEALTH_ORDER.get(str(status), CONTINUITY_HEALTH_ORDER["conflicted"])


def _freshness_rank(capsule: dict[str, Any], now: datetime) -> int:
    """Return the freshness-phase rank for *capsule*.

    Accepts both full capsule dicts (computes phase) and list-summary
    rows that already carry a ``phase`` key.
    """
    pre_computed = capsule.get("phase")
    if isinstance(pre_computed, str) and pre_computed in CONTINUITY_PHASE_SEVERITY:
        return CONTINUITY_PHASE_SEVERITY[pre_computed]
    phase, _ = _continuity_phase(capsule, now)
    return CONTINUITY_PHASE_SEVERITY.get(phase, CONTINUITY_PHASE_SEVERITY["expired"])


def _resume_adequate(capsule: dict[str, Any]) -> bool:
    """Return whether *capsule* has adequate resume quality.

    Mirrors the logic in ``trust._compute_resume_quality`` but operates
    on the raw dict so we avoid constructing a full pydantic model.
    Accepts a pre-computed ``resume_adequate`` key (set by list-summary
    rows in ``listing._capsule_list_summary``) to avoid requiring the
    nested ``continuity`` dict.
    """
    pre = capsule.get("resume_adequate")
    if isinstance(pre, bool):
        return pre
    cont = capsule.get("continuity")
    if not isinstance(cont, dict):
        return False
    return bool(
        cont.get("open_loops")
        and cont.get("top_priorities")
        and cont.get("active_constraints")
        and len(str(cont.get("stance_summary", ""))) >= RESUME_QUALITY_STANCE_MIN_LEN
    )


def _verification_rank(capsule: dict[str, Any]) -> int:
    """Return the verification signal rank (0–4).

    Higher rank = stronger verification.  Absent verification state is
    treated as rank 0 (equivalent to ``self_review`` / unverified).

    Accepts both full capsule dicts (reads ``verification_state.kind``)
    and list-summary rows that carry ``verification_kind`` directly.
    """
    vs = capsule.get("verification_state")
    if isinstance(vs, dict):
        kind = vs.get("kind") or capsule.get("verification_kind")
    else:
        kind = capsule.get("verification_kind")
    if not isinstance(kind, str):
        return 0
    return CONTINUITY_SIGNAL_RANK.get(kind, 0)


def _updated_age_seconds(capsule: dict[str, Any], now: datetime) -> int:
    """Return seconds since ``updated_at``, clamped to ``>= 0``."""
    raw = capsule.get("updated_at")
    if not raw:
        # Missing timestamp — treat as maximally stale.
        return 2**31 - 1
    try:
        updated_dt = _parse_iso(str(raw))
        if updated_dt is None:
            return 2**31 - 1
        return max(0, math.floor((now - updated_dt).total_seconds()))
    except Exception:
        return 2**31 - 1


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _salience_sort_key(
    row: dict[str, Any],
    now: datetime,
) -> tuple[int, int, int, int, int, int, str, str]:
    """Compute the full salience sort key for one loaded-capsule row.

    The row is expected to carry at least ``capsule``, ``health_status``,
    and ``selector`` keys (as produced by ``_load_selectors_with_fallback``).
    For list-summary rows the capsule data may be flattened into the row
    itself.

    Returns an 8-tuple ordered from highest to lowest priority:

    1. lifecycle_rank   (0–3, 99 for no descriptor)
    2. health_rank      (0–2)
    3. freshness_rank   (0–4)
    4. resume_rank      (0 = adequate, 1 = inadequate)
    5. neg_verification (-4 … 0; negated so stronger sorts first)
    6. updated_age_seconds (lower = more recent = better)
    7. subject_kind_str (alphabetical tiebreak)
    8. subject_id_str   (alphabetical tiebreak)
    """
    capsule = row.get("capsule", row)
    selector = row.get("selector")

    if isinstance(selector, dict):
        kind_str = str(selector.get("subject_kind", ""))
        id_str = str(selector.get("subject_id", ""))
    else:
        kind_str = str(capsule.get("subject_kind", ""))
        id_str = str(capsule.get("subject_id", ""))

    return (
        _lifecycle_rank(capsule),
        _health_rank(row),
        _freshness_rank(capsule, now),
        0 if _resume_adequate(capsule) else 1,
        -_verification_rank(capsule),
        _updated_age_seconds(capsule, now),
        kind_str,
        id_str,
    )


def _salience_block(
    row: dict[str, Any],
    now: datetime,
    rank: int,
) -> dict[str, Any]:
    """Build the per-capsule salience explanation block (§4a).

    All values are in human-readable, natural-direction form — negation
    and inversion are internal to the sort, not exposed here.
    """
    capsule = row.get("capsule", row)
    return {
        "rank": rank,
        "sort_key": {
            "lifecycle_rank": _lifecycle_rank(capsule),
            "health_rank": _health_rank(row),
            "freshness_rank": _freshness_rank(capsule, now),
            "resume_adequate": _resume_adequate(capsule),
            "verification_rank": _verification_rank(capsule),
            "updated_age_seconds": _updated_age_seconds(capsule, now),
        },
    }


def _salience_metadata(
    sorted_capsules: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any] | None:
    """Build aggregate salience metadata across all returned capsules (§4b).

    Returns ``None`` when *sorted_capsules* is empty (``present=false``).
    """
    if not sorted_capsules:
        return None

    best_lifecycle = SALIENCE_LIFECYCLE_NO_DESCRIPTOR
    worst_health = 0
    worst_freshness = 0
    all_adequate = True

    for row in sorted_capsules:
        capsule = row.get("capsule", row)
        lr = _lifecycle_rank(capsule)
        if lr < best_lifecycle:
            best_lifecycle = lr
        hr = _health_rank(row)
        if hr > worst_health:
            worst_health = hr
        fr = _freshness_rank(capsule, now)
        if fr > worst_freshness:
            worst_freshness = fr
        if not _resume_adequate(capsule):
            all_adequate = False

    return {
        "sort_applied": True,
        "capsule_count": len(sorted_capsules),
        "best_lifecycle_rank": best_lifecycle,
        "worst_health_rank": worst_health,
        "worst_freshness_rank": worst_freshness,
        "all_resume_adequate": all_adequate,
    }


def _salience_sort(
    loaded: list[dict[str, Any]],
    now: datetime,
) -> list[dict[str, Any]]:
    """Sort loaded capsule rows by salience key (§2).

    Returns a new list — does not mutate the input.
    """
    return sorted(loaded, key=lambda row: _salience_sort_key(row, now))
