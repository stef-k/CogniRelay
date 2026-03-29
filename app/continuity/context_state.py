"""Policy clusters for build_continuity_state.

Encapsulates multi-selector loading with fallback cascade,
verification-policy filtering, per-capsule trust-budget allocation
with trim, and aggregate trust-signal assembly.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.auth import AuthContext
from app.models import ContextRetrieveRequest

from app.continuity.constants import (
    CONTINUITY_HEALTH_ORDER,
    CONTINUITY_WARNING_ACTIVE_INVALID,
    CONTINUITY_WARNING_ACTIVE_MISSING,
    CONTINUITY_WARNING_CONFLICTED,
    CONTINUITY_WARNING_DEGRADED,
    CONTINUITY_WARNING_FALLBACK_MISSING,
    CONTINUITY_WARNING_FALLBACK_USED,
    CONTINUITY_WARNING_INVALID,
    CONTINUITY_WARNING_SALIENCE_OMITTED,
    CONTINUITY_WARNING_TRUST_SIGNALS_AGGREGATE_FAILED,
    CONTINUITY_WARNING_TRUST_SIGNALS_COMPACT,
    CONTINUITY_WARNING_TRUST_SIGNALS_FAILED,
    CONTINUITY_WARNING_TRUNCATED_MULTI,
    _SALIENCE_NULL_OVERHEAD_TOKENS,
    _TRUST_SIGNALS_NULL_OVERHEAD_TOKENS,
)
from app.continuity.freshness import (
    _capsule_health_summary,
    _continuity_phase,
    _verification_status,
)
from app.continuity.paths import (
    continuity_fallback_rel_path,
    continuity_rel_path,
)
from app.continuity.persistence import (
    _load_capsule,
    _load_fallback_snapshot,
)
from app.continuity.retrieval import (
    _format_selector,
    _qualify_warning,
)
from app.continuity.trimming import (
    _estimated_tokens,
    _render_value,
    _trim_capsule,
)
from app.continuity.salience import (
    _salience_block,
    _salience_sort,
)
from app.continuity.trust import (
    _build_aggregate_trust_signals,
    _build_compact_trust_signals,
    _build_trust_signals,
)

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1) Multi-selector loading with fallback cascade
# ---------------------------------------------------------------------------


def _load_selectors_with_fallback(
    *,
    repo_root: Path,
    auth: AuthContext,
    selectors: list[dict[str, Any]],
    req: ContextRetrieveRequest,
    now: datetime,
    multi_warning_mode: bool,
    omitted_selectors: list[str],
) -> tuple[list[dict[str, Any]], list[str], list[str], bool]:
    """Load capsules for resolved selectors, falling back per resilience policy.

    Returns ``(loaded, warnings, recovery_warnings, fallback_used)``.
    Appends omitted-selector labels to *omitted_selectors* in place.
    """
    loaded: list[dict[str, Any]] = []
    warnings: list[str] = []
    recovery_warnings: list[str] = []
    fallback_used = False

    for item in selectors:
        kind = item["subject_kind"]
        subject_id = item["subject_id"]
        rel = continuity_rel_path(kind, subject_id)
        try:
            auth.require_read_path(rel)
        except HTTPException as auth_exc:
            if auth_exc.status_code == 403:
                selector_label = _format_selector(kind, subject_id)
                recovery_warnings.append(_qualify_warning(CONTINUITY_WARNING_ACTIVE_MISSING, kind, subject_id, multi_mode=multi_warning_mode) + " (owner only)")
                omitted_selectors.append(selector_label)
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
                omitted_selectors.append(selector_label)
                continue
            if resilience_policy not in {"allow_fallback", "prefer_active"}:
                raise HTTPException(status_code=400, detail="Unsupported continuity_resilience_policy")
            fallback_rel = continuity_fallback_rel_path(kind, subject_id)
            try:
                auth.require_read_path(fallback_rel)
            except HTTPException as fallback_auth_exc:
                if fallback_auth_exc.status_code == 403:
                    recovery_warnings.append(_qualify_warning(CONTINUITY_WARNING_FALLBACK_MISSING, kind, subject_id, multi_mode=multi_warning_mode) + " (owner only)")
                    omitted_selectors.append(selector_label)
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
                        omitted_selectors.append(selector_label)
                        continue
                    if len(selectors) > 1:
                        warnings.append(_qualify_warning(CONTINUITY_WARNING_INVALID, kind, subject_id, multi_mode=multi_warning_mode))
                        omitted_selectors.append(selector_label)
                        continue
                    raise exc
                raise
        # --- thread descriptor superseded warning ---
        td = capsule.get("thread_descriptor") if isinstance(capsule, dict) else None
        if td and td.get("lifecycle") == "superseded":
            sid = capsule.get("subject_id", "unknown") if isinstance(capsule, dict) else "unknown"
            sby = td.get("superseded_by", "unknown")
            recovery_warnings.append(
                _qualify_warning(
                    f"continuity_capsule_superseded:thread:{sid}\u2192{sby}",
                    kind,
                    subject_id,
                    multi_mode=multi_warning_mode,
                )
            )
        phase, phase_warnings = _continuity_phase(capsule, now)
        warnings.extend(_qualify_warning(warning, kind, subject_id, multi_mode=multi_warning_mode) for warning in phase_warnings)
        if phase in {"expired", "expired_by_age"}:
            omitted_selectors.append(_format_selector(kind, subject_id))
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

    return loaded, warnings, recovery_warnings, fallback_used


# ---------------------------------------------------------------------------
# 2) Verification-policy filtering
# ---------------------------------------------------------------------------


def _filter_by_verification_policy(
    loaded: list[dict[str, Any]],
    policy: str | None,
    omitted_selectors: list[str],
) -> list[dict[str, Any]]:
    """Apply verification-policy filtering/sorting to loaded capsules.

    Returns the filtered/sorted list.  Appends omitted-selector labels
    to *omitted_selectors* in place.
    """
    if policy == "prefer_healthy":
        return sorted(
            loaded,
            key=lambda row: CONTINUITY_HEALTH_ORDER.get(str(row["health_status"]), CONTINUITY_HEALTH_ORDER["conflicted"]),
        )
    if policy == "require_healthy":
        filtered: list[dict[str, Any]] = []
        for row in loaded:
            if row["health_status"] == "healthy":
                filtered.append(row)
                continue
            selector = row["selector"]
            omitted_selectors.append(_format_selector(selector["subject_kind"], selector["subject_id"]))
        return filtered
    return loaded


# ---------------------------------------------------------------------------
# 3) Per-capsule trust-budget allocation and trim
# ---------------------------------------------------------------------------


def _trim_and_attach_trust(
    *,
    loaded: list[dict[str, Any]],
    reserve: int,
    now: datetime,
    multi_warning_mode: bool,
    omitted_selectors: list[str],
) -> tuple[list[dict[str, Any]], list[str], list[str], list[str]]:
    """Allocate token budget, trim capsules, and attach per-capsule trust and salience signals.

    The *loaded* list must already be in salience-sorted order.  Each
    capsule receives a ``salience`` block (§4a) with its 1-indexed rank
    and sort-key explanation.  If the salience block cannot fit within
    the per-capsule token allocation after trust signals, it is omitted
    and a ``continuity_salience_omitted`` recovery warning is emitted.

    Returns ``(trimmed_capsules, trimmed_selection_order, warnings, recovery_warnings)``.
    Appends omitted-selector labels to *omitted_selectors* in place.
    """
    count = len(loaded)
    base = reserve // count
    remainder = reserve % count

    trimmed_capsules: list[dict[str, Any]] = []
    trimmed_selection_order: list[str] = []
    warnings: list[str] = []
    recovery_warnings: list[str] = []

    # Track the salience rank for capsules that survive trimming.
    # Rank is 1-indexed and assigned in loaded (salience-sorted) order,
    # but only to capsules that make it into the final output.
    salience_rank = 0

    for idx, row in enumerate(loaded):
        allocation = base + (1 if idx < remainder else 0)
        selector = row["selector"]
        kind = selector["subject_kind"]
        subject_id = selector["subject_id"]
        resolution = selector["resolution"]

        # --- Build trust_signals BEFORE trimming to budget honestly ---
        trust_signals_obj, trust_tokens, is_compact, compact_ts, build_failed = _build_per_capsule_trust(
            row=row,
            now=now,
            allocation=allocation,
        )
        if build_failed:
            recovery_warnings.append(_qualify_warning(CONTINUITY_WARNING_TRUST_SIGNALS_FAILED, kind, subject_id, multi_mode=multi_warning_mode))

        # --- Pre-compute salience block and its token cost ---
        salience_rank += 1
        try:
            salience_obj = _salience_block(row, now, rank=salience_rank)
            salience_tokens = _estimated_tokens(_render_value(salience_obj))
        except Exception:
            _logger.warning("salience block build failed; degrading to null", exc_info=True)
            salience_obj = None
            salience_tokens = _SALIENCE_NULL_OVERHEAD_TOKENS
            recovery_warnings.append(_qualify_warning(CONTINUITY_WARNING_SALIENCE_OMITTED, kind, subject_id, multi_mode=multi_warning_mode))

        capsule_allocation = allocation - trust_tokens - salience_tokens
        trimmed, trimmed_fields = _trim_capsule(row["capsule"], capsule_allocation)
        if trimmed is None:
            omitted_selectors.append(_format_selector(kind, subject_id))
            warnings.append(_qualify_warning(CONTINUITY_WARNING_TRUNCATED_MULTI, kind, subject_id, multi_mode=multi_warning_mode))
            continue
        trimmed["source_state"] = row["source_state"]

        # Attach trust_signals — update trimmed/trimmed_fields on full signals,
        # then re-check that the post-mutation cost still fits within allocation.
        _attach_trust_to_trimmed(
            trimmed=trimmed,
            trimmed_fields=trimmed_fields,
            trust_signals_obj=trust_signals_obj,
            is_compact=is_compact,
            compact_ts=compact_ts,
            trust_tokens=trust_tokens,
            allocation=allocation,
            kind=kind,
            subject_id=subject_id,
            multi_warning_mode=multi_warning_mode,
            recovery_warnings=recovery_warnings,
        )

        # Attach salience block.  If the salience block was pre-computed
        # but the overall capsule now exceeds its allocation, drop the
        # salience block and emit a warning.
        if salience_obj is not None:
            total_tokens = _estimated_tokens(_render_value(trimmed)) + salience_tokens
            if total_tokens > allocation:
                trimmed["salience"] = None
                recovery_warnings.append(_qualify_warning(CONTINUITY_WARNING_SALIENCE_OMITTED, kind, subject_id, multi_mode=multi_warning_mode))
            else:
                trimmed["salience"] = salience_obj
        else:
            trimmed["salience"] = None

        trimmed_capsules.append(trimmed)
        trimmed_selection_order.append(f"{resolution}:{kind}:{subject_id}")
        if row["health_status"] == "degraded":
            warnings.append(_qualify_warning(CONTINUITY_WARNING_DEGRADED, kind, subject_id, multi_mode=multi_warning_mode))
        elif row["health_status"] == "conflicted":
            warnings.append(_qualify_warning(CONTINUITY_WARNING_CONFLICTED, kind, subject_id, multi_mode=multi_warning_mode))

    return trimmed_capsules, trimmed_selection_order, warnings, recovery_warnings


def _build_per_capsule_trust(
    *,
    row: dict[str, Any],
    now: datetime,
    allocation: int,
) -> tuple[dict[str, Any] | None, int, bool, dict[str, Any] | None, bool]:
    """Build full and compact trust signals for one capsule, choosing the best fit.

    Returns ``(trust_signals_obj, trust_tokens, is_compact, compact_ts, build_failed)``.
    On failure, returns ``(None, _TRUST_SIGNALS_NULL_OVERHEAD_TOKENS, False, None, True)``.
    """
    trust_signals_obj: dict[str, Any] | None = None
    trust_tokens = _TRUST_SIGNALS_NULL_OVERHEAD_TOKENS
    is_compact = False
    build_failed = False
    compact_ts: dict[str, Any] | None = None
    try:
        full_ts = _build_trust_signals(
            row["capsule"],
            now,
            source_state=row["source_state"],
        )
        compact_ts = _build_compact_trust_signals(
            row["capsule"],
            now,
            source_state=row["source_state"],
        )
        compact_ts_tokens = _estimated_tokens(_render_value(compact_ts))
        full_ts_tokens = _estimated_tokens(_render_value(full_ts))
        if full_ts_tokens < allocation:
            trust_signals_obj = full_ts
            trust_tokens = full_ts_tokens
        elif compact_ts_tokens < allocation:
            trust_signals_obj = compact_ts
            trust_tokens = compact_ts_tokens
            is_compact = True
    except Exception:
        _logger.warning("per-capsule trust_signals failed; degrading to null", exc_info=True)
        build_failed = True
    return trust_signals_obj, trust_tokens, is_compact, compact_ts, build_failed


def _attach_trust_to_trimmed(
    *,
    trimmed: dict[str, Any],
    trimmed_fields: list[str] | None,
    trust_signals_obj: dict[str, Any] | None,
    is_compact: bool,
    compact_ts: dict[str, Any] | None,
    trust_tokens: int,
    allocation: int,
    kind: str,
    subject_id: str,
    multi_warning_mode: bool,
    recovery_warnings: list[str],
) -> None:
    """Attach trust_signals to a trimmed capsule dict, downgrading to compact if needed."""
    if trust_signals_obj is not None:
        if not is_compact and not trust_signals_obj.get("compact"):
            trust_signals_obj["completeness"]["trimmed"] = bool(trimmed_fields)
            trust_signals_obj["completeness"]["trimmed_fields"] = list(trimmed_fields) if trimmed_fields else []
            updated_ts_tokens = _estimated_tokens(_render_value(trust_signals_obj))
            if updated_ts_tokens > trust_tokens and compact_ts is not None:
                compact_ts["completeness"]["trimmed"] = bool(trimmed_fields)
                fallback_compact_tokens = _estimated_tokens(_render_value(compact_ts))
                capsule_tokens = _estimated_tokens(_render_value(trimmed))
                if capsule_tokens + updated_ts_tokens > allocation and capsule_tokens + fallback_compact_tokens <= allocation:
                    trust_signals_obj = compact_ts
                    recovery_warnings.append(_qualify_warning(CONTINUITY_WARNING_TRUST_SIGNALS_COMPACT, kind, subject_id, multi_mode=multi_warning_mode))
        elif is_compact:
            trust_signals_obj["completeness"]["trimmed"] = bool(trimmed_fields)
            recovery_warnings.append(_qualify_warning(CONTINUITY_WARNING_TRUST_SIGNALS_COMPACT, kind, subject_id, multi_mode=multi_warning_mode))
        trimmed["trust_signals"] = trust_signals_obj
    else:
        trimmed["trust_signals"] = None


# ---------------------------------------------------------------------------
# 4) Aggregate trust-signal assembly
# ---------------------------------------------------------------------------


def _assemble_aggregate_trust(
    trimmed_capsules: list[dict[str, Any]],
    *,
    selectors_requested: int,
    selectors_returned: int,
    selectors_omitted: int,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Build the aggregate trust-signals block from per-capsule signals.

    Returns ``(aggregate_trust, recovery_warnings)``.
    """
    recovery_warnings: list[str] = []
    per_capsule_signals = [c["trust_signals"] for c in trimmed_capsules if c.get("trust_signals") is not None]
    if not per_capsule_signals:
        return None, recovery_warnings
    try:
        aggregate_trust = _build_aggregate_trust_signals(
            per_capsule_signals,
            selectors_requested=selectors_requested,
            selectors_returned=selectors_returned,
            selectors_omitted=selectors_omitted,
        )
    except Exception:
        _logger.warning("aggregate trust_signals failed; degrading to null", exc_info=True)
        aggregate_trust = None
        recovery_warnings.append(CONTINUITY_WARNING_TRUST_SIGNALS_AGGREGATE_FAILED)
    return aggregate_trust, recovery_warnings
