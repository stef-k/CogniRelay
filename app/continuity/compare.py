"""Capsule comparison and verification signal helpers."""

from __future__ import annotations

from typing import Any

from app.continuity.constants import (
    CONTINUITY_COMPARE_IGNORED_FIELDS,
    CONTINUITY_COMPARE_NESTED_ORDERS,
    CONTINUITY_COMPARE_TOP_LEVEL_ORDER,
    CONTINUITY_SIGNAL_RANK,
)
from app.models import ContinuityVerificationSignal


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


def _strongest_signal_kind(signals: list[ContinuityVerificationSignal]) -> str:
    """Return the strongest verification signal kind preserving request order on ties."""
    strongest = signals[0].kind
    for signal in signals[1:]:
        if CONTINUITY_SIGNAL_RANK[signal.kind] > CONTINUITY_SIGNAL_RANK[strongest]:
            strongest = signal.kind
    return strongest


def _signals_to_evidence_refs(signals: list[ContinuityVerificationSignal]) -> list[str]:
    """Derive bounded evidence refs from ordered verification signals."""
    return [signal.source_ref for signal in signals[:4]]
