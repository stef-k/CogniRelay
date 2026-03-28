"""Continuity revalidation state-transition policy.

Encapsulates the outcome-based capsule resolution and the verification
state-machine transitions (confirm / correct / degrade / conflict).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.models import (
    ContinuityCapsule,
    ContinuityCapsuleHealth,
    ContinuityVerificationState,
)

from app.continuity.compare import _compare_capsules
from app.continuity.validation import _normalize_compare_payload


def _resolve_revalidation_capsule(
    *,
    outcome: str,
    active: ContinuityCapsule,
    candidate: ContinuityCapsule | None,
    repo_root: Path,
) -> tuple[ContinuityCapsule, str, bool, list[str]]:
    """Determine which capsule to persist based on the requested outcome.

    Returns ``(final_capsule, result_outcome, updated, compare_changed_fields)``.
    *result_outcome* may differ from *outcome* when a ``correct`` request
    finds no actual diff (promoted to ``confirm``).
    """
    if outcome == "correct":
        if candidate is None:
            raise HTTPException(status_code=400, detail="candidate_capsule is required for outcome=correct")
        compare_changed_fields = _compare_capsules(
            _normalize_compare_payload(repo_root, active),
            _normalize_compare_payload(repo_root, candidate),
        )
        if not compare_changed_fields:
            return active.model_copy(deep=True), "confirm", False, []
        return candidate.model_copy(deep=True), "correct", True, compare_changed_fields

    return active.model_copy(deep=True), outcome, False, []


def _apply_verification_outcome(
    capsule: ContinuityCapsule,
    *,
    result_outcome: str,
    now_iso: str,
    strongest_signal: str,
    derived_status: str,
    evidence_refs: list[dict[str, Any]],
    reason: str | None,
) -> None:
    """Apply verification state-machine transition to *capsule* in place.

    Sets ``verified_at``, ``verification_kind``, ``verification_state``,
    and ``capsule_health`` according to the resolved *result_outcome*.
    """
    capsule.verified_at = now_iso
    capsule.verification_kind = strongest_signal  # type: ignore[assignment]

    if result_outcome == "conflict":
        capsule.verification_state = ContinuityVerificationState.model_validate({
            "status": "conflicted",
            "last_revalidated_at": now_iso,
            "strongest_signal": strongest_signal,
            "evidence_refs": evidence_refs,
            "conflict_summary": reason,
        })
        capsule.capsule_health = ContinuityCapsuleHealth.model_validate({
            "status": "conflicted",
            "reasons": [reason],
            "last_checked_at": now_iso,
        })
    elif result_outcome == "degrade":
        capsule.verification_state = ContinuityVerificationState.model_validate({
            "status": derived_status,
            "last_revalidated_at": now_iso,
            "strongest_signal": strongest_signal,
            "evidence_refs": evidence_refs,
        })
        capsule.capsule_health = ContinuityCapsuleHealth.model_validate({
            "status": "degraded",
            "reasons": [reason],
            "last_checked_at": now_iso,
        })
    else:
        capsule.verification_state = ContinuityVerificationState.model_validate({
            "status": derived_status,
            "last_revalidated_at": now_iso,
            "strongest_signal": strongest_signal,
            "evidence_refs": evidence_refs,
        })
        capsule.capsule_health = ContinuityCapsuleHealth.model_validate({
            "status": "healthy",
            "reasons": [],
            "last_checked_at": now_iso,
        })
