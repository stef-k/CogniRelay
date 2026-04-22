"""Canonical runtime hook orchestration for issue #215 slice 2."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from app.models import (
    ContextRetrieveRequest,
    ContinuityCapsule,
    ContinuityReadRequest,
    ContinuityUpsertRequest,
    CoordinationHandoffCreateRequest,
    SessionEndSnapshot,
)

_ELIGIBLE_FIELD_ORDER = (
    "top_priorities",
    "open_loops",
    "active_constraints",
    "active_concerns",
    "drift_signals",
    "stance_summary",
    "negative_decisions",
    "session_trajectory",
    "rationale_entries",
    "stable_preferences",
    "thread_descriptor.lifecycle",
    "thread_descriptor.superseded_by",
)

_SNAPSHOT_FIELD_SET = frozenset(
    {
        "open_loops",
        "top_priorities",
        "active_constraints",
        "stance_summary",
        "negative_decisions",
        "session_trajectory",
        "rationale_entries",
    }
)


class HookLocalStep(str, Enum):
    """Closed local continuity outcomes for write-capable hooks."""

    SKIPPED = "skipped"
    WROTE = "wrote"


@dataclass(frozen=True)
class HookExecutionDependencies:
    """Runtime callables used by canonical hook orchestration."""

    continuity_read: Callable[[ContinuityReadRequest, Any], dict[str, Any]]
    context_retrieve: Callable[[ContextRetrieveRequest, Any], dict[str, Any]]
    continuity_upsert: Callable[[ContinuityUpsertRequest, Any], dict[str, Any]]
    handoff_create: Callable[[CoordinationHandoffCreateRequest, Any], dict[str, Any]]


@dataclass(frozen=True)
class HookWriteResult:
    """Deterministic write-capable hook result."""

    local_step: HookLocalStep
    changed_fields: list[str]
    used_session_end_snapshot: bool = False
    handoff_created: bool = False
    continuity_result: dict[str, Any] | None = None
    handoff_result: dict[str, Any] | None = None


def _capsule_dict(capsule: ContinuityCapsule | dict[str, Any] | None) -> dict[str, Any] | None:
    if capsule is None:
        return None
    if isinstance(capsule, dict):
        return capsule
    return capsule.model_dump(mode="json")


def _eligible_field_values(capsule: ContinuityCapsule | dict[str, Any] | None) -> dict[str, Any]:
    payload = _capsule_dict(capsule)
    if payload is None:
        return {
            "top_priorities": [],
            "open_loops": [],
            "active_constraints": [],
            "active_concerns": [],
            "drift_signals": [],
            "stance_summary": "",
            "negative_decisions": [],
            "session_trajectory": [],
            "rationale_entries": [],
            "stable_preferences": [],
            "thread_descriptor.lifecycle": None,
            "thread_descriptor.superseded_by": None,
        }

    continuity = payload.get("continuity")
    if not isinstance(continuity, dict):
        continuity = {}
    thread_descriptor = payload.get("thread_descriptor")
    if not isinstance(thread_descriptor, dict):
        thread_descriptor = {}
    return {
        "top_priorities": list(continuity.get("top_priorities") or []),
        "open_loops": list(continuity.get("open_loops") or []),
        "active_constraints": list(continuity.get("active_constraints") or []),
        "active_concerns": list(continuity.get("active_concerns") or []),
        "drift_signals": list(continuity.get("drift_signals") or []),
        "stance_summary": continuity.get("stance_summary", ""),
        "negative_decisions": list(continuity.get("negative_decisions") or []),
        "session_trajectory": list(continuity.get("session_trajectory") or []),
        "rationale_entries": list(continuity.get("rationale_entries") or []),
        "stable_preferences": list(payload.get("stable_preferences") or []),
        "thread_descriptor.lifecycle": thread_descriptor.get("lifecycle"),
        "thread_descriptor.superseded_by": thread_descriptor.get("superseded_by"),
    }


def _changed_eligible_fields(
    candidate: ContinuityCapsule,
    persisted_capsule: dict[str, Any] | None,
) -> list[str]:
    candidate_values = _eligible_field_values(candidate)
    persisted_values = _eligible_field_values(persisted_capsule)
    changed_fields: list[str] = []
    for field_name in _ELIGIBLE_FIELD_ORDER:
        if candidate_values[field_name] != persisted_values[field_name]:
            changed_fields.append(field_name)
    return changed_fields


def _last_persisted_capsule(candidate: ContinuityCapsule, auth: Any, deps: HookExecutionDependencies) -> dict[str, Any] | None:
    result = deps.continuity_read(
        ContinuityReadRequest(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            allow_fallback=True,
        ),
        auth,
    )
    capsule = result.get("capsule")
    return capsule if isinstance(capsule, dict) else None


def execute_startup_hook(
    *,
    subject_kind: str,
    subject_id: str,
    auth: Any,
    deps: HookExecutionDependencies,
) -> dict[str, Any]:
    """Execute the canonical startup hook with the closed read contract."""
    return deps.continuity_read(
        ContinuityReadRequest(
            subject_kind=subject_kind,
            subject_id=subject_id,
            allow_fallback=True,
            view="startup",
        ),
        auth,
    )


def execute_pre_prompt_hook(
    *,
    req: ContextRetrieveRequest,
    auth: Any,
    deps: HookExecutionDependencies,
) -> dict[str, Any]:
    """Execute the canonical pre_prompt hook as a read-only retrieval step."""
    return deps.context_retrieve(req, auth)


def execute_post_prompt_hook(
    *,
    capsule: ContinuityCapsule,
    auth: Any,
    deps: HookExecutionDependencies,
) -> HookWriteResult:
    """Execute the canonical post_prompt hook with closed write eligibility."""
    persisted_capsule = _last_persisted_capsule(capsule, auth, deps)
    changed_fields = _changed_eligible_fields(capsule, persisted_capsule)
    if not changed_fields:
        return HookWriteResult(local_step=HookLocalStep.SKIPPED, changed_fields=[])
    result = deps.continuity_upsert(
        ContinuityUpsertRequest(
            subject_kind=capsule.subject_kind,
            subject_id=capsule.subject_id,
            capsule=capsule,
        ),
        auth,
    )
    return HookWriteResult(
        local_step=HookLocalStep.WROTE,
        changed_fields=changed_fields,
        continuity_result=result,
    )


def execute_pre_compaction_or_handoff_hook(
    *,
    capsule: ContinuityCapsule,
    auth: Any,
    deps: HookExecutionDependencies,
    session_end_snapshot: SessionEndSnapshot | None = None,
    real_handoff: CoordinationHandoffCreateRequest | None = None,
) -> HookWriteResult:
    """Execute the canonical pre_compaction_or_handoff hook in closed order."""
    persisted_capsule = _last_persisted_capsule(capsule, auth, deps)
    changed_fields = _changed_eligible_fields(capsule, persisted_capsule)
    continuity_result: dict[str, Any] | None = None
    used_snapshot = False

    if changed_fields:
        use_snapshot = False
        if session_end_snapshot is not None:
            outside_snapshot = [field_name for field_name in changed_fields if field_name not in _SNAPSHOT_FIELD_SET]
            use_snapshot = not outside_snapshot
        continuity_result = deps.continuity_upsert(
            ContinuityUpsertRequest(
                subject_kind=capsule.subject_kind,
                subject_id=capsule.subject_id,
                capsule=capsule,
                session_end_snapshot=session_end_snapshot if use_snapshot else None,
            ),
            auth,
        )
        local_step = HookLocalStep.WROTE
        used_snapshot = use_snapshot
    else:
        local_step = HookLocalStep.SKIPPED

    handoff_created = False
    handoff_result: dict[str, Any] | None = None
    if real_handoff is not None and real_handoff.recipient_peer != getattr(auth, "peer_id", None):
        handoff_result = deps.handoff_create(real_handoff, auth)
        handoff_created = True

    return HookWriteResult(
        local_step=local_step,
        changed_fields=changed_fields,
        used_session_end_snapshot=used_snapshot,
        handoff_created=handoff_created,
        continuity_result=continuity_result,
        handoff_result=handoff_result,
    )
