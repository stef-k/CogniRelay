"""Canonical runtime hook orchestration for issue #215 slice 2."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

from fastapi import HTTPException

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
_ELIGIBLE_FIELD_PATHS: dict[str, tuple[str, ...]] = {
    "top_priorities": ("continuity", "top_priorities"),
    "open_loops": ("continuity", "open_loops"),
    "active_constraints": ("continuity", "active_constraints"),
    "active_concerns": ("continuity", "active_concerns"),
    "drift_signals": ("continuity", "drift_signals"),
    "stance_summary": ("continuity", "stance_summary"),
    "negative_decisions": ("continuity", "negative_decisions"),
    "session_trajectory": ("continuity", "session_trajectory"),
    "rationale_entries": ("continuity", "rationale_entries"),
    "stable_preferences": ("stable_preferences",),
    "thread_descriptor.lifecycle": ("thread_descriptor", "lifecycle"),
    "thread_descriptor.superseded_by": ("thread_descriptor", "superseded_by"),
}

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


_MISSING = object()
_FIRST_WRITE_BASELINE: dict[str, Any] = {
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


def _compare_value(value: Any) -> Any:
    if value is _MISSING or value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_compare_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _compare_value(item) for key, item in value.items()}
    fields_set = getattr(value, "model_fields_set", None)
    if isinstance(fields_set, set):
        return {field_name: _compare_value(getattr(value, field_name)) for field_name in fields_set}
    return value


def _raw_field_value(payload: Any, field_name: str) -> Any:
    current = payload
    for part in _ELIGIBLE_FIELD_PATHS[field_name]:
        if current is None:
            return _MISSING
        if isinstance(current, dict):
            if part not in current:
                return _MISSING
            current = current[part]
            continue
        fields_set = getattr(current, "model_fields_set", None)
        if isinstance(fields_set, set) and part not in fields_set:
            return _MISSING
        if not hasattr(current, part):
            return _MISSING
        current = getattr(current, part)
    return _compare_value(current)


def _baseline_normalized_value(field_name: str, raw_value: Any) -> Any:
    baseline = _FIRST_WRITE_BASELINE[field_name]
    if raw_value is _MISSING or raw_value is None:
        return baseline
    return raw_value


def _effective_candidate(
    capsule: ContinuityCapsule,
    session_end_snapshot: SessionEndSnapshot | None,
) -> ContinuityCapsule:
    candidate = capsule.model_copy(deep=True)
    if session_end_snapshot is None:
        return candidate
    candidate.continuity.open_loops = list(session_end_snapshot.open_loops)
    candidate.continuity.top_priorities = list(session_end_snapshot.top_priorities)
    candidate.continuity.active_constraints = list(session_end_snapshot.active_constraints)
    candidate.continuity.stance_summary = session_end_snapshot.stance_summary
    if session_end_snapshot.negative_decisions is not None:
        candidate.continuity.negative_decisions = list(session_end_snapshot.negative_decisions)
    if session_end_snapshot.session_trajectory is not None:
        candidate.continuity.session_trajectory = list(session_end_snapshot.session_trajectory)
    if session_end_snapshot.rationale_entries is not None:
        candidate.continuity.rationale_entries = list(session_end_snapshot.rationale_entries)
    return candidate


def _changed_eligible_fields(
    candidate: ContinuityCapsule,
    persisted_capsule: dict[str, Any] | None,
    session_end_snapshot: SessionEndSnapshot | None = None,
) -> list[str]:
    effective_candidate = _effective_candidate(candidate, session_end_snapshot)
    changed_fields: list[str] = []
    for field_name in _ELIGIBLE_FIELD_ORDER:
        candidate_value = _raw_field_value(effective_candidate, field_name)
        if persisted_capsule is None:
            if _baseline_normalized_value(field_name, candidate_value) != _FIRST_WRITE_BASELINE[field_name]:
                changed_fields.append(field_name)
            continue
        persisted_value = _raw_field_value(persisted_capsule, field_name)
        if candidate_value != persisted_value:
            changed_fields.append(field_name)
    return changed_fields


def _persisted_thread_descriptor_values(persisted_capsule: dict[str, Any] | None) -> tuple[str | None, str | None]:
    if persisted_capsule is None:
        return None, None
    thread_descriptor = persisted_capsule.get("thread_descriptor")
    if not isinstance(thread_descriptor, dict):
        return None, None
    lifecycle = thread_descriptor.get("lifecycle")
    superseded_by = thread_descriptor.get("superseded_by")
    return lifecycle if isinstance(lifecycle, str) else None, superseded_by if isinstance(superseded_by, str) else None


def _candidate_thread_descriptor_values(candidate: ContinuityCapsule) -> tuple[str | None, str | None]:
    if candidate.thread_descriptor is None:
        return None, None
    return candidate.thread_descriptor.lifecycle, candidate.thread_descriptor.superseded_by


def _hook_upsert_request(
    *,
    capsule: ContinuityCapsule,
    persisted_capsule: dict[str, Any] | None,
    changed_fields: list[str],
    session_end_snapshot: SessionEndSnapshot | None = None,
) -> ContinuityUpsertRequest:
    lifecycle_changed = "thread_descriptor.lifecycle" in changed_fields
    superseded_by_changed = "thread_descriptor.superseded_by" in changed_fields
    lifecycle_transition: str | None = None
    superseded_by: str | None = None

    if lifecycle_changed or superseded_by_changed:
        stored_lifecycle, stored_superseded_by = _persisted_thread_descriptor_values(persisted_capsule)
        candidate_lifecycle, candidate_superseded_by = _candidate_thread_descriptor_values(capsule)

        if persisted_capsule is None:
            if candidate_lifecycle == "suspended":
                lifecycle_transition = "suspend"
            elif candidate_lifecycle == "concluded":
                lifecycle_transition = "conclude"
            elif candidate_lifecycle == "superseded":
                lifecycle_transition = "supersede"
                superseded_by = candidate_superseded_by
            elif candidate_lifecycle not in (None, "active"):
                raise HTTPException(status_code=400, detail="hook lifecycle delta is not persistable through continuity.upsert")
        else:
            effective_stored_lifecycle = stored_lifecycle or "active"
            effective_candidate_lifecycle = candidate_lifecycle or "active"

            if effective_stored_lifecycle != effective_candidate_lifecycle:
                transition_map = {
                    ("active", "suspended"): "suspend",
                    ("active", "concluded"): "conclude",
                    ("active", "superseded"): "supersede",
                    ("suspended", "active"): "resume",
                    ("suspended", "concluded"): "conclude",
                    ("suspended", "superseded"): "supersede",
                }
                lifecycle_transition = transition_map.get((effective_stored_lifecycle, effective_candidate_lifecycle))
                if lifecycle_transition is None:
                    raise HTTPException(status_code=400, detail="hook lifecycle delta is not persistable through continuity.upsert")
                if lifecycle_transition == "supersede":
                    superseded_by = candidate_superseded_by
            elif superseded_by_changed and candidate_superseded_by != stored_superseded_by:
                raise HTTPException(status_code=400, detail="hook superseded_by delta requires a lifecycle transition")

    return ContinuityUpsertRequest(
        subject_kind=capsule.subject_kind,
        subject_id=capsule.subject_id,
        capsule=capsule,
        session_end_snapshot=session_end_snapshot,
        lifecycle_transition=lifecycle_transition,
        superseded_by=superseded_by,
    )


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
        _hook_upsert_request(
            capsule=capsule,
            persisted_capsule=persisted_capsule,
            changed_fields=changed_fields,
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
    changed_fields = _changed_eligible_fields(capsule, persisted_capsule, session_end_snapshot)
    continuity_result: dict[str, Any] | None = None
    used_snapshot = False

    if changed_fields:
        use_snapshot = False
        if session_end_snapshot is not None:
            outside_snapshot = [field_name for field_name in changed_fields if field_name not in _SNAPSHOT_FIELD_SET]
            use_snapshot = not outside_snapshot
        continuity_result = deps.continuity_upsert(
            _hook_upsert_request(
                capsule=capsule,
                persisted_capsule=persisted_capsule,
                changed_fields=changed_fields,
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
