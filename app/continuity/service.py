"""Continuity capsule validation, storage, and retrieval shaping."""

from __future__ import annotations

import hashlib
import gzip
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from fastapi import HTTPException
from app.auth import AuthContext
from app.timestamps import parse_iso as _parse_iso, format_iso, format_compact, iso_now
from app.lifecycle_warnings import make_error_detail, make_warning
from app.storage import build_cold_gzip_bytes
from app.git_locking import repository_mutation_lock
from app.git_manager import GitManager
from app.git_safety import try_commit_paths, try_unstage_paths
from app.models import (
    ContinuityArchiveRequest,
    ContinuityColdRehydrateRequest,
    ContinuityColdStoreRequest,
    ContinuityCapsule,
    ContinuityCompareRequest,
    ContinuityDeleteRequest,
    ContinuityLifecycleRequest,
    ContinuityListRequest,
    ContinuityPatchRequest,
    ContinuityReadRequest,
    ContinuityRetentionApplyRequest,
    ContinuityRetentionPlanRequest,
    ContinuityRefreshPlanRequest,
    ContinuityRevalidateRequest,
    ContinuityUpsertRequest,
    IdentityAnchor,
    NegativeDecision,
    PatchOperation,
    RationaleEntry,
    SessionEndSnapshot,
    StablePreference,
    ContextRetrieveRequest,
)
from app.storage import canonical_json, safe_path, write_bytes_file, write_text_file
from app.continuity.constants import (
    CAPSULE_SIZE_LIMIT_BYTES,
    CONTINUITY_ARCHIVE_SCHEMA_TYPE,
    CONTINUITY_ARCHIVE_SCHEMA_VERSION,
    CONTINUITY_DIR_REL,
    CONTINUITY_REFRESH_STATE_REL,
    CONTINUITY_RETENTION_ARCHIVE_DAYS,
    CONTINUITY_RETENTION_STATE_REL,
    CONTINUITY_SIGNAL_STATUS,
    CONTINUITY_STATE_METADATA_FILES,
    CONTINUITY_WARNING_ACTIVE_INVALID,
    CONTINUITY_WARNING_ACTIVE_MISSING,
    CONTINUITY_WARNING_FALLBACK_MISSING,
    CONTINUITY_WARNING_FALLBACK_USED,
    CONTINUITY_WARNING_FALLBACK_WRITE_FAILED,
    CONTINUITY_WARNING_STARTUP_SUMMARY_BUILD_FAILED,
    CONTINUITY_WARNING_TRUST_SIGNALS_FAILED,
    PRESERVE_CAPSULE_DICT_FIELDS,
    PRESERVE_CAPSULE_LIST_FIELDS,
    PRESERVE_CAPSULE_OBJECT_FIELDS,
    PRESERVE_OPTIONAL_LIST_CONTINUITY_FIELDS,
    PRESERVE_OPTIONAL_OBJECT_CONTINUITY_FIELDS,
    PRESERVE_REQUIRED_LIST_CONTINUITY_FIELDS,
    PATCH_STRING_LIST_TARGETS,
    PATCH_STRUCTURED_LIST_TARGETS,
    PATCH_STRUCTURED_MATCH_KEYS,
    PATCH_TARGET_MAX_LENGTH,
    PATCH_THREAD_DESCRIPTOR_TARGETS,
    PRESERVE_THREAD_DESCRIPTOR_LIST_FIELDS,
    THREAD_LIFECYCLE_TRANSITIONS,
    THREAD_LIFECYCLE_TRANSITION_TARGETS,
)
from app.continuity.paths import (
    _archive_rel_path_from_envelope,
    _continuity_subject_lock,
    _normalize_subject_id,
    _validate_archive_rel_path,
    continuity_cold_storage_rel_path,
    continuity_cold_stub_rel_path,
    continuity_fallback_rel_path,
    continuity_rel_path,
)
from app.continuity.validation import (
    _final_capsule_payload,
    _normalize_capsule_fields,
    _normalize_compare_payload,
    _strip_service_managed_descriptor_fields,
    _strip_verification_fields_for_upsert,
    _validate_capsule,
    _validate_candidate_selector_match,
    _validate_lifecycle_transition_request,
    _validate_verification_signals,
)
from app.continuity.compare import (
    _compare_capsules,
    _signals_to_evidence_refs,
    _strongest_signal_kind,
)
from app.continuity.persistence import (
    _delete_commit_message,
    _load_archive_envelope,
    _load_capsule,
    _load_fallback_envelope_payload,
    _load_fallback_snapshot,
    _persist_active_capsule,
    _persist_fallback_snapshot,
    _reject_stale_or_conflicting_write,
    _reject_stale_timestamp,
    _restore_failed_archive,
    _restore_failed_cold_store,
    _restore_failed_refresh_state,
    _restore_failed_retention_state,
)
from app.continuity.retention import (
    _is_archive_stale,
    _retention_cold_state,
    _retention_plan_payload,
    _retention_warning_sort_key,
    _scan_retention_candidates,
)
from app.continuity.cold import _build_cold_stub_text, _load_cold_stub
from app.continuity.context_state import (
    _assemble_aggregate_trust,
    _filter_by_verification_policy,
    _load_selectors_with_fallback,
    _trim_and_attach_trust,
)
from app.continuity.salience import (
    _salience_metadata,
    _salience_sort,
)
from app.continuity.revalidation import (
    _apply_verification_outcome,
    _resolve_revalidation_capsule,
)
from app.continuity.listing import (
    _matches_thread_filters,
    _scan_active_summaries,
    _scan_archive_summaries,
    _scan_cold_summaries,
    _scan_fallback_summaries,
)
from app.continuity.retrieval import (
    _effective_selectors,
    _selector_key,
    _warning_mode_is_multi,
)
from app.continuity.freshness import (
    _capsule_health_summary,
    _verification_status,
)
from app.continuity.refresh import (
    _audit_recent_selectors,
    _refresh_priority,
    _refresh_reason_codes,
    _refresh_state_payload,
)
from app.continuity.trimming import (
    _budget,
    _estimated_tokens,
    _render_value,
)
from app.continuity.trust import (
    _build_startup_summary,
    _build_trust_signals,
    _compute_resume_quality,
)

_logger = logging.getLogger(__name__)


def _default_empty(field_name: str, field_type: str) -> Any:
    """Return the type-appropriate empty value for a merge-eligible field.

    *field_type* is one of ``"list"``, ``"object"``, ``"dict"``.
    """
    if field_type == "list":
        return []
    if field_type == "dict":
        return {}
    return None  # object


def _apply_preserve_merge(
    capsule: ContinuityCapsule,
    stored: dict[str, Any],
    raw_body: dict[str, Any],
    snapshot_touched_fields: frozenset[str],
) -> None:
    """Apply preserve-by-default field-level merge in place.

    For each merge-eligible field, the raw JSON body is inspected to determine
    the caller's intent (absent → preserve, null → clear, present → override).
    Fields touched by session_end_snapshot are treated as explicitly provided.
    """
    raw_capsule = raw_body.get("capsule", {})
    raw_continuity = raw_capsule.get("continuity", {})
    stored_continuity = stored.get("continuity", {})

    # --- ContinuityState required list fields ---
    # [] in raw JSON → preserve stored value; non-empty → override.
    for field_name in PRESERVE_REQUIRED_LIST_CONTINUITY_FIELDS:
        if field_name in snapshot_touched_fields:
            continue  # snapshot already set this
        raw_val = raw_continuity.get(field_name)
        if isinstance(raw_val, list) and len(raw_val) == 0:
            # "Not provided" placeholder — preserve stored value
            stored_val = stored_continuity.get(field_name)
            if stored_val is not None:
                setattr(capsule.continuity, field_name, stored_val)
        # else: non-empty list → already in capsule from Pydantic parse

    # --- ContinuityState optional list fields ---
    for field_name in PRESERVE_OPTIONAL_LIST_CONTINUITY_FIELDS:
        if field_name in snapshot_touched_fields:
            continue
        if field_name not in raw_continuity:
            # Absent → preserve stored value
            stored_val = stored_continuity.get(field_name)
            if stored_val is not None:
                setattr(capsule.continuity, field_name, stored_val)
        elif raw_continuity[field_name] is None:
            # Explicitly null → clear to empty list
            setattr(capsule.continuity, field_name, [])
        # else: value present (including []) → override, already in capsule

    # --- ContinuityState optional object fields ---
    for field_name in PRESERVE_OPTIONAL_OBJECT_CONTINUITY_FIELDS:
        if field_name not in raw_continuity:
            stored_val = stored_continuity.get(field_name)
            if stored_val is not None:
                setattr(capsule.continuity, field_name, stored_val)
        elif raw_continuity[field_name] is None:
            setattr(capsule.continuity, field_name, None)
        # else: present → override

    # --- Capsule-level list fields ---
    for field_name in PRESERVE_CAPSULE_LIST_FIELDS:
        if field_name not in raw_capsule:
            stored_val = stored.get(field_name)
            if stored_val is not None:
                setattr(capsule, field_name, stored_val)
        elif raw_capsule[field_name] is None:
            setattr(capsule, field_name, [])
        # else: override

    # --- Capsule-level object fields (except thread_descriptor) ---
    for field_name in PRESERVE_CAPSULE_OBJECT_FIELDS - {"thread_descriptor"}:
        if field_name not in raw_capsule:
            stored_val = stored.get(field_name)
            if stored_val is not None:
                setattr(capsule, field_name, stored_val)
        elif raw_capsule[field_name] is None:
            setattr(capsule, field_name, None)
        # else: override

    # --- Capsule-level dict fields (metadata) ---
    for field_name in PRESERVE_CAPSULE_DICT_FIELDS:
        if field_name not in raw_capsule:
            stored_val = stored.get(field_name)
            if stored_val is not None:
                setattr(capsule, field_name, stored_val)
        elif raw_capsule[field_name] is None:
            setattr(capsule, field_name, {})
        # else: override

    # --- thread_descriptor sub-field merge ---
    if "thread_descriptor" not in raw_capsule:
        # Absent → preserve entire stored descriptor
        stored_td = stored.get("thread_descriptor")
        if stored_td is not None:
            setattr(capsule, "thread_descriptor", stored_td)
    elif raw_capsule["thread_descriptor"] is None:
        # Explicitly null → clear
        capsule.thread_descriptor = None
    elif capsule.thread_descriptor is not None:
        # Present with value → merge sub-fields individually
        raw_td = raw_capsule["thread_descriptor"]
        stored_td = stored.get("thread_descriptor") or {}
        for td_field in PRESERVE_THREAD_DESCRIPTOR_LIST_FIELDS:
            if td_field not in raw_td:
                stored_sub = stored_td.get(td_field)
                if stored_sub is not None:
                    setattr(capsule.thread_descriptor, td_field, stored_sub)
            elif raw_td[td_field] is None:
                setattr(capsule.thread_descriptor, td_field, [])
            # else: override


def _session_end_snapshot_touched_fields(snapshot: SessionEndSnapshot | None) -> frozenset[str]:
    """Return the set of ContinuityState field names touched by a session-end snapshot."""
    if snapshot is None:
        return frozenset()
    # P0 fields are always touched
    touched: set[str] = {"open_loops", "top_priorities", "active_constraints", "stance_summary"}
    # P1 fields are touched only when not None
    if snapshot.negative_decisions is not None:
        touched.add("negative_decisions")
    if snapshot.session_trajectory is not None:
        touched.add("session_trajectory")
    if snapshot.rationale_entries is not None:
        touched.add("rationale_entries")
    return frozenset(touched)


def _apply_session_end_snapshot(capsule: ContinuityCapsule, snapshot: SessionEndSnapshot) -> None:
    """Merge session-end snapshot fields into capsule.continuity in place.

    P0 fields (always present in the snapshot) unconditionally override.
    P1 fields override only when not None; None means preserve capsule value.
    All other ContinuityState fields are left untouched.
    """
    cont = capsule.continuity
    # P0 — always override
    cont.open_loops = list(snapshot.open_loops)
    cont.top_priorities = list(snapshot.top_priorities)
    cont.active_constraints = list(snapshot.active_constraints)
    cont.stance_summary = snapshot.stance_summary
    # P1 — override only when explicitly provided
    if snapshot.negative_decisions is not None:
        cont.negative_decisions = list(snapshot.negative_decisions)
    if snapshot.session_trajectory is not None:
        cont.session_trajectory = list(snapshot.session_trajectory)
    if snapshot.rationale_entries is not None:
        cont.rationale_entries = list(snapshot.rationale_entries)


def continuity_upsert_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityUpsertRequest,
    raw_body: dict[str, Any] | None = None,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Validate and persist one continuity capsule with commit-on-change behavior."""
    auth.require("write:projects")
    capsule = _strip_verification_fields_for_upsert(req.capsule)
    if capsule.subject_kind != req.subject_kind or capsule.subject_id != req.subject_id:
        raise HTTPException(status_code=400, detail="Capsule subject does not match request subject")
    # Apply session-end snapshot merge before validation and before the
    # subject lock.  The snapshot only mutates capsule.continuity fields —
    # never updated_at or other capsule-level fields — so
    # _reject_stale_or_conflicting_write (which compares updated_at inside
    # the lock) remains correct after this in-memory merge.
    snapshot_applied = False
    if req.session_end_snapshot is not None:
        _apply_session_end_snapshot(capsule, req.session_end_snapshot)
        snapshot_applied = True
    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    auth.require_write_path(rel)
    _validate_lifecycle_transition_request(req)
    _strip_service_managed_descriptor_fields(capsule)
    # Phase 1: validate field bounds and structure on the caller-supplied
    # payload (lifecycle/superseded_by stripped to None).  This catches
    # malformed capsules early, before acquiring the subject lock.
    # The size check inside _validate_capsule runs on this pre-mutation
    # payload; the authoritative post-mutation size check is below (Phase 2).
    _validate_capsule(repo_root, capsule)
    path = safe_path(repo_root, rel)
    with _continuity_subject_lock(repo_root=repo_root, subject_kind=req.subject_kind, subject_id=req.subject_id):
        old_bytes = path.read_bytes() if path.exists() else None
        # --- preserve-by-default merge (runs inside lock, before lifecycle) ---
        if req.merge_mode == "preserve" and raw_body is not None and old_bytes is not None:
            stored = json.loads(old_bytes)
            snapshot_touched = _session_end_snapshot_touched_fields(req.session_end_snapshot)
            _apply_preserve_merge(capsule, stored, raw_body, snapshot_touched)
            # Re-construct capsule through the model so that any raw dicts
            # restored from the stored JSON become proper Pydantic instances.
            capsule = ContinuityCapsule.model_validate(
                capsule.model_dump(mode="json")
            )
            # Re-validate after merge since preserved stored values may
            # interact with incoming values in ways that need bounds checking.
            _validate_capsule(repo_root, capsule)
        # --- write-path normalization (strip, dedup) ---
        normalizations_applied = _normalize_capsule_fields(capsule)
        # --- lifecycle state machine (mutates capsule before serialization) ---
        if capsule.thread_descriptor is not None:
            old_parsed = json.loads(old_bytes) if old_bytes else None
            old_td = old_parsed.get("thread_descriptor") if old_parsed else None
            if old_td is None:
                # First creation or first descriptor addition
                if req.lifecycle_transition is not None:
                    raise HTTPException(status_code=400, detail="no thread_descriptor to transition; create one first")
                capsule.thread_descriptor.lifecycle = "active"
                capsule.thread_descriptor.superseded_by = None
            else:
                stored_lifecycle = old_td.get("lifecycle", "active")
                stored_superseded_by = old_td.get("superseded_by")
                if req.lifecycle_transition is None:
                    capsule.thread_descriptor.lifecycle = stored_lifecycle
                    capsule.thread_descriptor.superseded_by = stored_superseded_by
                else:
                    allowed = THREAD_LIFECYCLE_TRANSITIONS.get(stored_lifecycle)
                    if allowed is None:
                        raise HTTPException(
                            status_code=400,
                            detail=f"lifecycle transition not allowed from terminal state '{stored_lifecycle}'",
                        )
                    if req.lifecycle_transition not in allowed:
                        raise HTTPException(
                            status_code=400,
                            detail=f"lifecycle transition '{req.lifecycle_transition}' not allowed from '{stored_lifecycle}'",
                        )
                    capsule.thread_descriptor.lifecycle = THREAD_LIFECYCLE_TRANSITION_TARGETS[req.lifecycle_transition]
                    capsule.thread_descriptor.superseded_by = req.superseded_by if req.lifecycle_transition == "supersede" else None
        elif req.lifecycle_transition is not None:
            raise HTTPException(status_code=400, detail="no thread_descriptor to transition; create one first")
        canonical = canonical_json(capsule.model_dump(mode="json", exclude_none=True))
        new_bytes = canonical.encode("utf-8")
        # Phase 2 (authoritative): size check on the final payload including
        # service-managed lifecycle/superseded_by fields set by the state
        # machine above.  This is the binding check — Phase 1 is an early
        # reject on the smaller pre-mutation payload.
        if len(new_bytes) > CAPSULE_SIZE_LIMIT_BYTES:
            raise HTTPException(status_code=400, detail="Continuity capsule exceeds 12 KB serialized UTF-8")
        if old_bytes != new_bytes:
            stripped_req = ContinuityUpsertRequest(
                subject_kind=req.subject_kind,
                subject_id=req.subject_id,
                capsule=capsule,
                commit_message=req.commit_message,
                idempotency_key=req.idempotency_key,
            )
            _reject_stale_or_conflicting_write(path, stripped_req)
        capsule_sha256 = hashlib.sha256(new_bytes).hexdigest()
        created = not path.exists()
        changed = old_bytes != new_bytes
        committed = False
        fallback_warning: str | None = None
        fallback_warning_detail: str | None = None
        if changed:
            _persist_active_capsule(
                repo_root=repo_root,
                gm=gm,
                path=path,
                canonical=canonical,
                commit_message=req.commit_message or f"continuity: upsert {req.subject_kind} {req.subject_id}",
            )
            committed = True
            fallback_rel, fallback_status, fallback_warning_detail = _persist_fallback_snapshot(
                repo_root=repo_root,
                gm=gm,
                subject_kind=req.subject_kind,
                subject_id=req.subject_id,
                capsule=capsule.model_dump(mode="json", exclude_none=True),
            )
            if fallback_status == "failed":
                fallback_warning = CONTINUITY_WARNING_FALLBACK_WRITE_FAILED
        else:
            fallback_rel = continuity_fallback_rel_path(req.subject_kind, req.subject_id)
    audit_detail: dict[str, Any] = {
        "subject_kind": req.subject_kind,
        "subject_id": req.subject_id,
        "path": rel,
        "created": created,
        "updated": bool(changed and not created),
        "capsule_sha256": capsule_sha256,
        "idempotency_key": req.idempotency_key,
        "committed": committed,
        "fallback_path": fallback_rel,
        "fallback_warning": fallback_warning,
        "fallback_warning_detail": fallback_warning_detail,
    }
    if snapshot_applied:
        audit_detail["session_end_snapshot_applied"] = True
        audit_detail["resume_quality_adequate"] = _compute_resume_quality(capsule)["adequate"]
    if req.lifecycle_transition is not None:
        audit_detail["lifecycle_transition"] = req.lifecycle_transition
    if capsule.thread_descriptor is not None and capsule.thread_descriptor.lifecycle is not None:
        audit_detail["lifecycle"] = capsule.thread_descriptor.lifecycle
    audit(auth, "continuity_upsert", audit_detail)
    _warnings: list[dict[str, Any]] = []
    if fallback_warning:
        _warnings.append(
            make_warning(
                fallback_warning,
                fallback_warning_detail or "Fallback snapshot write failed",
                path=rel,
            )
        )
    result: dict[str, Any] = {
        "ok": True,
        "path": rel,
        "created": created,
        "updated": bool(changed and not created),
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "capsule_sha256": capsule_sha256,
        "normalizations_applied": normalizations_applied,
        "warnings": _warnings,
        "recovery_warnings": [fallback_warning] if fallback_warning else [],
        "fallback_warning_detail": fallback_warning_detail,
    }
    if snapshot_applied:
        result["session_end_snapshot_applied"] = True
        result["resume_quality"] = _compute_resume_quality(capsule)
    if capsule.thread_descriptor is not None and capsule.thread_descriptor.lifecycle is not None:
        result["lifecycle"] = capsule.thread_descriptor.lifecycle
    return result


def _resolve_patch_target_list(capsule: ContinuityCapsule, target: str) -> list[Any]:
    """Return the mutable list on the capsule for a given patch target path."""
    if target.startswith("continuity."):
        field = target.split(".", 1)[1]
        return getattr(capsule.continuity, field)
    if target.startswith("thread_descriptor."):
        field = target.split(".", 1)[1]
        if capsule.thread_descriptor is None:
            raise HTTPException(status_code=400, detail=f"capsule has no thread_descriptor for target '{target}'")
        return getattr(capsule.thread_descriptor, field)
    return getattr(capsule, target)


def _set_patch_target_list(capsule: ContinuityCapsule, target: str, value: list[Any]) -> None:
    """Replace the list on the capsule for a given patch target path."""
    if target.startswith("continuity."):
        field = target.split(".", 1)[1]
        setattr(capsule.continuity, field, value)
    elif target.startswith("thread_descriptor."):
        field = target.split(".", 1)[1]
        setattr(capsule.thread_descriptor, field, value)
    else:
        setattr(capsule, target, value)


def _validate_patch_operation(op: PatchOperation) -> None:
    """Validate per-operation parameter constraints; raises HTTP 400 on violation."""
    target = op.target
    # identity_anchors uses structured matching (kind:value) despite being
    # under thread_descriptor; keywords and scope_anchors are plain strings.
    is_structured = target in PATCH_STRUCTURED_LIST_TARGETS or target == "thread_descriptor.identity_anchors"
    is_string = (target in PATCH_STRING_LIST_TARGETS or target in PATCH_THREAD_DESCRIPTOR_TARGETS) and not is_structured

    if op.action == "append":
        if op.match is not None or op.index is not None:
            raise HTTPException(status_code=400, detail=f"invalid operation: append must not specify match or index on {target}")
        if op.value is None:
            raise HTTPException(status_code=400, detail=f"invalid operation: append requires value on {target}")
    elif op.action == "remove":
        if op.value is not None:
            raise HTTPException(status_code=400, detail=f"invalid operation: remove must not specify value on {target}")
        if is_string:
            if op.match is None:
                raise HTTPException(status_code=400, detail=f"invalid operation: remove requires match on string-list target {target}")
            if op.index is not None:
                raise HTTPException(status_code=400, detail=f"invalid operation: remove must not specify index on {target}")
        elif is_structured:
            if op.match is None:
                raise HTTPException(status_code=400, detail=f"invalid operation: remove requires match on structured-list target {target}")
            if op.index is not None:
                raise HTTPException(status_code=400, detail=f"invalid operation: remove must not specify index on {target}")
    elif op.action == "replace_at":
        if op.value is None:
            raise HTTPException(status_code=400, detail=f"invalid operation: replace_at requires value on {target}")
        if is_string:
            if op.index is None:
                raise HTTPException(status_code=400, detail=f"invalid operation: replace_at requires index on string-list target {target}")
            if op.match is not None:
                raise HTTPException(status_code=400, detail=f"invalid operation: replace_at must not specify match on string-list target {target}")
        elif is_structured:
            if op.match is None:
                raise HTTPException(status_code=400, detail=f"invalid operation: replace_at requires match on structured-list target {target}")
            if op.index is not None:
                raise HTTPException(status_code=400, detail=f"invalid operation: replace_at must not specify index on structured-list target {target}")


def _find_structured_item_index(items: list[Any], target: str, match_value: str) -> int:
    """Find the index of a structured-list item by its match key; returns -1 if not found."""
    match_key = PATCH_STRUCTURED_MATCH_KEYS.get(target)
    if match_key == "kind:value":
        # identity_anchors: match by "kind:value"
        for i, item in enumerate(items):
            kind = item.kind if hasattr(item, "kind") else item.get("kind", "")
            value = item.value if hasattr(item, "value") else item.get("value", "")
            if f"{kind}:{value}" == match_value:
                return i
    elif match_key:
        for i, item in enumerate(items):
            item_val = getattr(item, match_key, None) if hasattr(item, match_key) else item.get(match_key)
            if item_val == match_value:
                return i
    return -1


def _coerce_structured_value(target: str, value: Any) -> Any:
    """Coerce a raw dict value into the appropriate Pydantic model for a structured target."""
    if target == "continuity.negative_decisions":
        return NegativeDecision.model_validate(value) if isinstance(value, dict) else value
    if target == "continuity.rationale_entries":
        return RationaleEntry.model_validate(value) if isinstance(value, dict) else value
    if target == "stable_preferences":
        return StablePreference.model_validate(value) if isinstance(value, dict) else value
    if target == "thread_descriptor.identity_anchors":
        return IdentityAnchor.model_validate(value) if isinstance(value, dict) else value
    return value


def _apply_patch_operations(
    capsule: ContinuityCapsule,
    operations: list[PatchOperation],
) -> int:
    """Apply all patch operations in order; raises on first failure.

    Returns the number of operations successfully applied. If any
    operation fails, the capsule is in a partially mutated state — the
    caller must ensure atomicity by working on a snapshot.
    """
    for i, op in enumerate(operations):
        target_list = _resolve_patch_target_list(capsule, op.target)
        max_len = PATCH_TARGET_MAX_LENGTH.get(op.target)

        if op.action == "append":
            if max_len is not None and len(target_list) >= max_len:
                raise HTTPException(
                    status_code=400,
                    detail=f"append would exceed max length ({max_len}) for {op.target}",
                )
            coerced = _coerce_structured_value(op.target, op.value)
            target_list.append(coerced)

        elif op.action == "remove":
            is_structured_target = (
                op.target in PATCH_STRUCTURED_LIST_TARGETS
                or op.target == "thread_descriptor.identity_anchors"
            )
            if not is_structured_target:
                # String-list: match by exact string
                try:
                    idx = target_list.index(op.match)
                except ValueError:
                    raise HTTPException(
                        status_code=404,
                        detail=f"no matching item for remove on {op.target}",
                    )
                target_list.pop(idx)
            else:
                # Structured-list: match by key
                idx = _find_structured_item_index(target_list, op.target, op.match)  # type: ignore[arg-type]
                if idx < 0:
                    raise HTTPException(
                        status_code=404,
                        detail=f"no matching item for remove on {op.target}",
                    )
                target_list.pop(idx)

        elif op.action == "replace_at":
            is_structured_target = (
                op.target in PATCH_STRUCTURED_LIST_TARGETS
                or op.target == "thread_descriptor.identity_anchors"
            )
            if not is_structured_target:
                # String-list: by index
                if op.index is None or op.index < 0 or op.index >= len(target_list):
                    raise HTTPException(
                        status_code=404,
                        detail=f"index {op.index} out of bounds for {op.target} (length {len(target_list)})",
                    )
                target_list[op.index] = op.value
            else:
                # Structured-list: by key
                idx = _find_structured_item_index(target_list, op.target, op.match)  # type: ignore[arg-type]
                if idx < 0:
                    raise HTTPException(
                        status_code=404,
                        detail=f"no matching item for replace_at on {op.target}",
                    )
                coerced = _coerce_structured_value(op.target, op.value)
                target_list[idx] = coerced

    return len(operations)


def continuity_patch_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityPatchRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Apply partial list-field patch operations to an existing continuity capsule."""
    auth.require("write:projects")
    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    auth.require_write_path(rel)
    path = safe_path(repo_root, rel)

    # Pre-validate all operation parameter constraints before acquiring the lock.
    for op in req.operations:
        _validate_patch_operation(op)

    with _continuity_subject_lock(repo_root=repo_root, subject_kind=req.subject_kind, subject_id=req.subject_id):
        if not path.exists():
            raise HTTPException(status_code=404, detail="continuity capsule not found")

        old_bytes = path.read_bytes()
        try:
            capsule = ContinuityCapsule.model_validate(json.loads(old_bytes))
        except (json.JSONDecodeError, Exception) as exc:
            raise HTTPException(status_code=400, detail=f"stored capsule is invalid: {exc}") from exc

        # Stale-write guard
        _reject_stale_timestamp(req.updated_at, capsule.updated_at)

        # Snapshot for atomicity: if any operation fails, we discard the snapshot.
        capsule_snapshot = ContinuityCapsule.model_validate(json.loads(old_bytes))
        try:
            ops_applied = _apply_patch_operations(capsule_snapshot, req.operations)
        except HTTPException:
            raise  # atomic rejection — no mutations applied

        # Update timestamp
        capsule_snapshot.updated_at = req.updated_at

        # Post-patch normalization
        normalizations_applied = _normalize_capsule_fields(capsule_snapshot)

        # Full validation on mutated capsule
        _validate_capsule(repo_root, capsule_snapshot)

        # Serialize and persist
        canonical = canonical_json(capsule_snapshot.model_dump(mode="json", exclude_none=True))
        new_bytes = canonical.encode("utf-8")
        if len(new_bytes) > CAPSULE_SIZE_LIMIT_BYTES:
            raise HTTPException(status_code=400, detail="Continuity capsule exceeds 12 KB serialized UTF-8")

        capsule_sha256 = hashlib.sha256(new_bytes).hexdigest()
        changed = old_bytes != new_bytes
        committed = False
        fallback_warning: str | None = None
        fallback_warning_detail: str | None = None

        if changed:
            _persist_active_capsule(
                repo_root=repo_root,
                gm=gm,
                path=path,
                canonical=canonical,
                commit_message=req.commit_message or f"continuity: patch {req.subject_kind} {req.subject_id}",
            )
            committed = True
            fallback_rel, fallback_status, fallback_warning_detail = _persist_fallback_snapshot(
                repo_root=repo_root,
                gm=gm,
                subject_kind=req.subject_kind,
                subject_id=req.subject_id,
                capsule=capsule_snapshot.model_dump(mode="json", exclude_none=True),
            )
            if fallback_status == "failed":
                fallback_warning = CONTINUITY_WARNING_FALLBACK_WRITE_FAILED
        else:
            fallback_rel = continuity_fallback_rel_path(req.subject_kind, req.subject_id)

    audit_detail: dict[str, Any] = {
        "subject_kind": req.subject_kind,
        "subject_id": req.subject_id,
        "path": rel,
        "operations_applied": ops_applied,
        "capsule_sha256": capsule_sha256,
        "committed": committed,
    }
    audit(auth, "continuity_patch", audit_detail)

    _warnings: list[dict[str, Any]] = []
    if fallback_warning:
        _warnings.append(
            make_warning(
                fallback_warning,
                fallback_warning_detail or "Fallback snapshot write failed",
                path=rel,
            )
        )

    return {
        "ok": True,
        "path": rel,
        "updated": changed,
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "capsule_sha256": capsule_sha256,
        "operations_applied": ops_applied,
        "normalizations_applied": normalizations_applied,
        "warnings": _warnings,
        "recovery_warnings": [fallback_warning] if fallback_warning else [],
    }


def continuity_lifecycle_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityLifecycleRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Apply a standalone lifecycle transition to a thread or task capsule."""
    auth.require("write:projects")
    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    auth.require_write_path(rel)
    path = safe_path(repo_root, rel)

    # Validate superseded_by consistency
    if req.transition == "supersede" and req.superseded_by is None:
        raise HTTPException(status_code=400, detail="superseded_by is required when transition is 'supersede'")
    if req.transition != "supersede" and req.superseded_by is not None:
        raise HTTPException(status_code=400, detail="superseded_by is only allowed when transition is 'supersede'")

    with _continuity_subject_lock(repo_root=repo_root, subject_kind=req.subject_kind, subject_id=req.subject_id):
        if not path.exists():
            raise HTTPException(status_code=404, detail="continuity capsule not found")

        old_bytes = path.read_bytes()
        try:
            capsule = ContinuityCapsule.model_validate(json.loads(old_bytes))
        except (json.JSONDecodeError, Exception) as exc:
            raise HTTPException(status_code=400, detail=f"stored capsule is invalid: {exc}") from exc

        # Stale-write guard
        _reject_stale_timestamp(req.updated_at, capsule.updated_at)

        # Require thread_descriptor
        if capsule.thread_descriptor is None:
            raise HTTPException(status_code=400, detail="no thread_descriptor to transition")

        # Run lifecycle state machine (same logic as upsert)
        stored_lifecycle = capsule.thread_descriptor.lifecycle or "active"
        allowed = THREAD_LIFECYCLE_TRANSITIONS.get(stored_lifecycle)
        if allowed is None:
            raise HTTPException(
                status_code=400,
                detail=f"lifecycle transition not allowed from terminal state '{stored_lifecycle}'",
            )
        if req.transition not in allowed:
            raise HTTPException(
                status_code=400,
                detail=f"lifecycle transition '{req.transition}' not allowed from '{stored_lifecycle}'",
            )

        previous_lifecycle = stored_lifecycle
        capsule.thread_descriptor.lifecycle = THREAD_LIFECYCLE_TRANSITION_TARGETS[req.transition]
        capsule.thread_descriptor.superseded_by = req.superseded_by if req.transition == "supersede" else None
        capsule.updated_at = req.updated_at

        # Serialize and persist
        canonical = canonical_json(capsule.model_dump(mode="json", exclude_none=True))
        new_bytes = canonical.encode("utf-8")
        if len(new_bytes) > CAPSULE_SIZE_LIMIT_BYTES:
            raise HTTPException(status_code=400, detail="Continuity capsule exceeds 12 KB serialized UTF-8")

        capsule_sha256 = hashlib.sha256(new_bytes).hexdigest()
        changed = old_bytes != new_bytes
        committed = False
        fallback_warning: str | None = None
        fallback_warning_detail: str | None = None

        if changed:
            _persist_active_capsule(
                repo_root=repo_root,
                gm=gm,
                path=path,
                canonical=canonical,
                commit_message=req.commit_message or f"continuity: lifecycle {req.transition} {req.subject_kind} {req.subject_id}",
            )
            committed = True
            fallback_rel, fallback_status, fallback_warning_detail = _persist_fallback_snapshot(
                repo_root=repo_root,
                gm=gm,
                subject_kind=req.subject_kind,
                subject_id=req.subject_id,
                capsule=capsule.model_dump(mode="json", exclude_none=True),
            )
            if fallback_status == "failed":
                fallback_warning = CONTINUITY_WARNING_FALLBACK_WRITE_FAILED
        else:
            fallback_rel = continuity_fallback_rel_path(req.subject_kind, req.subject_id)

    audit_detail: dict[str, Any] = {
        "subject_kind": req.subject_kind,
        "subject_id": req.subject_id,
        "path": rel,
        "transition": req.transition,
        "lifecycle": capsule.thread_descriptor.lifecycle,
        "previous_lifecycle": previous_lifecycle,
        "capsule_sha256": capsule_sha256,
        "committed": committed,
    }
    audit(auth, "continuity_lifecycle", audit_detail)

    _warnings: list[dict[str, Any]] = []
    if fallback_warning:
        _warnings.append(
            make_warning(
                fallback_warning,
                fallback_warning_detail or "Fallback snapshot write failed",
                path=rel,
            )
        )

    return {
        "ok": True,
        "path": rel,
        "lifecycle": capsule.thread_descriptor.lifecycle,
        "previous_lifecycle": previous_lifecycle,
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "capsule_sha256": capsule_sha256,
        "warnings": _warnings,
        "recovery_warnings": [fallback_warning] if fallback_warning else [],
    }


def continuity_read_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContinuityReadRequest,
    now: datetime,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Read one active continuity capsule by exact selector with fallback degradation.

    The *now* parameter is used to compute ``trust_signals`` recency and
    phase.  Callers pass ``datetime.now(timezone.utc)`` at the request
    boundary so all age computations within a single response share the
    same reference instant.
    """
    auth.require("read:files")
    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    auth.require_read_path(rel)
    fallback_rel = continuity_fallback_rel_path(req.subject_kind, req.subject_id)
    recovery_warnings: list[str] = []
    try:
        capsule = _load_capsule(repo_root, rel, expected_subject=(req.subject_kind, req.subject_id))
        out = {
            "ok": True,
            "path": rel,
            "capsule": capsule,
            "archived": False,
            "source_state": "active",
            "recovery_warnings": recovery_warnings,
        }
    except HTTPException as exc:
        if exc.status_code == 404:
            recovery_warnings.append(CONTINUITY_WARNING_ACTIVE_MISSING)
        elif exc.status_code == 400:
            if "subject does not match" in str(exc.detail):
                raise
            recovery_warnings.append(CONTINUITY_WARNING_ACTIVE_INVALID)
        else:
            raise
        if not req.allow_fallback:
            raise
        auth.require_read_path(fallback_rel)
        try:
            capsule = _load_fallback_snapshot(repo_root, fallback_rel, expected_subject=(req.subject_kind, req.subject_id))
            recovery_warnings.append(CONTINUITY_WARNING_FALLBACK_USED)
            out = {
                "ok": True,
                "path": rel,
                "capsule": capsule,
                "archived": False,
                "source_state": "fallback",
                "recovery_warnings": recovery_warnings,
            }
        except HTTPException as fallback_exc:
            if fallback_exc.status_code in {400, 404}:
                recovery_warnings.append(CONTINUITY_WARNING_FALLBACK_MISSING)
                out = {
                    "ok": True,
                    "path": rel,
                    "capsule": None,
                    "archived": False,
                    "source_state": "missing",
                    "recovery_warnings": recovery_warnings,
                }
            else:
                raise
    # --- trust signals (before audit and view handling) ---
    if out.get("capsule") is not None:
        try:
            out["trust_signals"] = _build_trust_signals(
                out["capsule"],
                now,
                source_state=out["source_state"],
            )
        except Exception:
            _logger.warning("trust_signals build failed; degrading to null", exc_info=True)
            out["trust_signals"] = None
            out["recovery_warnings"].append(CONTINUITY_WARNING_TRUST_SIGNALS_FAILED)
    else:
        out["trust_signals"] = None
    # --- thread descriptor warnings ---
    capsule_dict = out.get("capsule")
    if capsule_dict is not None:
        td = capsule_dict.get("thread_descriptor")
        if td and td.get("lifecycle") == "superseded":
            sid = capsule_dict.get("subject_id", "unknown")
            sby = td.get("superseded_by", "unknown")
            recovery_warnings.append(f"continuity_capsule_superseded:thread:{sid}\u2192{sby}")
    audit(
        auth,
        "continuity_read",
        {
            "subject_kind": req.subject_kind,
            "subject_id": req.subject_id,
            "path": rel,
            "source_state": out["source_state"],
            "recovery_warnings": out["recovery_warnings"],
        },
    )
    if req.view == "startup":
        try:
            out["startup_summary"] = _build_startup_summary(out)
        except Exception:
            _logger.warning("startup_summary build failed; degrading to null summary", exc_info=True)
            out["startup_summary"] = None
            # out["recovery_warnings"] is the same list object populated earlier in this function.
            out["recovery_warnings"].append(CONTINUITY_WARNING_STARTUP_SUMMARY_BUILD_FAILED)
    return out


def continuity_list_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContinuityListRequest,
    now: datetime,
    retention_archive_days: int = CONTINUITY_RETENTION_ARCHIVE_DAYS,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """List active, fallback, and archive continuity summaries under the repository namespace.

    When ``req.sort`` is ``"salience"``, active rows are ordered by the
    deterministic salience sort key (§3b) and each active row receives a
    ``salience_rank`` integer (1-indexed).  Non-active rows (fallback,
    archive, cold) sort after all active rows in their existing
    alphabetical order and receive ``salience_rank: null``.
    """
    auth.require("read:files")
    summaries: list[dict[str, Any]] = _scan_active_summaries(repo_root, auth, req.subject_kind, now)
    if req.include_fallback:
        summaries.extend(_scan_fallback_summaries(repo_root, auth, req.subject_kind, now))
    if req.include_archived:
        summaries.extend(_scan_archive_summaries(repo_root, auth, req.subject_kind, now, retention_archive_days))
    if req.include_cold:
        summaries.extend(_scan_cold_summaries(repo_root, auth, req.subject_kind))
    has_thread_filters = any(getattr(req, f) is not None for f in ("lifecycle", "scope_anchor", "keyword", "label_exact", "anchor_kind", "anchor_value"))
    if has_thread_filters:
        summaries = [row for row in summaries if _matches_thread_filters(row, req)]
    filtered_count = len(summaries)

    if req.sort == "salience":
        # Partition into active vs non-active rows.
        active_rows = [r for r in summaries if r.get("artifact_state") == "active"]
        non_active_rows = [r for r in summaries if r.get("artifact_state") != "active"]
        # Salience-sort active rows.
        active_rows = _salience_sort(active_rows, now)
        for rank_idx, row in enumerate(active_rows):
            row["salience_rank"] = rank_idx + 1
        # Non-active rows: existing alphabetical sort, null rank.
        artifact_order = {"fallback": 0, "archived": 1, "cold": 2}
        non_active_rows.sort(key=lambda row: (str(row["subject_kind"]), str(row["subject_id"]), artifact_order.get(str(row.get("artifact_state")), 99), str(row["path"])))
        for row in non_active_rows:
            row["salience_rank"] = None
        summaries = active_rows + non_active_rows
    else:
        artifact_order = {"active": 0, "fallback": 1, "archived": 2, "cold": 3}
        summaries.sort(key=lambda row: (str(row["subject_kind"]), str(row["subject_id"]), artifact_order.get(str(row.get("artifact_state")), 99), str(row["path"])))

    summaries = summaries[: req.limit]
    audit(
        auth,
        "continuity_list",
        {
            "subject_kind": req.subject_kind,
            "count": len(summaries),
        },
    )
    result: dict[str, Any] = {"ok": True, "count": len(summaries), "capsules": summaries}
    result["unique_match"] = filtered_count == 1 if has_thread_filters else False
    return result


def continuity_delete_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityDeleteRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Delete selected continuity artifacts for one exact selector."""
    auth.require("write:projects")
    active_rel = continuity_rel_path(req.subject_kind, req.subject_id)
    fallback_rel = continuity_fallback_rel_path(req.subject_kind, req.subject_id)
    archive_prefix = f"{req.subject_kind}-{_normalize_subject_id(req.subject_id)}-"
    archive_base = repo_root / CONTINUITY_DIR_REL / "archive"
    with _continuity_subject_lock(repo_root=repo_root, subject_kind=req.subject_kind, subject_id=req.subject_id):
        selected_rels: list[str] = []
        missing_paths: list[str] = []
        paths_to_stage: list[Path] = []
        original_bytes: dict[Path, bytes] = {}

        if req.delete_active:
            auth.require_write_path(active_rel)
            active_path = safe_path(repo_root, active_rel)
            if active_path.exists():
                selected_rels.append(active_rel)
                paths_to_stage.append(active_path)
                original_bytes[active_path] = active_path.read_bytes()
            else:
                missing_paths.append(active_rel)

        if req.delete_fallback:
            auth.require_write_path(fallback_rel)
            fallback_path = safe_path(repo_root, fallback_rel)
            if fallback_path.exists():
                selected_rels.append(fallback_rel)
                paths_to_stage.append(fallback_path)
                original_bytes[fallback_path] = fallback_path.read_bytes()
            else:
                missing_paths.append(fallback_rel)

        if req.delete_archive and archive_base.exists() and archive_base.is_dir():
            archive_paths: list[tuple[str, Path]] = []
            for path in sorted(archive_base.iterdir(), key=lambda item: item.name):
                stem = path.stem
                if path.is_dir() or path.suffix.lower() != ".json" or not stem.startswith(archive_prefix):
                    continue
                archive_suffix = stem[len(archive_prefix) :]
                if re.fullmatch(r"\d{8}T\d{6}Z", archive_suffix) is None:
                    continue
                rel = str(path.relative_to(repo_root))
                auth.require_write_path(rel)
                archive_paths.append((rel, path))
            for rel, path in archive_paths:
                selected_rels.append(rel)
                paths_to_stage.append(path)
                original_bytes[path] = path.read_bytes()

        if not paths_to_stage:
            audit(
                auth,
                "continuity_delete",
                {
                    "subject_kind": req.subject_kind,
                    "subject_id": req.subject_id,
                    "deleted_paths": [],
                    "missing_paths": missing_paths,
                    "reason": req.reason,
                },
            )
            return {
                "ok": True,
                "deleted_paths": [],
                "missing_paths": missing_paths,
                "durable": True,
                "latest_commit": gm.latest_commit(),
                "warnings": [],
            }

        with repository_mutation_lock(repo_root):
            try:
                for path in paths_to_stage:
                    path.unlink()
                committed = gm.commit_paths(
                    paths_to_stage,
                    _delete_commit_message(req.subject_kind, req.subject_id, req.reason),
                )
                if not committed:
                    raise RuntimeError("Continuity delete commit produced no changes")
            except Exception as exc:
                try_unstage_paths(gm, paths_to_stage)
                restore_errors: list[str] = []
                for path, data in original_bytes.items():
                    try:
                        write_bytes_file(path, data)
                    except Exception as restore_exc:
                        restore_errors.append(f"{path}: {restore_exc}")
                if restore_errors:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Continuity delete failed: {exc}; rollback failed for {', '.join(restore_errors)}",
                    ) from exc
                raise HTTPException(status_code=500, detail=f"Continuity delete failed: {exc}") from exc
    audit(
        auth,
        "continuity_delete",
        {
            "subject_kind": req.subject_kind,
            "subject_id": req.subject_id,
            "deleted_paths": selected_rels,
            "missing_paths": missing_paths,
            "reason": req.reason,
        },
    )
    return {
        "ok": True,
        "deleted_paths": selected_rels,
        "missing_paths": missing_paths,
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "warnings": [],
    }


def continuity_refresh_plan_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityRefreshPlanRequest,
    now: datetime,
    retention_archive_days: int | None = None,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Build and persist a deterministic continuity refresh plan."""
    auth.require("read:files")
    auth.require("write:projects")
    refresh_rel = CONTINUITY_REFRESH_STATE_REL
    auth.require_write_path(refresh_rel)
    recent_selectors = _audit_recent_selectors(repo_root, now)
    base = repo_root / CONTINUITY_DIR_REL
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    if base.exists() and base.is_dir():
        for path in sorted(base.iterdir(), key=lambda item: item.name):
            if path.is_dir() or path.suffix.lower() != ".json":
                continue
            if path.name in CONTINUITY_STATE_METADATA_FILES:
                continue
            rel = str(path.relative_to(repo_root))
            if req.subject_kind and not path.name.startswith(f"{req.subject_kind}-"):
                continue
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
            selector = (str(capsule["subject_kind"]), str(capsule["subject_id"]))
            selector_key = _selector_key(selector[0], selector[1])
            seen.add(selector_key)
            codes = _refresh_reason_codes(
                capsule=capsule,
                fallback_only=False,
                recently_used=selector_key in recent_selectors,
                now=now,
            )
            if not req.include_healthy and (not codes or codes == ["recently_used"]):
                continue
            verification_status = _verification_status(capsule)
            health_status, _health_reasons = _capsule_health_summary(capsule)
            candidates.append(
                {
                    "subject_kind": selector[0],
                    "subject_id": selector[1],
                    "path": rel,
                    "health_status": health_status,
                    "verification_status": verification_status,
                    "last_revalidated_at": (
                        str(capsule.get("verification_state", {}).get("last_revalidated_at"))
                        if isinstance(capsule.get("verification_state"), dict) and capsule.get("verification_state", {}).get("last_revalidated_at")
                        else None
                    ),
                    "updated_at": capsule["updated_at"],
                    "reason_codes": codes,
                    "recommended_priority": _refresh_priority(
                        codes,
                        health_status=health_status,
                        verification_status=verification_status,
                    ),
                }
            )

    fallback_dir = repo_root / CONTINUITY_DIR_REL / "fallback"
    if fallback_dir.exists() and fallback_dir.is_dir():
        for path in sorted(fallback_dir.iterdir(), key=lambda item: item.name):
            if path.is_dir() or path.suffix.lower() != ".json":
                continue
            if req.subject_kind and not path.name.startswith(f"{req.subject_kind}-"):
                continue
            rel = str(path.relative_to(repo_root))
            try:
                auth.require_read_path(rel)
            except HTTPException:
                continue
            try:
                envelope = _load_fallback_envelope_payload(repo_root, rel)
            except Exception:
                continue
            capsule = envelope["capsule"]
            selector = (str(capsule["subject_kind"]), str(capsule["subject_id"]))
            selector_key = _selector_key(selector[0], selector[1])
            if selector_key in seen:
                continue
            active_rel = continuity_rel_path(selector[0], selector[1])
            try:
                auth.require_read_path(active_rel)
            except HTTPException:
                continue
            codes = _refresh_reason_codes(
                capsule=capsule,
                fallback_only=True,
                recently_used=selector_key in recent_selectors,
                now=now,
            )
            if not req.include_healthy and (not codes or codes == ["recently_used"]):
                continue
            verification_status = _verification_status(capsule)
            health_status, _health_reasons = _capsule_health_summary(capsule)
            candidates.append(
                {
                    "subject_kind": selector[0],
                    "subject_id": selector[1],
                    "path": active_rel,
                    "health_status": health_status,
                    "verification_status": verification_status,
                    "last_revalidated_at": (
                        str(capsule.get("verification_state", {}).get("last_revalidated_at"))
                        if isinstance(capsule.get("verification_state"), dict) and capsule.get("verification_state", {}).get("last_revalidated_at")
                        else None
                    ),
                    "updated_at": capsule["updated_at"],
                    "reason_codes": codes,
                    "recommended_priority": _refresh_priority(
                        codes,
                        health_status=health_status,
                        verification_status=verification_status,
                    ),
                }
            )

    priority_order = {"high": 0, "medium": 1, "low": 2}
    candidates.sort(key=lambda row: (priority_order.get(str(row["recommended_priority"]), 3), str(row["subject_kind"]), str(row["subject_id"])))
    candidates = candidates[: req.limit]

    refresh_path = safe_path(repo_root, refresh_rel)
    payload = _refresh_state_payload(now, candidates)
    canonical = canonical_json(payload)
    new_bytes = canonical.encode("utf-8")
    latest_commit: str
    with repository_mutation_lock(repo_root):
        old_bytes = refresh_path.read_bytes() if refresh_path.exists() else None
        latest_commit = gm.latest_commit()
        if old_bytes != new_bytes:
            try:
                write_text_file(refresh_path, canonical)
                committed = gm.commit_file(refresh_path, "continuity: refresh plan")
                if not committed:
                    raise RuntimeError("git commit produced no changes")
                latest_commit = gm.latest_commit()
            except Exception as exc:
                try_unstage_paths(gm, [refresh_path])
                raise _restore_failed_refresh_state(refresh_path, old_bytes, exc) from exc

    audit(
        auth,
        "continuity_refresh_plan",
        {
            "subject_kind": req.subject_kind,
            "count": len(candidates),
            "path": refresh_rel,
            "latest_commit": latest_commit,
        },
    )
    return {
        "ok": True,
        "count": len(candidates),
        "generated_at": payload["last_planned_at"],
        "candidates": candidates,
        "durable": True,
        "latest_commit": latest_commit,
        "warnings": [],
    }


def continuity_retention_plan_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityRetentionPlanRequest,
    now: datetime,
    retention_archive_days: int,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Build and persist a deterministic continuity retention plan."""
    auth.require("read:files")
    auth.require("write:projects")
    auth.require_write_path(CONTINUITY_RETENTION_STATE_REL)

    all_candidates, warnings = _scan_retention_candidates(
        repo_root=repo_root,
        auth=auth,
        subject_kind=req.subject_kind,
        generated_at=now,
        retention_archive_days=retention_archive_days,
    )
    candidates = all_candidates[: req.limit]

    retention_path = safe_path(repo_root, CONTINUITY_RETENTION_STATE_REL)
    payload = _retention_plan_payload(
        generated_at=now,
        req=req,
        candidates=candidates,
        warnings=warnings,
        total_candidates=len(all_candidates),
    )
    canonical = canonical_json(payload)
    new_bytes = canonical.encode("utf-8")
    latest_commit: str
    with repository_mutation_lock(repo_root):
        old_bytes = retention_path.read_bytes() if retention_path.exists() else None
        latest_commit = gm.latest_commit()
        if old_bytes != new_bytes:
            try:
                write_text_file(retention_path, canonical)
                committed = gm.commit_file(retention_path, "continuity: retention plan")
                if not committed:
                    raise RuntimeError("git commit produced no changes")
                latest_commit = gm.latest_commit()
            except Exception as exc:
                try_unstage_paths(gm, [retention_path])
                raise _restore_failed_retention_state(retention_path, old_bytes, exc) from exc

    audit(
        auth,
        "continuity_retention_plan",
        {
            "subject_kind": req.subject_kind,
            "count": len(candidates),
            "total_candidates": len(all_candidates),
            "has_more": len(all_candidates) > len(candidates),
            "path": CONTINUITY_RETENTION_STATE_REL,
            "warnings_count": len(warnings),
            "latest_commit": latest_commit,
        },
    )
    return {
        "ok": True,
        "count": len(candidates),
        "generated_at": payload["generated_at"],
        "path": CONTINUITY_RETENTION_STATE_REL,
        "durable": True,
        "latest_commit": latest_commit,
        "warnings": warnings,
        "candidates": candidates,
        "total_candidates": len(all_candidates),
        "has_more": len(all_candidates) > len(candidates),
    }


def continuity_retention_apply_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityRetentionApplyRequest,
    now: datetime,
    retention_archive_days: int,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Batch-apply continuity retention policy against exact archive paths."""
    auth.require("admin:peers")

    warnings: list[str] = []
    unique_paths: list[str] = []
    seen_paths: set[str] = set()
    for raw_path in list(req.source_archive_paths):
        path = str(raw_path)
        if path in seen_paths:
            warnings.append(f"duplicate_source_archive_path:{path}")
            continue
        seen_paths.add(path)
        unique_paths.append(path)
    warnings.sort(key=_retention_warning_sort_key)

    results: list[dict[str, Any]] = []
    for raw_path in unique_paths:
        result: dict[str, Any] = {
            "source_archive_path": raw_path,
            "ok": False,
            "status": "failed_internal",
            "detail": "",
            "cold_storage_path": None,
            "cold_stub_path": None,
        }
        try:
            source_archive_path = _validate_archive_rel_path(raw_path)
            cold_state, cold_storage_path, cold_stub_path = _retention_cold_state(repo_root, source_archive_path)
            result["cold_storage_path"] = cold_storage_path
            result["cold_stub_path"] = cold_stub_path

            try:
                auth.require_read_path(source_archive_path)
                auth.require_write_path(source_archive_path)
                auth.require_write_path(cold_storage_path)
                auth.require_write_path(cold_stub_path)
            except HTTPException as exc:
                result["status"] = "failed_authorization"
                result["detail"] = str(exc.detail)
                results.append(result)
                continue

            archive_path = safe_path(repo_root, source_archive_path)
            if cold_state in {"partial", "conflict"}:
                result["status"] = "failed_conflict"
                result["detail"] = "Partial or conflicting cold artifact state"
                results.append(result)
                continue
            if cold_state == "full":
                result["ok"] = True
                result["status"] = "skipped_already_cold"
                result["detail"] = "Matching cold payload and stub already exist"
                results.append(result)
                continue
            if not archive_path.exists() or not archive_path.is_file():
                result["ok"] = True
                result["status"] = "skipped_missing"
                result["detail"] = "Continuity archive envelope not found"
                results.append(result)
                continue

            envelope = _load_archive_envelope(repo_root, source_archive_path)
            if _archive_rel_path_from_envelope(envelope) != source_archive_path:
                raise HTTPException(status_code=400, detail="archive identity mismatch")
            archived_at = _parse_iso(str(envelope.get("archived_at") or ""))
            if not _is_archive_stale(archived_at=archived_at, now=now, retention_archive_days=retention_archive_days):
                result["ok"] = True
                result["status"] = "skipped_not_stale"
                result["detail"] = "Archive is not stale under current retention threshold"
                results.append(result)
                continue

            cold_result = continuity_cold_store_service(
                repo_root=repo_root,
                gm=gm,
                auth=auth,
                req=ContinuityColdStoreRequest(source_archive_path=source_archive_path),
                audit=audit,
            )
            result["ok"] = True
            result["status"] = "cold_stored"
            result["detail"] = "Cold-stored archived continuity envelope"
            result["cold_storage_path"] = cold_result["cold_storage_path"]
            result["cold_stub_path"] = cold_result["cold_stub_path"]
            results.append(result)
        except HTTPException as exc:
            if exc.status_code == 400:
                result["status"] = "failed_invalid_archive"
            elif exc.status_code == 403:
                result["status"] = "failed_authorization"
            elif exc.status_code == 404:
                result["ok"] = True
                result["status"] = "skipped_missing"
            elif exc.status_code == 409:
                current_source = str(result["source_archive_path"])
                if current_source.startswith(f"{CONTINUITY_DIR_REL}/archive/"):
                    cold_state, _cold_storage_path, _cold_stub_path = _retention_cold_state(repo_root, current_source)
                    if cold_state == "full":
                        result["ok"] = True
                        result["status"] = "skipped_already_cold"
                    else:
                        result["status"] = "failed_conflict"
                else:
                    result["status"] = "failed_conflict"
            else:
                result["status"] = "failed_internal"
            result["detail"] = str(exc.detail)
            results.append(result)
        except Exception as exc:
            result["status"] = "failed_internal"
            result["detail"] = str(exc)
            results.append(result)

    failed = sum(1 for row in results if str(row["status"]).startswith("failed_"))
    cold_stored = sum(1 for row in results if row["status"] == "cold_stored")
    audit(
        auth,
        "continuity_retention_apply",
        {
            "requested": len(req.source_archive_paths),
            "unique_requested": len(unique_paths),
            "processed": len(results),
            "cold_stored": cold_stored,
            "failed": failed,
        },
    )
    return {
        "ok": failed == 0,
        "requested": len(req.source_archive_paths),
        "unique_requested": len(unique_paths),
        "processed": len(results),
        "cold_stored": cold_stored,
        "failed": failed,
        "durable": True,
        "results": results,
        "warnings": warnings,
    }


def continuity_compare_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContinuityCompareRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Compare one active continuity capsule to a candidate without mutating storage."""
    auth.require("read:files")
    _validate_verification_signals(req.signals)
    _validate_candidate_selector_match(req.subject_kind, req.subject_id, req.candidate_capsule)
    candidate = _normalize_compare_payload(repo_root, req.candidate_capsule)
    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    auth.require_read_path(rel)
    active = _load_capsule(repo_root, rel, expected_subject=(req.subject_kind, req.subject_id))
    changed_fields = _compare_capsules(active, candidate)
    identical = not changed_fields
    strongest_signal = _strongest_signal_kind(req.signals)
    recommended_outcome = "confirm" if identical else ("correct" if strongest_signal != "self_review" else "conflict")
    audit(
        auth,
        "continuity_compare",
        {
            "subject_kind": req.subject_kind,
            "subject_id": req.subject_id,
            "path": rel,
            "strongest_signal": strongest_signal,
            "identical": identical,
            "recommended_outcome": recommended_outcome,
        },
    )
    return {
        "ok": True,
        "path": rel,
        "identical": identical,
        "changed_fields": changed_fields,
        "strongest_signal": strongest_signal,
        "recommended_outcome": recommended_outcome,
    }


def continuity_revalidate_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityRevalidateRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Confirm, correct, degrade, or conflict-mark one active continuity capsule."""
    auth.require("write:projects")
    _validate_verification_signals(req.signals)
    if req.outcome == "correct":
        if req.candidate_capsule is None:
            raise HTTPException(status_code=400, detail="candidate_capsule is required for outcome=correct")
        _validate_candidate_selector_match(req.subject_kind, req.subject_id, req.candidate_capsule)
        _normalize_compare_payload(repo_root, req.candidate_capsule)
    elif req.candidate_capsule is not None:
        raise HTTPException(status_code=400, detail=f"candidate_capsule is not allowed for outcome={req.outcome}")
    if req.outcome == "confirm" and req.reason is not None:
        raise HTTPException(status_code=400, detail="reason is not allowed for outcome=confirm")
    if req.outcome in {"degrade", "conflict"} and not req.reason:
        raise HTTPException(status_code=400, detail=f"reason is required for outcome={req.outcome}")

    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    auth.require_read_path(rel)
    auth.require_write_path(rel)
    active_path = safe_path(repo_root, rel)
    with _continuity_subject_lock(repo_root=repo_root, subject_kind=req.subject_kind, subject_id=req.subject_id):
        active = ContinuityCapsule.model_validate(_load_capsule(repo_root, rel, expected_subject=(req.subject_kind, req.subject_id)))
        now = datetime.now(timezone.utc).replace(microsecond=0)
        now_iso = format_iso(now)
        strongest_signal = _strongest_signal_kind(req.signals)
        derived_status = CONTINUITY_SIGNAL_STATUS[strongest_signal]
        evidence_refs = _signals_to_evidence_refs(req.signals)

        final_capsule, result_outcome, updated, compare_changed_fields = _resolve_revalidation_capsule(
            outcome=req.outcome,
            active=active,
            candidate=req.candidate_capsule,
            repo_root=repo_root,
        )
        _apply_verification_outcome(
            final_capsule,
            result_outcome=result_outcome,
            now_iso=now_iso,
            strongest_signal=strongest_signal,
            derived_status=derived_status,
            evidence_refs=evidence_refs,
            reason=req.reason,
        )

        _, canonical = _final_capsule_payload(repo_root, final_capsule)
        _persist_active_capsule(
            repo_root=repo_root,
            gm=gm,
            path=active_path,
            canonical=canonical,
            commit_message=f"continuity: revalidate {req.subject_kind} {req.subject_id}",
        )
        fallback_rel, fallback_status, fallback_warning_detail = _persist_fallback_snapshot(
            repo_root=repo_root,
            gm=gm,
            subject_kind=req.subject_kind,
            subject_id=req.subject_id,
            capsule=final_capsule.model_dump(mode="json", exclude_none=True),
        )
    audit(
        auth,
        "continuity_revalidate",
        {
            "subject_kind": req.subject_kind,
            "subject_id": req.subject_id,
            "path": rel,
            "outcome": result_outcome,
            "strongest_signal": strongest_signal,
            "updated": updated,
            "fallback_path": fallback_rel,
            "fallback_warning": CONTINUITY_WARNING_FALLBACK_WRITE_FAILED if fallback_status == "failed" else None,
            "fallback_warning_detail": fallback_warning_detail,
        },
    )
    _rev_warnings: list[dict[str, Any]] = []
    if fallback_status == "failed":
        _rev_warnings.append(
            make_warning(
                CONTINUITY_WARNING_FALLBACK_WRITE_FAILED,
                fallback_warning_detail or "Fallback snapshot write failed",
                path=rel,
            )
        )
    return {
        "ok": True,
        "path": rel,
        "outcome": result_outcome,
        "updated": updated,
        "verification_state": final_capsule.verification_state.model_dump(mode="json", exclude_none=True),
        "capsule_health": final_capsule.capsule_health.model_dump(mode="json", exclude_none=True),
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "warnings": _rev_warnings,
        "recovery_warnings": [CONTINUITY_WARNING_FALLBACK_WRITE_FAILED] if fallback_status == "failed" else [],
        "fallback_warning_detail": fallback_warning_detail,
    }


def continuity_archive_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityArchiveRequest,
    now: datetime,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Archive one active continuity capsule and remove the active file in one commit."""
    auth.require("write:projects")
    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    timestamp = format_compact(now)
    archive_rel = f"{CONTINUITY_DIR_REL}/archive/{req.subject_kind}-{_normalize_subject_id(req.subject_id)}-{timestamp}.json"
    auth.require_read_path(rel)
    auth.require_write_path(rel)
    auth.require_write_path(archive_rel)
    with _continuity_subject_lock(repo_root=repo_root, subject_kind=req.subject_kind, subject_id=req.subject_id):
        capsule = _load_capsule(repo_root, rel, expected_subject=(req.subject_kind, req.subject_id))
        archive_payload = {
            "schema_type": CONTINUITY_ARCHIVE_SCHEMA_TYPE,
            "schema_version": CONTINUITY_ARCHIVE_SCHEMA_VERSION,
            "archived_at": format_iso(now),
            "archived_by": auth.peer_id,
            "reason": req.reason,
            "active_path": rel,
            "capsule": capsule,
        }

        archive_path = safe_path(repo_root, archive_rel)
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        active_path = safe_path(repo_root, rel)
        active_bytes = active_path.read_bytes()
        # Both the archive write and the active-file deletion are performed
        # inside the git lock so that a process crash before commit cannot
        # leave the active capsule deleted without a durable archive.
        with repository_mutation_lock(repo_root):
            write_text_file(archive_path, canonical_json(archive_payload))
            try:
                active_path.unlink()
                committed = gm.commit_paths(
                    [archive_path, active_path],
                    f"continuity: archive {req.subject_kind} {req.subject_id}",
                )
            except Exception as exc:
                _logger.error("Continuity archive commit failed: %s", exc, exc_info=True)
                try_unstage_paths(gm, [archive_path, active_path])
                try:
                    _restore_failed_archive(active_path, archive_path, active_bytes)
                except Exception as restore_exc:
                    _logger.exception("Rollback also failed after archive commit error")
                    raise HTTPException(
                        status_code=500,
                        detail=make_error_detail(
                            operation="continuity_archive",
                            error_code="continuity_archive_rollback_failed",
                            error_detail=f"Continuity archive commit failed: {exc}; rollback failed: {restore_exc}",
                        ),
                    ) from exc
                raise HTTPException(
                    status_code=500,
                    detail=make_error_detail(
                        operation="continuity_archive",
                        error_code="continuity_archive_commit_failed",
                        error_detail=f"Continuity archive commit failed: {exc}",
                    ),
                ) from exc
            if not committed:
                _logger.error("Continuity archive commit produced no changes")
                try_unstage_paths(gm, [archive_path, active_path])
                try:
                    _restore_failed_archive(active_path, archive_path, active_bytes)
                except Exception as restore_exc:
                    _logger.exception("Rollback failed after archive no-changes error")
                    raise HTTPException(
                        status_code=500,
                        detail=make_error_detail(
                            operation="continuity_archive",
                            error_code="continuity_archive_rollback_failed",
                            error_detail=f"Continuity archive commit produced no changes; rollback failed: {restore_exc}",
                        ),
                    ) from restore_exc
                raise HTTPException(
                    status_code=500,
                    detail=make_error_detail(
                        operation="continuity_archive",
                        error_code="continuity_archive_commit_no_changes",
                        error_detail="Continuity archive commit produced no changes",
                    ),
                )

    audit(
        auth,
        "continuity_archive",
        {
            "subject_kind": req.subject_kind,
            "subject_id": req.subject_id,
            "archived_path": archive_rel,
            "removed_active_path": rel,
            "reason": req.reason,
        },
    )
    return {
        "ok": True,
        "archived_path": archive_rel,
        "removed_active_path": rel,
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "warnings": [],
    }


def continuity_cold_store_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityColdStoreRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Transform one archive envelope into a cold gzip plus searchable stub."""
    auth.require("admin:peers")
    source_archive_path = _validate_archive_rel_path(req.source_archive_path)
    auth.require_read_path(source_archive_path)
    auth.require_write_path(source_archive_path)
    cold_storage_path = continuity_cold_storage_rel_path(source_archive_path)
    cold_stub_path = continuity_cold_stub_rel_path(source_archive_path)
    auth.require_write_path(cold_storage_path)
    auth.require_write_path(cold_stub_path)

    archive_path = safe_path(repo_root, source_archive_path)
    cold_payload_file = safe_path(repo_root, cold_storage_path)
    cold_stub_file = safe_path(repo_root, cold_stub_path)

    # --- Crash recovery: cold-store completed but uncommitted ---
    # If archive is missing but both cold files exist with valid stub identity,
    # the prior cold-store deleted the archive inside the git lock but crashed
    # before the commit completed.  Recover by committing the existing state.
    if not archive_path.exists() and cold_payload_file.exists() and cold_stub_file.exists():
        try:
            _cold_fm = _load_cold_stub(repo_root, cold_stub_path)
            if _cold_fm.get("source_archive_path") == source_archive_path:
                with _continuity_subject_lock(
                    repo_root=repo_root,
                    subject_kind=_cold_fm["subject_kind"],
                    subject_id=_cold_fm["subject_id"],
                ):
                    # Re-verify under lock (TOCTOU prevention)
                    if not archive_path.exists() and cold_payload_file.exists() and cold_stub_file.exists():
                        _recovery_committed = bool(
                            try_commit_paths(
                                paths=[cold_payload_file, cold_stub_file, archive_path],
                                gm=gm,
                                commit_message=(f"continuity: cold-store recovery {_cold_fm['subject_kind']} {_cold_fm['subject_id']}"),
                            )
                        )
                        audit(
                            auth,
                            "continuity_cold_store",
                            {
                                "source_archive_path": source_archive_path,
                                "cold_storage_path": cold_storage_path,
                                "cold_stub_path": cold_stub_path,
                                "crash_recovery": True,
                            },
                        )
                        _cs_recovery_warnings: list[dict[str, Any]] = [
                            make_warning(
                                "continuity_cold_store_crash_recovery",
                                "Completed cold-store via crash recovery: archive was already deleted, cold files committed",
                            ),
                        ]
                        if not _recovery_committed:
                            _cs_recovery_warnings.append(
                                make_warning(
                                    "continuity_cold_store_recovery_not_durable",
                                    "Crash recovery completed on disk but git commit failed; state is not yet durable",
                                ),
                            )
                        return {
                            "ok": True,
                            "artifact_state": "cold",
                            "source_archive_path": source_archive_path,
                            "cold_storage_path": cold_storage_path,
                            "cold_stub_path": cold_stub_path,
                            "cold_stored_at": _cold_fm.get("cold_stored_at", ""),
                            "committed_files": [cold_storage_path, cold_stub_path, source_archive_path],
                            "durable": _recovery_committed,
                            "latest_commit": gm.latest_commit(),
                            "warnings": _cs_recovery_warnings,
                            "recovery_warnings": _cs_recovery_warnings,
                        }
        except Exception:
            _logger.warning(
                "Continuity cold-store crash recovery: cold stub validation failed; falling through to normal flow",
                exc_info=True,
            )

    if not archive_path.exists() or not archive_path.is_file():
        raise HTTPException(status_code=404, detail="Continuity archive envelope not found")
    envelope = _load_archive_envelope(repo_root, source_archive_path)
    capsule = envelope["capsule"]
    with _continuity_subject_lock(
        repo_root=repo_root,
        subject_kind=str(capsule["subject_kind"]),
        subject_id=str(capsule["subject_id"]),
    ):
        try:
            if not archive_path.exists() or not archive_path.is_file():
                raise HTTPException(status_code=404, detail="Continuity archive envelope not found")
            source_bytes = archive_path.read_bytes()
        except HTTPException:
            raise
        except FileNotFoundError as exc:
            raise HTTPException(status_code=409, detail=f"Continuity archive envelope changed during cold-store: {exc.filename or exc}") from exc

        try:
            envelope = _load_archive_envelope(repo_root, source_archive_path)
            if _archive_rel_path_from_envelope(envelope) != source_archive_path:
                raise HTTPException(status_code=400, detail="Continuity archive envelope identity does not match source_archive_path")
            # Crash recovery: if both archive and cold artifacts exist,
            # a prior cold-store wrote cold but crashed before committing
            # the archive deletion. Remove orphaned cold artifacts.
            if (cold_payload_file.exists() or cold_stub_file.exists()) and archive_path.exists():
                _logger.warning(
                    "Continuity cold-store crash recovery: removing orphaned cold artifacts for %s",
                    source_archive_path,
                )
                cold_payload_file.unlink(missing_ok=True)
                cold_stub_file.unlink(missing_ok=True)
            elif cold_payload_file.exists() or cold_stub_file.exists():
                raise HTTPException(status_code=409, detail="Continuity cold artifact already exists for source archive")

            now = datetime.now(timezone.utc).replace(microsecond=0)
            cold_stored_at = format_iso(now)
            gzip_bytes = build_cold_gzip_bytes(source_bytes)
            stub_text = _build_cold_stub_text(
                envelope=envelope,
                source_archive_path=source_archive_path,
                cold_storage_path=cold_storage_path,
                cold_stored_at=cold_stored_at,
                now=now,
            )
            write_bytes_file(cold_payload_file, gzip_bytes)
            write_text_file(cold_stub_file, stub_text)
            # Archive deletion and commit are inside the git lock so that a
            # process crash before commit cannot leave the archive deleted
            # without durable cold files.
            with repository_mutation_lock(repo_root):
                archive_path.unlink()
                committed = gm.commit_paths(
                    [cold_payload_file, cold_stub_file, archive_path],
                    f"continuity: cold-store {envelope['capsule']['subject_kind']} {envelope['capsule']['subject_id']}",
                )
                if not committed:
                    raise RuntimeError("Continuity cold-store commit produced no changes")
        except HTTPException:
            raise
        except Exception as exc:
            try_unstage_paths(gm, [cold_payload_file, cold_stub_file, archive_path])
            cleanup_errors = _restore_failed_cold_store(
                archive_path=archive_path,
                archive_bytes=source_bytes,
                cold_payload_path=cold_payload_file,
                cold_stub_path=cold_stub_file,
            )
            raise HTTPException(
                status_code=500,
                detail=make_error_detail(
                    operation="continuity_cold_store",
                    error_code="continuity_cold_store_failed",
                    error_detail=str(exc),
                    rollback_errors=cleanup_errors,
                ),
            ) from exc

    audit(
        auth,
        "continuity_cold_store",
        {
            "source_archive_path": source_archive_path,
            "cold_storage_path": cold_storage_path,
            "cold_stub_path": cold_stub_path,
        },
    )
    return {
        "ok": True,
        "artifact_state": "cold",
        "source_archive_path": source_archive_path,
        "cold_storage_path": cold_storage_path,
        "cold_stub_path": cold_stub_path,
        "cold_stored_at": cold_stored_at,
        "committed_files": [cold_storage_path, cold_stub_path, source_archive_path],
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "warnings": [],
        "recovery_warnings": [],
    }


def continuity_cold_rehydrate_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityColdRehydrateRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Restore one cold-stored archive envelope back into the hot archive namespace."""
    auth.require("admin:peers")
    if req.cold_stub_path:
        cold_stub_path = str(req.cold_stub_path)
        frontmatter = _load_cold_stub(repo_root, cold_stub_path)
        source_archive_path = frontmatter["source_archive_path"]
    else:
        source_archive_path = _validate_archive_rel_path(str(req.source_archive_path))
        cold_stub_path = continuity_cold_stub_rel_path(source_archive_path)
        frontmatter = _load_cold_stub(repo_root, cold_stub_path)
        if frontmatter["source_archive_path"] != source_archive_path:
            raise HTTPException(status_code=400, detail="Continuity cold stub identity does not match requested source archive")
    cold_storage_path = continuity_cold_storage_rel_path(source_archive_path)
    auth.require_read_path(cold_stub_path)
    auth.require_read_path(cold_storage_path)
    auth.require_write_path(source_archive_path)
    auth.require_write_path(cold_stub_path)
    auth.require_write_path(cold_storage_path)

    archive_path = safe_path(repo_root, source_archive_path)
    cold_payload_file = safe_path(repo_root, cold_storage_path)
    cold_stub_file = safe_path(repo_root, cold_stub_path)
    with _continuity_subject_lock(
        repo_root=repo_root,
        subject_kind=frontmatter["subject_kind"],
        subject_id=frontmatter["subject_id"],
    ):
        try:
            frontmatter = _load_cold_stub(repo_root, cold_stub_path)
            if frontmatter["source_archive_path"] != source_archive_path:
                raise HTTPException(status_code=400, detail="Continuity cold stub identity does not match requested source archive")
            if archive_path.exists():
                # --- Crash recovery: rehydrate wrote archive but crashed
                # before deleting cold files and committing. ---
                if cold_payload_file.exists() or cold_stub_file.exists():
                    try:
                        _check_envelope = _load_archive_envelope(repo_root, source_archive_path)
                        if _archive_rel_path_from_envelope(_check_envelope) == source_archive_path:
                            cold_payload_file.unlink(missing_ok=True)
                            cold_stub_file.unlink(missing_ok=True)
                            _rh_recovery_committed = bool(
                                try_commit_paths(
                                    paths=[archive_path, cold_payload_file, cold_stub_file],
                                    gm=gm,
                                    commit_message=(f"continuity: cold-rehydrate recovery {frontmatter['subject_kind']} {frontmatter['subject_id']}"),
                                )
                            )
                            audit(
                                auth,
                                "continuity_cold_rehydrate",
                                {
                                    "source_archive_path": source_archive_path,
                                    "cold_storage_path": cold_storage_path,
                                    "cold_stub_path": cold_stub_path,
                                    "crash_recovery": True,
                                },
                            )
                            _rh_warnings: list[dict[str, Any]] = [
                                make_warning(
                                    "continuity_cold_rehydrate_crash_recovery",
                                    "Completed rehydrate via crash recovery: archive already restored, removed orphaned cold files",
                                ),
                            ]
                            if not _rh_recovery_committed:
                                _rh_warnings.append(
                                    make_warning(
                                        "continuity_cold_rehydrate_recovery_not_durable",
                                        "Crash recovery completed on disk but git commit failed; state is not yet durable",
                                    ),
                                )
                            return {
                                "ok": True,
                                "artifact_state": "archived",
                                "source_archive_path": source_archive_path,
                                "restored_archive_path": source_archive_path,
                                "cold_storage_path": cold_storage_path,
                                "cold_stub_path": cold_stub_path,
                                "rehydrated_at": format_iso(iso_now()),
                                "committed_files": [source_archive_path, cold_storage_path, cold_stub_path],
                                "durable": _rh_recovery_committed,
                                "latest_commit": gm.latest_commit(),
                                "warnings": _rh_warnings,
                                "recovery_warnings": _rh_warnings,
                            }
                    except Exception:
                        _logger.warning(
                            "Continuity cold-rehydrate crash recovery: archive validation failed; falling through to 409",
                            exc_info=True,
                        )
                raise HTTPException(status_code=409, detail="Continuity archive envelope already exists")
            if not cold_payload_file.exists() or not cold_payload_file.is_file():
                raise HTTPException(status_code=404, detail="Continuity cold payload not found")
            cold_payload_bytes = cold_payload_file.read_bytes()
            cold_stub_bytes = cold_stub_file.read_bytes()
            archive_bytes = gzip.decompress(cold_payload_bytes)
            payload = json.loads(archive_bytes.decode("utf-8"))
            if payload.get("schema_type") != CONTINUITY_ARCHIVE_SCHEMA_TYPE:
                raise ValueError("wrong schema_type")
            if payload.get("schema_version") != CONTINUITY_ARCHIVE_SCHEMA_VERSION:
                raise ValueError("wrong schema_version")
            capsule = ContinuityCapsule.model_validate(payload.get("capsule")).model_dump(mode="json", exclude_none=True)
            expected_archive_path = _archive_rel_path_from_envelope({**payload, "capsule": capsule})
            if expected_archive_path != source_archive_path:
                raise HTTPException(status_code=400, detail="Continuity cold payload identity does not match requested source archive")
            if str(payload.get("active_path") or "") != continuity_rel_path(str(capsule["subject_kind"]), str(capsule["subject_id"])):
                raise HTTPException(status_code=400, detail="Invalid continuity archive envelope in cold payload: active_path mismatch")
        except HTTPException:
            raise
        except FileNotFoundError as exc:
            raise HTTPException(status_code=409, detail=f"Continuity cold artifact changed during rehydrate: {exc.filename or exc}") from exc
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid continuity cold payload: {exc}") from exc

        rehydrated_at = format_iso(iso_now())
        try:
            write_bytes_file(archive_path, archive_bytes)
            # Cold file deletions and commit are inside the git lock so
            # that a process crash before commit cannot leave cold files
            # deleted without a durable rehydrated archive.
            with repository_mutation_lock(repo_root):
                cold_payload_file.unlink()
                cold_stub_file.unlink()
                committed = gm.commit_paths(
                    [archive_path, cold_payload_file, cold_stub_file],
                    f"continuity: cold-rehydrate {capsule['subject_kind']} {capsule['subject_id']}",
                )
                if not committed:
                    raise RuntimeError("Continuity cold rehydrate commit produced no changes")
        except Exception as exc:
            try_unstage_paths(gm, [archive_path, cold_payload_file, cold_stub_file])
            rollback_errors: list[str] = []
            try:
                archive_path.unlink(missing_ok=True)
            except Exception as rollback_exc:
                rollback_errors.append(f"remove restored archive: {rollback_exc}")
            try:
                write_bytes_file(cold_payload_file, cold_payload_bytes)
            except Exception as rollback_exc:
                rollback_errors.append(f"restore cold payload: {rollback_exc}")
            try:
                write_bytes_file(cold_stub_file, cold_stub_bytes)
            except Exception as rollback_exc:
                rollback_errors.append(f"restore cold stub: {rollback_exc}")
            raise HTTPException(
                status_code=500,
                detail=make_error_detail(
                    operation="continuity_cold_rehydrate",
                    error_code="continuity_cold_rehydrate_failed",
                    error_detail=str(exc),
                    rollback_errors=rollback_errors,
                ),
            ) from exc

    audit(
        auth,
        "continuity_cold_rehydrate",
        {
            "source_archive_path": source_archive_path,
            "cold_storage_path": cold_storage_path,
            "cold_stub_path": cold_stub_path,
        },
    )
    return {
        "ok": True,
        "artifact_state": "archived",
        "source_archive_path": source_archive_path,
        "restored_archive_path": source_archive_path,
        "cold_storage_path": cold_storage_path,
        "cold_stub_path": cold_stub_path,
        "rehydrated_at": rehydrated_at,
        "committed_files": [source_archive_path, cold_storage_path, cold_stub_path],
        "durable": True,
        "latest_commit": gm.latest_commit(),
        "warnings": [],
        "recovery_warnings": [],
    }


def build_continuity_state(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContextRetrieveRequest,
    now: datetime,
) -> dict[str, Any]:
    """Load, trim, and package continuity state for context retrieval.

    Capsules are returned in **salience order** — a deterministic
    lexicographic sort over lifecycle, health, freshness, resume
    adequacy, and verification strength, with recency and identity
    tiebreakers guaranteeing total ordering.  Each capsule includes a
    ``salience`` block exposing its rank and sort-key components, and
    the top-level response carries ``salience_metadata`` with aggregate
    best/worst-case signal summaries.

    Each returned capsule also includes a per-capsule ``trust_signals``
    block (full or compact depending on token budget) that mechanically
    derives trust assessments from existing capsule state.  An aggregate
    ``trust_signals`` block summarises the worst-case across all
    per-capsule signals.

    The *now* parameter anchors all age computations so every signal in
    the response shares the same reference instant.
    """
    budget = _budget(req.max_tokens_estimate)
    state: dict[str, Any] = {
        "present": False,
        "requested_selectors": [],
        "omitted_selectors": [],
        "capsules": [],
        "selection_order": [],
        "budget": budget,
        "warnings": [],
        "fallback_used": False,
        "recovery_warnings": [],
        "trust_signals": None,
        "salience_metadata": None,
    }
    if req.continuity_mode == "off":
        return state
    multi_warning_mode = _warning_mode_is_multi(req)
    selectors, requested_selectors, pre_load_omitted = _effective_selectors(req)
    state["requested_selectors"] = requested_selectors
    state["omitted_selectors"] = list(pre_load_omitted)
    if not selectors:
        if req.continuity_mode == "required":
            raise HTTPException(status_code=404, detail="Continuity capsule not found")
        return state

    loaded, warnings, recovery_warnings, fallback_used = _load_selectors_with_fallback(
        repo_root=repo_root,
        auth=auth,
        selectors=selectors,
        req=req,
        now=now,
        multi_warning_mode=multi_warning_mode,
        omitted_selectors=state["omitted_selectors"],
    )

    if not loaded:
        if req.continuity_mode == "required":
            raise HTTPException(status_code=404, detail="Continuity capsule not found")
        state["warnings"] = warnings
        state["recovery_warnings"] = recovery_warnings
        state["fallback_used"] = fallback_used
        return state

    loaded = _filter_by_verification_policy(
        loaded,
        req.continuity_verification_policy,
        state["omitted_selectors"],
    )

    if not loaded:
        if req.continuity_mode == "required":
            raise HTTPException(status_code=404, detail="Continuity capsule not found")
        state["warnings"] = warnings
        state["recovery_warnings"] = recovery_warnings
        state["fallback_used"] = fallback_used
        return state

    # --- Salience sort: reorder loaded capsules before trimming (§3a) ---
    loaded = _salience_sort(loaded, now)

    trimmed_capsules, trimmed_selection_order, trim_warnings, trim_recovery, survived_rows = _trim_and_attach_trust(
        loaded=loaded,
        reserve=budget["continuity_tokens_reserved"],
        now=now,
        multi_warning_mode=multi_warning_mode,
        omitted_selectors=state["omitted_selectors"],
    )
    warnings.extend(trim_warnings)
    recovery_warnings.extend(trim_recovery)

    if not trimmed_capsules:
        if req.continuity_mode == "required":
            raise HTTPException(status_code=404, detail="Continuity capsule not found")
        state["warnings"] = warnings
        state["recovery_warnings"] = recovery_warnings
        state["fallback_used"] = fallback_used
        return state

    aggregate_trust, agg_recovery = _assemble_aggregate_trust(
        trimmed_capsules,
        selectors_requested=len(requested_selectors),
        selectors_returned=len(trimmed_capsules),
        selectors_omitted=len(state["omitted_selectors"]),
    )
    recovery_warnings.extend(agg_recovery)

    state["present"] = True
    state["capsules"] = trimmed_capsules
    state["selection_order"] = trimmed_selection_order
    state["warnings"] = warnings
    state["recovery_warnings"] = recovery_warnings
    state["fallback_used"] = fallback_used
    state["trust_signals"] = aggregate_trust
    state["salience_metadata"] = _salience_metadata(survived_rows, now)
    state["budget"]["continuity_tokens_used"] = sum(_estimated_tokens(_render_value(item)) for item in trimmed_capsules)
    return state
