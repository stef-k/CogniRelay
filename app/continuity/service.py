"""Continuity capsule validation, storage, and retrieval shaping."""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from fastapi import HTTPException
from pydantic import ValidationError

from app.auth import AuthContext
from app.git_manager import GitManager
from app.models import (
    ContinuityArchiveRequest,
    ContinuityCapsule,
    ContinuityListRequest,
    ContinuityReadRequest,
    ContinuityUpsertRequest,
    ContextRetrieveRequest,
)
from app.storage import StorageError, safe_path, write_text_file

CONTINUITY_DIR_REL = "memory/continuity"
CONTINUITY_SUBJECT_RE = re.compile(r"^(task|thread):(.+)$")
CONTINUITY_PATH_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")
CONTINUITY_DEFAULT_STALE: dict[str, int | None] = {
    "persistent": None,
    "durable": 15552000,
    "situational": 2592000,
    "ephemeral": 259200,
}
CONTINUITY_WARNING_STALE_SOFT = "continuity_stale_soft"
CONTINUITY_WARNING_STALE_HARD = "continuity_stale_hard"
CONTINUITY_WARNING_EXPIRED = "continuity_expired"
CONTINUITY_WARNING_TRUNCATED = "continuity_truncated_to_zero"
CONTINUITY_WARNING_TRUNCATED_MULTI = "continuity_capsule_truncated_to_zero"
CONTINUITY_INTERACTION_BOUNDARY_KINDS = {
    "person_switch",
    "thread_switch",
    "task_switch",
    "public_reply",
    "manual_checkpoint",
}


def _canonical_json(data: Any) -> str:
    """Serialize JSON deterministically for hashing and idempotency checks."""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO timestamp into a timezone-aware UTC datetime when possible."""
    if not value:
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)


def _require_utc_timestamp(value: str, field_name: str) -> datetime:
    """Require a valid UTC timestamp or raise an HTTP 400 error."""
    dt = _parse_iso(value)
    if dt is None:
        raise HTTPException(status_code=400, detail=f"Invalid UTC timestamp for {field_name}")
    if not (value.endswith("Z") or value.endswith("+00:00")):
        raise HTTPException(status_code=400, detail=f"Timestamp must be UTC for {field_name}")
    return dt


def _normalize_subject_id(subject_id: str) -> str:
    """Normalize a subject id into a filesystem-safe continuity key."""
    raw = subject_id.strip().lower()
    normalized = re.sub(r"[^a-z0-9._-]+", "-", raw)
    normalized = normalized.strip("-")
    if not normalized:
        raise HTTPException(status_code=400, detail="Normalized subject_id is empty")
    normalized = normalized[:120].strip("-")
    if not normalized:
        raise HTTPException(status_code=400, detail="Normalized subject_id is empty")
    return normalized


def continuity_rel_path(subject_kind: str, subject_id: str) -> str:
    """Return the repository-relative path for a continuity capsule."""
    normalized = _normalize_subject_id(subject_id)
    return f"{CONTINUITY_DIR_REL}/{subject_kind}-{normalized}.json"


def _validate_repo_relative_paths(repo_root: Path, paths: list[str], field_name: str) -> None:
    """Validate that repo-relative paths stay within the repository root."""
    for rel in paths:
        if not rel or not CONTINUITY_PATH_RE.match(rel):
            raise HTTPException(status_code=400, detail=f"Invalid repo-relative path in {field_name}")
        try:
            safe_path(repo_root, rel)
        except StorageError as e:
            raise HTTPException(status_code=400, detail=f"Invalid repo-relative path in {field_name}: {e}") from e


def _validate_capsule(repo_root: Path, capsule: ContinuityCapsule) -> tuple[dict[str, Any], str]:
    """Validate a capsule and return normalized payload plus canonical JSON."""
    _require_utc_timestamp(capsule.updated_at, "updated_at")
    _require_utc_timestamp(capsule.verified_at, "verified_at")
    if capsule.freshness and capsule.freshness.expires_at:
        _require_utc_timestamp(capsule.freshness.expires_at, "freshness.expires_at")
    for source_input in list(capsule.source.inputs):
        if len(source_input) > 200:
            raise HTTPException(status_code=400, detail="Value too long in source.inputs")
    for field_name in (
        "top_priorities",
        "active_concerns",
        "active_constraints",
        "open_loops",
        "drift_signals",
        "working_hypotheses",
        "long_horizon_commitments",
    ):
        for value in list(getattr(capsule.continuity, field_name)):
            if len(value) > 160:
                raise HTTPException(status_code=400, detail=f"Value too long in {field_name}")
    for value in list(capsule.continuity.session_trajectory):
        if len(value) > 80:
            raise HTTPException(status_code=400, detail="Value too long in continuity.session_trajectory")
    if len(capsule.continuity.stance_summary) > 240:
        raise HTTPException(status_code=400, detail="Value too long in continuity.stance_summary")
    if capsule.continuity.relationship_model:
        for value in capsule.continuity.relationship_model.preferred_style:
            if len(value) > 80:
                raise HTTPException(status_code=400, detail="Value too long in relationship_model.preferred_style")
        for value in capsule.continuity.relationship_model.sensitivity_notes:
            if len(value) > 120:
                raise HTTPException(status_code=400, detail="Value too long in relationship_model.sensitivity_notes")
    if capsule.attention_policy:
        for value in capsule.attention_policy.presence_bias_overrides:
            if len(value) > 160:
                raise HTTPException(status_code=400, detail="Value too long in attention_policy.presence_bias_overrides")
    if capsule.continuity.retrieval_hints:
        for field_name in ("must_include", "avoid"):
            for value in list(getattr(capsule.continuity.retrieval_hints, field_name)):
                if len(value) > 160:
                    raise HTTPException(status_code=400, detail=f"Value too long in retrieval_hints.{field_name}")
        _validate_repo_relative_paths(repo_root, list(capsule.continuity.retrieval_hints.load_next), "retrieval_hints.load_next")
    if capsule.canonical_sources:
        _validate_repo_relative_paths(repo_root, list(capsule.canonical_sources), "canonical_sources")
    if capsule.metadata and len(capsule.metadata) > 12:
        raise HTTPException(status_code=400, detail="Too many metadata keys")
    for key, value in capsule.metadata.items():
        if not isinstance(key, str):
            raise HTTPException(status_code=400, detail="Invalid metadata key")
        if isinstance(value, (dict, list)):
            raise HTTPException(status_code=400, detail="Metadata values must be scalar")
    boundary_kind = capsule.metadata.get("interaction_boundary_kind")
    if boundary_kind is not None:
        if capsule.source.update_reason != "interaction_boundary":
            raise HTTPException(status_code=400, detail="metadata.interaction_boundary_kind requires source.update_reason=interaction_boundary")
        if boundary_kind not in CONTINUITY_INTERACTION_BOUNDARY_KINDS:
            raise HTTPException(status_code=400, detail="Invalid metadata.interaction_boundary_kind")
    elif capsule.source.update_reason == "interaction_boundary":
        raise HTTPException(status_code=400, detail="metadata.interaction_boundary_kind is required when source.update_reason=interaction_boundary")
    payload = capsule.model_dump(mode="json", exclude_none=True)
    canonical = _canonical_json(payload)
    if len(canonical.encode("utf-8")) > 12 * 1024:
        raise HTTPException(status_code=400, detail="Continuity capsule exceeds 12 KB serialized UTF-8")
    return payload, canonical


def _resolve_selector(req: ContextRetrieveRequest) -> tuple[str, str, str] | None:
    """Resolve an explicit or inferred continuity selector from a request."""
    if bool(req.subject_kind) != bool(req.subject_id):
        raise HTTPException(status_code=400, detail="subject_kind and subject_id must be provided together")
    if req.subject_kind and req.subject_id:
        return req.subject_kind, req.subject_id, "explicit"
    m = CONTINUITY_SUBJECT_RE.match(req.task.strip())
    if not m:
        return None
    kind, value = m.group(1), m.group(2).strip()
    if kind not in {"task", "thread"} or not value:
        return None
    return kind, value, "inferred"


def _warning_mode_is_multi(req: ContextRetrieveRequest) -> bool:
    """Return whether retrieval should use V2 multi-capsule warning strings."""
    return "continuity_selectors" in req.model_fields_set and bool(req.continuity_selectors)


def _selector_key(subject_kind: str, subject_id: str) -> tuple[str, str]:
    """Return the normalized selector identity key used for deduplication."""
    return subject_kind, _normalize_subject_id(subject_id)


def _format_selector(subject_kind: str, subject_id: str) -> str:
    """Format a selector string using the original subject identifier."""
    return f"{subject_kind}:{subject_id}"


def _qualify_warning(warning: str, subject_kind: str, subject_id: str, *, multi_mode: bool) -> str:
    """Return a warning string in either V1 or V2 retrieval format."""
    if warning == CONTINUITY_WARNING_TRUNCATED_MULTI and not multi_mode:
        return CONTINUITY_WARNING_TRUNCATED
    if not multi_mode:
        return warning
    return f"{warning}:{subject_kind}:{subject_id}"


def _restore_failed_archive(active_path: Path, archive_path: Path, active_bytes: bytes) -> None:
    """Restore the active capsule and discard the archive envelope after a failed archive commit."""
    write_text_file(active_path, active_bytes.decode("utf-8"))
    if archive_path.exists():
        archive_path.unlink()


def _effective_selectors(req: ContextRetrieveRequest) -> tuple[list[dict[str, str]], list[str], list[str]]:
    """Build selected selectors, requested selectors, and selector-limit omissions for retrieval."""
    selectors: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    if req.subject_kind and req.subject_id:
        key = _selector_key(req.subject_kind, req.subject_id)
        selectors.append(
            {
                "subject_kind": req.subject_kind,
                "subject_id": req.subject_id,
                "resolution": "explicit",
            }
        )
        seen.add(key)

    for selector in req.continuity_selectors:
        key = _selector_key(selector.subject_kind, selector.subject_id)
        if key in seen:
            continue
        selectors.append(
            {
                "subject_kind": selector.subject_kind,
                "subject_id": selector.subject_id,
                "resolution": "explicit",
            }
        )
        seen.add(key)

    omitted: list[str] = []
    if selectors:
        requested = [_format_selector(item["subject_kind"], item["subject_id"]) for item in selectors]
        if len(selectors) > req.continuity_max_capsules:
            omitted = [_format_selector(item["subject_kind"], item["subject_id"]) for item in selectors[req.continuity_max_capsules :]]
            selectors = selectors[: req.continuity_max_capsules]
        return selectors, requested, omitted

    inferred = _resolve_selector(req)
    if inferred is None:
        return [], [], omitted
    kind, subject_id, resolution = inferred
    requested = [_format_selector(kind, subject_id)]
    return [{"subject_kind": kind, "subject_id": subject_id, "resolution": resolution}], requested, omitted


def _load_capsule(repo_root: Path, rel: str, *, expected_subject: tuple[str, str] | None = None) -> dict[str, Any]:
    """Load one capsule from disk and enforce optional subject matching."""
    path = safe_path(repo_root, rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Continuity capsule not found")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        capsule = ContinuityCapsule.model_validate(payload).model_dump(mode="json", exclude_none=True)
        if expected_subject is not None:
            expected_kind, expected_id = expected_subject
            capsule_kind = str(capsule.get("subject_kind") or "")
            capsule_subject_id = str(capsule.get("subject_id") or "")
            if capsule_kind != expected_kind or _normalize_subject_id(capsule_subject_id) != _normalize_subject_id(expected_id):
                raise HTTPException(status_code=400, detail="Continuity capsule subject does not match requested subject")
        return capsule
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity capsule: {e}") from e
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity capsule JSON: {e}") from e


def _effective_stale_seconds(capsule: dict[str, Any]) -> int | None:
    """Compute the effective stale threshold for a capsule."""
    freshness = capsule.get("freshness") if isinstance(capsule.get("freshness"), dict) else {}
    explicit = freshness.get("stale_after_seconds")
    if explicit is not None:
        return int(explicit)
    freshness_class = str(freshness.get("freshness_class") or "situational")
    return CONTINUITY_DEFAULT_STALE.get(freshness_class)


def _continuity_phase(capsule: dict[str, Any], now: datetime) -> tuple[str, list[str]]:
    """Determine freshness phase and warnings for the given capsule."""
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


def _estimated_tokens(text: str) -> int:
    """Estimate token usage with the V1 four-characters-per-token heuristic."""
    return int(math.ceil(len(text) / 4.0))


def _render_value(value: Any) -> str:
    """Render a JSON-like value into the internal token-accounting form."""
    if isinstance(value, list):
        return "\n".join(f"- {item}" for item in value)
    if isinstance(value, dict):
        return "\n".join(f"{key}: {_render_value(value[key])}" for key in value)
    return str(value)


def _truncate_string(value: str, max_tokens: int) -> str:
    """Truncate a string to fit a token budget using a character heuristic."""
    if max_tokens <= 0:
        return ""
    max_chars = max_tokens * 4
    if len(value) <= max_chars:
        return value
    if max_chars <= 3:
        return "." * max(0, max_chars)
    return value[: max_chars - 3] + "..."


def _truncate_list(items: list[str], max_tokens: int) -> list[str]:
    """Trim list entries until the rendered list fits the token budget."""
    if max_tokens <= 0:
        return []
    out = list(items)
    while out and _estimated_tokens(_render_value(out)) > max_tokens:
        out.pop()
    while out and _estimated_tokens(_render_value(out)) > max_tokens:
        trimmed = _truncate_string(out[-1], max_tokens)
        if not trimmed or trimmed == out[-1]:
            out.pop()
        else:
            out[-1] = trimmed
    return out


def _drop_nested(payload: dict[str, Any], dotted: str) -> None:
    """Drop a dotted nested key from a JSON-like payload when present."""
    parts = dotted.split(".")
    cur: Any = payload
    for key in parts[:-1]:
        if not isinstance(cur, dict):
            return
        cur = cur.get(key)
    if isinstance(cur, dict):
        cur.pop(parts[-1], None)


def _trim_capsule(capsule: dict[str, Any], max_tokens: int) -> dict[str, Any] | None:
    """Trim a capsule deterministically to fit the reserved continuity budget."""
    payload = json.loads(json.dumps(capsule, ensure_ascii=False))
    for dotted in (
        "metadata",
        "canonical_sources",
        "freshness",
        "attention_policy.presence_bias_overrides",
        "continuity.relationship_model.sensitivity_notes",
        "continuity.relationship_model.preferred_style",
        "continuity.retrieval_hints.avoid",
        "continuity.retrieval_hints.load_next",
        "continuity.working_hypotheses",
    ):
        if _estimated_tokens(_render_value(payload)) <= max_tokens:
            break
        _drop_nested(payload, dotted)

    continuity = payload.get("continuity")
    if not isinstance(continuity, dict):
        return None
    for field in (
        "retrieval_hints.must_include",
        "relationship_model",
        "long_horizon_commitments",
        "stance_summary",
        "drift_signals",
        "open_loops",
        "active_constraints",
        "active_concerns",
        "top_priorities",
    ):
        if _estimated_tokens(_render_value(payload)) <= max_tokens:
            break
        if field == "retrieval_hints.must_include":
            hints = continuity.get("retrieval_hints")
            if isinstance(hints, dict):
                hints["must_include"] = _truncate_list(list(hints.get("must_include") or []), max(1, max_tokens // 4))
                if not hints["must_include"]:
                    hints.pop("must_include", None)
        elif field == "relationship_model":
            model = continuity.get("relationship_model")
            if isinstance(model, dict):
                if model.get("trust_level") is not None:
                    model.pop("trust_level", None)
                elif model:
                    model.pop(sorted(model)[0], None)
                if not model:
                    continuity.pop("relationship_model", None)
        elif field == "stance_summary":
            continuity["stance_summary"] = _truncate_string(str(continuity.get("stance_summary", "")), max(1, max_tokens // 4))
        else:
            current = continuity.get(field)
            if isinstance(current, list):
                continuity[field] = _truncate_list(list(current), max(1, max_tokens // 4))
        if field in {"drift_signals", "open_loops", "active_constraints", "active_concerns", "top_priorities"} and not continuity.get(field):
            continuity[field] = []

    min_required = any(
        continuity.get(name)
        for name in ("top_priorities", "active_concerns", "active_constraints", "open_loops", "drift_signals", "stance_summary")
    )
    if not min_required or _estimated_tokens(_render_value(payload)) > max_tokens:
        return None
    return payload


def _budget(requested_max_tokens: int) -> dict[str, int]:
    """Compute the continuity token reservation from the requested budget."""
    token_budget_hint = min(requested_max_tokens, 4000)
    if token_budget_hint < 1000:
        reserved = min(150, max(0, int(token_budget_hint * 0.2)))
    else:
        reserved = min(800, max(200, int(token_budget_hint * 0.2)))
    return {
        "requested_max_tokens_estimate": requested_max_tokens,
        "token_budget_hint": token_budget_hint,
        "continuity_tokens_reserved": reserved,
        "continuity_tokens_used": 0,
    }


def _reject_stale_or_conflicting_write(path: Path, req: ContinuityUpsertRequest) -> None:
    """Reject older or equal-timestamp conflicting writes against the stored capsule."""
    if not path.exists() or not path.is_file():
        return
    try:
        current = ContinuityCapsule.model_validate(json.loads(path.read_text(encoding="utf-8")))
    except (ValidationError, json.JSONDecodeError):
        return
    incoming_updated = _require_utc_timestamp(req.capsule.updated_at, "updated_at")
    current_updated = _require_utc_timestamp(current.updated_at, "updated_at")
    if incoming_updated < current_updated:
        raise HTTPException(status_code=409, detail="Incoming continuity capsule is older than the current stored capsule")
    if incoming_updated == current_updated:
        raise HTTPException(status_code=409, detail="Incoming continuity capsule conflicts with the current stored capsule timestamp")


def continuity_upsert_service(
    *,
    repo_root: Path,
    gm: GitManager,
    auth: AuthContext,
    req: ContinuityUpsertRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Validate and persist one continuity capsule with commit-on-change behavior."""
    auth.require("write:projects")
    if req.capsule.subject_kind != req.subject_kind or req.capsule.subject_id != req.subject_id:
        raise HTTPException(status_code=400, detail="Capsule subject does not match request subject")
    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    auth.require_write_path(rel)
    _validate_capsule(repo_root, req.capsule)
    path = safe_path(repo_root, rel)
    canonical = _canonical_json(req.capsule.model_dump(mode="json", exclude_none=True))
    new_bytes = canonical.encode("utf-8")
    old_bytes = path.read_bytes() if path.exists() else None
    if old_bytes != new_bytes:
        _reject_stale_or_conflicting_write(path, req)
    capsule_sha256 = hashlib.sha256(new_bytes).hexdigest()
    created = not path.exists()
    changed = old_bytes != new_bytes
    if changed:
        write_text_file(path, canonical)
    committed = False
    if changed:
        committed = gm.commit_file(path, req.commit_message or f"continuity: upsert {req.subject_kind} {req.subject_id}")
    audit(
        auth,
        "continuity_upsert",
        {
            "subject_kind": req.subject_kind,
            "subject_id": req.subject_id,
            "path": rel,
            "created": created,
            "updated": bool(changed and not created),
            "capsule_sha256": capsule_sha256,
            "idempotency_key": req.idempotency_key,
            "committed": committed,
        },
    )
    return {
        "ok": True,
        "path": rel,
        "created": created,
        "updated": bool(changed and not created),
        "latest_commit": gm.latest_commit(),
        "capsule_sha256": capsule_sha256,
    }


def continuity_read_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContinuityReadRequest,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Read one active continuity capsule by exact selector."""
    auth.require("read:files")
    rel = continuity_rel_path(req.subject_kind, req.subject_id)
    auth.require_read_path(rel)
    capsule = _load_capsule(repo_root, rel, expected_subject=(req.subject_kind, req.subject_id))
    audit(auth, "continuity_read", {"subject_kind": req.subject_kind, "subject_id": req.subject_id, "path": rel})
    return {"ok": True, "path": rel, "capsule": capsule, "archived": False}


def continuity_list_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContinuityListRequest,
    now: datetime,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """List active continuity capsule summaries under the repository namespace."""
    auth.require("read:files")
    base = repo_root / CONTINUITY_DIR_REL
    summaries: list[dict[str, Any]] = []
    if base.exists() and base.is_dir():
        for path in sorted(base.iterdir(), key=lambda item: item.name):
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
                capsule = _load_capsule(repo_root, rel)
            except HTTPException as exc:
                if exc.status_code in {400, 404}:
                    continue
                raise
            phase, _ = _continuity_phase(capsule, now)
            freshness = capsule.get("freshness") if isinstance(capsule.get("freshness"), dict) else {}
            summaries.append(
                {
                    "subject_kind": capsule["subject_kind"],
                    "subject_id": capsule["subject_id"],
                    "path": rel,
                    "updated_at": capsule["updated_at"],
                    "verified_at": capsule["verified_at"],
                    "verification_kind": capsule.get("verification_kind"),
                    "freshness_class": freshness.get("freshness_class"),
                    "phase": phase,
                }
            )
    summaries.sort(key=lambda row: (str(row["subject_kind"]), str(row["subject_id"])))
    summaries = summaries[: req.limit]
    audit(
        auth,
        "continuity_list",
        {
            "subject_kind": req.subject_kind,
            "count": len(summaries),
        },
    )
    return {"ok": True, "count": len(summaries), "capsules": summaries}


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
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    archive_rel = f"{CONTINUITY_DIR_REL}/archive/{req.subject_kind}-{_normalize_subject_id(req.subject_id)}-{timestamp}.json"
    auth.require_read_path(rel)
    auth.require_write_path(rel)
    auth.require_write_path(archive_rel)

    capsule = _load_capsule(repo_root, rel, expected_subject=(req.subject_kind, req.subject_id))
    archive_payload = {
        "schema_type": "continuity_archive_envelope",
        "schema_version": "1.0",
        "archived_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "archived_by": auth.peer_id,
        "reason": req.reason,
        "active_path": rel,
        "capsule": capsule,
    }

    archive_path = safe_path(repo_root, archive_rel)
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    active_path = safe_path(repo_root, rel)
    active_bytes = active_path.read_bytes()
    write_text_file(archive_path, _canonical_json(archive_payload))
    # The active-path deletion must be staged before the git commit can atomically
    # record the archive write plus active-file removal. If the commit step fails,
    # restore the active capsule and discard the archive envelope immediately.
    active_path.unlink()
    try:
        committed = gm.commit_paths([archive_path, active_path], f"continuity: archive {req.subject_kind} {req.subject_id}")
    except Exception:
        _restore_failed_archive(active_path, archive_path, active_bytes)
        raise
    if not committed:
        _restore_failed_archive(active_path, archive_path, active_bytes)
        raise RuntimeError("Continuity archive commit produced no changes")

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
        "latest_commit": gm.latest_commit(),
    }


def build_continuity_state(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContextRetrieveRequest,
    now: datetime,
) -> dict[str, Any]:
    """Load, trim, and package continuity state for context retrieval."""
    budget = _budget(req.max_tokens_estimate)
    state = {
        "present": False,
        "requested_selectors": [],
        "omitted_selectors": [],
        "capsules": [],
        "selection_order": [],
        "budget": budget,
        "warnings": [],
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

    loaded: list[dict[str, Any]] = []
    warnings: list[str] = []
    for item in selectors:
        kind = item["subject_kind"]
        subject_id = item["subject_id"]
        resolution = item["resolution"]
        rel = continuity_rel_path(kind, subject_id)
        auth.require_read_path(rel)
        path = repo_root / rel
        if not path.exists():
            state["omitted_selectors"].append(_format_selector(kind, subject_id))
            continue
        capsule = _load_capsule(repo_root, rel, expected_subject=(kind, subject_id))
        phase, phase_warnings = _continuity_phase(capsule, now)
        warnings.extend(_qualify_warning(warning, kind, subject_id, multi_mode=multi_warning_mode) for warning in phase_warnings)
        if phase in {"expired", "expired_by_age"}:
            state["omitted_selectors"].append(_format_selector(kind, subject_id))
            continue
        loaded.append({"selector": item, "capsule": capsule})

    if not loaded:
        if req.continuity_mode == "required":
            raise HTTPException(status_code=404, detail="Continuity capsule not found")
        state["warnings"] = warnings
        return state

    reserve = budget["continuity_tokens_reserved"]
    count = len(loaded)
    base = reserve // count
    remainder = reserve % count

    trimmed_capsules: list[dict[str, Any]] = []
    trimmed_selection_order: list[str] = []
    for idx, row in enumerate(loaded):
        allocation = base + (1 if idx < remainder else 0)
        selector = row["selector"]
        kind = selector["subject_kind"]
        subject_id = selector["subject_id"]
        resolution = selector["resolution"]
        trimmed = _trim_capsule(row["capsule"], allocation)
        if trimmed is None:
            state["omitted_selectors"].append(_format_selector(kind, subject_id))
            warnings.append(_qualify_warning(CONTINUITY_WARNING_TRUNCATED_MULTI, kind, subject_id, multi_mode=multi_warning_mode))
            continue
        trimmed_capsules.append(trimmed)
        trimmed_selection_order.append(f"{resolution}:{kind}:{subject_id}")

    if not trimmed_capsules:
        if req.continuity_mode == "required":
            raise HTTPException(status_code=404, detail="Continuity capsule not found")
        state["warnings"] = warnings
        return state

    state["present"] = True
    state["capsules"] = trimmed_capsules
    state["selection_order"] = trimmed_selection_order
    state["warnings"] = warnings
    state["budget"]["continuity_tokens_used"] = sum(_estimated_tokens(_render_value(item)) for item in trimmed_capsules)
    return state
