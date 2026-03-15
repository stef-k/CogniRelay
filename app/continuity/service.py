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
from app.models import ContinuityCapsule, ContinuityUpsertRequest, ContextRetrieveRequest
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


def _canonical_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _parse_iso(value: str | None) -> datetime | None:
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
    dt = _parse_iso(value)
    if dt is None:
        raise HTTPException(status_code=400, detail=f"Invalid UTC timestamp for {field_name}")
    if not (value.endswith("Z") or value.endswith("+00:00")):
        raise HTTPException(status_code=400, detail=f"Timestamp must be UTC for {field_name}")
    return dt


def _normalize_subject_id(subject_id: str) -> str:
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
    normalized = _normalize_subject_id(subject_id)
    return f"{CONTINUITY_DIR_REL}/{subject_kind}-{normalized}.json"


def _validate_repo_relative_paths(repo_root: Path, paths: list[str], field_name: str) -> None:
    for rel in paths:
        if not rel or not CONTINUITY_PATH_RE.match(rel):
            raise HTTPException(status_code=400, detail=f"Invalid repo-relative path in {field_name}")
        try:
            safe_path(repo_root, rel)
        except StorageError as e:
            raise HTTPException(status_code=400, detail=f"Invalid repo-relative path in {field_name}: {e}") from e


def _validate_capsule(repo_root: Path, capsule: ContinuityCapsule) -> tuple[dict[str, Any], str]:
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
    payload = capsule.model_dump(mode="json", exclude_none=True)
    canonical = _canonical_json(payload)
    if len(canonical.encode("utf-8")) > 12 * 1024:
        raise HTTPException(status_code=400, detail="Continuity capsule exceeds 12 KB serialized UTF-8")
    return payload, canonical


def _resolve_selector(req: ContextRetrieveRequest) -> tuple[str, str, str] | None:
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


def _load_capsule(repo_root: Path, rel: str, *, expected_subject: tuple[str, str] | None = None) -> dict[str, Any]:
    path = safe_path(repo_root, rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Continuity capsule not found")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        capsule = ContinuityCapsule.model_validate(payload).model_dump(mode="json", exclude_none=True)
        if expected_subject is not None:
            expected_kind, expected_id = expected_subject
            if capsule.get("subject_kind") != expected_kind or capsule.get("subject_id") != expected_id:
                raise HTTPException(status_code=400, detail="Continuity capsule subject does not match requested subject")
        return capsule
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity capsule: {e}") from e
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity capsule JSON: {e}") from e


def _effective_stale_seconds(capsule: dict[str, Any]) -> int | None:
    freshness = capsule.get("freshness") if isinstance(capsule.get("freshness"), dict) else {}
    explicit = freshness.get("stale_after_seconds")
    if explicit is not None:
        return int(explicit)
    freshness_class = str(freshness.get("freshness_class") or "situational")
    return CONTINUITY_DEFAULT_STALE.get(freshness_class)


def _continuity_phase(capsule: dict[str, Any], now: datetime) -> tuple[str, list[str]]:
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
    return int(math.ceil(len(text) / 4.0))


def _render_value(value: Any) -> str:
    if isinstance(value, list):
        return "\n".join(f"- {item}" for item in value)
    if isinstance(value, dict):
        return "\n".join(f"{key}: {_render_value(value[key])}" for key in value)
    return str(value)


def _truncate_string(value: str, max_tokens: int) -> str:
    if max_tokens <= 0:
        return ""
    max_chars = max_tokens * 4
    if len(value) <= max_chars:
        return value
    if max_chars <= 3:
        return "." * max(0, max_chars)
    return value[: max_chars - 3] + "..."


def _truncate_list(items: list[str], max_tokens: int) -> list[str]:
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
    parts = dotted.split(".")
    cur: Any = payload
    for key in parts[:-1]:
        if not isinstance(cur, dict):
            return
        cur = cur.get(key)
    if isinstance(cur, dict):
        cur.pop(parts[-1], None)


def _trim_capsule(capsule: dict[str, Any], max_tokens: int) -> dict[str, Any] | None:
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


def build_continuity_state(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContextRetrieveRequest,
    now: datetime,
) -> dict[str, Any]:
    budget = _budget(req.max_tokens_estimate)
    state = {
        "present": False,
        "capsules": [],
        "selection_order": [],
        "budget": budget,
        "warnings": [],
    }
    if req.continuity_mode == "off":
        return state
    selector = _resolve_selector(req)
    if selector is None:
        if req.continuity_mode == "required":
            raise HTTPException(status_code=404, detail="Continuity capsule not found")
        return state
    kind, subject_id, resolution = selector
    rel = continuity_rel_path(kind, subject_id)
    auth.require_read_path(rel)
    path = repo_root / rel
    if not path.exists():
        if req.continuity_mode == "required":
            raise HTTPException(status_code=404, detail="Continuity capsule not found")
        return state
    capsule = _load_capsule(repo_root, rel, expected_subject=(kind, subject_id))
    phase, warnings = _continuity_phase(capsule, now)
    if phase not in {"expired", "expired_by_age"}:
        trimmed = _trim_capsule(capsule, budget["continuity_tokens_reserved"])
        if trimmed is not None:
            state["present"] = True
            state["capsules"] = [trimmed]
            state["selection_order"] = [f"{resolution}:{kind}:{subject_id}"]
            state["warnings"] = warnings
            state["budget"]["continuity_tokens_used"] = _estimated_tokens(_render_value(trimmed))
            return state
        state["warnings"] = warnings + [CONTINUITY_WARNING_TRUNCATED]
        return state
    state["warnings"] = warnings
    return state
