"""Token budget computation and deterministic capsule trimming."""

from __future__ import annotations

import json
import math
from typing import Any


def _estimated_tokens(text: str) -> int:
    """Estimate token usage with the repository four-characters-per-token heuristic."""
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


def _has_nested(payload: dict[str, Any], dotted: str) -> bool:
    """Return True if a dotted nested key exists in a JSON-like payload."""
    parts = dotted.split(".")
    cur: Any = payload
    for key in parts[:-1]:
        if not isinstance(cur, dict):
            return False
        cur = cur.get(key)
    return isinstance(cur, dict) and parts[-1] in cur


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


def _trim_capsule(capsule: dict[str, Any], max_tokens: int) -> tuple[dict[str, Any] | None, list[str]]:
    """Trim a capsule deterministically to fit the reserved continuity budget.

    Returns ``(trimmed_capsule_or_None, trimmed_fields)`` where
    *trimmed_fields* lists dotted paths of fields that were dropped or
    truncated during trimming.
    """
    payload = json.loads(json.dumps(capsule, ensure_ascii=False))
    dropped: list[str] = []
    # Lower-commitment fields (trailing_notes, curiosity_queue, negative_decisions) trim before
    # working_hypotheses so deliberate non-action survives longer than residual notes/curiosity,
    # while hypotheses still outlive all three.
    for dotted in (
        "metadata",
        "canonical_sources",
        "freshness",
        "attention_policy.presence_bias_overrides",
        "continuity.relationship_model.sensitivity_notes",
        "continuity.relationship_model.preferred_style",
        "continuity.retrieval_hints.avoid",
        "continuity.retrieval_hints.load_next",
        "continuity.trailing_notes",
        "continuity.curiosity_queue",
        "continuity.rationale_entries",
        "continuity.negative_decisions",
        "continuity.working_hypotheses",
        "stable_preferences",
    ):
        if _estimated_tokens(_render_value(payload)) <= max_tokens:
            break
        if _has_nested(payload, dotted):
            _drop_nested(payload, dotted)
            dropped.append(dotted)

    continuity = payload.get("continuity")
    if not isinstance(continuity, dict):
        return None, dropped
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
        dotted_field = f"continuity.{field}"
        if field == "retrieval_hints.must_include":
            hints = continuity.get("retrieval_hints")
            if isinstance(hints, dict):
                before = list(hints.get("must_include") or [])
                hints["must_include"] = _truncate_list(list(before), max(1, max_tokens // 4))
                if not hints["must_include"]:
                    hints.pop("must_include", None)
                    if before:
                        dropped.append("continuity.retrieval_hints.must_include")
                elif len(hints["must_include"]) < len(before):
                    dropped.append("continuity.retrieval_hints.must_include")
        elif field == "relationship_model":
            model = continuity.get("relationship_model")
            if isinstance(model, dict):
                if model.get("trust_level") is not None:
                    model.pop("trust_level", None)
                    dropped.append("continuity.relationship_model.trust_level")
                elif model:
                    removed_key = sorted(model)[0]
                    model.pop(removed_key, None)
                    dropped.append(f"continuity.relationship_model.{removed_key}")
                if not model:
                    continuity.pop("relationship_model", None)
        elif field == "long_horizon_commitments":
            current = continuity.get(field)
            if isinstance(current, list):
                before_len = len(current)
                continuity[field] = _truncate_list(list(current), max(1, max_tokens // 4))
                if len(continuity[field]) < before_len:
                    dropped.append(dotted_field)
                if not continuity[field]:
                    continuity.pop(field, None)
        elif field == "stance_summary":
            before_val = str(continuity.get("stance_summary", ""))
            continuity["stance_summary"] = _truncate_string(before_val, max(1, max_tokens // 4))
            if continuity["stance_summary"] != before_val:
                dropped.append(dotted_field)
        else:
            current = continuity.get(field)
            if isinstance(current, list):
                before_len = len(current)
                continuity[field] = _truncate_list(list(current), max(1, max_tokens // 4))
                if len(continuity[field]) < before_len:
                    dropped.append(dotted_field)
        if field in {"drift_signals", "open_loops", "active_constraints", "active_concerns", "top_priorities"} and not continuity.get(field):
            continuity[field] = []

    min_required = any(
        continuity.get(name)
        for name in ("top_priorities", "active_concerns", "active_constraints", "open_loops", "drift_signals", "stance_summary")
    )
    if not min_required or _estimated_tokens(_render_value(payload)) > max_tokens:
        return None, dropped
    return payload, dropped


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
