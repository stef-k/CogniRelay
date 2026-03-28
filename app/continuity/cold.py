"""Cold-storage stub building, parsing, and loading."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.continuity.constants import (
    CONTINUITY_COLD_INDEX_DIR_REL,
    CONTINUITY_COLD_STUB_FRONTMATTER_ORDER,
    CONTINUITY_COLD_STUB_SCHEMA_TYPE,
    CONTINUITY_COLD_STUB_SECTION_ORDER,
)
from app.continuity.freshness import (
    _capsule_health_summary,
    _continuity_phase,
    _verification_status,
)
from app.continuity.paths import (
    continuity_archive_rel_path_from_cold_artifact,
    continuity_cold_storage_rel_path,
    continuity_cold_stub_rel_path,
)
from app.storage import safe_path


def _normalize_stub_scalar(value: Any) -> str:
    """Normalize a stub scalar to one trimmed line with newlines replaced."""
    return str(value or "").replace("\r\n", "\n").replace("\r", "\n").replace("\n", " ").strip()


def _truncate_stub_text(value: Any, limit: int) -> str:
    """Apply the cold-stub scalar normalization and code-point truncation."""
    return _normalize_stub_scalar(value)[:limit]


def _render_cold_stub_list(items: Any, *, count: int, limit: int) -> list[str]:
    """Project a list-valued continuity field into bounded cold-stub bullets."""
    if not isinstance(items, list):
        return []
    return [_truncate_stub_text(item, limit) for item in items[:count]]


def _render_cold_negative_decisions(items: Any) -> list[str]:
    """Project negative decisions into deterministic stub bullets."""
    if not isinstance(items, list):
        return []
    lines: list[str] = []
    for item in items[:2]:
        if not isinstance(item, dict):
            continue
        decision = _truncate_stub_text(item.get("decision"), 160)
        rationale = _truncate_stub_text(item.get("rationale"), 240)
        lines.append(f"decision: {decision} | rationale: {rationale}")
    return lines


def _render_cold_rationale_entries(items: Any) -> list[str]:
    """Project rationale entries into bounded stub bullets (active entries only, max 3)."""
    if not isinstance(items, list):
        return []
    lines: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("status") != "active":
            continue
        tag = _truncate_stub_text(item.get("tag"), 80)
        kind = _truncate_stub_text(item.get("kind"), 20)
        summary = _truncate_stub_text(item.get("summary"), 160)
        lines.append(f"[{kind}] {tag}: {summary}")
        if len(lines) >= 3:
            break
    return lines


def _build_cold_stub_text(*, envelope: dict[str, Any], source_archive_path: str, cold_storage_path: str, cold_stored_at: str, now: datetime) -> str:
    """Build the deterministic searchable stub for one cold-stored archive envelope."""
    capsule = envelope["capsule"]
    continuity = capsule.get("continuity") if isinstance(capsule.get("continuity"), dict) else {}
    freshness = capsule.get("freshness") if isinstance(capsule.get("freshness"), dict) else {}
    verification_status = _verification_status(capsule)
    health_status, _ = _capsule_health_summary(capsule)
    phase, _ = _continuity_phase(capsule, now)
    frontmatter = {
        "type": CONTINUITY_COLD_STUB_SCHEMA_TYPE,
        "schema_version": '"1.0"',
        "artifact_state": "cold",
        "subject_kind": _normalize_stub_scalar(capsule.get("subject_kind")),
        "subject_id": _normalize_stub_scalar(capsule.get("subject_id")),
        "source_archive_path": _normalize_stub_scalar(source_archive_path),
        "cold_storage_path": _normalize_stub_scalar(cold_storage_path),
        "archived_at": _normalize_stub_scalar(envelope.get("archived_at")),
        "cold_stored_at": _normalize_stub_scalar(cold_stored_at),
        "verification_kind": _normalize_stub_scalar(capsule.get("verification_kind")),
        "verification_status": _normalize_stub_scalar(verification_status),
        "health_status": _normalize_stub_scalar(health_status),
        "freshness_class": _normalize_stub_scalar(freshness.get("freshness_class")),
        "phase": _normalize_stub_scalar(phase),
        "update_reason": _normalize_stub_scalar((capsule.get("source") or {}).get("update_reason") if isinstance(capsule.get("source"), dict) else ""),
    }
    sections = {
        "top_priorities": _render_cold_stub_list(continuity.get("top_priorities"), count=3, limit=160),
        "active_constraints": _render_cold_stub_list(continuity.get("active_constraints"), count=3, limit=160),
        "active_concerns": _render_cold_stub_list(continuity.get("active_concerns"), count=3, limit=160),
        "open_loops": _render_cold_stub_list(continuity.get("open_loops"), count=3, limit=160),
        "stance_summary": _truncate_stub_text(continuity.get("stance_summary"), 240),
        "drift_signals": _render_cold_stub_list(continuity.get("drift_signals"), count=5, limit=160),
        "session_trajectory": _render_cold_stub_list(continuity.get("session_trajectory"), count=3, limit=80),
        "trailing_notes": _render_cold_stub_list(continuity.get("trailing_notes"), count=3, limit=160),
        "curiosity_queue": _render_cold_stub_list(continuity.get("curiosity_queue"), count=3, limit=120),
        "negative_decisions": _render_cold_negative_decisions(continuity.get("negative_decisions")),
        "rationale_entries": _render_cold_rationale_entries(continuity.get("rationale_entries")),
    }
    lines = ["---"]
    for key in CONTINUITY_COLD_STUB_FRONTMATTER_ORDER:
        lines.append(f"{key}: {frontmatter[key]}")
    lines.append("---")
    for section in CONTINUITY_COLD_STUB_SECTION_ORDER:
        lines.append(f"## {section}")
        if section == "stance_summary":
            lines.append(str(sections[section]))
            continue
        for item in sections[section]:
            lines.append(f"- {item}")
        if not sections[section]:
            lines.append("")
    return "\n".join(lines).rstrip("\n") + "\n"


def _parse_cold_stub_text(text: str) -> tuple[list[tuple[str, str]], str]:
    """Parse a cold-stub frontmatter block and return ordered fields plus the body."""
    if not text.startswith("---\n"):
        raise HTTPException(status_code=400, detail="Invalid continuity cold stub frontmatter")
    parts = text.split("\n---\n", 1)
    if len(parts) != 2:
        raise HTTPException(status_code=400, detail="Invalid continuity cold stub frontmatter")
    frontmatter_raw = parts[0][4:]
    body = parts[1]
    values: list[tuple[str, str]] = []
    for line in frontmatter_raw.splitlines():
        if ":" not in line:
            raise HTTPException(status_code=400, detail="Invalid continuity cold stub frontmatter")
        key, value = line.split(":", 1)
        values.append((key.strip(), value.strip()))
    return values, body



def _load_cold_stub(repo_root: Path, rel: str) -> dict[str, str]:
    """Load and validate one continuity cold stub against shared path helpers."""
    if not rel.startswith(f"{CONTINUITY_COLD_INDEX_DIR_REL}/") or not rel.endswith(".md"):
        raise HTTPException(status_code=400, detail="cold_stub_path must be under memory/continuity/cold/index/")
    path = safe_path(repo_root, rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Continuity cold stub not found")
    try:
        ordered_fields, _body = _parse_cold_stub_text(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid continuity cold stub text: {exc}") from exc
    field_order = [key for key, _value in ordered_fields]
    if field_order != CONTINUITY_COLD_STUB_FRONTMATTER_ORDER:
        raise HTTPException(status_code=400, detail="Invalid continuity cold stub frontmatter order")
    frontmatter = dict(ordered_fields)
    if len(frontmatter) != len(CONTINUITY_COLD_STUB_FRONTMATTER_ORDER):
        raise HTTPException(status_code=400, detail="Invalid continuity cold stub frontmatter fields")
    if frontmatter.get("type") != CONTINUITY_COLD_STUB_SCHEMA_TYPE:
        raise HTTPException(status_code=400, detail="Invalid continuity cold stub type")
    if frontmatter.get("schema_version") != '"1.0"':
        raise HTTPException(status_code=400, detail="Invalid continuity cold stub schema_version")
    source_archive_path = frontmatter["source_archive_path"]
    expected_stub_path = continuity_cold_stub_rel_path(source_archive_path)
    expected_payload_path = continuity_cold_storage_rel_path(source_archive_path)
    if rel != expected_stub_path:
        raise HTTPException(status_code=400, detail="Continuity cold stub path does not match source archive identity")
    if frontmatter.get("cold_storage_path") != expected_payload_path:
        raise HTTPException(status_code=400, detail="Continuity cold stub payload path does not match source archive identity")
    if continuity_archive_rel_path_from_cold_artifact(rel) != source_archive_path:
        raise HTTPException(status_code=400, detail="Continuity cold stub archive identity does not match path")
    return frontmatter
