"""Selector resolution, warning qualification, and multi-capsule retrieval helpers."""

from __future__ import annotations

from fastapi import HTTPException

from app.continuity.constants import (
    CONTINUITY_SUBJECT_RE,
    CONTINUITY_WARNING_TRUNCATED,
    CONTINUITY_WARNING_TRUNCATED_MULTI,
)
from app.continuity.paths import _normalize_subject_id
from app.models import ContextRetrieveRequest


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
    """Return whether retrieval should use selector-qualified multi-capsule warning strings."""
    return "continuity_selectors" in req.model_fields_set and bool(req.continuity_selectors)


def _selector_key(subject_kind: str, subject_id: str) -> tuple[str, str]:
    """Return the normalized selector identity key used for deduplication."""
    return subject_kind, _normalize_subject_id(subject_id)


def _format_selector(subject_kind: str, subject_id: str) -> str:
    """Format a selector string using the original subject identifier."""
    return f"{subject_kind}:{subject_id}"


def _qualify_warning(warning: str, subject_kind: str, subject_id: str, *, multi_mode: bool) -> str:
    """Return a warning string in either single-capsule or selector-qualified retrieval format."""
    if warning == CONTINUITY_WARNING_TRUNCATED_MULTI and not multi_mode:
        return CONTINUITY_WARNING_TRUNCATED
    if not multi_mode:
        return warning
    return f"{warning}:{subject_kind}:{subject_id}"


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
