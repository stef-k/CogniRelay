"""Shared response-contract helpers for lifecycle operations.

Provides structured warning, error-detail, and lock-error builders that
all lifecycle modules (continuity, registry_lifecycle, artifact_lifecycle,
segment_history) use to produce uniform response envelopes.

This is a leaf module — it imports only ``fastapi`` and stdlib types.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException


def make_warning(
    code: str,
    detail: str,
    *,
    path: str | None = None,
    segment_id: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """Build a structured warning dict.

    The shape matches the ``segment_history`` reference implementation so
    that all modules emit warnings in a single, parseable format.
    """
    w: dict[str, Any] = {
        "code": code,
        "detail": detail,
        "path": path,
        "segment_id": segment_id,
    }
    w.update(extra)
    return w


def make_error_detail(
    *,
    ok: bool = False,
    operation: str,
    family: str | None = None,
    error_code: str,
    error_detail: str,
    **extra: Any,
) -> dict[str, Any]:
    """Build a structured ``HTTPException.detail`` envelope.

    Matches the ``segment_history`` reference implementation so that all
    modules produce uniform error bodies.
    """
    d: dict[str, Any] = {"ok": ok, "operation": operation}
    if family is not None:
        d["family"] = family
    d["error"] = {"code": error_code, "detail": error_detail}
    d.update(extra)
    return d


def make_lock_error(
    operation: str,
    family: str | None,
    exc: Exception,
    *,
    is_timeout: bool,
) -> HTTPException:
    """Construct a lock-failure ``HTTPException``.

    Lock timeout  → **409 Conflict** (transient contention, caller may retry).
    Lock infra    → **503 Service Unavailable** (infrastructure problem).

    These status codes match the ``segment_history`` reference implementation.
    """
    if is_timeout:
        return HTTPException(
            status_code=409,
            detail=make_error_detail(
                operation=operation,
                family=family,
                error_code=f"{operation}_source_lock_timeout",
                error_detail=str(exc),
            ),
        )
    return HTTPException(
        status_code=503,
        detail=make_error_detail(
            operation=operation,
            family=family,
            error_code=f"{operation}_lock_infrastructure_unavailable",
            error_detail=str(exc),
        ),
    )
