"""Path computation, subject normalization, per-subject locking, and archive-envelope identity."""

from __future__ import annotations

import hashlib
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.continuity.constants import (
    CONTINUITY_COLD_DIR_REL,
    CONTINUITY_COLD_INDEX_DIR_REL,
    CONTINUITY_DIR_REL,
)
from app.timestamps import format_compact


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


def continuity_fallback_rel_path(subject_kind: str, subject_id: str) -> str:
    """Return the repository-relative path for a continuity fallback snapshot."""
    normalized = _normalize_subject_id(subject_id)
    return f"{CONTINUITY_DIR_REL}/fallback/{subject_kind}-{normalized}.json"


def _validate_archive_rel_path(rel: str) -> str:
    """Require a repo-relative continuity archive-envelope path."""
    normalized = str(rel or "").strip()
    if not normalized.startswith(f"{CONTINUITY_DIR_REL}/archive/") or not normalized.endswith(".json"):
        raise HTTPException(status_code=400, detail="source_archive_path must be under memory/continuity/archive/")
    return normalized


def continuity_cold_storage_rel_path(source_archive_path: str) -> str:
    """Map an archive envelope path to its cold gzip payload path."""
    archive_rel = _validate_archive_rel_path(source_archive_path)
    return f"{CONTINUITY_COLD_DIR_REL}/{Path(archive_rel).name}.gz"


def continuity_cold_stub_rel_path(source_archive_path: str) -> str:
    """Map an archive envelope path to its hot cold-stub path."""
    archive_rel = _validate_archive_rel_path(source_archive_path)
    return f"{CONTINUITY_COLD_INDEX_DIR_REL}/{Path(archive_rel).stem}.md"


def continuity_archive_rel_path_from_cold_artifact(cold_artifact_path: str) -> str:
    """Derive the archive envelope path from a cold payload or stub path."""
    rel = str(cold_artifact_path or "").strip()
    if rel.startswith(f"{CONTINUITY_COLD_INDEX_DIR_REL}/") and rel.endswith(".md"):
        basename = Path(rel).stem + ".json"
        return f"{CONTINUITY_DIR_REL}/archive/{basename}"
    if rel.startswith(f"{CONTINUITY_COLD_DIR_REL}/") and rel.endswith(".json.gz"):
        basename = Path(rel).name[:-3]
        return f"{CONTINUITY_DIR_REL}/archive/{basename}"
    raise HTTPException(status_code=400, detail="Invalid continuity cold artifact path")


def _continuity_subject_lock_id(subject_kind: str, subject_id: str) -> str:
    """Return a safe per-subject lock id shared by all continuity mutation endpoints."""
    normalized = _normalize_subject_id(subject_id)
    digest = hashlib.sha256(f"{subject_kind}:{normalized}".encode("utf-8")).hexdigest()
    return f"continuity_{digest}"


@contextmanager
def _continuity_subject_lock(*, repo_root: Path, subject_kind: str, subject_id: str):
    """Acquire the per-subject mutation lock, translating domain exceptions.

    Converts ``ArtifactLockTimeout`` → HTTP 409 and
    ``ArtifactLockInfrastructureError`` → HTTP 503 via the shared
    ``make_lock_error`` helper so that continuity lock errors use the
    same status codes as all other lifecycle modules.
    """
    from app.coordination.locking import (
        ArtifactLockInfrastructureError,
        ArtifactLockTimeout,
        artifact_lock,
    )
    from app.lifecycle_warnings import make_lock_error

    try:
        with artifact_lock(
            _continuity_subject_lock_id(subject_kind, subject_id),
            lock_dir=repo_root / ".locks",
        ):
            yield
    except ArtifactLockTimeout as exc:
        raise make_lock_error("continuity", None, exc, is_timeout=True) from exc
    except ArtifactLockInfrastructureError as exc:
        raise make_lock_error("continuity", None, exc, is_timeout=False) from exc


def _archive_rel_path_from_envelope(payload: dict[str, Any]) -> str:
    """Derive the canonical archive-envelope path from a validated envelope payload."""
    # Function-level import to avoid circular dependency.
    from app.continuity.validation import _require_utc_timestamp

    capsule = payload.get("capsule")
    if not isinstance(capsule, dict):
        raise HTTPException(status_code=400, detail="Invalid continuity archive envelope capsule")
    subject_kind = str(capsule.get("subject_kind") or "")
    subject_id = str(capsule.get("subject_id") or "")
    archived_at = _require_utc_timestamp(str(payload.get("archived_at") or ""), "archived_at")
    timestamp = format_compact(archived_at)
    return f"{CONTINUITY_DIR_REL}/archive/{subject_kind}-{_normalize_subject_id(subject_id)}-{timestamp}.json"
