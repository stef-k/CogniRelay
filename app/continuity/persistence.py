"""Capsule persistence, loading, fallback snapshots, and archive envelopes."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from pydantic import ValidationError

from app.continuity.constants import (
    CONTINUITY_ARCHIVE_SCHEMA_TYPE,
    CONTINUITY_ARCHIVE_SCHEMA_VERSION,
    CONTINUITY_FALLBACK_SCHEMA_TYPE,
    CONTINUITY_FALLBACK_SCHEMA_VERSION,
)
from app.continuity.freshness import _capsule_health_summary, _verification_status
from app.continuity.paths import (
    _normalize_subject_id,
    continuity_fallback_rel_path,
    continuity_rel_path,
)
from app.continuity.validation import _require_utc_timestamp, _upgrade_legacy_structured_entry_timestamps
from app.git_locking import repository_mutation_lock
from app.git_manager import GitManager
from app.git_safety import try_unstage_paths
from app.lifecycle_warnings import make_error_detail
from app.models import ContinuityCapsule, ContinuityUpsertRequest
from app.storage import canonical_json, safe_path, write_bytes_file, write_text_file
from app.timestamps import format_iso, iso_now

_logger = logging.getLogger(__name__)


def _persist_active_capsule(
    *,
    repo_root: Path,
    gm: GitManager,
    path: Path,
    canonical: str,
    commit_message: str,
) -> None:
    """Persist an active capsule safely, restoring the prior durable file on commit failure."""
    old_bytes = path.read_bytes() if path.exists() else None
    with repository_mutation_lock(repo_root):
        write_text_file(path, canonical)
        try:
            gm.commit_file(path, commit_message)
        except Exception as exc:
            _logger.error("Continuity capsule persist failed: %s", exc, exc_info=True)
            restore_error: Exception | None = None
            try_unstage_paths(gm, [path])
            try:
                if old_bytes is None:
                    path.unlink(missing_ok=True)
                else:
                    write_bytes_file(path, old_bytes)
            except Exception as restore_exc:
                restore_error = restore_exc
                _logger.exception("Rollback also failed after continuity persist error")
            error_detail = f"Failed to persist continuity capsule: {exc}"
            if restore_error is not None:
                error_detail = f"{error_detail}; rollback failed: {restore_error}"
            raise HTTPException(
                status_code=500,
                detail=make_error_detail(
                    operation="continuity_persist",
                    error_code="continuity_persist_commit_failed" if restore_error is None else "continuity_persist_rollback_failed",
                    error_detail=error_detail,
                ),
            ) from exc


def _fallback_snapshot_payload(*, capsule: dict[str, Any], active_rel: str, captured_at: str) -> dict[str, Any]:
    """Build one fallback snapshot envelope from a validated active capsule."""
    health_status, _health_reasons = _capsule_health_summary(capsule)
    return {
        "schema_type": CONTINUITY_FALLBACK_SCHEMA_TYPE,
        "schema_version": CONTINUITY_FALLBACK_SCHEMA_VERSION,
        "captured_at": captured_at,
        "source_path": active_rel,
        "verification_status": _verification_status(capsule),
        "health_status": health_status,
        "capsule": capsule,
    }


def _restore_failed_fallback_snapshot(path: Path, old_bytes: bytes | None, exc: Exception) -> str:
    """Restore the prior fallback snapshot bytes after a failed commit and return audit detail."""
    restore_error: Exception | None = None
    try:
        if old_bytes is None:
            path.unlink(missing_ok=True)
        else:
            write_bytes_file(path, old_bytes)
    except Exception as restore_exc:
        restore_error = restore_exc
    detail = f"Failed to persist continuity fallback snapshot: {exc}"
    if restore_error is not None:
        detail = f"{detail}; rollback failed: {restore_error}"
    return detail


def _restore_failed_refresh_state(path: Path, old_bytes: bytes | None, exc: Exception) -> HTTPException:
    """Return a refresh-state persistence error after restoring the prior durable bytes."""
    restore_error: Exception | None = None
    try:
        if old_bytes is None:
            path.unlink(missing_ok=True)
        else:
            write_bytes_file(path, old_bytes)
    except Exception as restore_exc:
        restore_error = restore_exc
    detail = f"Failed to persist continuity refresh state: {exc}"
    if restore_error is not None:
        detail = f"{detail}; rollback failed: {restore_error}"
    return HTTPException(status_code=500, detail=detail)


def _restore_failed_retention_state(path: Path, old_bytes: bytes | None, exc: Exception) -> HTTPException:
    """Return a retention-state persistence error after restoring the prior durable bytes."""
    restore_error: Exception | None = None
    try:
        if old_bytes is None:
            path.unlink(missing_ok=True)
        else:
            write_bytes_file(path, old_bytes)
    except Exception as restore_exc:
        restore_error = restore_exc
    detail = f"Failed to persist continuity retention state: {exc}; prior durable plan was restored"
    if restore_error is not None:
        detail = f"Failed to persist continuity retention state: {exc}; rollback failed: {restore_error}"
    return HTTPException(status_code=500, detail=detail)


def _load_fallback_envelope_payload(repo_root: Path, rel: str) -> dict[str, Any]:
    """Load and validate a fallback snapshot envelope payload."""
    path = safe_path(repo_root, rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Continuity fallback snapshot not found")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity fallback snapshot JSON: {e}") from e
    if payload.get("schema_type") != CONTINUITY_FALLBACK_SCHEMA_TYPE:
        raise HTTPException(status_code=400, detail="Invalid continuity fallback snapshot schema_type")
    if payload.get("schema_version") != CONTINUITY_FALLBACK_SCHEMA_VERSION:
        raise HTTPException(status_code=400, detail="Invalid continuity fallback snapshot schema_version")
    nested = payload.get("capsule")
    if not isinstance(nested, dict):
        raise HTTPException(status_code=400, detail="Invalid continuity fallback snapshot capsule")
    try:
        payload["capsule"] = ContinuityCapsule.model_validate(
            _upgrade_legacy_structured_entry_timestamps(nested)
        ).model_dump(mode="json", exclude_none=True)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity fallback snapshot capsule: {e}") from e
    return payload


def _persist_fallback_snapshot(
    *,
    repo_root: Path,
    gm: GitManager,
    subject_kind: str,
    subject_id: str,
    capsule: dict[str, Any],
 ) -> tuple[str, str, str | None]:
    """Persist the fallback snapshot and restore prior fallback bytes if this write fails."""
    fallback_rel = continuity_fallback_rel_path(subject_kind, subject_id)
    path: Path | None = None
    old_bytes: bytes | None = None
    try:
        path = safe_path(repo_root, fallback_rel)
        old_bytes = path.read_bytes() if path.exists() else None
        payload = _fallback_snapshot_payload(
            capsule=capsule,
            active_rel=continuity_rel_path(subject_kind, subject_id),
            captured_at=str(capsule.get("updated_at") or capsule.get("verified_at") or format_iso(iso_now())),
        )
        canonical = canonical_json(payload)
        new_bytes = canonical.encode("utf-8")
        if old_bytes == new_bytes:
            return fallback_rel, "unchanged", None
        write_text_file(path, canonical)
        # Fallback snapshots are defense-in-depth; use a shorter git
        # lock timeout (15 s) to reduce thread-pool exhaustion risk when
        # the primary capsule write already consumed the full 60 s budget.
        with repository_mutation_lock(repo_root, timeout=15.0):
            committed = gm.commit_file(path, f"continuity: update fallback {subject_kind} {subject_id}")
            if not committed:
                raise RuntimeError("git commit produced no changes")
    except Exception as exc:
        if path is not None:
            try_unstage_paths(gm, [path])
            return fallback_rel, "failed", _restore_failed_fallback_snapshot(path, old_bytes, exc)
        return fallback_rel, "failed", f"Failed to persist continuity fallback snapshot: {exc}"
    return fallback_rel, "committed", None


def _delete_commit_message(subject_kind: str, subject_id: str, reason: str) -> str:
    """Build a bounded delete commit subject while keeping the full reason in audit detail."""
    message = f"continuity: delete {subject_kind} {subject_id} - {reason}"
    if len(message) <= 120:
        return message
    return message[:117] + "..."


def _load_fallback_snapshot(repo_root: Path, rel: str, *, expected_subject: tuple[str, str]) -> dict[str, Any]:
    """Load and validate one fallback snapshot envelope, returning the nested capsule."""
    payload = _load_fallback_envelope_payload(repo_root, rel)
    capsule = payload["capsule"]
    expected_kind, expected_id = expected_subject
    capsule_kind = str(capsule.get("subject_kind") or "")
    capsule_subject_id = str(capsule.get("subject_id") or "")
    if capsule_kind != expected_kind or _normalize_subject_id(capsule_subject_id) != _normalize_subject_id(expected_id):
        raise HTTPException(status_code=400, detail="Continuity fallback snapshot subject does not match requested subject")
    return capsule


def _load_archive_envelope(repo_root: Path, rel: str) -> dict[str, Any]:
    """Load and validate an archive envelope."""
    path = safe_path(repo_root, rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Continuity archive envelope not found")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail="Continuity archive envelope not found") from e
    except UnicodeDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity archive envelope text: {e}") from e
    except OSError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity archive envelope text: {e}") from e
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity archive envelope JSON: {e}") from e
    if payload.get("schema_type") != CONTINUITY_ARCHIVE_SCHEMA_TYPE:
        raise HTTPException(status_code=400, detail="Invalid continuity archive envelope schema_type")
    if payload.get("schema_version") != CONTINUITY_ARCHIVE_SCHEMA_VERSION:
        raise HTTPException(status_code=400, detail="Invalid continuity archive envelope schema_version")
    capsule = payload.get("capsule")
    if not isinstance(capsule, dict):
        raise HTTPException(status_code=400, detail="Invalid continuity archive envelope capsule")
    try:
        payload["capsule"] = ContinuityCapsule.model_validate(
            _upgrade_legacy_structured_entry_timestamps(capsule)
        ).model_dump(mode="json", exclude_none=True)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity archive envelope capsule: {e}") from e
    return payload


def _load_capsule(repo_root: Path, rel: str, *, expected_subject: tuple[str, str] | None = None) -> dict[str, Any]:
    """Load one capsule from disk and enforce optional subject matching."""
    path = safe_path(repo_root, rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Continuity capsule not found")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        capsule = ContinuityCapsule.model_validate(
            _upgrade_legacy_structured_entry_timestamps(payload)
        ).model_dump(mode="json", exclude_none=True)
        if expected_subject is not None:
            expected_kind, expected_id = expected_subject
            capsule_kind = str(capsule.get("subject_kind") or "")
            capsule_subject_id = str(capsule.get("subject_id") or "")
            if capsule_kind != expected_kind or _normalize_subject_id(capsule_subject_id) != _normalize_subject_id(expected_id):
                raise HTTPException(status_code=400, detail="Continuity capsule subject does not match requested subject")
        return capsule
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity capsule JSON: {e}") from e
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid continuity capsule: {e}") from e


def _restore_failed_archive(active_path: Path, archive_path: Path, active_bytes: bytes) -> None:
    """Restore the active capsule and discard the archive envelope after a failed archive commit."""
    errors: list[str] = []
    first_exc: Exception | None = None
    try:
        write_bytes_file(active_path, active_bytes)
    except Exception as exc:
        first_exc = exc
        errors.append(f"restore active: {exc}")
    try:
        archive_path.unlink(missing_ok=True)
    except Exception as exc:
        if first_exc is None:
            first_exc = exc
        errors.append(f"remove archive: {exc}")
    if errors:
        raise RuntimeError(
            f"Failed to restore archived continuity capsule: {'; '.join(errors)}"
        ) from first_exc


def _restore_failed_cold_store(*, archive_path: Path, archive_bytes: bytes, cold_payload_path: Path, cold_stub_path: Path) -> list[str]:
    """Restore the source archive and remove partial cold files after a failed cold-store commit."""
    errors: list[str] = []
    try:
        write_bytes_file(archive_path, archive_bytes)
    except Exception as exc:
        errors.append(f"restore archive: {exc}")
    for path in (cold_payload_path, cold_stub_path):
        try:
            path.unlink(missing_ok=True)
        except Exception as exc:
            errors.append(f"remove {path}: {exc}")
    return errors


def _reject_stale_timestamp(incoming_updated_at: str, stored_updated_at: str) -> None:
    """Reject if the incoming timestamp is not strictly newer than the stored one."""
    incoming_dt = _require_utc_timestamp(incoming_updated_at, "updated_at")
    stored_dt = _require_utc_timestamp(stored_updated_at, "updated_at")
    if incoming_dt < stored_dt:
        raise HTTPException(status_code=409, detail="Incoming continuity capsule is older than the current stored capsule")
    if incoming_dt == stored_dt:
        raise HTTPException(status_code=409, detail="Incoming continuity capsule conflicts with the current stored capsule timestamp")


def _reject_stale_or_conflicting_write(path: Path, req: ContinuityUpsertRequest) -> None:
    """Reject older or equal-timestamp conflicting writes against the stored capsule."""
    if not path.exists() or not path.is_file():
        return
    try:
        current = ContinuityCapsule.model_validate(
            _upgrade_legacy_structured_entry_timestamps(json.loads(path.read_text(encoding="utf-8")))
        )
    except (ValidationError, json.JSONDecodeError):
        return
    _reject_stale_timestamp(req.capsule.updated_at, current.updated_at)
