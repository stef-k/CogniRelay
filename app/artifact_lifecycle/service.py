"""Artifact lifecycle: externalize, stub, and history management for coordination and terminal artifacts.

Implements the namespace-specific execution contract defined in issue #113.
Each artifact family has its own maintenance pass logic; the shared substrate
(history_id naming, stub creation, rollback) is defined in shared helpers at the
top of this module.

Families handled:
  - handoff: terminal handoffs (accepted_advisory, deferred, rejected)
  - shared_history: superseded shared coordination versions (synchronous pre-write capture)
  - reconciliation: resolved reconciliation artifacts
  - task_done: done tasks
  - patch_applied: applied patches
"""

from __future__ import annotations

import json
import logging
import gzip
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from fastapi import HTTPException

from app.timestamps import format_compact, format_iso, parse_iso as _parse_iso, iso_now as _iso_now

from app.lifecycle_warnings import make_error_detail, make_lock_error, make_warning

from app.auth import AuthContext
from app.coordination.locking import (
    ArtifactLockInfrastructureError,
    ArtifactLockTimeout,
    artifact_lock,
)
from app.git_safety import try_commit_paths
from app.segment_history.locking import (
    LockInfrastructureError,
    SegmentHistoryLockTimeout,
    segment_history_source_lock,
)
from app.storage import build_cold_gzip_bytes, safe_path, write_bytes_file, write_text_file

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------

HANDOFFS_DIR_REL = "memory/coordination/handoffs"
HANDOFFS_HISTORY_DIR_REL = "memory/coordination/handoffs/history"

SHARED_DIR_REL = "memory/coordination/shared"
SHARED_HISTORY_DIR_REL = "memory/coordination/shared/history"

RECONCILIATIONS_DIR_REL = "memory/coordination/reconciliations"
RECONCILIATIONS_HISTORY_DIR_REL = "memory/coordination/reconciliations/history"

TASKS_DONE_DIR_REL = "tasks/done"
TASKS_HISTORY_DONE_DIR_REL = "tasks/history/done"

PATCHES_APPLIED_DIR_REL = "patches/applied"
PATCHES_HISTORY_APPLIED_DIR_REL = "patches/history/applied"

_ARTIFACT_HISTORY_DIRS_BY_FAMILY: dict[str, str] = {
    "handoff": HANDOFFS_HISTORY_DIR_REL,
    "shared_history": SHARED_HISTORY_DIR_REL,
    "reconciliation": RECONCILIATIONS_HISTORY_DIR_REL,
    "task_done": TASKS_HISTORY_DONE_DIR_REL,
    "patch_applied": PATCHES_HISTORY_APPLIED_DIR_REL,
}

_ARTIFACT_HISTORY_SCHEMA_TYPES_BY_FAMILY: dict[str, str] = {
    "handoff": "handoff_history_unit",
    "shared_history": "shared_history_unit",
    "reconciliation": "reconciliation_history_unit",
    "task_done": "task_done_history_unit",
    "patch_applied": "patch_applied_history_unit",
}

# Terminal handoff statuses per spec
_TERMINAL_HANDOFF_STATUSES = frozenset({"accepted_advisory", "deferred", "rejected"})

# ---------------------------------------------------------------------------
# ISO timestamp helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Shared history_id naming
# ---------------------------------------------------------------------------


_history_timestamp_str = format_compact


def _next_history_id(
    family: str,
    cut_at: datetime,
    history_dir: Path,
    reserved_ids: set[str] | None = None,
) -> str:
    """Allocate the next history_id for a family + timestamp pair.

    Format: <family>__<YYYYMMDDTHHMMSSZ>__<sequence>
    Sequence is zero-padded width 4, starts at 0001.

    ``reserved_ids`` tracks IDs already allocated in the current pass
    (before files are written to disk) to avoid collisions.
    """
    ts_str = _history_timestamp_str(cut_at)
    prefix = f"{family}__{ts_str}__"
    existing_seqs: list[int] = []
    # Scan both the payload directory and the index/ stub directory so that
    # orphan stubs (payload missing) are also accounted for in allocation.
    scan_dirs = [history_dir]
    index_dir = history_dir / "index"
    if index_dir.exists() and index_dir.is_dir():
        scan_dirs.append(index_dir)
    for scan_dir in scan_dirs:
        if not scan_dir.exists() or not scan_dir.is_dir():
            continue
        for child in scan_dir.iterdir():
            name = child.stem if child.suffix == ".json" else child.name
            if name.startswith(prefix):
                suffix = name[len(prefix):]
                try:
                    seq = int(suffix)
                    existing_seqs.append(seq)
                except ValueError:
                    _logger.warning("Malformed history sequence suffix in %s", child.name)
    # Include sequences from reserved IDs not yet on disk
    if reserved_ids:
        for rid in reserved_ids:
            if rid.startswith(prefix):
                suffix = rid[len(prefix):]
                try:
                    existing_seqs.append(int(suffix))
                except ValueError:
                    pass
    next_seq = max(existing_seqs, default=0) + 1
    if next_seq < 1:
        next_seq = 1
    new_id = f"{prefix}{next_seq:04d}"
    if reserved_ids is not None:
        reserved_ids.add(new_id)
    return new_id


# ---------------------------------------------------------------------------
# Shared stub creation
# ---------------------------------------------------------------------------


def _create_stub(
    *,
    family: str,
    history_id: str,
    payload_path: str,
    created_at: datetime,
    source_path: str,
    summary: dict[str, Any],
) -> dict[str, Any]:
    """Build an artifact_history_stub per the shared contract."""
    return {
        "schema_type": "artifact_history_stub",
        "schema_version": "1.0",
        "family": family,
        "history_id": history_id,
        "payload_path": payload_path,
        "created_at": format_iso(created_at),
        "source_path": source_path,
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# Shared rollback helpers
# ---------------------------------------------------------------------------


def _capture_rollback(paths: list[Path]) -> list[tuple[Path, bytes | None]]:
    """Capture prior bytes for rollback."""
    plan: list[tuple[Path, bytes | None]] = []
    seen: set[Path] = set()
    for p in paths:
        resolved = p.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        plan.append((p, p.read_bytes() if p.exists() else None))
    return plan


def _restore_rollback(plan: list[tuple[Path, bytes | None]]) -> None:
    """Best-effort restore from rollback plan."""
    for path, old_bytes in plan:
        try:
            if old_bytes is None:
                path.unlink(missing_ok=True)
            else:
                write_bytes_file(path, old_bytes)
        except Exception:
            _logger.exception("Rollback restore failed for %s", path)


def _remove_paths(paths: list[Path]) -> None:
    """Best-effort removal of files created during a failed family pass."""
    for path in paths:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            _logger.exception("Rollback cleanup failed for %s", path)


def _delete_hot_artifacts_or_rollback(
    *,
    repo_root: Path,
    hot_rel_paths: list[str],
    written_rel_paths: list[str],
) -> None:
    """Delete hot artifacts as one rollback unit for a family maintenance pass."""
    hot_paths = [safe_path(repo_root, rel) for rel in hot_rel_paths]
    rollback = _capture_rollback(hot_paths)
    try:
        for hot_path in hot_paths:
            hot_path.unlink(missing_ok=True)
    except Exception:
        _restore_rollback(rollback)
        _remove_paths([safe_path(repo_root, rel) for rel in written_rel_paths])
        raise


def artifact_history_cold_dir_rel(history_dir_rel: str) -> str:
    """Return the cold payload directory for one history namespace."""
    return f"{history_dir_rel}/cold"


def artifact_history_cold_storage_rel_path(payload_rel: str) -> str:
    """Map a hot artifact-history payload path to its cold gzip payload path."""
    rel = str(payload_rel or "").strip().strip("/")
    for history_dir_rel in _ARTIFACT_HISTORY_DIRS_BY_FAMILY.values():
        prefix = f"{history_dir_rel}/"
        if rel.startswith(prefix) and rel.endswith(".json") and "/index/" not in rel and "/cold/" not in rel:
            return f"{artifact_history_cold_dir_rel(history_dir_rel)}/{Path(rel).name}.gz"
    raise HTTPException(status_code=400, detail="Invalid artifact-history payload path")


def artifact_history_payload_rel_path_from_cold_artifact(cold_artifact_path: str) -> str:
    """Derive the hot payload path from an artifact-history cold payload path."""
    rel = str(cold_artifact_path or "").strip().strip("/")
    for history_dir_rel in _ARTIFACT_HISTORY_DIRS_BY_FAMILY.values():
        cold_prefix = f"{artifact_history_cold_dir_rel(history_dir_rel)}/"
        if rel.startswith(cold_prefix) and rel.endswith(".json.gz"):
            stem = Path(rel).name[:-3]
            return f"{history_dir_rel}/{stem}"
    raise HTTPException(status_code=400, detail="Invalid artifact-history cold payload path")


# ---------------------------------------------------------------------------
# Shared JSON I/O
# ---------------------------------------------------------------------------


def _load_json_file(path: Path) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Load a JSON artifact file with graceful degradation.

    Returns (data, warning). If data is None, the file is unreadable.
    The warning (when present) is a structured dict from ``make_warning``.
    """
    if not path.exists() or not path.is_file():
        return None, None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError):
        return None, make_warning("artifact_corrupt", f"Cannot parse artifact: {path.name}", path=str(path))
    except Exception:
        return None, make_warning("artifact_unreadable", f"Cannot read artifact: {path.name}", path=str(path))
    if not isinstance(data, dict):
        return None, make_warning("artifact_not_dict", f"Artifact is not a dict: {path.name}", path=str(path))
    return data, None


def _write_json(path: Path, data: dict[str, Any]) -> None:
    """Write JSON data to path atomically."""
    write_text_file(path, json.dumps(data, ensure_ascii=False, indent=2))


def _write_pair_exclusive(
    payload_path: Path,
    payload: dict[str, Any],
    stub_path: Path,
    stub: dict[str, Any],
) -> bool:
    """Write a payload+stub pair using exclusive creates.

    Returns True on success, False if either file already exists (concurrent
    writer got there first).  On non-collision errors the exception propagates.
    """
    try:
        _write_json_exclusive(payload_path, payload)
    except FileExistsError:
        return False

    try:
        _write_json_exclusive(stub_path, stub)
    except FileExistsError:
        payload_path.unlink(missing_ok=True)
        return False
    except Exception:
        payload_path.unlink(missing_ok=True)
        raise

    return True


def _write_json_exclusive(path: Path, data: dict[str, Any]) -> None:
    """Write JSON data to path, failing if the file already exists.

    Uses O_CREAT | O_EXCL to claim the filename atomically, then delegates
    to ``write_text_file`` for durable content (temp+fsync+rename).  If the
    durable write fails, the empty sentinel is removed so a future retry or
    concurrent writer can reclaim the sequence.
    """
    import os
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    os.close(fd)
    try:
        write_text_file(path, json.dumps(data, ensure_ascii=False, indent=2))
    except Exception:
        path.unlink(missing_ok=True)
        raise


# ---------------------------------------------------------------------------
# Generic single-artifact externalization
# ---------------------------------------------------------------------------


def _externalize_single_artifact(
    *,
    repo_root: Path,
    family: str,
    schema_type: str,
    artifact_id: str,
    source_rel: str,
    artifact: dict[str, Any],
    summary: dict[str, Any],
    history_dir_rel: str,
    cut_at: datetime,
    reserved_ids: set[str] | None = None,
) -> tuple[str, str, str, dict[str, Any], dict[str, Any]]:
    """Build the payload and stub for one artifact externalization.

    Returns (history_id, payload_rel, stub_rel, payload_dict, stub_dict).
    """
    history_dir = safe_path(repo_root, history_dir_rel)
    history_dir.mkdir(parents=True, exist_ok=True)
    history_id = _next_history_id(family, cut_at, history_dir, reserved_ids=reserved_ids)

    payload = {
        "schema_type": schema_type,
        "schema_version": "1.0",
        "family": family,
        "history_id": history_id,
        "artifact_id": artifact_id,
        "source_path": source_rel,
        "cut_at": format_iso(cut_at),
        "artifact": artifact,
        "summary": summary,
    }

    payload_rel = f"{history_dir_rel}/{history_id}.json"

    # Payload lives in history_dir; stub goes in history_dir/index/
    # following the established registry_lifecycle pattern.
    stub_dir_rel = f"{history_dir_rel}/index"
    stub_dir = safe_path(repo_root, stub_dir_rel)
    stub_dir.mkdir(parents=True, exist_ok=True)
    stub_rel = f"{stub_dir_rel}/{history_id}.json"

    stub = _create_stub(
        family=family,
        history_id=history_id,
        payload_path=payload_rel,
        created_at=cut_at,
        source_path=source_rel,
        summary=summary,
    )

    return history_id, payload_rel, stub_rel, payload, stub


# ===================================================================
# FAMILY: handoff
# ===================================================================


def _handoff_retention_timestamp(artifact: dict[str, Any]) -> tuple[datetime | None, dict[str, Any] | None]:
    """Derive the retention timestamp for a terminal handoff.

    Per spec: consumed_at when present, otherwise updated_at, otherwise created_at.
    """
    for field in ("consumed_at", "updated_at", "created_at"):
        ts = _parse_iso(artifact.get(field))
        if ts is not None:
            return ts, None
    return None, make_warning("handoff_retention_missing", f"No retention timestamp for handoff: {artifact.get('handoff_id', 'unknown')}")


def _handoff_summary(artifact: dict[str, Any]) -> dict[str, Any]:
    """Build the summary block for a handoff history unit."""
    return {
        "sender_peer": artifact.get("sender_peer"),
        "recipient_peer": artifact.get("recipient_peer"),
        "recipient_status": artifact.get("recipient_status"),
        "task_id": artifact.get("task_id"),
        "thread_id": artifact.get("thread_id"),
        "created_at": artifact.get("created_at"),
        "terminal_at": artifact.get("consumed_at") or artifact.get("updated_at") or artifact.get("created_at"),
    }


def handoff_maintenance_pass(
    *,
    repo_root: Path,
    now: datetime,
    terminal_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Run one maintenance pass for terminal handoff artifacts."""
    warnings: list[dict[str, Any]] = []
    cutoff = now - timedelta(days=terminal_retention_days)

    handoffs_dir = safe_path(repo_root, HANDOFFS_DIR_REL)
    if not handoffs_dir.exists() or not handoffs_dir.is_dir():
        return {"ok": True, "family": "handoff", "externalized": 0, "warnings": warnings}

    # Snapshot: load all handoff artifacts
    eligible: list[dict[str, Any]] = []
    for path in sorted(handoffs_dir.iterdir(), key=lambda p: p.name):
        if path.is_dir() or path.suffix.lower() != ".json":
            continue
        handoff_id = path.stem
        try:
            snapshot_bytes = path.read_bytes()
        except Exception:
            snapshot_bytes = None
        artifact, warn = _load_json_file(path)
        if warn:
            warnings.append(warn)
        if artifact is None:
            continue

        # Check terminal status
        status = str(artifact.get("recipient_status") or "pending")
        if status not in _TERMINAL_HANDOFF_STATUSES:
            continue

        # Check retention timestamp
        ret_ts, ts_warn = _handoff_retention_timestamp(artifact)
        if ts_warn:
            warnings.append(ts_warn)
        if ret_ts is None:
            continue
        if ret_ts > cutoff:
            continue

        source_rel = f"{HANDOFFS_DIR_REL}/{handoff_id}.json"
        eligible.append(
            {
                "artifact_id": handoff_id,
                "artifact": artifact,
                "cut_at": now,
                "source_rel": source_rel,
                "summary": _handoff_summary(artifact),
                "snapshot_bytes": snapshot_bytes,
            }
        )
        if len(eligible) >= batch_limit:
            break

    if not eligible:
        return {"ok": True, "family": "handoff", "externalized": 0, "warnings": warnings}

    pass_result = _externalize_selected_family(
        repo_root=repo_root,
        family="handoff",
        schema_type="handoff_history_unit",
        history_dir_rel=HANDOFFS_HISTORY_DIR_REL,
        eligible=eligible,
        collision_warning_prefix="handoff_history_collision",
        mutable_hot_family=True,
    )
    warnings.extend(pass_result["warnings"])
    written_paths = pass_result["written_paths"]
    deleted_paths = pass_result["deleted_paths"]
    externalized_ids = pass_result["externalized_ids"]

    # Best-effort: remove externalized handoffs from the query sidecar index
    from app.coordination.query_index import try_delete_handoff
    for hid in externalized_ids:
        try_delete_handoff(hid)

    return {
        "ok": True,
        "family": "handoff",
        "externalized": len(externalized_ids),
        "written_paths": written_paths,
        "deleted_paths": deleted_paths,
        "warnings": warnings,
        "audit_events": [{"event": "artifact_handoff_maintenance", "detail": {
            "family": "handoff", "externalized": len(externalized_ids),
        }}],
    }


# ===================================================================
# FAMILY: shared_history (synchronous pre-write capture)
# ===================================================================


def _shared_history_summary(artifact: dict[str, Any]) -> dict[str, Any]:
    """Build the summary block for a shared history unit."""
    participants = artifact.get("participant_peers")
    return {
        "shared_id": artifact.get("shared_id"),
        "owner_peer": artifact.get("owner_peer"),
        "version": artifact.get("version"),
        "participant_peer_count": len(participants) if isinstance(participants, list) else 0,
        "task_id": artifact.get("task_id"),
        "thread_id": artifact.get("thread_id"),
        "updated_at": artifact.get("updated_at"),
    }


def externalize_superseded_shared(
    *,
    repo_root: Path,
    now: datetime,
    previous_artifact: dict[str, Any],
    hot_retention_days: int,
) -> dict[str, Any] | None:
    """Externalize a superseded shared artifact version during replacement.

    Called synchronously during the mutating shared update. Returns the result
    dict if a history unit was created, or None if the version was within the
    hot retention window or had an unparsable updated_at.
    """
    updated_at = _parse_iso(previous_artifact.get("updated_at"))
    if updated_at is None:
        _logger.warning(
            "Superseded shared artifact %s has no parseable updated_at; not externalizing",
            previous_artifact.get("shared_id", "unknown"),
        )
        return None
    cutoff = now - timedelta(days=hot_retention_days)
    if updated_at > cutoff:
        return None  # Still within hot window

    shared_id = str(previous_artifact.get("shared_id") or "unknown")
    source_rel = f"{SHARED_DIR_REL}/{shared_id}.json"
    summary = _shared_history_summary(previous_artifact)

    # Use exclusive writes to prevent concurrent captures from silently
    # overwriting each other (different shared_ids can race at the same second).
    max_retries = 3
    for attempt in range(max_retries):
        history_id, payload_rel, stub_rel, payload, stub = _externalize_single_artifact(
            repo_root=repo_root,
            family="shared_history",
            schema_type="shared_history_unit",
            artifact_id=shared_id,
            source_rel=source_rel,
            artifact=previous_artifact,
            summary=summary,
            history_dir_rel=SHARED_HISTORY_DIR_REL,
            cut_at=now,
        )

        payload_path = safe_path(repo_root, payload_rel)
        stub_path = safe_path(repo_root, stub_rel)

        # Write payload first with O_EXCL — if it collides, we wrote nothing.
        try:
            _write_json_exclusive(payload_path, payload)
        except FileExistsError:
            # O_EXCL guarantees we wrote nothing; no rollback needed.
            if attempt == max_retries - 1:
                raise
            continue

        # Payload written; now write stub.
        try:
            _write_json_exclusive(stub_path, stub)
            break  # Both written successfully
        except FileExistsError:
            # We wrote the payload but stub collided; clean up our payload.
            payload_path.unlink(missing_ok=True)
            if attempt == max_retries - 1:
                raise
            continue
        except Exception:
            # Non-collision failure on stub; clean up our payload.
            payload_path.unlink(missing_ok=True)
            raise

    return {
        "history_id": history_id,
        "payload_path": payload_rel,
        "stub_path": stub_rel,
    }


# ===================================================================
# FAMILY: reconciliation
# ===================================================================


def _reconciliation_summary(artifact: dict[str, Any]) -> dict[str, Any]:
    """Build the summary block for a reconciliation history unit."""
    claims = artifact.get("claims")
    participants = artifact.get("participant_peers")
    return {
        "owner_peer": artifact.get("owner_peer"),
        "status": artifact.get("status"),
        "resolution": artifact.get("resolution_outcome"),
        "claim_count": len(claims) if isinstance(claims, list) else 0,
        "participant_peer_count": len(participants) if isinstance(participants, list) else 0,
        "task_id": artifact.get("task_id"),
        "thread_id": artifact.get("thread_id"),
        "updated_at": artifact.get("updated_at"),
    }


def reconciliation_maintenance_pass(
    *,
    repo_root: Path,
    now: datetime,
    resolved_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Run one maintenance pass for resolved reconciliation artifacts."""
    warnings: list[dict[str, Any]] = []
    cutoff = now - timedelta(days=resolved_retention_days)

    recon_dir = safe_path(repo_root, RECONCILIATIONS_DIR_REL)
    if not recon_dir.exists() or not recon_dir.is_dir():
        return {"ok": True, "family": "reconciliation", "externalized": 0, "warnings": warnings}

    eligible: list[dict[str, Any]] = []
    for path in sorted(recon_dir.iterdir(), key=lambda p: p.name):
        if path.is_dir() or path.suffix.lower() != ".json":
            continue
        recon_id = path.stem
        try:
            snapshot_bytes = path.read_bytes()
        except Exception:
            snapshot_bytes = None
        artifact, warn = _load_json_file(path)
        if warn:
            warnings.append(warn)
        if artifact is None:
            continue

        if str(artifact.get("status") or "open") != "resolved":
            continue

        updated_at = _parse_iso(artifact.get("updated_at"))
        if updated_at is None:
            warnings.append(make_warning("reconciliation_retention_missing", f"No retention timestamp for reconciliation: {recon_id}"))
            continue
        if updated_at > cutoff:
            continue

        source_rel = f"{RECONCILIATIONS_DIR_REL}/{recon_id}.json"
        eligible.append(
            {
                "artifact_id": recon_id,
                "artifact": artifact,
                "cut_at": now,
                "source_rel": source_rel,
                "summary": _reconciliation_summary(artifact),
                "snapshot_bytes": snapshot_bytes,
            }
        )
        if len(eligible) >= batch_limit:
            break

    if not eligible:
        return {"ok": True, "family": "reconciliation", "externalized": 0, "warnings": warnings}

    pass_result = _externalize_selected_family(
        repo_root=repo_root,
        family="reconciliation",
        schema_type="reconciliation_history_unit",
        history_dir_rel=RECONCILIATIONS_HISTORY_DIR_REL,
        eligible=eligible,
        collision_warning_prefix="reconciliation_history_collision",
        mutable_hot_family=True,
    )
    warnings.extend(pass_result["warnings"])
    written_paths = pass_result["written_paths"]
    deleted_paths = pass_result["deleted_paths"]
    externalized_ids = pass_result["externalized_ids"]

    # Best-effort: remove externalized reconciliations from the query sidecar index
    from app.coordination.query_index import try_delete_reconciliation
    for rid in externalized_ids:
        try_delete_reconciliation(rid)

    return {
        "ok": True,
        "family": "reconciliation",
        "externalized": len(externalized_ids),
        "written_paths": written_paths,
        "deleted_paths": deleted_paths,
        "warnings": warnings,
        "audit_events": [{"event": "artifact_reconciliation_maintenance", "detail": {
            "family": "reconciliation", "externalized": len(externalized_ids),
        }}],
    }


# ===================================================================
# FAMILY: task_done
# ===================================================================


def _task_done_summary(artifact: dict[str, Any]) -> dict[str, Any]:
    """Build the summary block for a done-task history unit."""
    return {
        "task_id": artifact.get("task_id"),
        "owner_peer": artifact.get("owner_peer"),
        "status": artifact.get("status", "done"),
        "thread_id": artifact.get("thread_id"),
        "updated_at": artifact.get("updated_at"),
    }


def task_done_maintenance_pass(
    *,
    repo_root: Path,
    now: datetime,
    hot_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Run one maintenance pass for done-task artifacts."""
    warnings: list[dict[str, Any]] = []
    cutoff = now - timedelta(days=hot_retention_days)

    done_dir = safe_path(repo_root, TASKS_DONE_DIR_REL)
    if not done_dir.exists() or not done_dir.is_dir():
        return {"ok": True, "family": "task_done", "externalized": 0, "warnings": warnings}

    eligible: list[dict[str, Any]] = []
    for path in sorted(done_dir.iterdir(), key=lambda p: p.name):
        if path.is_dir() or path.suffix.lower() != ".json":
            continue
        task_id = path.stem
        artifact, warn = _load_json_file(path)
        if warn:
            warnings.append(warn)
        if artifact is None:
            continue

        updated_at = _parse_iso(artifact.get("updated_at"))
        if updated_at is None:
            warnings.append(make_warning("task_done_retention_missing", f"No retention timestamp for task: {task_id}"))
            continue
        if updated_at > cutoff:
            continue

        source_rel = f"{TASKS_DONE_DIR_REL}/{task_id}.json"
        eligible.append(
            {
                "artifact_id": task_id,
                "artifact": artifact,
                "cut_at": now,
                "source_rel": source_rel,
                "summary": _task_done_summary(artifact),
            }
        )
        if len(eligible) >= batch_limit:
            break

    if not eligible:
        return {"ok": True, "family": "task_done", "externalized": 0, "warnings": warnings}

    pass_result = _externalize_selected_family(
        repo_root=repo_root,
        family="task_done",
        schema_type="task_done_history_unit",
        history_dir_rel=TASKS_HISTORY_DONE_DIR_REL,
        eligible=eligible,
        collision_warning_prefix="task_done_history_collision",
        mutable_hot_family=False,
    )
    warnings.extend(pass_result["warnings"])
    written_paths = pass_result["written_paths"]
    deleted_paths = pass_result["deleted_paths"]
    externalized_ids = pass_result["externalized_ids"]

    return {
        "ok": True,
        "family": "task_done",
        "externalized": len(externalized_ids),
        "written_paths": written_paths,
        "deleted_paths": deleted_paths,
        "warnings": warnings,
        "audit_events": [{"event": "artifact_task_done_maintenance", "detail": {
            "family": "task_done", "externalized": len(externalized_ids),
        }}],
    }


# ===================================================================
# FAMILY: patch_applied
# ===================================================================


def _patch_applied_summary(artifact: dict[str, Any]) -> dict[str, Any]:
    """Build the summary block for an applied-patch history unit."""
    return {
        "patch_id": artifact.get("patch_id"),
        "patch_type": artifact.get("patch_type"),
        "target_path": artifact.get("target_path"),
        "status": artifact.get("status", "applied"),
        "applied_commit": artifact.get("applied_commit"),
        "updated_at": artifact.get("updated_at"),
    }


def _family_summary(family: str, artifact: dict[str, Any]) -> dict[str, Any]:
    """Build the required summary block for one artifact-history family."""
    if family == "handoff":
        return _handoff_summary(artifact)
    if family == "shared_history":
        return _shared_history_summary(artifact)
    if family == "reconciliation":
        return _reconciliation_summary(artifact)
    if family == "task_done":
        return _task_done_summary(artifact)
    if family == "patch_applied":
        return _patch_applied_summary(artifact)
    raise HTTPException(status_code=400, detail=f"Unsupported artifact-history family: {family}")


def _family_artifact_id_key(family: str) -> str:
    """Return the artifact JSON key that carries the family identity."""
    return {
        "handoff": "handoff_id",
        "shared_history": "shared_id",
        "reconciliation": "reconciliation_id",
        "task_done": "task_id",
        "patch_applied": "patch_id",
    }[family]


def _family_source_rel(family: str, artifact_id: str) -> str:
    """Return the authoritative hot source path for one family artifact."""
    base_dir_rel = {
        "handoff": HANDOFFS_DIR_REL,
        "shared_history": SHARED_DIR_REL,
        "reconciliation": RECONCILIATIONS_DIR_REL,
        "task_done": TASKS_DONE_DIR_REL,
        "patch_applied": PATCHES_APPLIED_DIR_REL,
    }[family]
    return f"{base_dir_rel}/{artifact_id}.json"


def _family_cold_timestamp(summary: dict[str, Any], *, family: str) -> datetime | None:
    """Return the configured cold-eligibility timestamp for one family summary."""
    field = {
        "handoff": "terminal_at",
        "shared_history": "updated_at",
        "reconciliation": "updated_at",
        "task_done": "updated_at",
        "patch_applied": "updated_at",
    }[family]
    return _parse_iso(str(summary.get(field) or ""))


def _externalize_selected_family(
    *,
    repo_root: Path,
    family: str,
    schema_type: str,
    history_dir_rel: str,
    eligible: list[dict[str, Any]],
    collision_warning_prefix: str,
    mutable_hot_family: bool,
) -> dict[str, Any]:
    """Externalize one selected family set as a single rollback unit."""
    warnings: list[dict[str, Any]] = []
    reserved_ids: set[str] = set()
    written_paths: list[str] = []
    deleted_paths: list[str] = []
    externalized_ids: list[str] = []
    rollback_plan: list[tuple[Path, bytes | None]] = []
    rollback_seen: set[Path] = set()
    lock_dir = repo_root / ".locks"

    try:
        for row in eligible:
            artifact_id = row["artifact_id"]

            def _process_one() -> None:
                current_path = safe_path(repo_root, row["source_rel"])
                current_bytes: bytes | None
                try:
                    current_bytes = current_path.read_bytes()
                except FileNotFoundError:
                    warnings.append(make_warning(f"{family}_hot_changed", f"Hot artifact changed since snapshot: {artifact_id}"))
                    return
                except OSError:
                    warnings.append(make_warning("artifact_unreadable", f"Cannot read artifact: {current_path.name}", path=str(current_path)))
                    return
                if mutable_hot_family and current_bytes != row["snapshot_bytes"]:
                    warnings.append(make_warning(f"{family}_hot_changed", f"Hot artifact changed since snapshot: {artifact_id}"))
                    return

                # Retry on O_EXCL collision (concurrent ID allocation)
                _max_ext_retries = 3
                for _ext_attempt in range(_max_ext_retries):
                    _, payload_rel, stub_rel, payload, stub = _externalize_single_artifact(
                        repo_root=repo_root,
                        family=family,
                        schema_type=schema_type,
                        artifact_id=artifact_id,
                        source_rel=row["source_rel"],
                        artifact=row["artifact"],
                        summary=row["summary"],
                        history_dir_rel=history_dir_rel,
                        cut_at=row["cut_at"],
                        reserved_ids=reserved_ids,
                    )
                    if _write_pair_exclusive(
                        safe_path(repo_root, payload_rel), payload,
                        safe_path(repo_root, stub_rel), stub,
                    ):
                        break
                    if _ext_attempt == _max_ext_retries - 1:
                        warnings.append(make_warning(collision_warning_prefix, f"History ID collision after retries: {artifact_id}"))
                        return
                if current_path.resolve() not in rollback_seen:
                    rollback_seen.add(current_path.resolve())
                    rollback_plan.append((current_path, current_bytes))
                written_paths.extend([payload_rel, stub_rel])
                current_path.unlink(missing_ok=True)
                deleted_paths.append(row["source_rel"])
                externalized_ids.append(artifact_id)

            if mutable_hot_family:
                try:
                    with artifact_lock(artifact_id, lock_dir=lock_dir):
                        _process_one()
                except ArtifactLockTimeout as exc:
                    raise make_lock_error(
                        "artifact_lifecycle_maintenance", family, exc, is_timeout=True,
                    ) from exc
                except ArtifactLockInfrastructureError as exc:
                    raise make_lock_error(
                        "artifact_lifecycle_maintenance", family, exc, is_timeout=False,
                    ) from exc
            else:
                _process_one()
    except Exception:
        _restore_rollback(rollback_plan)
        _remove_paths([safe_path(repo_root, rel) for rel in written_paths])
        raise

    return {
        "externalized": len(externalized_ids),
        "written_paths": written_paths,
        "deleted_paths": deleted_paths,
        "externalized_ids": externalized_ids,
        "warnings": warnings,
    }


def patch_applied_maintenance_pass(
    *,
    repo_root: Path,
    now: datetime,
    hot_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Run one maintenance pass for applied-patch artifacts."""
    warnings: list[dict[str, Any]] = []
    cutoff = now - timedelta(days=hot_retention_days)

    applied_dir = safe_path(repo_root, PATCHES_APPLIED_DIR_REL)
    if not applied_dir.exists() or not applied_dir.is_dir():
        return {"ok": True, "family": "patch_applied", "externalized": 0, "warnings": warnings}

    eligible: list[dict[str, Any]] = []
    for path in sorted(applied_dir.iterdir(), key=lambda p: p.name):
        if path.is_dir() or path.suffix.lower() != ".json":
            continue
        patch_id = path.stem
        artifact, warn = _load_json_file(path)
        if warn:
            warnings.append(warn)
        if artifact is None:
            continue

        updated_at = _parse_iso(artifact.get("updated_at"))
        if updated_at is None:
            warnings.append(make_warning("patch_applied_retention_missing", f"No retention timestamp for patch: {patch_id}"))
            continue
        if updated_at > cutoff:
            continue

        source_rel = f"{PATCHES_APPLIED_DIR_REL}/{patch_id}.json"
        eligible.append(
            {
                "artifact_id": patch_id,
                "artifact": artifact,
                "cut_at": now,
                "source_rel": source_rel,
                "summary": _patch_applied_summary(artifact),
            }
        )
        if len(eligible) >= batch_limit:
            break

    if not eligible:
        return {"ok": True, "family": "patch_applied", "externalized": 0, "warnings": warnings}

    pass_result = _externalize_selected_family(
        repo_root=repo_root,
        family="patch_applied",
        schema_type="patch_applied_history_unit",
        history_dir_rel=PATCHES_HISTORY_APPLIED_DIR_REL,
        eligible=eligible,
        collision_warning_prefix="patch_applied_history_collision",
        mutable_hot_family=False,
    )
    warnings.extend(pass_result["warnings"])
    written_paths = pass_result["written_paths"]
    deleted_paths = pass_result["deleted_paths"]
    externalized_ids = pass_result["externalized_ids"]

    return {
        "ok": True,
        "family": "patch_applied",
        "externalized": len(externalized_ids),
        "written_paths": written_paths,
        "deleted_paths": deleted_paths,
        "warnings": warnings,
        "audit_events": [{"event": "artifact_patch_applied_maintenance", "detail": {
            "family": "patch_applied", "externalized": len(externalized_ids),
        }}],
    }


# ===================================================================
# Artifact-history cold-store / rehydrate
# ===================================================================


def _validate_artifact_history_payload_rel_path(payload_rel: str) -> tuple[str, str]:
    """Validate one hot artifact-history payload path and return family + history dir."""
    rel = str(payload_rel or "").strip().strip("/")
    for family, history_dir_rel in _ARTIFACT_HISTORY_DIRS_BY_FAMILY.items():
        prefix = f"{history_dir_rel}/"
        if rel.startswith(prefix) and rel.endswith(".json") and "/index/" not in rel and "/cold/" not in rel:
            return family, history_dir_rel
    raise HTTPException(status_code=400, detail="Invalid artifact-history payload path")


def _load_artifact_history_payload(repo_root: Path, payload_rel: str) -> dict[str, Any]:
    """Load and validate one hot artifact-history payload."""
    family, _history_dir_rel = _validate_artifact_history_payload_rel_path(payload_rel)
    path = safe_path(repo_root, payload_rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Artifact-history payload not found")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid artifact-history payload: {exc}") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid artifact-history payload")
    if payload.get("schema_type") != _ARTIFACT_HISTORY_SCHEMA_TYPES_BY_FAMILY[family]:
        raise HTTPException(status_code=400, detail="Artifact-history payload schema_type does not match path")
    if payload.get("schema_version") != "1.0":
        raise HTTPException(status_code=400, detail="Artifact-history payload schema_version does not match path")
    if payload.get("family") != family:
        raise HTTPException(status_code=400, detail="Artifact-history payload family does not match path")
    if payload.get("history_id") != path.stem:
        raise HTTPException(status_code=400, detail="Artifact-history payload history_id does not match path")
    if _parse_iso(str(payload.get("cut_at") or "")) is None:
        raise HTTPException(status_code=400, detail="Artifact-history payload cut_at is invalid")
    if not isinstance(payload.get("artifact"), dict):
        raise HTTPException(status_code=400, detail="Artifact-history payload artifact must be an object")
    if not isinstance(payload.get("summary"), dict):
        raise HTTPException(status_code=400, detail="Artifact-history payload summary must be an object")
    artifact_id_key = _family_artifact_id_key(family)
    artifact = payload["artifact"]
    artifact_id = str(payload.get("artifact_id") or "")
    if not artifact_id or str(artifact.get(artifact_id_key) or "") != artifact_id:
        raise HTTPException(status_code=400, detail="Artifact-history payload artifact identity mismatch")
    expected_source_rel = _family_source_rel(family, artifact_id)
    if payload.get("source_path") != expected_source_rel:
        raise HTTPException(status_code=400, detail="Artifact-history payload source_path does not match artifact identity")
    if payload.get("summary") != _family_summary(family, artifact):
        raise HTTPException(status_code=400, detail="Artifact-history payload summary does not match artifact content")
    return payload


def _load_artifact_history_stub(repo_root: Path, stub_rel: str) -> dict[str, Any]:
    """Load and validate one artifact-history stub."""
    rel = str(stub_rel or "").strip().strip("/")
    path = safe_path(repo_root, rel)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Artifact-history stub not found")
    try:
        stub = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid artifact-history stub: {exc}") from exc
    if not isinstance(stub, dict):
        raise HTTPException(status_code=400, detail="Invalid artifact-history stub")
    if stub.get("schema_type") != "artifact_history_stub" or stub.get("schema_version") != "1.0":
        raise HTTPException(status_code=400, detail="Invalid artifact-history stub schema")
    family = str(stub.get("family") or "")
    if family not in _ARTIFACT_HISTORY_DIRS_BY_FAMILY:
        raise HTTPException(status_code=400, detail="Invalid artifact-history stub family")
    expected_stub_rel = f"{_ARTIFACT_HISTORY_DIRS_BY_FAMILY[family]}/index/{path.name}"
    if rel != expected_stub_rel:
        raise HTTPException(status_code=400, detail="Artifact-history stub path does not match family identity")
    if stub.get("history_id") != path.stem:
        raise HTTPException(status_code=400, detail="Artifact-history stub history_id does not match path")
    if not isinstance(stub.get("summary"), dict):
        raise HTTPException(status_code=400, detail="Artifact-history stub summary must be an object")
    if not isinstance(stub.get("source_path"), str) or not stub["source_path"].strip():
        raise HTTPException(status_code=400, detail="Artifact-history stub source_path is required")
    payload_path = str(stub.get("payload_path") or "")
    if not payload_path:
        raise HTTPException(status_code=400, detail="Artifact-history stub payload_path is required")
    return stub


def _restore_failed_artifact_history_cold_store(
    *,
    source_payload_path: Path,
    source_payload_bytes: bytes,
    cold_payload_path: Path,
    stub_path: Path,
    original_stub: dict[str, Any],
) -> list[str]:
    """Restore the original hot payload and stub after failed cold-store commit."""
    errors: list[str] = []
    try:
        write_bytes_file(source_payload_path, source_payload_bytes)
    except Exception as exc:
        errors.append(f"restore payload: {exc}")
    for path in (cold_payload_path,):
        try:
            path.unlink(missing_ok=True)
        except Exception as exc:
            errors.append(f"remove {path}: {exc}")
    try:
        _write_json(stub_path, original_stub)
    except Exception as exc:
        errors.append(f"restore stub: {exc}")
    return errors


def artifact_history_cold_store_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Cold-store one artifact-history payload into a gzip payload plus hot JSON stub."""
    auth.require("admin:peers")
    source_payload_path = str(req.source_payload_path)
    auth.require_read_path(source_payload_path)
    auth.require_write_path(source_payload_path)
    family, history_dir_rel = _validate_artifact_history_payload_rel_path(source_payload_path)
    cold_storage_path = artifact_history_cold_storage_rel_path(source_payload_path)
    cold_payload_path = safe_path(repo_root, cold_storage_path)
    hot_payload_path = safe_path(repo_root, source_payload_path)
    stub_rel = f"{history_dir_rel}/index/{hot_payload_path.name}"
    auth.require_read_path(stub_rel)
    auth.require_write_path(stub_rel)
    auth.require_write_path(cold_storage_path)

    # Serialize concurrent cold-store/rehydrate operations on the same payload
    lock_key = f"artifact_history_cold:{source_payload_path}"
    lock_dir = repo_root / ".locks"
    try:
        lock_ctx = segment_history_source_lock(lock_key, lock_dir=lock_dir)
    except LockInfrastructureError as exc:
        raise make_lock_error("artifact_history_cold_store", family, exc, is_timeout=False) from exc
    try:
        with lock_ctx:
            # --- Crash recovery (must precede stub identity validation) ---
            # If both hot and cold exist, a prior cold-store crashed mid-way.
            # Check stub direction to decide which copy is canonical.
            if cold_payload_path.exists() and hot_payload_path.exists():
                try:
                    _crash_stub = _load_artifact_history_stub(repo_root, stub_rel)
                    _stub_points_cold = _crash_stub.get("payload_path") == cold_storage_path
                except Exception:
                    _logger.warning(
                        "Artifact cold-store crash recovery: could not read "
                        "stub %s; defaulting to discard cold and redo",
                        stub_rel,
                        exc_info=True,
                    )
                    _stub_points_cold = False

                if _stub_points_cold:
                    # Stub already mutated → cold is canonical, hot is orphan.
                    # Delete hot, attempt recovery commit, return success.
                    _logger.warning(
                        "Artifact cold-store crash recovery: stub already "
                        "points to cold; removing orphaned hot file %s",
                        source_payload_path,
                    )
                    hot_payload_path.unlink(missing_ok=True)
                    _recovery_committed = bool(gm and try_commit_paths(
                        paths=[
                            cold_payload_path,
                            safe_path(repo_root, stub_rel),
                            hot_payload_path,
                        ],
                        gm=gm,
                        commit_message=(
                            f"artifact-history: cold-store recovery "
                            f"{family} {hot_payload_path.stem}"
                        ),
                    ))
                    audit(
                        auth,
                        "artifact_history_cold_store",
                        {
                            "family": family,
                            "source_payload_path": source_payload_path,
                            "cold_storage_path": cold_storage_path,
                            "cold_stub_path": stub_rel,
                            "crash_recovery": True,
                        },
                    )
                    _recovery_warnings: list[dict[str, Any]] = [
                        make_warning(
                            "artifact_history_cold_store_crash_recovery",
                            "Completed cold-store via crash recovery: "
                            "stub already pointed to cold, removed orphaned hot file",
                        ),
                    ]
                    if not _recovery_committed and gm is not None:
                        _recovery_warnings.append(
                            make_warning(
                                "artifact_history_cold_store_recovery_not_durable",
                                "Crash recovery completed on disk but git commit "
                                "failed; state is not yet durable",
                            ),
                        )
                    return {
                        "ok": True,
                        "family": family,
                        "artifact_state": "cold",
                        "source_payload_path": source_payload_path,
                        "cold_storage_path": cold_storage_path,
                        "cold_stub_path": stub_rel,
                        "committed_files": [cold_storage_path, stub_rel, source_payload_path],
                        "durable": _recovery_committed,
                        "latest_commit": gm.latest_commit() if gm is not None else None,
                        "warnings": _recovery_warnings,
                        "recovery_warnings": _recovery_warnings,
                    }
                else:
                    # Stub still points to hot → cold is orphan (crash
                    # happened before stub mutation). Remove cold and
                    # fall through to a clean cold-store.
                    _logger.warning(
                        "Artifact cold-store crash recovery: removing "
                        "orphaned cold file %s",
                        cold_storage_path,
                    )
                    cold_payload_path.unlink(missing_ok=True)

            # --- Normal flow ---
            payload = _load_artifact_history_payload(repo_root, source_payload_path)
            stub = _load_artifact_history_stub(repo_root, stub_rel)
            if stub.get("payload_path") != source_payload_path:
                raise HTTPException(status_code=409, detail="Artifact-history stub does not point at the hot payload")
            if stub.get("summary") != payload.get("summary") or stub.get("source_path") != payload.get("source_path"):
                raise HTTPException(status_code=400, detail="Artifact-history stub does not match payload")
            source_bytes = hot_payload_path.read_bytes()
            if cold_payload_path.exists():
                raise HTTPException(status_code=409, detail="Artifact-history cold payload already exists")

            try:
                gzip_bytes = build_cold_gzip_bytes(source_bytes)
                write_bytes_file(cold_payload_path, gzip_bytes)
                updated_stub = dict(stub)
                updated_stub["payload_path"] = cold_storage_path
                _write_json(safe_path(repo_root, stub_rel), updated_stub)
                hot_payload_path.unlink()
                committed = bool(gm and try_commit_paths(
                    paths=[cold_payload_path, safe_path(repo_root, stub_rel), hot_payload_path],
                    gm=gm,
                    commit_message=f"artifact-history: cold-store {family} {payload['history_id']}",
                ))
                if gm is not None and not committed:
                    raise RuntimeError("Artifact-history cold-store commit produced no changes")
            except Exception as exc:
                cleanup_errors = _restore_failed_artifact_history_cold_store(
                    source_payload_path=hot_payload_path,
                    source_payload_bytes=source_bytes,
                    cold_payload_path=cold_payload_path,
                    stub_path=safe_path(repo_root, stub_rel),
                    original_stub=stub,
                )
                raise HTTPException(
                    status_code=500,
                    detail=make_error_detail(
                        operation="artifact_history_cold_store",
                        family=family,
                        error_code="artifact_history_cold_store_failed",
                        error_detail=str(exc),
                        rollback_errors=cleanup_errors,
                    ),
                ) from exc
    except SegmentHistoryLockTimeout as exc:
        raise make_lock_error("artifact_history_cold_store", family, exc, is_timeout=True) from exc

    audit(
        auth,
        "artifact_history_cold_store",
        {
            "family": family,
            "source_payload_path": source_payload_path,
            "cold_storage_path": cold_storage_path,
            "cold_stub_path": stub_rel,
        },
    )
    return {
        "ok": True,
        "family": family,
        "artifact_state": "cold",
        "source_payload_path": source_payload_path,
        "cold_storage_path": cold_storage_path,
        "cold_stub_path": stub_rel,
        "committed_files": [cold_storage_path, stub_rel, source_payload_path],
        "durable": True,
        "latest_commit": gm.latest_commit() if gm is not None else None,
        "warnings": [],
        "recovery_warnings": [],
    }


def artifact_history_cold_rehydrate_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Rehydrate one artifact-history cold payload back into its hot history path."""
    auth.require("admin:peers")
    if getattr(req, "cold_stub_path", None):
        cold_stub_path = str(req.cold_stub_path)
        stub = _load_artifact_history_stub(repo_root, cold_stub_path)
        source_payload_path = artifact_history_payload_rel_path_from_cold_artifact(str(stub.get("payload_path") or ""))
    else:
        source_payload_path = str(req.source_payload_path)
        _family, history_dir_rel = _validate_artifact_history_payload_rel_path(source_payload_path)
        cold_stub_path = f"{history_dir_rel}/index/{Path(source_payload_path).name}"
        stub = _load_artifact_history_stub(repo_root, cold_stub_path)
    cold_storage_path = str(stub.get("payload_path") or "")
    auth.require_read_path(cold_stub_path)
    auth.require_read_path(cold_storage_path)
    auth.require_write_path(cold_stub_path)
    auth.require_write_path(cold_storage_path)
    auth.require_write_path(source_payload_path)

    stub_path = safe_path(repo_root, cold_stub_path)
    hot_payload_path = safe_path(repo_root, source_payload_path)
    cold_payload_path = safe_path(repo_root, cold_storage_path)

    # Serialize concurrent cold-store/rehydrate operations on the same payload
    lock_key = f"artifact_history_cold:{source_payload_path}"
    lock_dir = repo_root / ".locks"
    try:
        lock_ctx = segment_history_source_lock(lock_key, lock_dir=lock_dir)
    except LockInfrastructureError as exc:
        raise make_lock_error("artifact_history_cold_rehydrate", None, exc, is_timeout=False) from exc
    try:
        with lock_ctx:
            # --- Crash recovery: both hot and cold exist ---
            # Compute expected cold path independently of the stub (which
            # may have been mutated to point to hot before the crash).
            _expected_cold_rel = artifact_history_cold_storage_rel_path(source_payload_path)
            _expected_cold_path = safe_path(repo_root, _expected_cold_rel)
            if hot_payload_path.exists() and _expected_cold_path.exists():
                _crash_stub: dict[str, Any] = {}
                try:
                    _crash_stub = _load_artifact_history_stub(repo_root, cold_stub_path)
                    _stub_points_hot = _crash_stub.get("payload_path") == source_payload_path
                except Exception:
                    _logger.warning(
                        "Artifact cold-rehydrate crash recovery: could not "
                        "read stub; defaulting to discard hot and redo",
                        exc_info=True,
                    )
                    _stub_points_hot = False

                if _stub_points_hot:
                    # Rehydrate completed (stub points to hot) → cold is orphan.
                    _logger.warning(
                        "Artifact cold-rehydrate crash recovery: stub "
                        "points to hot; removing orphaned cold file %s",
                        _expected_cold_rel,
                    )
                    _expected_cold_path.unlink(missing_ok=True)
                    _recovery_committed = bool(gm and try_commit_paths(
                        paths=[hot_payload_path, stub_path, _expected_cold_path],
                        gm=gm,
                        commit_message=(
                            f"artifact-history: cold-rehydrate recovery "
                            f"{hot_payload_path.stem}"
                        ),
                    ))
                    audit(
                        auth,
                        "artifact_history_cold_rehydrate",
                        {
                            "source_payload_path": source_payload_path,
                            "cold_storage_path": _expected_cold_rel,
                            "cold_stub_path": cold_stub_path,
                            "crash_recovery": True,
                        },
                    )
                    _rh_recovery_warnings: list[dict[str, Any]] = [
                        make_warning(
                            "artifact_history_cold_rehydrate_crash_recovery",
                            "Completed rehydrate via crash recovery: "
                            "stub already pointed to hot, removed orphaned cold file",
                        ),
                    ]
                    if not _recovery_committed and gm is not None:
                        _rh_recovery_warnings.append(
                            make_warning(
                                "artifact_history_cold_rehydrate_recovery_not_durable",
                                "Crash recovery completed on disk but git commit "
                                "failed; state is not yet durable",
                            ),
                        )
                    return {
                        "ok": True,
                        "family": _crash_stub.get("family", ""),
                        "artifact_state": "hot",
                        "source_payload_path": source_payload_path,
                        "restored_payload_path": source_payload_path,
                        "cold_storage_path": _expected_cold_rel,
                        "cold_stub_path": cold_stub_path,
                        "committed_files": [source_payload_path, _expected_cold_rel, cold_stub_path],
                        "durable": _recovery_committed,
                        "latest_commit": gm.latest_commit() if gm is not None else None,
                        "warnings": _rh_recovery_warnings,
                        "recovery_warnings": _rh_recovery_warnings,
                    }
                else:
                    # Rehydrate incomplete (stub still points to cold) →
                    # hot is orphan from partial rehydrate.
                    _logger.warning(
                        "Artifact cold-rehydrate crash recovery: removing "
                        "orphaned hot file %s",
                        source_payload_path,
                    )
                    hot_payload_path.unlink(missing_ok=True)
                    # Fall through to normal rehydrate

            elif hot_payload_path.exists():
                raise HTTPException(status_code=409, detail="Artifact-history payload already exists")

            # --- Normal flow ---
            if not cold_payload_path.exists() or not cold_payload_path.is_file():
                raise HTTPException(status_code=404, detail="Artifact-history cold payload not found")
            try:
                cold_payload_bytes = cold_payload_path.read_bytes()
                cold_stub_bytes = stub_path.read_bytes()
                payload_bytes = gzip.decompress(cold_payload_bytes)
                payload = json.loads(payload_bytes.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("payload is not an object")
                if payload.get("history_id") != Path(source_payload_path).stem:
                    raise ValueError("history_id mismatch")
                if payload.get("summary") != stub.get("summary") or payload.get("source_path") != stub.get("source_path"):
                    raise ValueError("stub/payload mismatch")
                _load_artifact_history_payload_from_dict(payload=payload, payload_rel=source_payload_path)
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"Invalid artifact-history cold payload: {exc}") from exc

            updated_stub = dict(stub)
            updated_stub["payload_path"] = source_payload_path
            try:
                write_bytes_file(hot_payload_path, payload_bytes)
                _write_json(stub_path, updated_stub)
                cold_payload_path.unlink()
                committed = bool(gm and try_commit_paths(
                    paths=[hot_payload_path, stub_path, cold_payload_path],
                    gm=gm,
                    commit_message=f"artifact-history: cold-rehydrate {payload['family']} {payload['history_id']}",
                ))
                if gm is not None and not committed:
                    raise RuntimeError("Artifact-history cold rehydrate commit produced no changes")
            except Exception as exc:
                rollback_errors: list[str] = []
                try:
                    hot_payload_path.unlink(missing_ok=True)
                except Exception as rollback_exc:
                    rollback_errors.append(f"remove restored payload: {rollback_exc}")
                try:
                    write_bytes_file(cold_payload_path, cold_payload_bytes)
                except Exception as rollback_exc:
                    rollback_errors.append(f"restore cold payload: {rollback_exc}")
                try:
                    write_bytes_file(stub_path, cold_stub_bytes)
                except Exception as rollback_exc:
                    rollback_errors.append(f"restore cold stub: {rollback_exc}")
                raise HTTPException(
                    status_code=500,
                    detail=make_error_detail(
                        operation="artifact_history_cold_rehydrate",
                        error_code="artifact_history_cold_rehydrate_failed",
                        error_detail=str(exc),
                        rollback_errors=rollback_errors,
                    ),
                ) from exc
    except SegmentHistoryLockTimeout as exc:
        raise make_lock_error("artifact_history_cold_rehydrate", None, exc, is_timeout=True) from exc

    audit(
        auth,
        "artifact_history_cold_rehydrate",
        {
            "source_payload_path": source_payload_path,
            "cold_storage_path": cold_storage_path,
            "cold_stub_path": cold_stub_path,
        },
    )
    return {
        "ok": True,
        "family": payload["family"],
        "artifact_state": "hot",
        "source_payload_path": source_payload_path,
        "restored_payload_path": source_payload_path,
        "cold_storage_path": cold_storage_path,
        "cold_stub_path": cold_stub_path,
        "committed_files": [source_payload_path, cold_storage_path, cold_stub_path],
        "durable": True,
        "latest_commit": gm.latest_commit() if gm is not None else None,
        "warnings": [],
        "recovery_warnings": [],
    }


def _load_artifact_history_payload_from_dict(*, payload: dict[str, Any], payload_rel: str) -> None:
    """Validate one artifact-history payload object against its repo-relative path."""
    family, _history_dir_rel = _validate_artifact_history_payload_rel_path(payload_rel)
    if payload.get("schema_type") != _ARTIFACT_HISTORY_SCHEMA_TYPES_BY_FAMILY[family]:
        raise ValueError("schema_type mismatch")
    if payload.get("schema_version") != "1.0":
        raise ValueError("schema_version mismatch")
    if payload.get("family") != family:
        raise ValueError("family mismatch")
    if payload.get("history_id") != Path(payload_rel).stem:
        raise ValueError("history_id mismatch")
    if _parse_iso(str(payload.get("cut_at") or "")) is None:
        raise ValueError("cut_at mismatch")
    if not isinstance(payload.get("artifact"), dict):
        raise ValueError("artifact missing")
    if not isinstance(payload.get("summary"), dict):
        raise ValueError("summary missing")
    artifact_id = str(payload.get("artifact_id") or "")
    artifact_id_key = _family_artifact_id_key(family)
    if not artifact_id or str(payload["artifact"].get(artifact_id_key) or "") != artifact_id:
        raise ValueError("artifact identity mismatch")
    if payload.get("source_path") != _family_source_rel(family, artifact_id):
        raise ValueError("source_path mismatch")
    if payload.get("summary") != _family_summary(family, payload["artifact"]):
        raise ValueError("summary mismatch")


def artifact_history_cold_apply_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    now: datetime,
    settings: Any,
    families: list[str] | None,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Apply artifact-history cold-store eligibility using the configured thresholds."""
    requested = families if families else ["handoff", "shared_history", "reconciliation", "task_done", "patch_applied"]
    cold_after_days_by_family = {
        "handoff": int(settings.handoff_cold_after_days),
        "shared_history": int(settings.shared_history_cold_after_days),
        "reconciliation": int(settings.reconciliation_cold_after_days),
        "task_done": int(settings.task_done_cold_after_days),
        "patch_applied": int(settings.patch_applied_cold_after_days),
    }
    results: dict[str, Any] = {}
    warnings: list[dict[str, Any]] = []
    cold_stored = 0

    for family in requested:
        history_dir = safe_path(repo_root, _ARTIFACT_HISTORY_DIRS_BY_FAMILY[family])
        eligible_paths: list[str] = []
        if history_dir.exists() and history_dir.is_dir():
            cutoff = now - timedelta(days=cold_after_days_by_family[family])
            for path in sorted(history_dir.glob("*.json")):
                if not path.is_file():
                    continue
                rel = str(path.relative_to(repo_root))
                try:
                    payload = _load_artifact_history_payload(repo_root, rel)
                except HTTPException:
                    warnings.append(make_warning(
                        "artifact_history_payload_unreadable",
                        f"Cannot read payload: {rel}",
                        path=rel,
                    ))
                    continue
                cold_ts = _family_cold_timestamp(payload["summary"], family=family)
                if cold_ts is None:
                    warnings.append(make_warning("artifact_history_cold_timestamp_missing", f"No cold-eligibility timestamp: {rel}", path=rel))
                    continue
                stub_rel = f"{_ARTIFACT_HISTORY_DIRS_BY_FAMILY[family]}/index/{path.name}"
                try:
                    stub = _load_artifact_history_stub(repo_root, stub_rel)
                except HTTPException:
                    warnings.append(make_warning("artifact_history_invalid_stub", f"Cannot read stub: {stub_rel}", path=stub_rel))
                    continue
                if stub.get("payload_path") != rel:
                    continue
                if cold_ts <= cutoff:
                    eligible_paths.append(rel)
        family_results: list[dict[str, Any]] = []
        for rel in eligible_paths:
            try:
                family_results.append(
                    artifact_history_cold_store_service(
                        repo_root=repo_root,
                        gm=gm,
                        auth=auth,
                        req=type("Req", (), {"source_payload_path": rel})(),
                        audit=audit,
                    )
                )
                cold_stored += 1
            except HTTPException as exc:
                warnings.append(make_warning(
                    "artifact_history_cold_store_failed",
                    str(exc.detail),
                    path=rel,
                ))
        results[family] = {"eligible": len(eligible_paths), "cold_stored": len(family_results), "results": family_results}

    return {"ok": True, "cold_stored": cold_stored, "families": results, "warnings": warnings}


# ===================================================================
# Orchestrator: run artifact lifecycle maintenance
# ===================================================================


def artifact_lifecycle_maintenance_service(
    *,
    repo_root: Path,
    gm: Any,
    now: datetime | None = None,
    families: list[str] | None = None,
    settings: Any,
    audit: Callable[[Any, str, dict[str, Any]], None] | None = None,
    auth: Any = None,
) -> dict[str, Any]:
    """Run artifact lifecycle maintenance for the requested families.

    Processes families in the spec-defined order, stopping after one family
    reaches the batch limit. Returns aggregated results.
    """
    if now is None:
        now = _iso_now()

    all_families = ["handoff", "reconciliation", "task_done", "patch_applied"]
    requested = families if families else all_families
    # Enforce spec order
    ordered = [f for f in all_families if f in requested]

    results: dict[str, Any] = {}
    all_warnings: list[dict[str, Any]] = []
    all_written: list[str] = []
    all_deleted: list[str] = []
    cold_apply_result: dict[str, Any] | None = None
    batch_limit = int(settings.artifact_history_batch_limit)
    remaining_budget = batch_limit

    for family in ordered:
        if remaining_budget <= 0:
            break
        try:
            if family == "handoff":
                result = handoff_maintenance_pass(
                    repo_root=repo_root,
                    now=now,
                    terminal_retention_days=int(settings.handoff_terminal_retention_days),
                    batch_limit=remaining_budget,
                )
            elif family == "reconciliation":
                result = reconciliation_maintenance_pass(
                    repo_root=repo_root,
                    now=now,
                    resolved_retention_days=int(settings.reconciliation_resolved_retention_days),
                    batch_limit=remaining_budget,
                )
            elif family == "task_done":
                result = task_done_maintenance_pass(
                    repo_root=repo_root,
                    now=now,
                    hot_retention_days=int(settings.task_done_hot_retention_days),
                    batch_limit=remaining_budget,
                )
            elif family == "patch_applied":
                result = patch_applied_maintenance_pass(
                    repo_root=repo_root,
                    now=now,
                    hot_retention_days=int(settings.patch_applied_hot_retention_days),
                    batch_limit=remaining_budget,
                )
            else:
                continue
        except Exception:
            _logger.error(
                "Artifact lifecycle maintenance failed for family %s; continuing with remaining families",
                family,
                exc_info=True,
            )
            results[family] = {"ok": False, "family": family, "error": f"maintenance_failed:{family}"}
            all_warnings.append(make_warning("artifact_maintenance_failed", f"Maintenance pass failed for {family}", family=family))
            continue

        results[family] = result
        all_warnings.extend(result.get("warnings", []))
        written = result.get("written_paths", [])
        all_written.extend(written)
        deleted = result.get("deleted_paths", [])
        all_deleted.extend(deleted)

        externalized = result.get("externalized", 0)
        remaining_budget -= externalized
        if remaining_budget <= 0:
            break

    if auth is not None and gm is not None:
        cold_apply_result = artifact_history_cold_apply_service(
            repo_root=repo_root,
            gm=gm,
            auth=auth,
            now=now,
            settings=settings,
            families=families,
            audit=audit or (lambda *_args, **_kwargs: None),
        )
        all_warnings.extend(cold_apply_result.get("warnings", []))

    # Git commit all written and deleted paths
    committed_files: list[str] = []
    git_warnings: list[dict[str, Any]] = []
    commit_paths_list = (
        [safe_path(repo_root, rel) for rel in all_written]
        + [safe_path(repo_root, rel) for rel in all_deleted]
    )
    if commit_paths_list and gm is not None:
        if try_commit_paths(
            paths=commit_paths_list,
            gm=gm,
            commit_message="artifact-lifecycle: maintenance pass",
        ):
            committed_files = list(all_written) + list(all_deleted)
        else:
            git_warnings.append(make_warning("artifact_maintenance_not_durable", "Data written to disk but not committed to git"))

    all_warnings.extend(git_warnings)

    any_family_failed = any(
        isinstance(r, dict) and not r.get("ok", True)
        for r in results.values()
    )
    durable = not bool(git_warnings)

    response: dict[str, Any] = {
        "ok": not any_family_failed,
        "durable": durable,

        "families": results,
        "cold_apply": cold_apply_result,
        "committed_files": committed_files,
        "warnings": all_warnings if all_warnings else [],
    }
    if not durable:
        response["at_risk_paths"] = list(all_written)
    if gm is not None:
        response["latest_commit"] = gm.latest_commit()

    if audit and auth:
        audit(auth, "artifact_lifecycle_maintenance", {
            "families": list(results.keys()),
            "committed": len(committed_files),
            "durable": durable,
            "warning_count": len(all_warnings),
        })
        # Emit per-family audit events collected from family passes
        for fam_name, fam_result in results.items():
            if isinstance(fam_result, dict):
                for evt in fam_result.get("audit_events", []):
                    try:
                        audit(auth, evt["event"], evt["detail"])
                    except Exception:
                        _logger.warning("Failed to emit audit event for family %s", fam_name, exc_info=True)

    return response
