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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from app.git_safety import try_commit_paths
from app.storage import safe_path, write_bytes_file, write_text_file

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

# Terminal handoff statuses per spec
_TERMINAL_HANDOFF_STATUSES = frozenset({"accepted_advisory", "deferred", "rejected"})

# ---------------------------------------------------------------------------
# ISO timestamp helpers
# ---------------------------------------------------------------------------


def _parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO timestamp string into a timezone-aware UTC datetime.

    Naive timestamps (no offset) are assumed UTC to prevent TypeError when
    compared against UTC-aware cutoffs.
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _iso_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Shared history_id naming
# ---------------------------------------------------------------------------


def _history_timestamp_str(cut_at: datetime) -> str:
    """Convert cut_at to the YYYYMMDDTHHMMSSZ format per spec."""
    utc = cut_at.astimezone(timezone.utc).replace(microsecond=0)
    return utc.strftime("%Y%m%dT%H%M%SZ")


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
    if history_dir.exists() and history_dir.is_dir():
        for child in history_dir.iterdir():
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
        "created_at": created_at.isoformat(),
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


# ---------------------------------------------------------------------------
# Shared JSON I/O
# ---------------------------------------------------------------------------


def _load_json_file(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    """Load a JSON artifact file with graceful degradation.

    Returns (data, warning). If data is None, the file is unreadable.
    """
    if not path.exists() or not path.is_file():
        return None, None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError):
        return None, f"artifact_corrupt:{path.name}"
    except Exception:
        return None, f"artifact_unreadable:{path.name}"
    if not isinstance(data, dict):
        return None, f"artifact_not_dict:{path.name}"
    return data, None


def _write_json(path: Path, data: dict[str, Any]) -> None:
    """Write JSON data to path atomically."""
    write_text_file(path, json.dumps(data, ensure_ascii=False, indent=2))


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
        "cut_at": cut_at.isoformat(),
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


def _handoff_retention_timestamp(artifact: dict[str, Any]) -> tuple[datetime | None, str | None]:
    """Derive the retention timestamp for a terminal handoff.

    Per spec: consumed_at when present, otherwise updated_at, otherwise created_at.
    """
    for field in ("consumed_at", "updated_at", "created_at"):
        ts = _parse_iso(artifact.get(field))
        if ts is not None:
            return ts, None
    return None, f"handoff_retention_missing:{artifact.get('handoff_id', 'unknown')}"


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
    warnings: list[str] = []
    cutoff = now - timedelta(days=terminal_retention_days)

    handoffs_dir = safe_path(repo_root, HANDOFFS_DIR_REL)
    if not handoffs_dir.exists() or not handoffs_dir.is_dir():
        return {"ok": True, "family": "handoff", "externalized": 0, "warnings": warnings}

    # Snapshot: load all handoff artifacts
    eligible: list[tuple[str, dict[str, Any], datetime, str]] = []
    for path in sorted(handoffs_dir.iterdir(), key=lambda p: p.name):
        if path.is_dir() or path.suffix.lower() != ".json":
            continue
        handoff_id = path.stem
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
        eligible.append((handoff_id, artifact, ret_ts, source_rel))
        if len(eligible) >= batch_limit:
            break

    if not eligible:
        return {"ok": True, "family": "handoff", "externalized": 0, "warnings": warnings}

    # Build payloads and stubs
    write_plan: list[tuple[Path, str, dict[str, Any]]] = []
    delete_plan: list[Path] = []
    reserved_ids: set[str] = set()

    for handoff_id, artifact, _ret_ts, source_rel in eligible:
        summary = _handoff_summary(artifact)
        _, payload_rel, stub_rel, payload, stub = _externalize_single_artifact(
            repo_root=repo_root,
            family="handoff",
            schema_type="handoff_history_unit",
            artifact_id=handoff_id,
            source_rel=source_rel,
            artifact=artifact,
            summary=summary,
            history_dir_rel=HANDOFFS_HISTORY_DIR_REL,
            cut_at=now,
            reserved_ids=reserved_ids,
        )
        write_plan.append((safe_path(repo_root, payload_rel), payload_rel, payload))
        write_plan.append((safe_path(repo_root, stub_rel), stub_rel, stub))
        delete_plan.append(safe_path(repo_root, source_rel))

    # Rollback includes both writes and deletes
    all_paths = [p for p, _, _ in write_plan] + delete_plan
    rollback = _capture_rollback(all_paths)
    try:
        for path, _, data in write_plan:
            _write_json(path, data)
        for hot_path in delete_plan:
            hot_path.unlink(missing_ok=True)
    except Exception:
        _restore_rollback(rollback)
        raise

    # Best-effort: remove externalized handoffs from the query sidecar index
    from app.coordination.query_index import try_delete_handoff
    for handoff_id, _, _, _ in eligible:
        try_delete_handoff(handoff_id)

    return {
        "ok": True,
        "family": "handoff",
        "externalized": len(eligible),
        "written_paths": [rel for _, rel, _ in write_plan],
        "deleted_paths": [f"{HANDOFFS_DIR_REL}/{hid}.json" for hid, _, _, _ in eligible],
        "warnings": warnings,
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
    warnings: list[str] = []
    cutoff = now - timedelta(days=resolved_retention_days)

    recon_dir = safe_path(repo_root, RECONCILIATIONS_DIR_REL)
    if not recon_dir.exists() or not recon_dir.is_dir():
        return {"ok": True, "family": "reconciliation", "externalized": 0, "warnings": warnings}

    eligible: list[tuple[str, dict[str, Any], datetime, str]] = []
    for path in sorted(recon_dir.iterdir(), key=lambda p: p.name):
        if path.is_dir() or path.suffix.lower() != ".json":
            continue
        recon_id = path.stem
        artifact, warn = _load_json_file(path)
        if warn:
            warnings.append(warn)
        if artifact is None:
            continue

        if str(artifact.get("status") or "open") != "resolved":
            continue

        updated_at = _parse_iso(artifact.get("updated_at"))
        if updated_at is None:
            warnings.append(f"reconciliation_retention_missing:{recon_id}")
            continue
        if updated_at > cutoff:
            continue

        source_rel = f"{RECONCILIATIONS_DIR_REL}/{recon_id}.json"
        eligible.append((recon_id, artifact, updated_at, source_rel))
        if len(eligible) >= batch_limit:
            break

    if not eligible:
        return {"ok": True, "family": "reconciliation", "externalized": 0, "warnings": warnings}

    write_plan: list[tuple[Path, str, dict[str, Any]]] = []
    delete_plan: list[Path] = []
    reserved_ids: set[str] = set()

    for recon_id, artifact, _ts, source_rel in eligible:
        summary = _reconciliation_summary(artifact)
        _, payload_rel, stub_rel, payload, stub = _externalize_single_artifact(
            repo_root=repo_root,
            family="reconciliation",
            schema_type="reconciliation_history_unit",
            artifact_id=recon_id,
            source_rel=source_rel,
            artifact=artifact,
            summary=summary,
            history_dir_rel=RECONCILIATIONS_HISTORY_DIR_REL,
            cut_at=now,
            reserved_ids=reserved_ids,
        )
        write_plan.append((safe_path(repo_root, payload_rel), payload_rel, payload))
        write_plan.append((safe_path(repo_root, stub_rel), stub_rel, stub))
        delete_plan.append(safe_path(repo_root, source_rel))

    all_paths = [p for p, _, _ in write_plan] + delete_plan
    rollback = _capture_rollback(all_paths)
    try:
        for path, _, data in write_plan:
            _write_json(path, data)
        for hot_path in delete_plan:
            hot_path.unlink(missing_ok=True)
    except Exception:
        _restore_rollback(rollback)
        raise

    # Best-effort: remove externalized reconciliations from the query sidecar index
    from app.coordination.query_index import try_delete_reconciliation
    for recon_id, _, _, _ in eligible:
        try_delete_reconciliation(recon_id)

    return {
        "ok": True,
        "family": "reconciliation",
        "externalized": len(eligible),
        "written_paths": [rel for _, rel, _ in write_plan],
        "deleted_paths": [f"{RECONCILIATIONS_DIR_REL}/{rid}.json" for rid, _, _, _ in eligible],
        "warnings": warnings,
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
    warnings: list[str] = []
    cutoff = now - timedelta(days=hot_retention_days)

    done_dir = safe_path(repo_root, TASKS_DONE_DIR_REL)
    if not done_dir.exists() or not done_dir.is_dir():
        return {"ok": True, "family": "task_done", "externalized": 0, "warnings": warnings}

    eligible: list[tuple[str, dict[str, Any], datetime, str]] = []
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
            warnings.append(f"task_done_retention_missing:{task_id}")
            continue
        if updated_at > cutoff:
            continue

        source_rel = f"{TASKS_DONE_DIR_REL}/{task_id}.json"
        eligible.append((task_id, artifact, updated_at, source_rel))
        if len(eligible) >= batch_limit:
            break

    if not eligible:
        return {"ok": True, "family": "task_done", "externalized": 0, "warnings": warnings}

    write_plan: list[tuple[Path, str, dict[str, Any]]] = []
    delete_plan: list[Path] = []
    reserved_ids: set[str] = set()

    for task_id, artifact, _ts, source_rel in eligible:
        summary = _task_done_summary(artifact)
        _, payload_rel, stub_rel, payload, stub = _externalize_single_artifact(
            repo_root=repo_root,
            family="task_done",
            schema_type="task_done_history_unit",
            artifact_id=task_id,
            source_rel=source_rel,
            artifact=artifact,
            summary=summary,
            history_dir_rel=TASKS_HISTORY_DONE_DIR_REL,
            cut_at=now,
            reserved_ids=reserved_ids,
        )
        write_plan.append((safe_path(repo_root, payload_rel), payload_rel, payload))
        write_plan.append((safe_path(repo_root, stub_rel), stub_rel, stub))
        delete_plan.append(safe_path(repo_root, source_rel))

    all_paths = [p for p, _, _ in write_plan] + delete_plan
    rollback = _capture_rollback(all_paths)
    try:
        for path, _, data in write_plan:
            _write_json(path, data)
        for hot_path in delete_plan:
            hot_path.unlink(missing_ok=True)
    except Exception:
        _restore_rollback(rollback)
        raise

    return {
        "ok": True,
        "family": "task_done",
        "externalized": len(eligible),
        "written_paths": [rel for _, rel, _ in write_plan],
        "deleted_paths": [f"{TASKS_DONE_DIR_REL}/{tid}.json" for tid, _, _, _ in eligible],
        "warnings": warnings,
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


def patch_applied_maintenance_pass(
    *,
    repo_root: Path,
    now: datetime,
    hot_retention_days: int,
    batch_limit: int,
) -> dict[str, Any]:
    """Run one maintenance pass for applied-patch artifacts."""
    warnings: list[str] = []
    cutoff = now - timedelta(days=hot_retention_days)

    applied_dir = safe_path(repo_root, PATCHES_APPLIED_DIR_REL)
    if not applied_dir.exists() or not applied_dir.is_dir():
        return {"ok": True, "family": "patch_applied", "externalized": 0, "warnings": warnings}

    eligible: list[tuple[str, dict[str, Any], datetime, str]] = []
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
            warnings.append(f"patch_applied_retention_missing:{patch_id}")
            continue
        if updated_at > cutoff:
            continue

        source_rel = f"{PATCHES_APPLIED_DIR_REL}/{patch_id}.json"
        eligible.append((patch_id, artifact, updated_at, source_rel))
        if len(eligible) >= batch_limit:
            break

    if not eligible:
        return {"ok": True, "family": "patch_applied", "externalized": 0, "warnings": warnings}

    write_plan: list[tuple[Path, str, dict[str, Any]]] = []
    delete_plan: list[Path] = []
    reserved_ids: set[str] = set()

    for patch_id, artifact, _ts, source_rel in eligible:
        summary = _patch_applied_summary(artifact)
        _, payload_rel, stub_rel, payload, stub = _externalize_single_artifact(
            repo_root=repo_root,
            family="patch_applied",
            schema_type="patch_applied_history_unit",
            artifact_id=patch_id,
            source_rel=source_rel,
            artifact=artifact,
            summary=summary,
            history_dir_rel=PATCHES_HISTORY_APPLIED_DIR_REL,
            cut_at=now,
            reserved_ids=reserved_ids,
        )
        write_plan.append((safe_path(repo_root, payload_rel), payload_rel, payload))
        write_plan.append((safe_path(repo_root, stub_rel), stub_rel, stub))
        delete_plan.append(safe_path(repo_root, source_rel))

    all_paths = [p for p, _, _ in write_plan] + delete_plan
    rollback = _capture_rollback(all_paths)
    try:
        for path, _, data in write_plan:
            _write_json(path, data)
        for hot_path in delete_plan:
            hot_path.unlink(missing_ok=True)
    except Exception:
        _restore_rollback(rollback)
        raise

    return {
        "ok": True,
        "family": "patch_applied",
        "externalized": len(eligible),
        "written_paths": [rel for _, rel, _ in write_plan],
        "deleted_paths": [f"{PATCHES_APPLIED_DIR_REL}/{pid}.json" for pid, _, _, _ in eligible],
        "warnings": warnings,
    }


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
    all_warnings: list[str] = []
    all_written: list[str] = []
    all_deleted: list[str] = []
    batch_limit = int(settings.artifact_history_batch_limit)

    for family in ordered:
        try:
            if family == "handoff":
                result = handoff_maintenance_pass(
                    repo_root=repo_root,
                    now=now,
                    terminal_retention_days=int(settings.handoff_terminal_retention_days),
                    batch_limit=batch_limit,
                )
            elif family == "reconciliation":
                result = reconciliation_maintenance_pass(
                    repo_root=repo_root,
                    now=now,
                    resolved_retention_days=int(settings.reconciliation_resolved_retention_days),
                    batch_limit=batch_limit,
                )
            elif family == "task_done":
                result = task_done_maintenance_pass(
                    repo_root=repo_root,
                    now=now,
                    hot_retention_days=int(settings.task_done_hot_retention_days),
                    batch_limit=batch_limit,
                )
            elif family == "patch_applied":
                result = patch_applied_maintenance_pass(
                    repo_root=repo_root,
                    now=now,
                    hot_retention_days=int(settings.patch_applied_hot_retention_days),
                    batch_limit=batch_limit,
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
            all_warnings.append(f"artifact_maintenance_failed:{family}")
            continue

        results[family] = result
        all_warnings.extend(result.get("warnings", []))
        written = result.get("written_paths", [])
        all_written.extend(written)
        deleted = result.get("deleted_paths", [])
        all_deleted.extend(deleted)

        # Spec: stop after one family reaches the batch limit
        externalized = result.get("externalized", 0)
        if externalized >= batch_limit:
            break

    # Git commit all written and deleted paths
    committed_files: list[str] = []
    git_warnings: list[str] = []
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
            git_warnings.append("artifact_maintenance_not_durable: data written to disk but not committed to git")

    all_warnings.extend(git_warnings)

    any_family_failed = any(
        isinstance(r, dict) and not r.get("ok", True)
        for r in results.values()
    )
    degraded = bool(git_warnings)

    response: dict[str, Any] = {
        "ok": not any_family_failed,
        "degraded": degraded,
        "families": results,
        "committed_files": committed_files,
        "warnings": all_warnings if all_warnings else [],
    }
    if gm is not None:
        response["latest_commit"] = gm.latest_commit()

    if audit and auth:
        audit(auth, "artifact_lifecycle_maintenance", {"families": list(results.keys()), "committed": len(committed_files)})

    return response
