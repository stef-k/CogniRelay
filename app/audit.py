"""Audit log helpers for repository-backed API events."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)


class WriteTimeRolloverError(Exception):
    """Raised when write-time rollover fails and the append must not proceed.

    Carries a stable ``code`` for structured error responses.
    """

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


def _check_write_time_rollover_locked(
    path: Path, rollover_bytes: int, repo_root: Path, gm: Any
) -> None:
    """Perform write-time rollover if threshold exceeded.

    **Caller must already hold the segment-history source lock.**  This
    function checks the file size, rolls the source if above threshold,
    commits, and cleans up the manifest — all without acquiring any
    additional segment-history source locks.

    Raises :class:`WriteTimeRolloverError` on failure so the append is
    blocked (per spec, the triggering append must fail on rollover failure).
    """
    try:
        size = path.stat().st_size
    except OSError:
        return
    if size < rollover_bytes:
        return

    # Trigger rollover — import lazily to avoid circular imports and keep
    # the common (no-rollover) path as fast as possible.
    from app.segment_history.families import FAMILIES
    from app.segment_history.manifest import (
        ManifestOccupied as _ManifestOccupied,
        read_manifest as _read_manifest,
        remove_manifest as _remove_manifest,
        write_manifest as _write_manifest,
    )
    from app.segment_history.service import (
        _derive_stream_key,
        _next_segment_id,
        _roll_jsonl_source,
    )

    family = "api_audit"
    config = FAMILIES[family]
    now = datetime.now(timezone.utc)
    rel = str(path.relative_to(repo_root))
    stream_key = _derive_stream_key(family, rel)

    # Check for pending batch residue under lock.  If a stale manifest
    # exists from a prior crash, attempt inline reconciliation so that
    # audit appends are not permanently blocked once the file exceeds the
    # rollover threshold.  Only raise if reconciliation fails and the
    # manifest still references this source.
    try:
        mf = _read_manifest(repo_root, family)
    except ValueError:
        mf = None
    if mf is not None and rel in mf.get("source_paths", []):
        from app.segment_history.service import _reconcile_manifest_residue

        _reconcile_manifest_residue(
            repo_root, family, "write_time_rollover", gm,
            locked_source_paths={rel},
        )
        # Re-read after reconciliation — it removes the manifest on success
        try:
            mf = _read_manifest(repo_root, family)
        except ValueError:
            mf = None
        if mf is not None and rel in mf.get("source_paths", []):
            raise WriteTimeRolloverError(
                "segment_history_pending_batch_residue",
                f"A pending batch operation lists this source: {rel}",
            )
        # Reconciliation may have truncated the source to remove
        # already-rolled data.  Re-check size to avoid re-rolling a
        # source that is now below the threshold.
        try:
            size = path.stat().st_size
        except OSError:
            return
        if size < rollover_bytes:
            return

    history_dir = repo_root / config.history_dir
    stub_dir = repo_root / config.stub_dir
    segment_id = _next_segment_id(family, stream_key, now, history_dir)
    payload_path = history_dir / f"{segment_id}.jsonl"
    stub_path = stub_dir / f"{segment_id}.json"

    content = path.read_text(encoding="utf-8", errors="replace")
    summary = config.build_summary(content)

    # Write crash-recovery manifest before mutations so that a
    # process crash mid-roll leaves a signal for reconciliation.
    try:
        _write_manifest(
            repo_root,
            operation="write_time_rollover",
            family=family,
            source_paths=[rel],
            segment_ids=[segment_id],
            target_paths=[
                str(payload_path.relative_to(repo_root)),
                str(stub_path.relative_to(repo_root)),
            ],
        )
    except _ManifestOccupied as exc:
        raise WriteTimeRolloverError(exc.code, str(exc)) from exc

    try:
        result = _roll_jsonl_source(
            source_path=path,
            payload_path=payload_path,
            family=family,
            segment_id=segment_id,
            stream_key=stream_key,
            rolled_at=now,
            stub_dir=stub_dir,
            summary=summary,
            repo_root=repo_root,
        )
    except WriteTimeRolloverError:
        # Clean up any partially-written target files (e.g. payload
        # written but stub write failed) before removing the manifest.
        for tp in [payload_path, stub_path]:
            if tp.is_file():
                try:
                    tp.unlink()
                except OSError:
                    pass
        _remove_manifest(repo_root, family)
        raise
    except Exception as exc:
        for tp in [payload_path, stub_path]:
            if tp.is_file():
                try:
                    tp.unlink()
                except OSError:
                    pass
        _remove_manifest(repo_root, family)
        raise WriteTimeRolloverError(
            "segment_history_write_time_rollover_failed",
            f"Write-time rollover local writes failed: {exc}",
        ) from exc
    if result is None:
        _remove_manifest(repo_root, family)
        _log.warning("Write-time rollover skipped (partial line only) for %s", path)
        return
    _stub, created = result

    # Commit with git serialization lock
    commit_succeeded = False
    if gm is not None:
        from app.git_locking import repository_mutation_lock

        try:
            with repository_mutation_lock(repo_root):
                commit_paths = created + [path]
                gm.commit_paths(
                    commit_paths,
                    f"segment-history: roll {family} {stream_key}",
                )
            commit_succeeded = True
        except Exception:
            _log.warning("Write-time rollover commit failed for %s", segment_id)
            # Local writes succeeded; leave the manifest so that the next
            # _reconcile_manifest_residue call can commit the orphaned files
            # instead of silently losing them on crash + repo restore.

    # Only remove the manifest after a successful commit.  When the commit
    # fails the manifest must survive so crash recovery can still find and
    # commit the orphaned rolled payload, stub, and truncated source.
    if commit_succeeded or gm is None:
        _remove_manifest(repo_root, family)


def _check_write_time_rollover(
    path: Path, rollover_bytes: int, repo_root: Path, gm: Any
) -> None:
    """Backward-compatible wrapper that acquires its own lock.

    Prefer :func:`append_audit` which holds the lock across both the
    rollover check and the append to prevent concurrent rollover from
    replacing the file while an append fd is open.
    """
    try:
        size = path.stat().st_size
    except OSError:
        return
    if size < rollover_bytes:
        return

    from app.segment_history.locking import (
        SegmentHistoryLockTimeout,
        segment_history_source_lock,
    )
    from app.segment_history.service import _derive_stream_key

    family = "api_audit"
    rel = str(path.relative_to(repo_root))
    stream_key = _derive_stream_key(family, rel)
    lock_key = f"segment_history:{family}:{stream_key}"
    lock_dir = repo_root / ".locks" / "segment_history"

    try:
        with segment_history_source_lock(lock_key, lock_dir=lock_dir):
            _check_write_time_rollover_locked(path, rollover_bytes, repo_root, gm)
    except SegmentHistoryLockTimeout as exc:
        raise WriteTimeRolloverError(
            "segment_history_source_lock_timeout",
            str(exc),
        ) from exc


_AUDIT_APPEND_LOCK_TIMEOUT: float = 2.0
"""Short lock timeout for audit appends.

Using a short timeout (2 s) instead of the default 30 s prevents audit
events from being silently dropped during the entire maintenance lock
hold window (which can exceed 60 s when git commits time out).  When
the lock cannot be acquired within this budget, the append falls back
to a lockless write — accepting a microsecond-scale race where a
concurrent maintenance roll's atomic rename could orphan the line, but
this is strictly better than guaranteed 100 % event loss.
"""


def append_audit(
    repo_root: Path, event: str, peer_id: str, detail: dict[str, Any],
    *, rollover_bytes: int = 0, gm: Any = None,
) -> None:
    """Append one structured API audit event to the repository log.

    When *rollover_bytes* > 0 and a *gm* (git manager) is provided,
    the source lock is held across both the rollover check and the file
    append to prevent a concurrent rollover from replacing the file
    (via atomic rename) while this call's open fd still points to the
    old inode.

    When the source lock cannot be acquired within
    :data:`_AUDIT_APPEND_LOCK_TIMEOUT` (e.g. because maintenance holds
    it), the append falls back to a lockless write so that audit events
    are not silently dropped for the duration of the maintenance window.

    Raises :class:`WriteTimeRolloverError` if write-time rollover fails
    (pending batch residue or rollover local write failure).  Lock
    timeouts no longer propagate — the event is written regardless.
    """
    path = repo_root / "logs" / "api_audit.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)

    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "peer_id": peer_id,
        "detail": detail,
    }
    line = json.dumps(row, ensure_ascii=False) + "\n"

    from app.segment_history.locking import (
        SegmentHistoryLockTimeout,
        segment_history_source_lock,
    )
    from app.segment_history.service import _derive_stream_key

    rel = str(path.relative_to(repo_root))
    stream_key = _derive_stream_key("api_audit", rel)
    lock_key = f"segment_history:api_audit:{stream_key}"
    lock_dir = repo_root / ".locks" / "segment_history"

    try:
        with segment_history_source_lock(
            lock_key, lock_dir=lock_dir, timeout=_AUDIT_APPEND_LOCK_TIMEOUT,
        ):
            if rollover_bytes > 0 and gm is not None:
                _check_write_time_rollover_locked(
                    path, rollover_bytes, repo_root, gm,
                )
            with path.open("a", encoding="utf-8") as f:
                f.write(line)
            return  # Append succeeded under lock — done.
    except SegmentHistoryLockTimeout:
        # Lock held by a concurrent maintenance operation.  Fall back to
        # a lockless append — accepts a microsecond-scale race where the
        # atomic rename in _roll_jsonl_source could orphan this line, but
        # this is strictly better than guaranteed event loss for the
        # entire maintenance window (30–60+ seconds).
        _log.debug(
            "Audit append falling back to lockless mode: source lock held "
            "by concurrent operation for key %s",
            lock_key,
        )

    # Fallback: lockless append.  Audit JSON lines are well under
    # PIPE_BUF (4096 on Linux), so concurrent appends from multiple
    # requests do not interleave.
    with path.open("a", encoding="utf-8") as f:
        f.write(line)
