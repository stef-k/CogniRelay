"""Locked append helpers for segment-history families.

Every append to an active source file belonging to a segment-history family
must acquire the family's per-source advisory lock before writing.  When the
locked snapshot exceeds a rollover trigger (size, day-boundary, max-hot-age,
or inactivity), write-time rollover is performed before the append.

The helpers in this module centralise that logic so that every caller
(api_audit, ops_runs, messages, episodic, journal) follows the same
lock-acquire -> rollover-check -> append path.

Error propagation (per spec):
- Lock timeout -> raise ``SegmentHistoryAppendError``.
- Rollover failure -> raise ``SegmentHistoryAppendError``.
- Journal non-current-day -> raise ``SegmentHistoryAppendError``.

Best-effort callers (``audit_event``, ``_append_ops_run``) must catch
``SegmentHistoryAppendError`` and degrade gracefully.  Primary-path callers
let it propagate to their existing error handling.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)


class SegmentHistoryAppendError(Exception):
    """Raised when a locked append cannot proceed.

    Causes: source-lock timeout, write-time rollover failure (local write
    or git commit), or journal non-current-day rejection.

    Best-effort callers (``audit_event``, ``_append_ops_run``) must catch
    this and degrade gracefully.  Primary-path callers let it propagate
    to their existing error handling.
    """

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")

# ---------------------------------------------------------------------------
# Path -> family reverse lookup
# ---------------------------------------------------------------------------
# Ordered longest-prefix-first so that ``messages/threads/`` matches before
# ``messages/``.
_PATH_PREFIX_TO_FAMILY: list[tuple[str, str]] = [
    ("messages/threads/", "message_thread"),
    ("messages/inbox/", "message_stream"),
    ("messages/outbox/", "message_stream"),
    ("messages/relay/", "message_stream"),
    ("messages/acks/", "message_stream"),
    ("memory/episodic/", "episodic"),
    ("journal/", "journal"),
    ("logs/api_audit", "api_audit"),
    ("logs/ops_runs", "ops_runs"),
]


def _family_for_path(rel: str) -> str | None:
    """Derive the segment-history family from a repo-relative path.

    Returns ``None`` if the path does not belong to any segment-history
    family (e.g. a task file or an arbitrary JSONL path).
    """
    # Exclude history subtrees — they are rolled segments, not active sources.
    if "/history/" in rel:
        return None
    for prefix, fam in _PATH_PREFIX_TO_FAMILY:
        if rel.startswith(prefix):
            return fam
    return None


# ---------------------------------------------------------------------------
# Core locked append
# ---------------------------------------------------------------------------
_DEFAULT_LOCK_TIMEOUT: float = 2.0
"""Short lock timeout for append paths.

Using a short timeout (2 s) instead of the default 30 s prevents events
from being silently dropped during the entire maintenance lock-hold window
(which can exceed 60 s when git commits time out).  When the lock cannot be
acquired within this budget, the append falls back to a lockless write —
accepting a race window spanning the full ``_roll_jsonl_source`` execution
(~50-200 ms of file I/O) where the atomic rename could orphan the appended
line, but this is strictly better than guaranteed event loss.
"""


def locked_append_jsonl(
    path: Path,
    record: Any,
    *,
    repo_root: Path,
    gm: Any = None,
    settings: Any = None,
    family: str | None = None,
    lock_timeout: float = _DEFAULT_LOCK_TIMEOUT,
) -> None:
    """Append one JSON-line record with source-lock and write-time rollover.

    If *family* is ``None``, it is derived from the repo-relative path.
    Paths that do not belong to a segment-history family are passed through
    to a plain ``append_jsonl`` without locking.

    Raises :class:`SegmentHistoryAppendError` on lock timeout or
    write-time rollover failure.
    """
    from app.storage import append_jsonl

    rel = str(path.relative_to(repo_root))

    if family is None:
        family = _family_for_path(rel)
    if family is None:
        # Not a segment-history path — plain unlocked append.
        append_jsonl(path, record)
        return

    line = json.dumps(record, ensure_ascii=False) + "\n"

    from app.segment_history.locking import (
        SegmentHistoryLockTimeout,
        segment_history_source_lock,
    )
    from app.segment_history.service import _derive_stream_key

    stream_key = _derive_stream_key(family, rel)
    lock_key = f"segment_history:{family}:{stream_key}"
    lock_dir = repo_root / ".locks" / "segment_history"

    from app.audit import WriteTimeRolloverError

    try:
        with segment_history_source_lock(
            lock_key, lock_dir=lock_dir, timeout=lock_timeout,
        ):
            _check_and_rollover_locked(path, family, repo_root, gm, settings)
            # Journal: reject appends to non-current-day files under lock.
            if family == "journal":
                _reject_non_current_day_journal(path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                f.write(line)
    except SegmentHistoryLockTimeout as exc:
        raise SegmentHistoryAppendError(
            "segment_history_source_lock_timeout", str(exc),
        ) from exc
    except WriteTimeRolloverError as exc:
        raise SegmentHistoryAppendError(exc.code, exc.detail) from exc


def locked_append_jsonl_multi(
    paths: list[Path],
    record: dict[str, Any],
    *,
    repo_root: Path,
    gm: Any = None,
    settings: Any = None,
    lock_timeout: float = _DEFAULT_LOCK_TIMEOUT,
) -> None:
    """Append one JSON-line record to multiple files with source-lock protection.

    Acquires per-source locks for all paths that belong to segment-history
    families (in sorted order to prevent deadlocks), checks rollover for
    each, then appends.

    Raises :class:`SegmentHistoryAppendError` on lock timeout or
    write-time rollover failure.
    """
    from app.storage import append_jsonl_multi

    if not paths:
        return

    line = json.dumps(record, ensure_ascii=False) + "\n"

    # Classify paths into segment-history (need locks) and non-segment
    # (plain append).
    sh_entries: list[tuple[Path, str, str]] = []  # (path, family, lock_key)
    non_sh_paths: list[Path] = []

    from app.segment_history.locking import (
        SegmentHistoryLockTimeout,
        segment_history_source_lock,
    )
    from app.segment_history.service import _derive_stream_key

    for p in paths:
        rel = str(p.relative_to(repo_root))
        fam = _family_for_path(rel)
        if fam is None:
            non_sh_paths.append(p)
        else:
            sk = _derive_stream_key(fam, rel)
            lk = f"segment_history:{fam}:{sk}"
            sh_entries.append((p, fam, lk))

    # Append non-segment-history paths without locks.
    if non_sh_paths:
        append_jsonl_multi(non_sh_paths, record)

    if not sh_entries:
        return

    # Sort by lock key to acquire in consistent order.
    sh_entries.sort(key=lambda e: e[2])
    lock_dir = repo_root / ".locks" / "segment_history"

    from app.audit import WriteTimeRolloverError

    try:
        acquired: list[Any] = []
        try:
            for path_entry, fam_entry, lk_entry in sh_entries:
                ctx = segment_history_source_lock(
                    lk_entry, lock_dir=lock_dir, timeout=lock_timeout,
                )
                lock_handle = ctx.__enter__()
                acquired.append((ctx, lock_handle))

            # All locks acquired — check rollover and append under locks.
            for path_entry, fam_entry, _lk_entry in sh_entries:
                _check_and_rollover_locked(
                    path_entry, fam_entry, repo_root, gm, settings,
                )
                if fam_entry == "journal":
                    _reject_non_current_day_journal(path_entry)
                path_entry.parent.mkdir(parents=True, exist_ok=True)
                with path_entry.open("a", encoding="utf-8") as f:
                    f.write(line)
        finally:
            # Release all acquired locks in reverse order.
            for ctx, _handle in reversed(acquired):
                try:
                    ctx.__exit__(None, None, None)
                except Exception:
                    pass
    except SegmentHistoryLockTimeout as exc:
        raise SegmentHistoryAppendError(
            "segment_history_source_lock_timeout", str(exc),
        ) from exc
    except WriteTimeRolloverError as exc:
        raise SegmentHistoryAppendError(exc.code, exc.detail) from exc


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _check_and_rollover_locked(
    path: Path,
    family: str,
    repo_root: Path,
    gm: Any,
    settings: Any,
) -> None:
    """Check write-time rollover eligibility and perform rollover if needed.

    Must be called while holding the source lock for *path*.
    Lets ``WriteTimeRolloverError`` propagate — per spec the triggering
    append must fail on rollover failure.
    """
    if gm is None or settings is None:
        return

    from app.segment_history.families import (
        FAMILIES,
        check_rollover_eligible,
    )

    if family not in FAMILIES:
        return

    # Journal rollover is maintenance-driven only (spec carve-out).
    # Journal appends only need lock + non-current-day rejection.
    if family == "journal":
        return

    now = datetime.now(timezone.utc)

    # Check full family-specific eligibility (size, day-boundary,
    # max_hot_days, inactivity) from the locked snapshot.
    if not path.is_file():
        return
    if not check_rollover_eligible(path, family, settings, now):
        return

    # Eligible — perform write-time rollover.
    from app.audit import _check_write_time_rollover_locked

    config = FAMILIES[family]
    rollover_bytes = 0
    if config.has_size_rollover:
        from app.segment_history.families import _get_rollover_bytes_setting
        rb = _get_rollover_bytes_setting(family, settings)
        rollover_bytes = rb if rb is not None else 0

    _check_write_time_rollover_locked(
        path, rollover_bytes, repo_root, gm, family=family,
    )


def _reject_non_current_day_journal(path: Path) -> None:
    """Reject journal appends to non-current-day files.

    Per spec: journal writers must determine the target day bucket again
    while holding the source lock.  If the locked re-check shows the target
    path is now non-current-day, the writer must reject that append.
    """
    from app.audit import WriteTimeRolloverError

    import re
    day_re = re.compile(r"\d{4}-\d{2}-\d{2}\.md$")
    if not day_re.search(path.name):
        return
    file_day = path.stem  # e.g. "2026-03-19"
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if file_day != today:
        raise WriteTimeRolloverError(
            "segment_history_journal_non_current_day",
            f"Journal append rejected: file day {file_day} != current UTC day {today}. "
            f"Retry against the current-day path.",
        )
