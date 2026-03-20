"""Per-source advisory file locking for segment-history mutation sequences."""

from __future__ import annotations

import fcntl
import hashlib
import logging
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from fastapi import HTTPException

_log = logging.getLogger(__name__)

LOCK_TIMEOUT_SECONDS: float = 30.0
_LOCK_POLL_INTERVAL: float = 0.05

_lock_dir_ready: set[str] = set()


def _ensure_lock_dir(lock_dir: Path) -> None:
    """Create the lock directory once per unique path, per process lifetime."""
    key = str(lock_dir)
    if key in _lock_dir_ready:
        return
    try:
        lock_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        _log.error("Cannot create lock directory %s: %s", lock_dir, exc)
        raise HTTPException(
            status_code=503,
            detail="Segment-history lock infrastructure unavailable",
        ) from exc
    _lock_dir_ready.add(key)


def _safe_lock_filename(lock_key: str) -> str:
    """Derive a filesystem-safe lock filename from an arbitrary lock key.

    Uses SHA-256 of the key to avoid path-traversal issues with colon-separated
    keys like ``segment_history:journal:logs/journal/2026-03-20.jsonl``.
    """
    return hashlib.sha256(lock_key.encode("utf-8")).hexdigest() + ".lock"


@contextmanager
def segment_history_source_lock(
    lock_key: str, *, lock_dir: Path, timeout: float = LOCK_TIMEOUT_SECONDS
) -> Generator[None, None, None]:
    """Acquire an exclusive per-source advisory lock for a segment-history mutation.

    Unlike ``artifact_lock``, this accepts arbitrary colon-separated keys
    (e.g. ``segment_history:journal:logs/journal/2026-03-20.jsonl``) and
    hashes them to derive a safe lock filename.
    """
    _ensure_lock_dir(lock_dir)
    lock_path = lock_dir / _safe_lock_filename(lock_key)
    try:
        lock_file = lock_path.open("w")
    except OSError as exc:
        _log.error("Cannot open lock file %s: %s", lock_path, exc)
        raise HTTPException(
            status_code=503,
            detail="Segment-history lock infrastructure unavailable",
        ) from exc
    try:
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    lock_file.close()
                    _log.error(
                        "Lock acquisition timed out for %s after %.1fs",
                        lock_key,
                        timeout,
                    )
                    raise HTTPException(
                        status_code=503,
                        detail="Segment-history lock acquisition timed out",
                    ) from None
                time.sleep(_LOCK_POLL_INTERVAL)
        yield
    finally:
        lock_file.close()


@contextmanager
def acquire_sorted_source_locks(
    keys: list[str], *, lock_dir: Path, total_budget: float = 30.0
) -> Generator[None, None, None]:
    """Acquire exclusive locks on multiple sources in sorted order.

    Sorting prevents deadlocks when concurrent callers target overlapping
    source sets.  The *total_budget* is the wall-clock limit across all
    individual lock acquisitions.
    """
    sorted_keys = sorted(set(keys))
    if not sorted_keys:
        yield
        return

    _ensure_lock_dir(lock_dir)
    held: list = []  # open file handles
    deadline = time.monotonic() + total_budget
    try:
        for key in sorted_keys:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise HTTPException(
                    status_code=503,
                    detail="Segment-history batch lock budget exhausted",
                )
            lock_path = lock_dir / _safe_lock_filename(key)
            try:
                lock_file = lock_path.open("w")
            except OSError as exc:
                _log.error("Cannot open lock file %s: %s", lock_path, exc)
                raise HTTPException(
                    status_code=503,
                    detail="Segment-history lock infrastructure unavailable",
                ) from exc
            held.append(lock_file)
            while True:
                try:
                    fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        _log.error(
                            "Batch lock budget exhausted acquiring %s",
                            key,
                        )
                        raise HTTPException(
                            status_code=503,
                            detail="Segment-history batch lock budget exhausted",
                        ) from None
                    time.sleep(_LOCK_POLL_INTERVAL)
        yield
    finally:
        for fh in held:
            fh.close()
