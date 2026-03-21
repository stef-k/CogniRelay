"""Repository-level locking for git-backed mutation sequences.

The git-commit serialization lock waits indefinitely (no timeout) per
the segment-history spec.  Callers must never acquire the git lock
before acquiring per-source locks — source locks have a 30 s timeout
and are the only concurrency-bounding mechanism.
"""

from __future__ import annotations

import fcntl
import hashlib
import logging
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from fastapi import HTTPException

_thread_state = threading.local()
_log = logging.getLogger(__name__)
_lock_dir_ready_guard = threading.Lock()
_lock_dir_ready: set[str] = set()


def _repo_lock_id(repo_root: Path) -> str:
    """Return a stable advisory-lock id for one repository root."""
    digest = hashlib.sha256(str(repo_root.resolve()).encode("utf-8")).hexdigest()
    return f"git_repo_{digest}"


def _ensure_lock_dir(lock_dir: Path) -> None:
    """Create the lock directory once per unique path and process lifetime."""
    key = str(lock_dir)
    with _lock_dir_ready_guard:
        if key in _lock_dir_ready:
            return
    try:
        lock_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        _log.error("Cannot create git lock directory %s: %s", lock_dir, exc)
        raise HTTPException(status_code=503, detail="Git lock infrastructure unavailable") from exc
    with _lock_dir_ready_guard:
        _lock_dir_ready.add(key)


@contextmanager
def _repo_file_lock(repo_root: Path) -> Generator[None, None, None]:
    """Acquire the per-repository advisory file lock."""
    lock_dir = repo_root / ".locks"
    _ensure_lock_dir(lock_dir)
    lock_path = lock_dir / f"{_repo_lock_id(repo_root)}.lock"
    try:
        lock_file = lock_path.open("w")
    except OSError as exc:
        _log.error("Cannot open git lock file %s: %s", lock_path, exc)
        raise HTTPException(status_code=503, detail="Git lock infrastructure unavailable") from exc
    try:
        # Blocking wait — no timeout per spec.  Source locks (30 s timeout)
        # bound concurrency; the git lock is only held during brief
        # git-add/commit operations.
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        yield
    finally:
        lock_file.close()


@contextmanager
def repository_mutation_lock(repo_root: Path) -> Generator[None, None, None]:
    """Serialize git-backed mutations for one repository, including nested calls."""
    resolved = repo_root.resolve()
    key = str(resolved)
    depths = getattr(_thread_state, "depths", None)
    if depths is None:
        depths = {}
        _thread_state.depths = depths

    depth = depths.get(key, 0)
    if depth:
        depths[key] = depth + 1
        try:
            yield
        finally:
            remaining = depths[key] - 1
            if remaining:
                depths[key] = remaining
            else:
                depths.pop(key, None)
        return

    with _repo_file_lock(resolved):
        depths[key] = 1
        try:
            yield
        finally:
            depths.pop(key, None)
