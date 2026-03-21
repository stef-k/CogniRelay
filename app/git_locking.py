"""Repository-level locking for git-backed mutation sequences.

NOTE: Lock acquisition uses ``time.sleep()`` polling which blocks the
calling thread-pool thread.  Under high concurrency, this can exhaust
the ASGI thread pool and stall all endpoints.  A proper fix (async lock
polling or a concurrency semaphore) is deferred to a separate issue.
"""

from __future__ import annotations

import fcntl
import hashlib
import logging
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from fastapi import HTTPException

_thread_state = threading.local()
_log = logging.getLogger(__name__)
_lock_dir_ready_guard = threading.Lock()
_lock_dir_ready: set[str] = set()
_LOCK_TIMEOUT_SECONDS = 30.0
_LOCK_POLL_INTERVAL = 0.05


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
        deadline = time.monotonic() + _LOCK_TIMEOUT_SECONDS
        while True:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    lock_file.close()
                    _log.error("Git mutation lock acquisition timed out for %s", repo_root)
                    raise HTTPException(status_code=503, detail="Git lock acquisition timed out") from None
                time.sleep(_LOCK_POLL_INTERVAL)
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
