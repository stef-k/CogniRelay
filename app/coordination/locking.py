"""Per-artifact advisory file locking for coordination mutation sequences.

Raises domain exceptions (``ArtifactLockTimeout``,
``ArtifactLockInfrastructureError``) so that callers can translate them
into the appropriate HTTP semantics via ``lifecycle_warnings.make_lock_error``.
"""

from __future__ import annotations

import fcntl
import logging
import re
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from fastapi import HTTPException

_log = logging.getLogger(__name__)

_SAFE_ID = re.compile(r"^[a-z0-9_]+$")

LOCK_TIMEOUT_SECONDS: float = 30.0
_LOCK_POLL_INTERVAL: float = 0.05

_lock_dir_ready_guard = threading.Lock()
_lock_dir_ready: set[str] = set()


# ---------------------------------------------------------------------------
# Domain exceptions — callers translate via lifecycle_warnings.make_lock_error
# ---------------------------------------------------------------------------


class ArtifactLockTimeout(Exception):
    """Lock acquisition timed out for a specific artifact."""

    def __init__(self, artifact_id: str, timeout: float) -> None:
        self.artifact_id = artifact_id
        self.timeout = timeout
        super().__init__(f"Lock timed out for {artifact_id} after {timeout}s")


class ArtifactLockInfrastructureError(Exception):
    """Lock infrastructure is unavailable (directory/file creation failed)."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def purge_stale_lockfiles(lock_dir: Path) -> int:
    """Remove all lockfiles from the lock directory.

    Safe to call only at application startup before any requests are served.
    Returns the number of files removed.
    """
    removed = 0
    if not lock_dir.is_dir():
        return removed
    for entry in lock_dir.iterdir():
        if entry.is_file() and entry.suffix == ".lock":
            try:
                entry.unlink()
                removed += 1
            except OSError:
                _log.warning("Could not remove stale lockfile: %s", entry)
    if removed:
        _log.info("Purged %d stale lockfile(s) from %s", removed, lock_dir)
    return removed


def _ensure_lock_dir(lock_dir: Path) -> None:
    """Create the lock directory once per unique path, per process lifetime.

    The check and mkdir are performed under the same guard to prevent
    two threads from both attempting mkdir on the same path.
    """
    key = str(lock_dir)
    with _lock_dir_ready_guard:
        if key in _lock_dir_ready:
            return
        try:
            lock_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            _log.error("Cannot create lock directory %s: %s", lock_dir, exc)
            raise ArtifactLockInfrastructureError(
                f"Cannot create lock directory {lock_dir}: {exc}"
            ) from exc
        _lock_dir_ready.add(key)


@contextmanager
def artifact_lock(
    artifact_id: str, *, lock_dir: Path, timeout: float = LOCK_TIMEOUT_SECONDS
) -> Generator[None, None, None]:
    """Acquire an exclusive per-artifact advisory lock for the duration of a mutation.

    Uses ``fcntl.flock`` on a dedicated lockfile so that concurrent requests
    targeting the *same* artifact are serialized while unrelated artifacts
    remain fully parallel.  The lock is always released when the file
    descriptor is closed, including on exceptions.

    Raises ``HTTPException(400)`` if ``artifact_id`` contains path-traversal
    characters.  Raises ``ArtifactLockTimeout`` on timeout and
    ``ArtifactLockInfrastructureError`` on infrastructure failure — callers
    translate these via ``lifecycle_warnings.make_lock_error``.
    """
    if not _SAFE_ID.fullmatch(artifact_id):
        raise HTTPException(status_code=400, detail="Invalid artifact id for locking")

    _ensure_lock_dir(lock_dir)
    lock_path = lock_dir / f"{artifact_id}.lock"
    try:
        lock_file = lock_path.open("w")
    except OSError as exc:
        _log.error("Cannot open lock file %s: %s", lock_path, exc)
        raise ArtifactLockInfrastructureError(
            f"Cannot open lock file {lock_path}: {exc}"
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
                    _log.error("Lock acquisition timed out for artifact %s after %.1fs", artifact_id, timeout)
                    raise ArtifactLockTimeout(artifact_id, timeout) from None
                time.sleep(_LOCK_POLL_INTERVAL)
        yield
    finally:
        lock_file.close()
