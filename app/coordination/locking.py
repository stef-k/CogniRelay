"""Per-artifact advisory file locking for coordination mutation sequences."""

from __future__ import annotations

import fcntl
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

LOCK_DIR = Path("/tmp/cognirelay-locks")


@contextmanager
def artifact_lock(artifact_id: str) -> Generator[None, None, None]:
    """Acquire an exclusive per-artifact advisory lock for the duration of a mutation.

    Uses ``fcntl.flock`` on a dedicated lockfile so that concurrent requests
    targeting the *same* artifact are serialized while unrelated artifacts
    remain fully parallel.  The lock is always released on exit, including
    on exceptions.
    """
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    lock_path = LOCK_DIR / f"{artifact_id}.lock"
    lock_file = lock_path.open("w")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()
