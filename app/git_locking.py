"""Repository-level locking for git-backed mutation sequences.

The git-commit serialization lock uses a safety timeout (default 60 s)
to prevent system-wide deadlocks.  Callers must never acquire the git
lock before acquiring per-source locks — source locks have a 30 s
timeout and are the only concurrency-bounding mechanism.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import logging
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

_thread_state = threading.local()
_log = logging.getLogger(__name__)
_lock_dir_ready_guard = threading.Lock()
_lock_dir_ready: set[str] = set()


class GitLockInfrastructureError(RuntimeError):
    """Raised when git lock infrastructure cannot be created.

    Callers translate this to the appropriate HTTP status.
    """


class GitLockTimeout(RuntimeError):
    """Raised when the repository mutation lock cannot be acquired within the timeout."""

_GIT_LOCK_TIMEOUT: float = 60.0
_GIT_LOCK_POLL: float = 0.1


def _repo_lock_id(repo_root: Path) -> str:
    """Return a stable advisory-lock id for one repository root."""
    digest = hashlib.sha256(str(repo_root.resolve()).encode("utf-8")).hexdigest()
    return f"git_repo_{digest}"


def _ensure_lock_dir(lock_dir: Path) -> None:
    """Create the lock directory once per unique path and process lifetime.

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
            _log.error("Cannot create git lock directory %s: %s", lock_dir, exc)
            raise GitLockInfrastructureError(f"Git lock infrastructure unavailable: {exc}") from exc
        _lock_dir_ready.add(key)


@contextmanager
def _repo_file_lock(repo_root: Path) -> Generator[None, None, None]:
    """Acquire the per-repository advisory file lock."""
    lock_dir = repo_root / ".locks"
    _ensure_lock_dir(lock_dir)
    lock_path = lock_dir / f"{_repo_lock_id(repo_root)}.lock"
    try:
        lock_file = lock_path.open("w")
    except OSError:
        # Invalidate cache and retry once — directory may have been removed
        with _lock_dir_ready_guard:
            _lock_dir_ready.discard(str(lock_dir))
        _ensure_lock_dir(lock_dir)
        try:
            lock_file = lock_path.open("w")
        except OSError as exc:
            _log.error("Cannot open git lock file %s after retry: %s", lock_path, exc)
            raise GitLockInfrastructureError(f"Cannot open git lock file {lock_path}: {exc}") from exc
    try:
        deadline = time.monotonic() + _GIT_LOCK_TIMEOUT
        while True:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    lock_file.close()
                    raise GitLockTimeout(
                        f"Repository mutation lock timed out after {_GIT_LOCK_TIMEOUT}s"
                    ) from None
                time.sleep(_GIT_LOCK_POLL)
            except OSError as exc:
                if exc.errno == errno.EINTR:
                    continue
                lock_file.close()
                raise GitLockInfrastructureError(
                    f"Unexpected flock error on git lock: {exc}"
                ) from exc
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
