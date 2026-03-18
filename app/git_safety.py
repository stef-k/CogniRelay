"""Safe git-commit wrappers with rollback on failure.

These helpers ensure that a failed ``git commit`` never leaves the repository
in a state where on-disk content diverges from the committed history without
the caller knowing.  Each variant captures enough prior state to restore the
file system and then surfaces a descriptive ``HTTPException`` to the agent.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fastapi import HTTPException

_log = logging.getLogger(__name__)


def safe_commit_new_file(
    *,
    path: Path,
    gm: Any,
    commit_message: str,
    error_detail: str,
) -> bool:
    """Commit a newly written file, deleting it on commit failure.

    Returns the boolean produced by ``gm.commit_file`` on success.
    Raises :class:`~fastapi.HTTPException` with *error_detail* on failure
    after removing the orphaned file from disk.
    """
    try:
        return gm.commit_file(path, commit_message)
    except Exception as exc:
        try:
            if path.exists():
                path.unlink()
        except Exception:
            _log.exception("Rollback (delete) failed for %s", path)
        raise HTTPException(status_code=500, detail=error_detail) from exc


def safe_commit_updated_file(
    *,
    path: Path,
    gm: Any,
    commit_message: str,
    error_detail: str,
    old_bytes: bytes | None,
) -> bool:
    """Commit an updated (or deleted) file, restoring prior content on failure.

    *old_bytes* should be the content of *path* **before** the caller's
    mutation.  Pass ``None`` if the file did not exist previously — the
    rollback will then delete the file instead.

    Returns the boolean produced by ``gm.commit_file`` on success.
    Raises :class:`~fastapi.HTTPException` with *error_detail* on failure.
    """
    try:
        return gm.commit_file(path, commit_message)
    except Exception as exc:
        try:
            if old_bytes is None:
                if path.exists():
                    path.unlink()
            else:
                path.write_bytes(old_bytes)
        except Exception:
            _log.exception("Rollback (restore) failed for %s", path)
        raise HTTPException(status_code=500, detail=error_detail) from exc


def try_commit_file(
    *,
    path: Path,
    gm: Any,
    commit_message: str,
) -> bool:
    """Attempt to commit a file, logging and suppressing git failures.

    Use this for artifacts that are regenerable and whose commit failure
    should not abort the request (e.g. derived index files).
    """
    try:
        return gm.commit_file(path, commit_message)
    except Exception:
        _log.exception("Git commit failed (non-fatal) for %s", path)
        return False
