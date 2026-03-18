"""Safe git-commit wrappers with rollback on failure.

These helpers ensure that a failed ``git commit`` never leaves the repository
in a state where on-disk content diverges from the committed history without
the caller knowing.  Each variant captures enough prior state to restore the
file system **and** the git index, then surfaces a descriptive
``HTTPException`` to the agent.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Protocol, runtime_checkable

from fastapi import HTTPException

_log = logging.getLogger(__name__)


@runtime_checkable
class GitCommitter(Protocol):
    """Structural type for the subset of GitManager used by commit-safe helpers."""

    repo_root: Path

    def commit_file(self, path: Path, message: str) -> bool: ...

    def commit_paths(self, paths: list[Path], message: str) -> bool: ...


def _unstage(gm: GitCommitter, paths: list[Path]) -> None:
    """Best-effort unstage paths from the git index after a failed commit."""
    try:
        rels = [str(p.relative_to(gm.repo_root)) for p in paths]
        subprocess.run(
            ["git", "reset", "HEAD", "--", *rels],
            cwd=gm.repo_root,
            check=False,
            text=True,
            capture_output=True,
        )
    except Exception:
        _log.exception("Failed to unstage paths after rollback")


def _restore_files(rollback_plan: list[tuple[Path, bytes | None]]) -> None:
    """Restore files according to the rollback plan, logging any failures."""
    for path, old_bytes in rollback_plan:
        try:
            if old_bytes is None:
                if path.exists():
                    path.unlink()
            else:
                path.write_bytes(old_bytes)
        except Exception:
            _log.exception("Rollback (restore) failed for %s", path)


def safe_commit_new_file(
    *,
    path: Path,
    gm: GitCommitter,
    commit_message: str,
    error_detail: str,
) -> bool:
    """Commit a newly written file, deleting it on commit failure.

    Returns the boolean produced by ``gm.commit_file`` on success.
    Raises :class:`~fastapi.HTTPException` with *error_detail* on failure
    after removing the orphaned file from disk and unstaging from the index.
    """
    try:
        return gm.commit_file(path, commit_message)
    except Exception as exc:
        _unstage(gm, [path])
        _restore_files([(path, None)])
        raise HTTPException(status_code=500, detail=error_detail) from exc


def safe_commit_updated_file(
    *,
    path: Path,
    gm: GitCommitter,
    commit_message: str,
    error_detail: str,
    old_bytes: bytes | None,
) -> bool:
    """Commit an updated (or deleted) file, restoring prior content on failure.

    *old_bytes* should be the content of *path* **before** the caller's
    mutation.  Pass ``None`` if the file did not exist previously — the
    rollback will then delete the file instead.

    Returns the boolean produced by ``gm.commit_file`` on success.
    Raises :class:`~fastapi.HTTPException` with *error_detail* on failure
    after restoring the file and unstaging from the index.
    """
    try:
        return gm.commit_file(path, commit_message)
    except Exception as exc:
        _unstage(gm, [path])
        _restore_files([(path, old_bytes)])
        raise HTTPException(status_code=500, detail=error_detail) from exc


def safe_commit_paths(
    *,
    rollback_plan: list[tuple[Path, bytes | None]],
    gm: GitCommitter,
    commit_message: str,
    error_detail: str,
) -> bool:
    """Commit multiple paths atomically, rolling back all on failure.

    *rollback_plan* is a list of ``(path, old_bytes)`` pairs.  On failure
    every path is unstaged from the index and restored to its prior content.

    Returns the boolean produced by ``gm.commit_paths`` on success.
    Raises :class:`~fastapi.HTTPException` with *error_detail* on failure.
    """
    paths = [path for path, _ in rollback_plan]
    try:
        return gm.commit_paths(paths, commit_message)
    except Exception as exc:
        _unstage(gm, paths)
        _restore_files(rollback_plan)
        raise HTTPException(status_code=500, detail=error_detail) from exc


def try_commit_file(
    *,
    path: Path,
    gm: GitCommitter,
    commit_message: str,
) -> bool:
    """Attempt to commit a file, logging and suppressing git failures.

    Use this for artifacts that are regenerable and whose commit failure
    should not abort the request (e.g. derived index files, nonce indexes).
    """
    try:
        return gm.commit_file(path, commit_message)
    except Exception:
        _log.exception("Git commit failed (non-fatal) for %s", path)
        return False
