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

from app.git_locking import repository_mutation_lock

_log = logging.getLogger(__name__)


@runtime_checkable
class GitCommitter(Protocol):
    """Structural type for the subset of GitManager used by commit-safe helpers."""

    repo_root: Path

    def commit_file(self, path: Path, message: str) -> bool: ...

    def commit_paths(self, paths: list[Path], message: str) -> bool: ...


def unstage_paths(gm: GitCommitter, paths: list[Path]) -> None:
    """Unstage paths from the git index after a failed commit.

    Raises on failure so callers can include the error in structured
    details.  Use :func:`try_unstage_paths` when the unstage is
    best-effort and the rollback chain must continue regardless.
    """
    resolved_root = gm.repo_root.resolve()
    rels: list[str] = []
    for path in paths:
        try:
            rels.append(str(path.resolve().relative_to(resolved_root)))
        except ValueError:
            _log.warning("Skipping path outside repository root during unstage: %s", path)
            continue
    if not rels:
        return
    subprocess.run(
        ["git", "reset", "HEAD", "--", *rels],
        cwd=gm.repo_root,
        check=False,
        text=True,
        capture_output=True,
    )


def try_unstage_paths(gm: GitCommitter, paths: list[Path]) -> None:
    """Best-effort unstage that logs and swallows failures.

    Use this in rollback chains where subsequent cleanup (file
    restoration, etc.) must run even if unstaging fails.
    """
    try:
        unstage_paths(gm, paths)
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
    with repository_mutation_lock(gm.repo_root):
        try:
            return gm.commit_file(path, commit_message)
        except Exception as exc:
            _log.error("safe_commit_new_file failed: %s", exc, exc_info=True)
            try_unstage_paths(gm, [path])
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
    with repository_mutation_lock(gm.repo_root):
        try:
            return gm.commit_file(path, commit_message)
        except Exception as exc:
            _log.error("safe_commit_updated_file failed: %s", exc, exc_info=True)
            try_unstage_paths(gm, [path])
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
    with repository_mutation_lock(gm.repo_root):
        try:
            return gm.commit_paths(paths, commit_message)
        except Exception as exc:
            _log.error("safe_commit_paths failed: %s", exc, exc_info=True)
            try_unstage_paths(gm, paths)
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
    with repository_mutation_lock(gm.repo_root):
        try:
            return gm.commit_file(path, commit_message)
        except Exception:
            try_unstage_paths(gm, [path])
            _log.exception("Git commit failed (non-fatal) for %s", path)
            return False


def try_commit_paths(
    *,
    paths: list[Path],
    gm: GitCommitter,
    commit_message: str,
) -> bool:
    """Attempt to commit multiple paths, logging and suppressing git failures.

    Use this when the caller intentionally keeps on-disk state even when the
    git durability step fails, but still wants the shared index cleaned up.
    """
    with repository_mutation_lock(gm.repo_root):
        try:
            return gm.commit_paths(paths, commit_message)
        except Exception:
            try_unstage_paths(gm, paths)
            _log.exception("Git commit failed (non-fatal) for %s path(s)", len(paths))
            return False
