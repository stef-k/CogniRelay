"""Shared coordination service helpers."""

from __future__ import annotations

import logging
import re
from app.timestamps import format_iso, iso_now
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.auth import AuthContext
from app.git_locking import repository_mutation_lock
from app.git_safety import unstage_paths
from app.storage import canonical_json, write_bytes_file, write_text_file

_log = logging.getLogger(__name__)

_UUID_HEX = re.compile(r"^[0-9a-f]{32}$")


def utc_now() -> str:
    """Return a normalized UTC timestamp for persisted coordination artifacts."""
    return format_iso(iso_now())


def is_admin(auth: AuthContext) -> bool:
    """Return whether the caller can bypass peer-scoped coordination visibility checks."""
    return "admin:peers" in getattr(auth, "scopes", set())


def query_identity_allowed(auth: AuthContext, peer_id: str | None) -> bool:
    """Return whether the caller may query for the provided coordination peer identity."""
    if peer_id is None:
        return True
    if is_admin(auth):
        return True
    return getattr(auth, "peer_id", "") == peer_id


def validate_prefixed_hex_id(value: str, *, prefix: str, detail: str) -> None:
    """Validate one coordination artifact id before probing the filesystem."""
    if not value.startswith(prefix):
        raise HTTPException(status_code=400, detail=detail)
    suffix = value[len(prefix) :]
    if not _UUID_HEX.fullmatch(suffix):
        raise HTTPException(status_code=400, detail=detail)


def persist_new_artifact(
    *,
    path: Path,
    rel: str,
    gm: Any,
    artifact: dict[str, Any],
    commit_message: str,
    error_detail: str,
) -> str:
    """Persist a newly created coordination artifact and delete it on commit failure.

    The write and commit happen inside repository_mutation_lock so that
    concurrent readers never observe an uncommitted file and a lock-timeout
    cannot leave an orphaned write on disk.
    """
    with repository_mutation_lock(gm.repo_root):
        try:
            write_text_file(path, canonical_json(artifact))
        except Exception as exc:
            _log.error(
                "Coordination artifact write failed for %s: %s",
                path, exc, exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail=f"{error_detail}: write failed: {exc}",
            ) from exc
        try:
            committed = gm.commit_file(path, commit_message)
            if not committed:
                raise RuntimeError("git commit produced no changes")
        except Exception as exc:
            _log.error(
                "Coordination artifact persist failed for %s: %s",
                path, exc, exc_info=True,
            )
            errors: list[str] = []
            try:
                unstage_paths(gm, [path])
            except Exception as unstage_exc:
                errors.append(f"unstage failed: {unstage_exc}")
                _log.exception("Unstage failed for %s", path)
            try:
                path.unlink(missing_ok=True)
            except Exception as restore_exc:
                errors.append(f"rollback failed: {restore_exc}")
                _log.exception("Rollback failed for %s", path)
            detail = f"{error_detail}: {exc}"
            if errors:
                detail = f"{detail}; {'; '.join(errors)}"
            raise HTTPException(status_code=500, detail=detail) from exc
    return rel


def persist_updated_artifact(
    *,
    path: Path,
    rel: str,
    gm: Any,
    artifact: dict[str, Any],
    commit_message: str,
    error_detail: str,
) -> str:
    """Persist an updated coordination artifact and restore prior bytes on failure.

    The old-bytes snapshot is captured before the lock (read-only; callers
    hold artifact_lock which prevents concurrent writes).  The write and
    commit happen inside repository_mutation_lock so that concurrent readers
    never observe an uncommitted update and a lock-timeout cannot leave the
    file in a dirty state.
    """
    old_bytes = path.read_bytes() if path.exists() else None
    with repository_mutation_lock(gm.repo_root):
        try:
            write_text_file(path, canonical_json(artifact))
        except Exception as exc:
            _log.error(
                "Coordination artifact write failed for %s: %s",
                path, exc, exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail=f"{error_detail}: write failed: {exc}",
            ) from exc
        try:
            committed = gm.commit_file(path, commit_message)
            if not committed:
                raise RuntimeError("git commit produced no changes")
        except Exception as exc:
            _log.error(
                "Coordination artifact update persist failed for %s: %s",
                path, exc, exc_info=True,
            )
            errors: list[str] = []
            try:
                unstage_paths(gm, [path])
            except Exception as unstage_exc:
                errors.append(f"unstage failed: {unstage_exc}")
                _log.exception("Unstage failed for %s", path)
            try:
                if old_bytes is None:
                    path.unlink(missing_ok=True)
                else:
                    write_bytes_file(path, old_bytes)
            except Exception as restore_exc:
                errors.append(f"rollback failed: {restore_exc}")
                _log.exception("Rollback failed for %s", path)
            detail = f"{error_detail}: {exc}"
            if errors:
                detail = f"{detail}; {'; '.join(errors)}"
            raise HTTPException(status_code=500, detail=detail) from exc
    return rel
