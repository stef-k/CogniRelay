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
from app.storage import canonical_json, write_text_file

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
    """Persist a newly created coordination artifact and delete it on commit failure."""
    write_text_file(path, canonical_json(artifact))
    with repository_mutation_lock(gm.repo_root):
        try:
            committed = gm.commit_file(path, commit_message)
            if not committed:
                raise RuntimeError("git commit produced no changes")
        except Exception as exc:
            unstage_paths(gm, [path])
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                _log.exception("Rollback failed for %s", path)
            raise HTTPException(status_code=500, detail=error_detail) from exc
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
    """Persist an updated coordination artifact and restore prior bytes on failure."""
    old_bytes = path.read_bytes() if path.exists() else None
    write_text_file(path, canonical_json(artifact))
    with repository_mutation_lock(gm.repo_root):
        try:
            committed = gm.commit_file(path, commit_message)
            if not committed:
                raise RuntimeError("git commit produced no changes")
        except Exception as exc:
            unstage_paths(gm, [path])
            try:
                if old_bytes is None:
                    if path.exists():
                        path.unlink()
                else:
                    path.write_bytes(old_bytes)
            except Exception:
                _log.exception("Rollback failed for %s", path)
            raise HTTPException(status_code=500, detail=error_detail) from exc
    return rel
