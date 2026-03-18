"""Low-level repository path and file helpers."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any


ALLOWED_TOP_LEVEL = {
    "journal",
    "essays",
    "projects",
    "memory",
    "messages",
    "peers",
    "snapshots",
    "tasks",
    "patches",
    "runs",
    "index",
    "archive",
    "config",
    "logs",
    "backups",
}


class StorageError(ValueError):
    """Raised when a storage path or file operation violates repo constraints."""
    pass


def safe_path(repo_root: Path, relative_path: str) -> Path:
    """Resolve a repository-relative path while enforcing top-level guards."""
    if not relative_path or relative_path.startswith("/"):
        raise StorageError("Path must be a non-empty relative path")

    rel = Path(relative_path)
    if rel.parts and rel.parts[0] not in ALLOWED_TOP_LEVEL:
        raise StorageError(f"Top-level path not allowed: {rel.parts[0]}")

    resolved = (repo_root / rel).resolve()
    if repo_root not in resolved.parents and resolved != repo_root:
        raise StorageError("Path escapes repository root")
    return resolved


def read_text_file(path: Path) -> str:
    """Read a UTF-8 text file from disk."""
    return path.read_text(encoding="utf-8")


def write_text_file(path: Path, content: str) -> None:
    """Write UTF-8 text content atomically via write-to-temp-then-rename.

    Creates parent directories, writes to a temp file with fsync, then
    atomically renames. On failure the original file is untouched and
    the temp file is cleaned up. Re-raises the original exception.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    fd_owned = True
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            fd_owned = False
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        if fd_owned:
            try:
                os.close(fd)
            except OSError:
                logging.warning("Failed to close fd %d during cleanup", fd)
        try:
            os.unlink(tmp_path)
        except OSError:
            logging.warning("Failed to clean up temp file: %s", tmp_path)
        raise


def write_bytes_file(path: Path, data: bytes) -> None:
    """Write binary content atomically via write-to-temp-then-rename.

    Creates parent directories, writes to a temp file with fsync, then
    atomically renames. On failure the original file is untouched and
    the temp file is cleaned up. Re-raises the original exception.

    Currently used for restoring raw snapshots during rollback paths
    in continuity services.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    fd_owned = True
    try:
        with os.fdopen(fd, "wb") as f:
            fd_owned = False
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        if fd_owned:
            try:
                os.close(fd)
            except OSError:
                logging.warning("Failed to close fd %d during cleanup", fd)
        try:
            os.unlink(tmp_path)
        except OSError:
            logging.warning("Failed to clean up temp file: %s", tmp_path)
        raise


def canonical_json(data: Any) -> str:
    """Serialize JSON deterministically for hashing and idempotency checks."""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def append_jsonl(path: Path, record: Any) -> None:
    """Append one JSON line record to a file, creating parents as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
