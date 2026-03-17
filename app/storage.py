"""Low-level repository path and file helpers."""

from __future__ import annotations

import json
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
    """Write UTF-8 text content atomically using write-to-temp-then-rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def canonical_json(data: Any) -> str:
    """Serialize JSON deterministically for hashing and idempotency checks."""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def append_jsonl(path: Path, record: Any) -> None:
    """Append one JSON line record to a file, creating parents as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
