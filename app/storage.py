"""Low-level repository path and file helpers."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _try_fsync_directory(dir_path: Path) -> None:
    """Best-effort directory fsync after a successful rename.

    Logs a warning on failure rather than raising, because the atomic
    rename has already succeeded at the call site.
    """
    try:
        _fsync_directory(dir_path)
    except OSError:
        logging.warning(
            "Directory fsync failed for %s — file is written but directory "
            "entry may not be durable until the kernel flushes it to disk",
            dir_path,
            exc_info=True,
        )


def _fsync_directory(dir_path: Path) -> None:
    """Fsync a directory to make its entries durable after a rename.

    Required for rename durability on ext4; the risk window is larger on
    ``data=writeback`` mounts. On Windows this is a no-op because the
    Windows API does not support opening a directory as a file descriptor.
    """
    if os.name == "nt":
        return
    fd = os.open(str(dir_path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


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
    atomically renames and fsyncs the parent directory for rename
    durability. On failure the original file is untouched and the temp
    file is cleaned up. Re-raises the original exception.

    Directory fsync failure is logged as a warning rather than raised
    because the atomic rename has already succeeded at that point.
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
    _try_fsync_directory(path.parent)


def write_bytes_file(path: Path, data: bytes) -> None:
    """Write binary content atomically via write-to-temp-then-rename.

    Creates parent directories, writes to a temp file with fsync, then
    atomically renames and fsyncs the parent directory for rename
    durability. On failure the original file is untouched and the temp
    file is cleaned up. Re-raises the original exception.

    Directory fsync failure is logged as a warning rather than raised
    because the atomic rename has already succeeded at that point.

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
    _try_fsync_directory(path.parent)


def canonical_json(data: Any) -> str:
    """Serialize JSON deterministically for hashing and idempotency checks."""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def append_jsonl(path: Path, record: Any) -> None:
    """Append one JSON line record to a file, creating parents as needed.

    ``record`` must be JSON-serializable. Calls fsync after writing for
    durability. On a crash, consumers should tolerate a truncated trailing
    line as the accepted failure mode. Raises ``OSError`` on I/O failure
    (the record may have been partially written but is not guaranteed durable).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False) + "\n"
    with path.open("a", encoding="utf-8") as f:
        try:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
        except OSError:
            logging.error("append_jsonl I/O failed for %s — record may not be durable", path, exc_info=True)
            raise


@dataclass(frozen=True, slots=True)
class _AppendTarget:
    """Tracks a single file's state for multi-file append rollback."""

    path: Path
    prior_size: int
    is_new: bool


def _rollback_appends(targets: list[_AppendTarget]) -> None:
    """Best-effort rollback of already-appended files after a mid-loop failure.

    Truncates each file back to its prior size, or deletes it if it was newly
    created. Logs errors on failure rather than raising so the original
    exception propagates cleanly.
    """
    for t in targets:
        try:
            if t.is_new:
                t.path.unlink(missing_ok=True)
            else:
                with t.path.open("r+b") as f:
                    f.truncate(t.prior_size)
                    f.flush()
                    os.fsync(f.fileno())
        except OSError:
            logging.error(
                "rollback failed for %s — file may contain a partial append",
                t.path,
                exc_info=True,
            )


def append_jsonl_multi(paths: list[Path], record: dict[str, Any]) -> None:
    """Append one JSON line record to multiple files atomically.

    Serializes the record once, then appends to each unique path in
    sequence with fsync.  On ``OSError`` at file N, files 0..N-1 are
    truncated back to their prior size (or deleted if newly created),
    and the original exception is re-raised.

    Duplicate paths (after resolution) are collapsed so the record is
    appended only once per physical file.

    Raises ``TypeError`` if the record is not JSON-serializable (before
    any I/O occurs).
    """
    if not paths:
        return

    # Serialize once upfront — catches TypeError before any I/O.
    line = json.dumps(record, ensure_ascii=False) + "\n"

    # Deduplicate paths so a self-send (sender == recipient) doesn't
    # append twice to the same physical file.
    seen: set[Path] = set()
    unique_paths: list[Path] = []
    for p in paths:
        resolved = p.resolve()
        if resolved not in seen:
            seen.add(resolved)
            unique_paths.append(p)

    # Append to each file in sequence.  Size capture, mkdir, and
    # append are merged per-file to minimise the TOCTOU window.
    targets: list[_AppendTarget] = []
    for i, p in enumerate(unique_paths):
        prior_size = -1
        is_new = False
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            is_new = not p.exists()
            with p.open("a", encoding="utf-8") as f:
                prior_size = f.tell()
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            targets.append(_AppendTarget(path=p, prior_size=prior_size, is_new=is_new))
        except OSError:
            # Include the current file in rollback if we captured its
            # prior size (meaning the file was opened successfully).
            if prior_size >= 0:
                targets.append(_AppendTarget(path=p, prior_size=prior_size, is_new=is_new))
            logging.error(
                "append_jsonl_multi I/O failed at file %d/%d (%s) — rolling back",
                i + 1,
                len(unique_paths),
                p,
                exc_info=True,
            )
            _rollback_appends(targets)
            raise

    # Fsync parent directories of newly created files for entry
    # durability (consistent with write_text_file / write_bytes_file).
    fsynced_dirs: set[Path] = set()
    for t in targets:
        if t.is_new and t.path.parent not in fsynced_dirs:
            _try_fsync_directory(t.path.parent)
            fsynced_dirs.add(t.path.parent)
