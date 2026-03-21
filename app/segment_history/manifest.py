"""Manifest read/write/cleanup for segment-history crash recovery."""

from __future__ import annotations

import errno
import fcntl
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from app.storage import write_text_file

_log = logging.getLogger(__name__)

_MANIFEST_LOCK_TIMEOUT: float = 10.0
_MANIFEST_LOCK_POLL: float = 0.05

SEGMENT_HISTORY_DIR_REL = ".cognirelay/segment-history"


class ManifestOccupied(Exception):
    """Raised when a manifest already exists for a family and is owned by another operation.

    This prevents concurrent operations on non-overlapping sources from
    clobbering each other's crash-recovery manifests.
    """

    code: str = "segment_history_manifest_occupied"

    def __init__(self, family: str, existing_op: str) -> None:
        self.family = family
        self.existing_op = existing_op
        super().__init__(f"Manifest slot for family '{family}' is occupied by a '{existing_op}' operation; back off and retry")


def _manifest_dir(repo_root: Path) -> Path:
    """Return the manifest directory, creating it with .gitignore if needed."""
    d = repo_root / SEGMENT_HISTORY_DIR_REL
    if not d.is_dir():
        d.mkdir(parents=True, exist_ok=True)
    # Ensure .cognirelay/ root is gitignored so no manifest or metadata leaks
    cognirelay_gitignore = repo_root / ".cognirelay" / ".gitignore"
    if not cognirelay_gitignore.exists():
        cognirelay_gitignore.write_text("*\n", encoding="utf-8")
    return d


def manifest_path(repo_root: Path, family: str = "") -> Path:
    """Return the path to the crash-recovery manifest file.

    When *family* is provided, uses ``<family>.manifest.json`` per spec.
    """
    filename = f"{family}.manifest.json" if family else "manifest.json"
    return _manifest_dir(repo_root) / filename


def _manifest_lock_path(repo_root: Path, family: str) -> Path:
    """Return the advisory lock file path for the manifest of a family."""
    d = _manifest_dir(repo_root)
    suffix = f"{family}.manifest" if family else "manifest"
    return d / f".{suffix}.lock"


def write_manifest(
    repo_root: Path,
    *,
    operation: str,
    family: str,
    source_paths: list[str],
    segment_ids: list[str],
    target_paths: list[str] | None = None,
    cleanup_paths: list[str] | None = None,
    started_at: str | None = None,
) -> Path:
    """Write a crash-recovery manifest before beginning mutations.

    The clobber check and the write are serialized by an exclusive
    per-family advisory file lock so that concurrent non-overlapping
    operations cannot both pass the check and clobber each other's
    manifest (TOCTOU prevention).

    If a manifest already exists for this family and its ``source_paths``
    do not overlap with the new operation's sources, raises
    :class:`ManifestOccupied` to prevent clobbering another operation's
    crash-recovery pointer.  When sources *do* overlap the caller is
    assumed to hold the relevant source locks and the manifest is
    overwritten (reconciliation already ran under those locks).

    Returns the path to the written manifest.
    """
    # Validate target_paths pairing before acquiring the lock — this is a
    # caller bug, not a concurrency concern, so fail fast.
    effective_targets = target_paths or []
    if len(effective_targets) % 2 != 0:
        raise ValueError(f"target_paths must have even length (payload/stub pairs), got {len(effective_targets)}")
    for i in range(0, len(effective_targets), 2):
        payload_entry = effective_targets[i]
        stub_entry = effective_targets[i + 1] if i + 1 < len(effective_targets) else ""
        if stub_entry and not stub_entry.endswith(".json"):
            raise ValueError(f"target_paths[{i + 1}] should be a stub (.json), got: {stub_entry}")
        if payload_entry and payload_entry.endswith(".json"):
            raise ValueError(f"target_paths[{i}] should be a payload (not .json), got: {payload_entry}")

    path = manifest_path(repo_root, family)
    lock_path = _manifest_lock_path(repo_root, family)

    # Serialize the clobber check and write with an exclusive file lock
    # to prevent the TOCTOU where two concurrent non-overlapping
    # operations both see no manifest and both write.
    lock_file = lock_path.open("a")
    try:
        deadline = time.monotonic() + _MANIFEST_LOCK_TIMEOUT
        while True:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    lock_file.close()
                    raise ManifestOccupied(family, "lock_timeout") from None
                time.sleep(_MANIFEST_LOCK_POLL)
            except OSError as exc:
                if exc.errno == errno.EINTR:
                    continue  # Retry on signal interruption
                lock_file.close()
                raise ManifestOccupied(family, "lock_error") from exc

        # Guard: refuse to clobber a manifest owned by a non-overlapping operation.
        if path.is_file():
            try:
                existing_text = path.read_text(encoding="utf-8")
                existing = json.loads(existing_text)
                existing_sources = set(existing.get("source_paths", []))
                new_sources = set(source_paths)
                if existing_sources and not existing_sources & new_sources:
                    existing_op = existing.get("operation", "unknown")
                    raise ManifestOccupied(family, existing_op)
                # Sources overlap — caller holds relevant locks and
                # reconciliation already ran.  Warn if non-overlapping
                # sources from the old manifest lose crash-recovery info.
                lost_sources = existing_sources - new_sources
                if lost_sources:
                    _log.warning(
                        "Manifest clobber for family '%s': overwriting manifest whose sources %s are not covered by new operation; crash-recovery for those sources depends on reconciliation",
                        family,
                        sorted(lost_sources),
                    )
            except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
                # Corrupt or unreadable manifest — safe to overwrite; the
                # reconciliation path would have cleaned it up anyway.
                _log.warning(
                    "Overwriting corrupt manifest for family '%s': %s",
                    family,
                    exc,
                )

        payload = {
            "schema_type": "segment_history_manifest",
            "schema_version": "1.0",
            "operation": operation,
            "family": family,
            "source_paths": source_paths,
            "segment_ids": segment_ids,
            "target_paths": effective_targets,
            "cleanup_paths": cleanup_paths or [],
            "started_at": started_at or datetime.now(timezone.utc).isoformat(),
        }
        # NOTE: write_text_file must write directly to `path` (or via a
        # sibling temp-rename that resolves atomically before this function
        # returns) so that the flock above protects the entire check-and-write
        # sequence against concurrent clobber.
        write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
        return path
    finally:
        lock_file.close()


def read_manifest(repo_root: Path, family: str = "") -> dict | None:
    """Read and parse an existing manifest, returning None if absent.

    Returns the parsed dict if readable, or None if the manifest does not
    exist.  Raises ValueError if the file exists but is corrupt.
    """
    path = manifest_path(repo_root, family)
    if not path.is_file():
        return None
    try:
        text = path.read_text(encoding="utf-8")
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("Manifest is not a JSON object")
        return data
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Corrupt manifest at {path}: {exc}") from exc


def remove_manifest(
    repo_root: Path,
    family: str = "",
    *,
    expected_operation: str | None = None,
) -> bool:
    """Remove the manifest file if it exists.

    Acquires the per-family advisory lock to prevent removing a manifest
    that a concurrent :func:`write_manifest` just created for a different
    operation.

    When *expected_operation* is provided, the manifest is only removed if
    its ``operation`` field matches.  This prevents one operation from
    accidentally removing another operation's crash-recovery pointer.

    Returns True if removed.
    """
    path = manifest_path(repo_root, family)
    lock_path = _manifest_lock_path(repo_root, family)
    try:
        lock_file = lock_path.open("a")
    except OSError as exc:
        _log.warning("Could not open manifest lock for removal %s: %s", path, exc)
        return False
    try:
        # Use non-blocking flock with timeout to prevent indefinite thread
        # stalls that could exhaust the ASGI thread pool on a 24/7 system.
        deadline = time.monotonic() + _MANIFEST_LOCK_TIMEOUT
        while True:
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    _log.warning(
                        "Manifest removal lock timeout for %s; manifest preserved for next reconciliation pass",
                        path,
                    )
                    lock_file.close()
                    return False
                time.sleep(_MANIFEST_LOCK_POLL)
            except OSError as exc:
                if exc.errno == errno.EINTR:
                    continue  # Retry on signal interruption
                _log.warning("Unexpected flock error during manifest removal: %s", exc)
                lock_file.close()
                return False

        if not path.is_file():
            return False
        if expected_operation is not None:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if data.get("operation") != expected_operation:
                    return False
            except (json.JSONDecodeError, UnicodeDecodeError, OSError):
                _log.debug("Corrupt manifest during removal check for %s; safe to remove", path)
        try:
            path.unlink()
            return True
        except OSError as exc:
            _log.warning("Could not remove manifest %s: %s", path, exc)
    finally:
        lock_file.close()
    return False
