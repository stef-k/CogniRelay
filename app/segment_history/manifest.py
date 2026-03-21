"""Manifest read/write/cleanup for segment-history crash recovery."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from app.storage import write_text_file

_log = logging.getLogger(__name__)

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
        super().__init__(
            f"Manifest slot for family '{family}' is occupied by a "
            f"'{existing_op}' operation; back off and retry"
        )


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


def write_manifest(
    repo_root: Path,
    *,
    operation: str,
    family: str,
    source_paths: list[str],
    segment_ids: list[str],
    target_paths: list[str] | None = None,
    started_at: str | None = None,
) -> Path:
    """Write a crash-recovery manifest before beginning mutations.

    If a manifest already exists for this family and its ``source_paths``
    do not overlap with the new operation's sources, raises
    :class:`ManifestOccupied` to prevent clobbering another operation's
    crash-recovery pointer.  When sources *do* overlap the caller is
    assumed to hold the relevant source locks and the manifest is
    overwritten (reconciliation already ran under those locks).

    Returns the path to the written manifest.
    """
    path = manifest_path(repo_root, family)

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
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            # Corrupt or unreadable manifest — safe to overwrite; the
            # reconciliation path would have cleaned it up anyway.
            pass

    # Validate target_paths pairing: must be even-length with alternating
    # [payload, stub, payload, stub, ...] entries.  The reconciliation
    # logic relies on this convention to classify orphans vs. committed
    # pairs — a violated pairing could cause silent data loss.
    effective_targets = target_paths or []
    if len(effective_targets) % 2 != 0:
        raise ValueError(
            f"target_paths must have even length (payload/stub pairs), "
            f"got {len(effective_targets)}"
        )
    for i in range(0, len(effective_targets), 2):
        payload_entry = effective_targets[i]
        stub_entry = effective_targets[i + 1] if i + 1 < len(effective_targets) else ""
        if stub_entry and not stub_entry.endswith(".json"):
            raise ValueError(
                f"target_paths[{i + 1}] should be a stub (.json), "
                f"got: {stub_entry}"
            )
        if payload_entry and payload_entry.endswith(".json"):
            raise ValueError(
                f"target_paths[{i}] should be a payload (not .json), "
                f"got: {payload_entry}"
            )

    payload = {
        "schema_type": "segment_history_manifest",
        "schema_version": "1.0",
        "operation": operation,
        "family": family,
        "source_paths": source_paths,
        "segment_ids": segment_ids,
        "target_paths": effective_targets,
        "started_at": started_at or datetime.now(timezone.utc).isoformat(),
    }
    write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
    return path


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


def remove_manifest(repo_root: Path, family: str = "") -> bool:
    """Remove the manifest file if it exists.  Returns True if removed."""
    path = manifest_path(repo_root, family)
    if path.is_file():
        try:
            path.unlink()
            return True
        except OSError as exc:
            _log.warning("Could not remove manifest %s: %s", path, exc)
    return False
