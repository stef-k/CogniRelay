"""Manifest read/write/cleanup for segment-history crash recovery."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from app.storage import write_text_file

_log = logging.getLogger(__name__)

SEGMENT_HISTORY_DIR_REL = ".cognirelay/segment-history"


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

    Returns the path to the written manifest.
    """
    path = manifest_path(repo_root, family)
    payload = {
        "schema_type": "segment_history_manifest",
        "schema_version": "1.0",
        "operation": operation,
        "family": family,
        "source_paths": source_paths,
        "segment_ids": segment_ids,
        "target_paths": target_paths or [],
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
