"""Pure content-analysis utilities for segment-history families.

These helpers operate on in-memory content strings and have no dependencies
on the segment-history service layer, locking, or manifest infrastructure.
They are extracted here to break the circular import between ``families.py``
and ``service.py`` — both can safely import from this module at top level.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JSONL summary helpers
# ---------------------------------------------------------------------------
def count_lines(content: str) -> int:
    """Count newline-terminated lines in content.

    Per spec, ``line_count`` means count of lines ending with ``\\n``,
    regardless of JSON parseability.
    """
    if not content:
        return 0
    return content.count("\n")


def byte_size(content: str | bytes) -> int:
    """Return byte size of content."""
    if isinstance(content, str):
        return len(content.encode("utf-8"))
    return len(content)


def first_nonempty_line_preview(content: str, max_len: int = 200) -> str:
    """Return the first non-empty line truncated to *max_len*, or empty string."""
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped:
            return stripped[:max_len]
    return ""


def sample_json_field(content: str, field: str, limit: int) -> list[str]:
    """Extract up to *limit* unique values for a JSON field from JSONL content."""
    seen: set[str] = set()
    result: list[str] = []
    for line in content.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
            val = row.get(field)
            if val is not None:
                s = str(val)
                if s not in seen:
                    seen.add(s)
                    result.append(s)
                    if len(result) >= limit:
                        break
        except (json.JSONDecodeError, AttributeError):
            continue
    return result


def first_last_json_field(content: str, field: str) -> tuple[str | None, str | None]:
    """Return the first and last values of a JSON field in JSONL content."""
    first: str | None = None
    last: str | None = None
    for line in content.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
            val = row.get(field)
            if val is not None:
                s = str(val)
                if first is None:
                    first = s
                last = s
        except (json.JSONDecodeError, AttributeError):
            continue
    return first, last


def json_field_counts(content: str, field: str, limit: int) -> dict[str, int]:
    """Count occurrences of each value for a JSON field, returning top *limit*."""
    counts: dict[str, int] = {}
    for line in content.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
            val = row.get(field)
            if val is not None:
                s = str(val)
                counts[s] = counts.get(s, 0) + 1
        except (json.JSONDecodeError, AttributeError):
            continue
    # Sort by count descending, take top limit
    sorted_items = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:limit]
    return dict(sorted_items)


# ---------------------------------------------------------------------------
# Shared timestamp parsing (M5 — deduplicated from 4 locations)
# ---------------------------------------------------------------------------
def parse_event_timestamp(ts_str: str) -> datetime:
    """Parse an event timestamp in any of the accepted formats.

    Accepted formats:
    - Compact 16-char: ``20260320T120000Z``
    - Date-only 10-char: ``2026-03-20``
    - ISO 8601 with or without trailing ``Z``: ``2026-03-20T12:00:00+00:00``

    Raises ``ValueError`` on unparseable input.
    """
    if "T" in ts_str and ts_str.endswith("Z") and len(ts_str) == 16:
        return datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    if len(ts_str) == 10:
        return datetime.strptime(ts_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# Stream key derivation (moved from service.py to break circular imports)
# ---------------------------------------------------------------------------

# Family-specific source-path prefix stripping for stream key derivation
_FAMILY_PREFIX_STRIP: dict[str, str] = {
    "journal": "journal/",
    "api_audit": "logs/",
    "ops_runs": "logs/",
    "message_stream": "messages/",
    "message_thread": "messages/threads/",
    "episodic": "memory/episodic/",
}


def _derive_stream_key(family: str, source_path: str) -> str:
    """Derive a per-family stream key from a source path.

    Algorithm:
    - Strip the family-specific prefix from the source path
    - Replace remaining ``/`` with ``__``
    - Remove the file extension

    Examples::

        journal/2026/2026-03-19.md       -> 2026__2026-03-19
        logs/api_audit.jsonl             -> api_audit
        messages/inbox/alice.jsonl       -> inbox__alice
        messages/threads/t1.jsonl        -> t1
        memory/episodic/observations.jsonl -> observations
    """
    prefix = _FAMILY_PREFIX_STRIP.get(family, "")
    key = source_path
    if prefix and key.startswith(prefix):
        key = key[len(prefix) :]
    # Remove file extension
    dot = key.rfind(".")
    if dot > 0:
        key = key[:dot]
    # Validate: if the original filename contains __ the stream key is
    # ambiguous (cannot distinguish path separators from literal __).
    basename = source_path.rsplit("/", 1)[-1] if "/" in source_path else source_path
    name_dot = basename.rfind(".")
    name_part = basename[:name_dot] if name_dot > 0 else basename
    if "__" in name_part:
        _log.warning(
            "Source filename contains '__' which collides with path separator "
            "in stream key derivation: %s (family=%s)",
            source_path, family,
        )
    # Replace / with __
    key = key.replace("/", "__")
    if not key:
        _log.warning("Empty stream key derived from path: %s (family=%s)", source_path, family)
    return key


# ---------------------------------------------------------------------------
# Structured warning helper — re-export from the shared lifecycle module.
# lifecycle_warnings is a leaf module (imports only fastapi + stdlib) so
# this import is always safe from circular-import issues.
# ---------------------------------------------------------------------------
from app.lifecycle_warnings import make_warning as _make_warning  # noqa: E402, F401
