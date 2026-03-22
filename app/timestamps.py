"""Canonical datetime parsing, formatting, and validation for CogniRelay.

All modules should import from here instead of defining private helpers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp into a timezone-aware UTC datetime.

    - Accepts both ``Z`` and ``±offset`` suffixes.
    - Naive timestamps (no offset) are assumed UTC.
    - Returns ``None`` for ``None``, empty, or malformed input.
    """
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def require_utc_iso(value: str | None, field_name: str) -> datetime:
    """Parse and validate an ISO timestamp as explicit UTC.

    Raises ``ValueError`` if the value is missing, malformed, or lacks an
    explicit UTC suffix (``Z`` or ``+00:00``).  Used for mission-critical
    paths where naive timestamps must be rejected, not silently assumed.
    """
    if not value:
        raise ValueError(f"{field_name}: missing required UTC timestamp")
    raw = str(value).strip()
    if not (raw.endswith("Z") or raw.endswith("+00:00")):
        raise ValueError(f"{field_name}: timestamp must be explicit UTC: {value!r}")
    dt = parse_iso(raw)
    if dt is None:
        raise ValueError(f"{field_name}: malformed ISO timestamp: {value!r}")
    return dt


def iso_now() -> datetime:
    """Return the current UTC time with microseconds stripped."""
    return datetime.now(timezone.utc).replace(microsecond=0)


def format_iso(dt: datetime) -> str:
    """Format a datetime as ISO 8601 with ``Z`` suffix.

    Converts to UTC, strips microseconds, replaces ``+00:00`` with ``Z``.
    This is the canonical serialization format for all CogniRelay stored
    timestamps.
    """
    utc = dt.astimezone(timezone.utc).replace(microsecond=0)
    return utc.isoformat().replace("+00:00", "Z")


def format_compact(dt: datetime) -> str:
    """Format a datetime as compact ``YYYYMMDDTHHMMSSZ`` for IDs.

    Converts to UTC and strips microseconds.
    """
    return dt.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def is_iso_timestamp(value: Any) -> bool:
    """Return whether a value is a parseable ISO 8601 timestamp."""
    if not isinstance(value, str) or not value.strip():
        return False
    return parse_iso(value) is not None


def iso_to_posix(value: str | None) -> float:
    """Parse an ISO timestamp to a POSIX float for sorting.

    Returns ``0.0`` for ``None`` or malformed values so that unparseable
    timestamps sort last.
    """
    dt = parse_iso(value)
    if dt is None:
        return 0.0
    return dt.timestamp()
