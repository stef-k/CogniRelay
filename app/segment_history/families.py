"""Family-specific configuration and logic for segment-history lifecycle.

Each family defines its rollover triggers, summary builders, cold-eligibility
checks, and active-source discovery rules.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from app.segment_history.service import (
    _byte_size,
    _count_lines,
    _first_last_json_field,
    _first_nonempty_line_preview,
    _json_field_counts,
    _sample_json_field,
)

# ---------------------------------------------------------------------------
# Family definition
# ---------------------------------------------------------------------------
_DAY_BUCKET_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\.md$")


@dataclass(frozen=True)
class FamilyConfig:
    """Configuration for one segment-history family."""

    name: str
    source_dirs: list[str]
    history_dir: str
    stub_dir: str

    # Rollover triggers — a family uses size, day-boundary, or special rules.
    has_size_rollover: bool = True
    has_day_boundary_rollover: bool = True

    # Summary builder — receives content string, returns summary dict
    build_summary: Callable[[str], dict[str, Any]] = field(default=lambda: _default_summary)

    # Cold eligibility field — the stub summary field checked against cold_after_days
    cold_eligibility_field: str = "last_event_at"


def _default_summary(content: str) -> dict[str, Any]:
    """Fallback summary for families that don't override."""
    return {
        "line_count": _count_lines(content),
        "byte_size": _byte_size(content),
    }


# ---------------------------------------------------------------------------
# Per-family summary builders
# ---------------------------------------------------------------------------
def _journal_summary(content: str) -> dict[str, Any]:
    """Summary for journal day-bucket segments."""
    return {
        "line_count": _count_lines(content),
        "byte_size": _byte_size(content),
        "first_nonempty_line_preview": _first_nonempty_line_preview(content),
    }


def _api_audit_summary(content: str) -> dict[str, Any]:
    """Summary for API audit log segments."""
    first, last = _first_last_json_field(content, "ts")
    return {
        "first_event_at": first,
        "last_event_at": last,
        "line_count": _count_lines(content),
        "byte_size": _byte_size(content),
        "event_name_sample": _sample_json_field(content, "event", 5),
    }


def _ops_runs_summary(content: str) -> dict[str, Any]:
    """Summary for ops run log segments."""
    first_started, _ = _first_last_json_field(content, "ts")
    _, last_finished = _first_last_json_field(content, "finished_at")
    return {
        "first_started_at": first_started,
        "last_finished_at": last_finished,
        "line_count": _count_lines(content),
        "byte_size": _byte_size(content),
        "job_id_counts": _json_field_counts(content, "job_id", 10),
    }


def _message_stream_summary(content: str) -> dict[str, Any]:
    """Summary for message stream segments (inbox/outbox/relay/acks)."""
    # Event timestamp: sent_at for inbox/outbox/relay, ack_at for acks.
    # Use sent_at as primary; fall back to ack_at for ack-only files.
    first, last = _first_last_json_field(content, "sent_at")
    if first is None:
        first, last = _first_last_json_field(content, "ack_at")
    return {
        "first_event_at": first,
        "last_event_at": last,
        "line_count": _count_lines(content),
        "byte_size": _byte_size(content),
        "message_id_sample": _sample_json_field(content, "id", 5),
        "thread_id_sample": _sample_json_field(content, "thread_id", 5),
    }


def _message_thread_summary(content: str) -> dict[str, Any]:
    """Summary for message thread segments."""
    first, last = _first_last_json_field(content, "sent_at")
    participants = _sample_json_field(content, "from", 10)
    to_vals = _sample_json_field(content, "to", 10)
    seen = set(participants)
    for v in to_vals:
        if v not in seen:
            seen.add(v)
            participants.append(v)
            if len(participants) >= 10:
                break
    return {
        "first_event_at": first,
        "last_event_at": last,
        "line_count": _count_lines(content),
        "byte_size": _byte_size(content),
        "participant_sample": participants,
    }


def _episodic_summary(content: str) -> dict[str, Any]:
    """Summary for episodic memory segments."""
    first, last = _first_last_json_field(content, "at")
    return {
        "first_event_at": first,
        "last_event_at": last,
        "line_count": _count_lines(content),
        "byte_size": _byte_size(content),
        "subject_kind_counts": _json_field_counts(content, "subject_kind", 10),
    }


# ---------------------------------------------------------------------------
# Family registry
# ---------------------------------------------------------------------------
FAMILIES: dict[str, FamilyConfig] = {
    "journal": FamilyConfig(
        name="journal",
        source_dirs=["journal"],
        history_dir="journal/history",
        stub_dir="journal/history/index",
        has_size_rollover=False,
        has_day_boundary_rollover=True,
        build_summary=_journal_summary,
        cold_eligibility_field="day",
    ),
    "api_audit": FamilyConfig(
        name="api_audit",
        source_dirs=["logs"],
        history_dir="logs/history/api_audit",
        stub_dir="logs/history/api_audit/index",
        has_size_rollover=True,
        has_day_boundary_rollover=True,
        build_summary=_api_audit_summary,
        cold_eligibility_field="last_event_at",
    ),
    "ops_runs": FamilyConfig(
        name="ops_runs",
        source_dirs=["logs"],
        history_dir="logs/history/ops_runs",
        stub_dir="logs/history/ops_runs/index",
        has_size_rollover=True,
        has_day_boundary_rollover=True,
        build_summary=_ops_runs_summary,
        cold_eligibility_field="last_finished_at",
    ),
    "message_stream": FamilyConfig(
        name="message_stream",
        source_dirs=[
            "messages/inbox",
            "messages/outbox",
            "messages/acks",
            "messages/relay",
        ],
        history_dir="messages/history/stream",
        stub_dir="messages/history/stream/index",
        has_size_rollover=True,
        has_day_boundary_rollover=False,
        build_summary=_message_stream_summary,
        cold_eligibility_field="last_event_at",
    ),
    "message_thread": FamilyConfig(
        name="message_thread",
        source_dirs=["messages/threads"],
        history_dir="messages/history/threads",
        stub_dir="messages/history/threads/index",
        has_size_rollover=True,
        has_day_boundary_rollover=False,
        build_summary=_message_thread_summary,
        cold_eligibility_field="last_event_at",
    ),
    "episodic": FamilyConfig(
        name="episodic",
        source_dirs=["memory/episodic"],
        history_dir="memory/episodic/history",
        stub_dir="memory/episodic/history/index",
        has_size_rollover=True,
        has_day_boundary_rollover=True,
        build_summary=_episodic_summary,
        cold_eligibility_field="last_event_at",
    ),
}


# ---------------------------------------------------------------------------
# Active source discovery
# ---------------------------------------------------------------------------
def discover_active_sources(
    family: str, repo_root: Path
) -> list[Path]:
    """Discover active source files for a family, excluding history dirs.

    Returns sorted list of existing source files.
    """
    config = FAMILIES[family]
    sources: list[Path] = []

    # Journal uses .md files in journal/<year>/ subdirectories
    if family == "journal":
        journal_dir = repo_root / "journal"
        if journal_dir.is_dir():
            for year_dir in sorted(journal_dir.iterdir()):
                if not year_dir.is_dir():
                    continue
                if year_dir.name == "history":
                    continue
                for entry in sorted(year_dir.iterdir()):
                    if entry.is_dir():
                        continue
                    if not entry.name.endswith(".md"):
                        continue
                    sources.append(entry)
        return sorted(sources)

    for src_dir_rel in config.source_dirs:
        src_dir = repo_root / src_dir_rel
        if not src_dir.is_dir():
            continue
        for entry in sorted(src_dir.iterdir()):
            # Skip history subdirectories
            if entry.is_dir():
                continue
            # Skip non-JSONL files
            if not entry.name.endswith(".jsonl"):
                continue
            # Exclude files inside history paths
            try:
                rel = str(entry.relative_to(repo_root))
            except ValueError:
                continue
            if "/history/" in rel:
                continue
            sources.append(entry)

    return sorted(sources)


# ---------------------------------------------------------------------------
# Rollover eligibility checks
# ---------------------------------------------------------------------------
def _get_rollover_bytes_setting(family: str, settings: Any) -> int | None:
    """Return the rollover byte threshold for a family, or None if no size rollover."""
    mapping = {
        "api_audit": "audit_log_rollover_bytes",
        "ops_runs": "ops_run_rollover_bytes",
        "message_stream": "message_stream_rollover_bytes",
        "message_thread": "message_thread_rollover_bytes",
        "episodic": "episodic_rollover_bytes",
    }
    attr = mapping.get(family)
    if attr is None:
        return None
    return getattr(settings, attr, None)


def _get_cold_after_days_setting(family: str, settings: Any) -> int:
    """Return the cold-after-days setting for a family."""
    mapping = {
        "journal": "journal_cold_after_days",
        "api_audit": "audit_log_cold_after_days",
        "ops_runs": "ops_run_cold_after_days",
        "message_stream": "message_stream_cold_after_days",
        "message_thread": "message_thread_cold_after_days",
        "episodic": "episodic_cold_after_days",
    }
    return getattr(settings, mapping[family])


def _get_retention_days_setting(family: str, settings: Any) -> int:
    """Return the retention-days setting for a family."""
    mapping = {
        "journal": "journal_retention_days",
        "api_audit": "audit_log_retention_days",
        "ops_runs": "ops_run_retention_days",
        "message_stream": "message_stream_retention_days",
        "message_thread": "message_thread_retention_days",
        "episodic": "episodic_retention_days",
    }
    return getattr(settings, mapping[family])


def is_size_rollover_eligible(
    source_path: Path, family: str, settings: Any
) -> bool:
    """Check if a source file exceeds its family's size rollover threshold."""
    config = FAMILIES[family]
    if not config.has_size_rollover:
        return False
    threshold = _get_rollover_bytes_setting(family, settings)
    if threshold is None:
        return False
    try:
        return source_path.stat().st_size >= threshold
    except OSError:
        return False


def is_journal_day_rollover_eligible(
    source_path: Path, now: datetime
) -> bool:
    """Check if a journal day-bucket file belongs to a past UTC day.

    Only files matching the ``YYYY-MM-DD.jsonl`` naming pattern for a
    day strictly before *now*'s UTC date are eligible.
    """
    if not _DAY_BUCKET_RE.match(source_path.name):
        return False
    file_day = source_path.stem  # e.g. "2026-03-19"
    today = now.astimezone(timezone.utc).strftime("%Y-%m-%d")
    return file_day < today


def is_message_stream_max_hot_days_eligible(
    source_path: Path, settings: Any, now: datetime
) -> bool:
    """Check if a message stream source exceeds max_hot_days based on mtime."""
    max_hot = getattr(settings, "message_stream_max_hot_days", 14)
    try:
        mtime = source_path.stat().st_mtime
    except OSError:
        return False
    mtime_dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    age_days = (now - mtime_dt).total_seconds() / 86400
    return age_days >= max_hot


def is_message_thread_inactivity_eligible(
    source_path: Path, settings: Any, now: datetime
) -> bool:
    """Check if a message thread source exceeds inactivity_days based on mtime."""
    inactivity = getattr(settings, "message_thread_inactivity_days", 30)
    try:
        mtime = source_path.stat().st_mtime
    except OSError:
        return False
    mtime_dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    age_days = (now - mtime_dt).total_seconds() / 86400
    return age_days >= inactivity


def check_rollover_eligible(
    source_path: Path, family: str, settings: Any, now: datetime
) -> bool:
    """Check if a source file is eligible for rollover under its family's rules."""
    config = FAMILIES[family]

    # Journal: day-bucket transition only
    if family == "journal":
        return is_journal_day_rollover_eligible(source_path, now)

    # Message thread: size or inactivity (no day-boundary)
    if family == "message_thread":
        if is_size_rollover_eligible(source_path, family, settings):
            return True
        return is_message_thread_inactivity_eligible(source_path, settings, now)

    # Message stream: size or max_hot_days (no day-boundary)
    if family == "message_stream":
        if is_size_rollover_eligible(source_path, family, settings):
            return True
        return is_message_stream_max_hot_days_eligible(source_path, settings, now)

    # All others: size or day-boundary
    if config.has_size_rollover and is_size_rollover_eligible(source_path, family, settings):
        return True

    return False


def get_source_file_filter(family: str) -> str | None:
    """Return a specific filename filter for single-file families, or None.

    For families like api_audit and ops_runs that have a single known source
    file, returns the filename. For multi-file families returns None.
    """
    single_source = {
        "api_audit": "api_audit.jsonl",
        "ops_runs": "ops_runs.jsonl",
    }
    return single_source.get(family)
