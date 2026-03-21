"""Shared substrate and operation services for segment-history lifecycle."""

from __future__ import annotations

import gzip
import io
import json
import logging
import re
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from fastapi.responses import JSONResponse

from app.storage import write_bytes_file, write_text_file

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SEGMENT_HISTORY_GZIP_LEVEL = 9
SEGMENT_HISTORY_STUB_SCHEMA_TYPE = "segment_history_stub"
SEGMENT_HISTORY_STUB_SCHEMA_VERSION = "1.0"

# Segment ID format: {family}__{stream_key}__{YYYYMMDDTHHMMSSZ}__{seq:04d}
# The stream_key component is opaque and may itself contain "__".
# We validate by checking the family prefix, the 4-digit seq suffix, and
# the timestamp component immediately before the seq.
_SEGMENT_ID_TAIL_RE = re.compile(
    r"^(.+)__(\d{8}T\d{6}Z)__(\d{4})$"
)

# Family-specific source-path prefix stripping for stream key derivation
_FAMILY_PREFIX_STRIP: dict[str, str] = {
    "journal": "journal/",
    "api_audit": "logs/",
    "ops_runs": "logs/",
    "message_stream": "messages/",
    "message_thread": "messages/threads/",
    "episodic": "memory/episodic/",
}

# File extension used by each family for payloads
_FAMILY_EXTENSION: dict[str, str] = {
    "journal": ".md",
    "api_audit": ".jsonl",
    "ops_runs": ".jsonl",
    "message_stream": ".jsonl",
    "message_thread": ".jsonl",
    "episodic": ".jsonl",
}


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------
def _segment_timestamp_str(dt: datetime) -> str:
    """Format a datetime as the canonical segment timestamp ``YYYYMMDDTHHMMSSZ``."""
    utc = dt.astimezone(timezone.utc)
    return utc.strftime("%Y%m%dT%H%M%SZ")


# ---------------------------------------------------------------------------
# Stream key derivation
# ---------------------------------------------------------------------------
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
        key = key[len(prefix):]
    # Remove file extension
    dot = key.rfind(".")
    if dot > 0:
        key = key[:dot]
    # Replace / with __
    key = key.replace("/", "__")
    return key


# ---------------------------------------------------------------------------
# Segment ID allocation
# ---------------------------------------------------------------------------
def _next_segment_id(
    family: str, stream_key: str, rolled_at: datetime, target_dir: Path
) -> str:
    """Allocate the next segment ID for a family+stream at the given timestamp.

    Format: ``{family}__{stream_key}__{YYYYMMDDTHHMMSSZ}__{seq:04d}``

    Scans *target_dir* for existing segment files to determine the next
    sequence number.
    """
    ts = _segment_timestamp_str(rolled_at)
    prefix = f"{family}__{stream_key}__{ts}__"

    max_seq = 0
    if target_dir.is_dir():
        for entry in target_dir.iterdir():
            name = entry.name
            # Strip known extensions to get the segment ID portion
            for ext in (".jsonl.gz", ".md.gz", ".jsonl", ".md", ".json"):
                if name.endswith(ext):
                    name = name[: -len(ext)]
                    break
            if name.startswith(prefix):
                tail = name[len(prefix):]
                try:
                    seq = int(tail)
                    if seq > max_seq:
                        max_seq = seq
                except ValueError:
                    continue

    return f"{family}__{stream_key}__{ts}__{max_seq + 1:04d}"


def _validate_segment_id(family: str, segment_id: str) -> tuple[str, str, str, int] | None:
    """Parse a segment ID into ``(family, stream_key, timestamp, sequence)`` or return None.

    Validates:
    - Starts with ``{family}__``
    - Suffix after final ``__`` is 4-digit sequence
    - Component before that is ``YYYYMMDDTHHMMSSZ``
    - Everything between ``{family}__`` and ``__{timestamp}__{seq}`` is the opaque stream_key
    """
    if not segment_id.startswith(f"{family}__"):
        return None
    # Strip family prefix
    remainder = segment_id[len(family) + 2:]  # after "family__"
    m = _SEGMENT_ID_TAIL_RE.match(remainder)
    if not m:
        # Might not have a stream_key — try direct match for timestamp__seq
        ts_seq_re = re.match(r"^(\d{8}T\d{6}Z)__(\d{4})$", remainder)
        if ts_seq_re:
            return family, "", ts_seq_re.group(1), int(ts_seq_re.group(2))
        return None
    stream_key, ts, seq_str = m.group(1), m.group(2), m.group(3)
    return family, stream_key, ts, int(seq_str)


# ---------------------------------------------------------------------------
# Structured warning helper
# ---------------------------------------------------------------------------
def _make_warning(
    code: str,
    detail: str,
    *,
    path: str | None = None,
    segment_id: str | None = None,
) -> dict[str, Any]:
    """Build a structured warning object."""
    return {
        "code": code,
        "detail": detail,
        "path": path,
        "segment_id": segment_id,
    }


# ---------------------------------------------------------------------------
# Stub creation and mutation
# ---------------------------------------------------------------------------
def _create_stub(
    *,
    family: str,
    segment_id: str,
    source_path: str,
    stream_key: str,
    rolled_at: str,
    payload_path: str,
    summary: dict[str, Any],
) -> dict[str, Any]:
    """Build a ``segment_history_stub`` schema 1.0 JSON document.

    The stub does NOT include ``cold_stored_at`` when hot (key absent, not null).
    """
    return {
        "schema_type": SEGMENT_HISTORY_STUB_SCHEMA_TYPE,
        "schema_version": SEGMENT_HISTORY_STUB_SCHEMA_VERSION,
        "family": family,
        "segment_id": segment_id,
        "source_path": source_path,
        "stream_key": stream_key,
        "rolled_at": rolled_at,
        "created_at": rolled_at,
        "payload_path": payload_path,
        "summary": summary,
    }


def _mutate_stub_cold(
    stub: dict[str, Any], cold_path: str, cold_stored_at: str
) -> dict[str, Any]:
    """Mutate a stub to reflect cold-store completion.

    Moves ``payload_path`` to the cold location and adds ``cold_stored_at``.
    """
    stub = dict(stub)
    stub["cold_stored_at"] = cold_stored_at
    stub["payload_path"] = cold_path
    return stub


def _mutate_stub_rehydrate(stub: dict[str, Any], hot_path: str) -> dict[str, Any]:
    """Mutate a stub to reflect rehydration back to hot storage.

    Moves ``payload_path`` back to hot location and removes ``cold_stored_at``.
    """
    stub = dict(stub)
    stub.pop("cold_stored_at", None)
    stub["payload_path"] = hot_path
    return stub


# ---------------------------------------------------------------------------
# Gzip cold-store primitives
# ---------------------------------------------------------------------------
def _build_cold_gzip_bytes(source_bytes: bytes) -> bytes:
    """Build deterministic gzip bytes for a rolled segment payload."""
    buf = io.BytesIO()
    with gzip.GzipFile(
        fileobj=buf,
        mode="wb",
        filename="",
        mtime=0,
        compresslevel=SEGMENT_HISTORY_GZIP_LEVEL,
    ) as handle:
        handle.write(source_bytes)
    return buf.getvalue()


def _decompress_cold_payload(compressed: bytes) -> bytes:
    """Decompress a cold payload and validate CRC integrity.

    Raises ValueError if the payload is corrupt.
    """
    try:
        return gzip.decompress(compressed)
    except (gzip.BadGzipFile, zlib.error, OSError) as exc:
        raise ValueError(f"Cold payload corrupt: {exc}") from exc


# ---------------------------------------------------------------------------
# Journal year extraction
# ---------------------------------------------------------------------------
def _journal_year_from_source(source_rel: str) -> str:
    """Extract the year component from a journal source path.

    E.g. ``journal/2026/2026-03-19.md`` -> ``2026``.
    Falls back to the first 4 chars of the filename stem.
    """
    parts = source_rel.split("/")
    # Expected: journal/<year>/<date>.md
    if len(parts) >= 2:
        candidate = parts[1] if parts[0] == "journal" else parts[0]
        if len(candidate) == 4 and candidate.isdigit():
            return candidate
    # Fallback: parse from filename stem  (2026-03-19 -> 2026)
    stem = parts[-1].rsplit(".", 1)[0] if parts else ""
    if len(stem) >= 4:
        return stem[:4]
    return "0000"


# ---------------------------------------------------------------------------
# Rollover primitives
# ---------------------------------------------------------------------------
def _roll_jsonl_source(
    *,
    source_path: Path,
    payload_path: Path,
    family: str,
    segment_id: str,
    stream_key: str,
    rolled_at: datetime,
    stub_dir: Path,
    summary: dict[str, Any],
    repo_root: Path,
) -> tuple[dict[str, Any], list[Path]] | None:
    """Roll a JSONL source file into a segment payload and stub.

    Reads the source, writes the rolled payload (temp+fsync+rename), writes
    the stub, and replaces the active source with an empty file.

    Returns ``(stub_dict, created_paths)`` for commit tracking, or ``None``
    when the file contains only a partial unterminated line (no complete lines
    to roll).
    """
    # Read source content
    source_bytes = source_path.read_bytes()
    content = source_bytes.decode("utf-8", errors="replace")

    # Handle partial trailing line (carry-forward)
    lines = content.split("\n")
    carry = ""
    if lines and not content.endswith("\n"):
        carry = lines.pop()
    rolled_content = "\n".join(lines)
    if rolled_content and not rolled_content.endswith("\n"):
        rolled_content += "\n"

    # If no complete newline-terminated lines exist, skip the roll
    if not rolled_content.strip():
        return None

    # Write payload
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    write_text_file(payload_path, rolled_content)

    # Write stub
    stub_path = stub_dir / f"{segment_id}.json"
    stub_dir.mkdir(parents=True, exist_ok=True)
    source_rel = str(source_path.relative_to(repo_root))
    payload_rel = str(payload_path.relative_to(repo_root))
    stub = _create_stub(
        family=family,
        segment_id=segment_id,
        source_path=source_rel,
        stream_key=stream_key,
        rolled_at=_segment_timestamp_str(rolled_at),
        payload_path=payload_rel,
        summary=summary,
    )
    write_text_file(stub_path, json.dumps(stub, ensure_ascii=False, indent=2))

    # Replace active source with carry-forward content
    write_text_file(source_path, carry)

    return stub, [payload_path, stub_path]


def _roll_journal_source(
    *,
    source_path: Path,
    payload_path: Path,
    family: str,
    segment_id: str,
    stream_key: str,
    rolled_at: datetime,
    stub_dir: Path,
    summary: dict[str, Any],
    repo_root: Path,
) -> tuple[dict[str, Any], list[Path]]:
    """Roll a journal day-bucket source with exact byte preservation.

    Journal sources are day-bucketed files that are rolled as complete units.
    Unlike JSONL rollover, the entire file is moved without partial-line
    carry-forward (the day is complete).
    """
    source_bytes = source_path.read_bytes()

    # Write payload with exact content preservation
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    write_bytes_file(payload_path, source_bytes)

    # Write stub
    stub_path = stub_dir / f"{segment_id}.json"
    stub_dir.mkdir(parents=True, exist_ok=True)
    source_rel = str(source_path.relative_to(repo_root))
    payload_rel = str(payload_path.relative_to(repo_root))
    stub = _create_stub(
        family=family,
        segment_id=segment_id,
        source_path=source_rel,
        stream_key=stream_key,
        rolled_at=_segment_timestamp_str(rolled_at),
        payload_path=payload_rel,
        summary=summary,
    )
    write_text_file(stub_path, json.dumps(stub, ensure_ascii=False, indent=2))

    # Remove the hot day-bucket source (it's fully captured)
    source_path.unlink(missing_ok=True)

    return stub, [payload_path, stub_path]


# ---------------------------------------------------------------------------
# Rollback helpers
# ---------------------------------------------------------------------------
def _capture_rollback_state(paths: list[Path]) -> list[tuple[Path, bytes | None]]:
    """Capture the current bytes of each path for later rollback."""
    state: list[tuple[Path, bytes | None]] = []
    for p in paths:
        if p.is_file():
            state.append((p, p.read_bytes()))
        else:
            state.append((p, None))
    return state


def _restore_rollback_state(state: list[tuple[Path, bytes | None]]) -> None:
    """Restore files to their captured state."""
    for p, old_bytes in state:
        try:
            if old_bytes is None:
                if p.exists():
                    p.unlink()
            else:
                p.parent.mkdir(parents=True, exist_ok=True)
                write_bytes_file(p, old_bytes)
        except Exception:
            _log.exception("Rollback restore failed for %s", p)


def _remove_created_paths(paths: list[Path]) -> None:
    """Remove paths that were created during a failed operation."""
    for p in paths:
        try:
            if p.is_file():
                p.unlink()
        except OSError:
            _log.warning("Could not remove created path during rollback: %s", p)


# ---------------------------------------------------------------------------
# Cold-store / rehydrate target path derivation
# ---------------------------------------------------------------------------
def _cold_payload_path(hot_payload_path: Path) -> Path:
    """Derive the cold payload path from a hot payload path.

    Cold goes in a sibling ``cold/`` directory with ``.gz`` appended.

    ``logs/history/api_audit/api_audit__api_audit__20260320T120000Z__0001.jsonl``
    becomes
    ``logs/history/api_audit/cold/api_audit__api_audit__20260320T120000Z__0001.jsonl.gz``
    """
    return hot_payload_path.parent / "cold" / (hot_payload_path.name + ".gz")


_MESSAGE_STREAM_KINDS = ("inbox", "outbox", "relay", "acks")


def _message_stream_kind_from_source(source_rel: str) -> str:
    """Extract the stream kind (inbox|outbox|relay|acks) from a message_stream source path.

    E.g. ``messages/inbox/alice.jsonl`` -> ``inbox``.
    """
    # source_rel is like messages/<kind>/<file>.jsonl
    parts = source_rel.split("/")
    if len(parts) >= 2 and parts[0] == "messages" and parts[1] in _MESSAGE_STREAM_KINDS:
        return parts[1]
    # Fallback: try to extract from stream_key (inbox__alice -> inbox)
    stream_key = _derive_stream_key("message_stream", source_rel)
    kind = stream_key.split("__")[0]
    return kind if kind in _MESSAGE_STREAM_KINDS else "inbox"


def _message_stream_history_dir(repo_root: Path, source_rel: str) -> Path:
    """Return the per-kind history dir for a message_stream source."""
    kind = _message_stream_kind_from_source(source_rel)
    return repo_root / "messages" / "history" / kind


def _message_stream_stub_dir(repo_root: Path, source_rel: str) -> Path:
    """Return the per-kind stub dir for a message_stream source."""
    kind = _message_stream_kind_from_source(source_rel)
    return repo_root / "messages" / "history" / kind / "index"


def _message_stream_stub_dirs(repo_root: Path) -> list[Path]:
    """Return all 4 message_stream stub directories for scanning."""
    return [
        repo_root / "messages" / "history" / kind / "index"
        for kind in _MESSAGE_STREAM_KINDS
    ]


def _rehydrate_hot_path(
    family: str, segment_id: str, source_path: str, repo_root: Path
) -> Path:
    """Derive the canonical hot restoration target from family, segment_id, and source_path.

    Per spec, the hot target is derived canonically per family:
    - journal: journal/history/<year>/<segment_id>.md
    - api_audit: logs/history/api_audit/<segment_id>.jsonl
    - ops_runs: logs/history/ops_runs/<segment_id>.jsonl
    - message_stream: messages/history/<stream_kind>/<segment_id>.jsonl
    - message_thread: messages/history/threads/<segment_id>.jsonl
    - episodic: memory/episodic/history/<segment_id>.jsonl
    """
    ext = _FAMILY_EXTENSION.get(family, ".jsonl")
    filename = f"{segment_id}{ext}"

    if family == "journal":
        year = _journal_year_from_source(source_path)
        return repo_root / "journal" / "history" / year / filename
    if family == "api_audit":
        return repo_root / "logs" / "history" / "api_audit" / filename
    if family == "ops_runs":
        return repo_root / "logs" / "history" / "ops_runs" / filename
    if family == "message_stream":
        # Derive stream_kind from source_path: messages/<kind>/file -> <kind>
        stream_key = _derive_stream_key(family, source_path)
        # stream_key e.g. "inbox__alice" -> stream_kind is first component
        parts = stream_key.split("__")
        stream_kind = parts[0] if parts else "stream"
        return repo_root / "messages" / "history" / stream_kind / filename
    if family == "message_thread":
        return repo_root / "messages" / "history" / "threads" / filename
    if family == "episodic":
        return repo_root / "memory" / "episodic" / "history" / filename
    # Fallback
    return repo_root / filename


# Keep backward compatibility alias for tests
def _rehydrate_target_path(cold_payload_path: Path) -> Path:
    """Derive the hot restoration target from a cold payload path.

    Inverse of ``_cold_payload_path``. This is a backward-compat helper;
    the canonical derivation uses ``_rehydrate_hot_path`` instead.
    """
    # Remove .gz suffix and move out of cold/ subdir
    hot_name = cold_payload_path.name
    if hot_name.endswith(".gz"):
        hot_name = hot_name[: -len(".gz")]
    return cold_payload_path.parent.parent / hot_name


# ---------------------------------------------------------------------------
# JSONL summary helpers
# ---------------------------------------------------------------------------
def _count_lines(content: str) -> int:
    """Count newline-terminated lines in content.

    Per spec, ``line_count`` means count of lines ending with ``\\n``,
    regardless of JSON parseability.
    """
    if not content:
        return 0
    return content.count("\n")


def _byte_size(content: str | bytes) -> int:
    """Return byte size of content."""
    if isinstance(content, str):
        return len(content.encode("utf-8"))
    return len(content)


def _first_nonempty_line_preview(content: str, max_len: int = 200) -> str | None:
    """Return the first non-empty line truncated to *max_len*, or None."""
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped:
            return stripped[:max_len]
    return None


def _sample_json_field(
    content: str, field: str, limit: int
) -> list[str]:
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


def _first_last_json_field(content: str, field: str) -> tuple[str | None, str | None]:
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


def _json_field_counts(
    content: str, field: str, limit: int
) -> dict[str, int]:
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
    sorted_items = sorted(counts.items(), key=lambda x: -x[1])[:limit]
    return dict(sorted_items)


# ===========================================================================
# Phase 8: Residue detection and manifest reconciliation
# ===========================================================================
def _reconcile_manifest_residue(
    repo_root: Path, caller_family: str, caller_op: str, gm: Any
) -> dict[str, str] | None:
    """Check for and reconcile a leftover manifest from a prior crash.

    Returns None if no manifest exists, or a dict with a ``warning`` key
    describing the reconciliation action taken.
    """
    from app.segment_history.manifest import read_manifest, remove_manifest

    try:
        manifest = read_manifest(repo_root, caller_family)
    except ValueError:
        # Corrupt manifest -- clean up
        remove_manifest(repo_root, caller_family)
        return {"warning": "segment_history_manifest_unreadable_cleanup"}

    if manifest is None:
        return None

    recorded_op = manifest.get("operation", "unknown")
    family = manifest.get("family", "unknown")
    segment_ids = manifest.get("segment_ids", [])

    # Best-effort: try to commit any files that were fully written
    # before the crash.  If nothing to commit, just clean up.
    if segment_ids:
        _log.warning(
            "Found stale segment-history manifest (op=%s family=%s segments=%d). "
            "Attempting best-effort cleanup.",
            recorded_op, family, len(segment_ids),
        )

    remove_manifest(repo_root, caller_family)
    return {
        "warning": f"segment_history_manifest_residue:{recorded_op}:{family}:{len(segment_ids)}"
    }


# ---------------------------------------------------------------------------
# Audit event emission helper
# ---------------------------------------------------------------------------
def _emit_audit(
    audit: Callable[..., Any] | None,
    event: str,
    detail: dict[str, Any],
) -> None:
    """Call the audit callable if provided, swallowing errors."""
    if audit is None:
        return
    try:
        audit(event, detail)
    except Exception:
        _log.warning("Audit emission failed for event %s", event, exc_info=True)


# ===========================================================================
# Phase 5: Maintenance service
# ===========================================================================
def segment_history_maintenance_service(
    *,
    family: str,
    repo_root: Path,
    settings: Any,
    gm: Any,
    now: datetime | None = None,
    audit: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Discover, roll, and commit eligible sources for one family.

    Returns a response envelope with ``ok``, ``operation``, ``family``,
    rolled segment IDs, commit info, and warnings.
    """
    from app.segment_history.families import (
        FAMILIES,
        check_rollover_eligible,
        discover_active_sources,
    )
    from app.segment_history.locking import acquire_sorted_source_locks
    from app.segment_history.manifest import remove_manifest, write_manifest

    if now is None:
        now = datetime.now(timezone.utc)

    config = FAMILIES[family]
    warnings: list[dict[str, Any]] = []

    # 0. Reconcile any leftover manifest residue
    residue = _reconcile_manifest_residue(repo_root, family, "maintenance", gm)
    if residue:
        warnings.append(_make_warning(
            "segment_history_manifest_residue",
            residue["warning"],
        ))

    # 1. Discover active sources
    sources = discover_active_sources(family, repo_root)

    # 2. Skip 0-byte sources; delete empty ack files with warning
    eligible: list[Path] = []
    for src in sources:
        try:
            size = src.stat().st_size
        except OSError:
            continue
        if size == 0:
            # Delete empty ack files outside atomic unit
            if src.parent.name == "acks" and str(src.parent).endswith("messages/acks"):
                try:
                    src.unlink()
                    warnings.append(_make_warning(
                        "segment_history_empty_ack_deleted",
                        f"Removed empty ack file: {src.name}",
                        path=str(src),
                    ))
                except OSError:
                    pass
            continue
        if check_rollover_eligible(src, family, settings, now):
            eligible.append(src)

    # 3. Sort and apply batch limit
    eligible.sort()
    batch_limit = getattr(settings, "segment_history_batch_limit", 500)
    batch_limit_reached = len(eligible) > batch_limit
    eligible = eligible[:batch_limit]

    if not eligible:
        return {
            "ok": True,
            "operation": "segment_history_maintenance",
            "family": family,
            "selection_count": 0,
            "rolled_count": 0,
            "batch_limit_reached": False,
            "rolled_segment_ids": [],
            "durable": True,
            "committed_files": [],
            "latest_commit": None,
            "warnings": warnings,
        }

    # 4. Prepare
    selection_count = 0
    rolled_segment_ids: list[str] = []
    all_created: list[Path] = []
    all_source_rels: list[str] = []
    lock_keys: list[str] = []

    for src in eligible:
        rel = str(src.relative_to(repo_root))
        stream_key = _derive_stream_key(family, rel)
        lock_keys.append(f"segment_history:{family}:{stream_key}")
        all_source_rels.append(rel)

    lock_dir = repo_root / ".locks" / "segment_history"
    source_rollback: list[tuple[Path, bytes | None]] = []

    try:
        # 5. Acquire sorted source locks
        with acquire_sorted_source_locks(lock_keys, lock_dir=lock_dir):
            # Post-lock: recount eligible for selection_count
            locked_eligible: list[Path] = []
            for src in eligible:
                if not src.is_file():
                    warnings.append(_make_warning(
                        "segment_history_source_missing_under_lock",
                        f"Source file missing after lock acquisition: {src.name}",
                        path=str(src),
                    ))
                    continue
                try:
                    size = src.stat().st_size
                except OSError:
                    warnings.append(_make_warning(
                        "segment_history_source_missing_under_lock",
                        f"Source file stat failed after lock acquisition: {src.name}",
                        path=str(src),
                    ))
                    continue
                if size == 0:
                    continue
                locked_eligible.append(src)

            selection_count = len(locked_eligible)

            # 6. Pre-compute segment IDs and target paths for manifest
            planned: list[tuple[Path, str, str, Path, Path]] = []  # (src, rel, seg_id, payload, stub)
            planned_source_rels: list[str] = []
            planned_segment_ids: list[str] = []
            planned_target_paths: list[str] = []
            for src in locked_eligible:
                rel = str(src.relative_to(repo_root))
                sk = _derive_stream_key(family, rel)
                if family == "journal":
                    year = _journal_year_from_source(rel)
                    hist = repo_root / "journal" / "history" / year
                    sd = repo_root / "journal" / "history" / year / "index"
                elif family == "message_stream":
                    hist = _message_stream_history_dir(repo_root, rel)
                    sd = _message_stream_stub_dir(repo_root, rel)
                else:
                    hist = repo_root / config.history_dir
                    sd = repo_root / config.stub_dir
                seg_id = _next_segment_id(family, sk, now, hist)
                ext = _FAMILY_EXTENSION.get(family, ".jsonl")
                pp = hist / f"{seg_id}{ext}"
                sp = sd / f"{seg_id}.json"
                planned.append((src, rel, seg_id, pp, sp))
                planned_source_rels.append(rel)
                planned_segment_ids.append(seg_id)
                planned_target_paths.append(str(pp.relative_to(repo_root)))
                planned_target_paths.append(str(sp.relative_to(repo_root)))

            # Write manifest with frozen selected set under lock
            if planned:
                write_manifest(
                    repo_root,
                    operation="maintenance",
                    family=family,
                    source_paths=planned_source_rels,
                    segment_ids=planned_segment_ids,
                    target_paths=planned_target_paths,
                )

            # Capture source state for rollback
            source_rollback = _capture_rollback_state(locked_eligible)

            # 7. Roll each eligible source under lock
            for src, rel, segment_id, payload_path, _stub_path_plan in planned:
                stream_key = _derive_stream_key(family, rel)
                stub_dir = _stub_path_plan.parent

                # Read content for summary
                content = src.read_text(encoding="utf-8", errors="replace")
                summary = config.build_summary(content)

                # Add day field for journal summaries
                if family == "journal":
                    summary["day"] = src.stem
                # Add stream_kind and stream_key for message_stream summaries;
                # apply ack-specific overrides per spec.
                elif family == "message_stream":
                    sk_kind = _message_stream_kind_from_source(rel)
                    summary["stream_kind"] = sk_kind
                    summary["stream_key"] = stream_key
                    from app.segment_history.families import fixup_message_stream_summary
                    fixup_message_stream_summary(summary, content, sk_kind)
                # Add thread_id for message_thread summaries
                elif family == "message_thread":
                    summary["thread_id"] = src.stem

                # Roll
                if family == "journal":
                    stub, created = _roll_journal_source(
                        source_path=src,
                        payload_path=payload_path,
                        family=family,
                        segment_id=segment_id,
                        stream_key=stream_key,
                        rolled_at=now,
                        stub_dir=stub_dir,
                        summary=summary,
                        repo_root=repo_root,
                    )
                else:
                    result = _roll_jsonl_source(
                        source_path=src,
                        payload_path=payload_path,
                        family=family,
                        segment_id=segment_id,
                        stream_key=stream_key,
                        rolled_at=now,
                        stub_dir=stub_dir,
                        summary=summary,
                        repo_root=repo_root,
                    )
                    if result is None:
                        warnings.append(_make_warning(
                            "segment_history_only_partial_line",
                            f"Source has only a partial unterminated line, skipping roll: {rel}",
                            path=rel,
                        ))
                        continue
                    stub, created = result

                rolled_segment_ids.append(segment_id)
                all_created.extend(created)

                # Emit audit event
                _emit_audit(audit, "segment_history_roll", {
                    "family": family,
                    "segment_id": segment_id,
                    "source_path": rel,
                    "payload_path": str(payload_path.relative_to(repo_root)),
                    "warning_count": len(warnings),
                })

    except Exception:
        # Rollback: restore source files to pre-roll state, then remove created
        if source_rollback:
            _restore_rollback_state(source_rollback)
        _remove_created_paths(all_created)
        remove_manifest(repo_root, family)
        raise

    # 8. Git commit
    durable = True
    committed_files: list[str] = []
    latest_commit: str | None = None

    if all_created:
        commit_paths = all_created + [
            src for src in eligible if src.is_file()
        ]
        commit_message = (
            f"segment-history: roll {family} {selection_count}"
        )
        try:
            from app.git_locking import repository_mutation_lock

            with repository_mutation_lock(repo_root):
                success = gm.commit_paths(commit_paths, commit_message)
                if success:
                    latest_commit = gm.latest_commit()
                    committed_files = [str(p.relative_to(repo_root)) for p in commit_paths if p.is_file()]
                else:
                    durable = False
                    warnings.append(_make_warning(
                        "segment_history_git_commit_failed",
                        "Git commit failed for maintenance roll",
                    ))
        except Exception:
            durable = False
            warnings.append(_make_warning(
                "segment_history_git_commit_failed",
                "Git commit failed for maintenance roll",
            ))

    # 9. Remove manifest
    remove_manifest(repo_root, family)

    return {
        "ok": True,
        "operation": "segment_history_maintenance",
        "family": family,
        "selection_count": selection_count,
        "rolled_count": len(rolled_segment_ids),
        "batch_limit_reached": batch_limit_reached,
        "rolled_segment_ids": rolled_segment_ids,
        "durable": durable,
        "committed_files": committed_files,
        "latest_commit": latest_commit,
        "warnings": warnings,
    }


# ===========================================================================
# Phase 6: Cold-store service
# ===========================================================================
def segment_history_cold_store_service(
    *,
    family: str,
    repo_root: Path,
    settings: Any,
    gm: Any,
    segment_ids: list[str] | None = None,
    now: datetime | None = None,
    audit: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Compress rolled segments into cold storage.

    Scans stub directory for eligible stubs (no ``cold_stored_at``, meets
    cold_after_days threshold), compresses the hot payload, mutates the stub,
    removes the hot rolled payload, and commits.
    """
    from app.segment_history.families import FAMILIES, _get_cold_after_days_setting
    from app.segment_history.locking import acquire_sorted_source_locks
    from app.segment_history.manifest import remove_manifest, write_manifest

    if now is None:
        now = datetime.now(timezone.utc)

    config = FAMILIES[family]
    cold_after_days = _get_cold_after_days_setting(family, settings)
    warnings: list[dict[str, Any]] = []

    # 0. Reconcile any leftover manifest residue
    residue = _reconcile_manifest_residue(repo_root, family, "cold_store", gm)
    if residue:
        warnings.append(_make_warning(
            "segment_history_manifest_residue",
            residue["warning"],
        ))

    # 1. Discover candidates by scanning stub dir
    candidates: list[tuple[str, dict, Path]] = []  # (segment_id, stub, stub_path)

    # Build the list of stub dirs to scan — multi-dir for journal and message_stream
    stub_dirs_to_scan: list[Path] = []
    if family == "journal":
        journal_history = repo_root / "journal" / "history"
        if journal_history.is_dir():
            for year_dir in sorted(journal_history.iterdir()):
                if year_dir.is_dir() and year_dir.name.isdigit():
                    idx_dir = year_dir / "index"
                    if idx_dir.is_dir():
                        stub_dirs_to_scan.append(idx_dir)
    elif family == "message_stream":
        stub_dirs_to_scan = _message_stream_stub_dirs(repo_root)
    else:
        stub_base_dir = repo_root / config.stub_dir
        stub_dirs_to_scan = [stub_base_dir]

    for sd in stub_dirs_to_scan:
        if not sd.is_dir():
            continue
        for entry in sorted(sd.iterdir()):
            if not entry.name.endswith(".json"):
                continue
            try:
                stub = json.loads(entry.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                warnings.append(_make_warning(
                    "segment_history_stub_unreadable",
                    f"Cannot read stub file: {entry.name}",
                    path=str(entry),
                ))
                continue

            # Skip already cold-stored
            if stub.get("cold_stored_at"):
                continue

            # Check cold eligibility
            elig_field = config.cold_eligibility_field
            summary = stub.get("summary", {})
            elig_value = summary.get(elig_field)
            if elig_value is None:
                # Use rolled_at as fallback
                elig_value = stub.get("rolled_at")

            if elig_value:
                try:
                    # Parse timestamp - handle both ISO and compact formats
                    ts_str = str(elig_value)
                    if "T" in ts_str and ts_str.endswith("Z") and len(ts_str) == 16:
                        # Compact: 20260320T120000Z
                        elig_dt = datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
                    else:
                        # Try ISO or date-only
                        if len(ts_str) == 10:  # YYYY-MM-DD
                            elig_dt = datetime.strptime(ts_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        else:
                            elig_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    age_days = (now - elig_dt).total_seconds() / 86400
                    if age_days < cold_after_days:
                        continue
                except (ValueError, TypeError):
                    continue

            seg_id = stub.get("segment_id", entry.stem)

            # Filter by requested segment_ids
            if segment_ids and seg_id not in segment_ids:
                continue

            candidates.append((seg_id, stub, entry))

    # Sort by segment_id, apply batch limit
    candidates.sort(key=lambda x: x[0])
    batch_limit = getattr(settings, "segment_history_batch_limit", 500)
    batch_limit_reached = len(candidates) > batch_limit
    candidates = candidates[:batch_limit]

    if not candidates:
        return {
            "ok": True,
            "operation": "segment_history_cold_store",
            "family": family,
            "selection_count": 0,
            "cold_stored_count": 0,
            "batch_limit_reached": False,
            "cold_segment_ids": [],
            "durable": True,
            "committed_files": [],
            "latest_commit": None,
            "warnings": warnings,
        }

    # Derive lock keys from source paths using _derive_stream_key
    lock_keys = []
    for c in candidates:
        src_path = c[1].get("source_path", c[0])
        sk = _derive_stream_key(family, src_path)
        lock_keys.append(f"segment_history:{family}:{sk}")

    lock_dir = repo_root / ".locks" / "segment_history"
    cold_stored_ids: list[str] = []
    commit_paths: list[Path] = []

    try:
        with acquire_sorted_source_locks(lock_keys, lock_dir=lock_dir):
            # Re-read stubs under lock and revalidate (5 checks per spec)
            validated: list[tuple[str, dict, Path]] = []
            for seg_id, pre_lock_stub, stub_path in candidates:
                if not stub_path.is_file():
                    warnings.append(_make_warning(
                        "segment_history_stub_missing_under_lock",
                        f"Stub file missing after lock acquisition: {seg_id}",
                        segment_id=seg_id,
                    ))
                    continue
                try:
                    stub = json.loads(stub_path.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    warnings.append(_make_warning(
                        "segment_history_stub_unreadable",
                        f"Cannot read stub under lock: {seg_id}",
                        segment_id=seg_id,
                    ))
                    continue
                if stub.get("cold_stored_at"):
                    # Already cold-stored by another process
                    continue
                # Check source_path identity
                if stub.get("source_path") != pre_lock_stub.get("source_path"):
                    warnings.append(_make_warning(
                        "segment_history_stub_source_changed",
                        f"Stub source_path changed under lock: {seg_id}",
                        segment_id=seg_id,
                    ))
                    continue
                # Re-check cold eligibility under lock
                elig_field = config.cold_eligibility_field
                summary = stub.get("summary", {})
                elig_value = summary.get(elig_field)
                if elig_value is None:
                    elig_value = stub.get("rolled_at")
                if elig_value:
                    try:
                        ts_str = str(elig_value)
                        if "T" in ts_str and ts_str.endswith("Z") and len(ts_str) == 16:
                            elig_dt = datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
                        elif len(ts_str) == 10:
                            elig_dt = datetime.strptime(ts_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        else:
                            elig_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        age_days = (now - elig_dt).total_seconds() / 86400
                        if age_days < cold_after_days:
                            continue
                    except (ValueError, TypeError):
                        continue
                validated.append((seg_id, stub, stub_path))

            selection_count = len(validated)

            # Write manifest with frozen validated set under lock
            if validated:
                v_source_paths = [s.get("source_path", "") for _, s, _ in validated]
                v_seg_ids = [sid for sid, _, _ in validated]
                v_target_paths: list[str] = []
                for sid, s, sp in validated:
                    pp = s.get("payload_path", "")
                    cp = str(_cold_payload_path(repo_root / pp).relative_to(repo_root)) if pp else ""
                    v_target_paths.extend([cp, str(sp.relative_to(repo_root))])
                write_manifest(
                    repo_root,
                    operation="cold_store",
                    family=family,
                    source_paths=v_source_paths,
                    segment_ids=v_seg_ids,
                    target_paths=v_target_paths,
                )

            for seg_id, stub, stub_path in validated:
                payload_rel = stub.get("payload_path", "")
                hot_payload = repo_root / payload_rel

                if not hot_payload.is_file():
                    warnings.append(_make_warning(
                        "segment_history_hot_payload_missing",
                        f"Hot payload file missing for segment: {seg_id}",
                        segment_id=seg_id,
                    ))
                    continue

                # Compress
                source_bytes = hot_payload.read_bytes()
                compressed = _build_cold_gzip_bytes(source_bytes)
                cold_path = _cold_payload_path(hot_payload)
                cold_path.parent.mkdir(parents=True, exist_ok=True)
                write_bytes_file(cold_path, compressed)

                # Mutate stub: payload_path moves to cold location, add cold_stored_at
                cold_rel = str(cold_path.relative_to(repo_root))
                updated_stub = _mutate_stub_cold(
                    stub, cold_rel, now.isoformat()
                )
                write_text_file(
                    stub_path,
                    json.dumps(updated_stub, ensure_ascii=False, indent=2),
                )

                # Remove hot payload
                try:
                    hot_payload.unlink()
                except OSError:
                    warnings.append(_make_warning(
                        "segment_history_hot_payload_remove_failed",
                        f"Could not remove hot payload after cold-store: {seg_id}",
                        segment_id=seg_id,
                    ))

                cold_stored_ids.append(seg_id)
                commit_paths.extend([cold_path, stub_path])

                # Emit audit event
                _emit_audit(audit, "segment_history_cold_store", {
                    "family": family,
                    "segment_id": seg_id,
                    "source_path": stub.get("source_path", ""),
                    "cold_payload_path": cold_rel,
                    "warning_count": len(warnings),
                })

    except Exception:
        remove_manifest(repo_root, family)
        raise

    # Git commit
    durable = True
    committed_files: list[str] = []
    latest_commit: str | None = None

    if commit_paths:
        msg = f"segment-history: cold-store {family} {selection_count}"
        try:
            from app.git_locking import repository_mutation_lock

            with repository_mutation_lock(repo_root):
                success = gm.commit_paths(commit_paths, msg)
                if success:
                    latest_commit = gm.latest_commit()
                    committed_files = [
                        str(p.relative_to(repo_root)) for p in commit_paths if p.is_file()
                    ]
                else:
                    durable = False
                    warnings.append(_make_warning(
                        "segment_history_git_commit_failed",
                        "Git commit failed for cold-store",
                    ))
        except Exception:
            durable = False
            warnings.append(_make_warning(
                "segment_history_git_commit_failed",
                "Git commit failed for cold-store",
            ))

    remove_manifest(repo_root, family)

    return {
        "ok": True,
        "operation": "segment_history_cold_store",
        "family": family,
        "selection_count": selection_count,
        "cold_stored_count": len(cold_stored_ids),
        "batch_limit_reached": batch_limit_reached,
        "cold_segment_ids": cold_stored_ids,
        "durable": durable,
        "committed_files": committed_files,
        "latest_commit": latest_commit,
        "warnings": warnings,
    }


# ===========================================================================
# Phase 7: Rehydrate service
# ===========================================================================
def segment_history_cold_rehydrate_service(
    *,
    family: str,
    segment_id: str,
    repo_root: Path,
    gm: Any,
    audit: Callable[..., Any] | None = None,
) -> dict[str, Any] | JSONResponse:
    """Decompress a single cold-stored segment back to hot storage.

    Locates the stub, validates cold state, decompresses the cold payload,
    writes the hot payload, mutates the stub, removes the cold payload, and
    commits.

    Returns a structured envelope on both success and error (no HTTPException).
    """
    from app.segment_history.families import FAMILIES
    from app.segment_history.locking import segment_history_source_lock

    config = FAMILIES[family]
    warnings: list[dict[str, Any]] = []

    def _error_response(status: int, code: str, detail: str) -> JSONResponse:
        return JSONResponse(
            status_code=status,
            content={
                "ok": False,
                "operation": "segment_history_cold_rehydrate",
                "family": family,
                "segment_id": segment_id,
                "error": {"code": code, "detail": detail},
            },
        )

    # Validate segment_id
    parsed = _validate_segment_id(family, segment_id)
    if parsed is None:
        return _error_response(
            400,
            "segment_history_invalid_segment_id",
            f"Invalid segment ID format: {segment_id}",
        )

    # Find stub — multi-dir scan for journal and message_stream
    stub_path: Path | None = None
    stub_matches: list[Path] = []

    if family == "journal":
        journal_history = repo_root / "journal" / "history"
        if journal_history.is_dir():
            for year_dir in sorted(journal_history.iterdir()):
                if year_dir.is_dir() and year_dir.name.isdigit():
                    candidate = year_dir / "index" / f"{segment_id}.json"
                    if candidate.is_file():
                        stub_matches.append(candidate)
    elif family == "message_stream":
        for sd in _message_stream_stub_dirs(repo_root):
            candidate = sd / f"{segment_id}.json"
            if candidate.is_file():
                stub_matches.append(candidate)
    else:
        candidate = repo_root / config.stub_dir / f"{segment_id}.json"
        if candidate.is_file():
            stub_matches.append(candidate)

    if len(stub_matches) > 1:
        return JSONResponse(
            status_code=409,
            content={
                "ok": False,
                "operation": "segment_history_cold_rehydrate",
                "family": family,
                "segment_id": segment_id,
                "error": {
                    "code": "segment_history_ambiguous_segment_id",
                    "detail": f"Multiple stubs found for segment: {segment_id}",
                },
            },
        )
    stub_path = stub_matches[0] if stub_matches else None

    if stub_path is None:
        return _error_response(
            404,
            "segment_history_stub_not_found",
            f"Stub not found for segment: {segment_id}",
        )

    try:
        stub = json.loads(stub_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return _error_response(
            409,
            "segment_history_stub_unreadable",
            f"Cannot read stub for segment: {segment_id}",
        )

    # Must be cold-stored
    if not stub.get("cold_stored_at"):
        return _error_response(
            409,
            "segment_history_not_cold",
            "The requested segment is already hot and cannot be rehydrated.",
        )

    cold_payload_rel = stub.get("payload_path", "")
    cold_path = repo_root / cold_payload_rel

    if not cold_path.is_file():
        return _error_response(
            409,
            "segment_history_cold_payload_missing",
            f"Cold payload file missing for segment: {segment_id}",
        )

    # Derive hot restoration target canonically from family/segment_id/source_path
    source_path_str = stub.get("source_path", "")

    # Check for pending batch residue — single-source ops must not proceed
    # if the family manifest lists this source path.
    from app.segment_history.manifest import read_manifest as _read_manifest
    try:
        mf = _read_manifest(repo_root, family)
    except ValueError:
        mf = None
    if mf is not None and source_path_str in mf.get("source_paths", []):
        return _error_response(
            409,
            "segment_history_pending_batch_residue",
            f"A pending batch operation lists this source; "
            f"reconciliation must complete first: {source_path_str}",
        )

    hot_path = _rehydrate_hot_path(family, segment_id, source_path_str, repo_root)

    # Lock key derived from source_path via _derive_stream_key
    sk = _derive_stream_key(family, source_path_str)
    lock_key = f"segment_history:{family}:{sk}"
    lock_dir = repo_root / ".locks" / "segment_history"

    with segment_history_source_lock(lock_key, lock_dir=lock_dir):
        # Conflict check under lock: canonical hot target must not exist
        if hot_path.is_file():
            hot_rel = str(hot_path.relative_to(repo_root))
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "operation": "segment_history_cold_rehydrate",
                    "family": family,
                    "segment_id": segment_id,
                    "error": {
                        "code": "segment_history_rehydrate_conflict",
                        "detail": "target rolled payload already exists",
                    },
                    "conflict_path": hot_rel,
                },
            )

        # Re-check cold payload under lock
        if not cold_path.is_file():
            return _error_response(
                409,
                "segment_history_cold_payload_missing",
                f"Cold payload file missing for segment (under lock): {segment_id}",
            )

        compressed = cold_path.read_bytes()
        try:
            decompressed = _decompress_cold_payload(compressed)
        except ValueError:
            return _error_response(
                409,
                "segment_history_cold_payload_corrupt",
                f"Cold payload is corrupt for segment: {segment_id}",
            )

        # Write hot payload
        hot_path.parent.mkdir(parents=True, exist_ok=True)
        write_bytes_file(hot_path, decompressed)

        # Mutate stub: payload_path back to hot, remove cold_stored_at
        hot_rel = str(hot_path.relative_to(repo_root))
        updated_stub = _mutate_stub_rehydrate(stub, hot_rel)
        write_text_file(
            stub_path,
            json.dumps(updated_stub, ensure_ascii=False, indent=2),
        )

        # Remove cold payload
        cold_removed = True
        try:
            cold_path.unlink()
        except OSError:
            cold_removed = False
            warnings.append(_make_warning(
                "segment_history_cold_payload_remove_failed",
                f"Could not remove cold payload after rehydrate: {segment_id}",
                segment_id=segment_id,
            ))

    # Git commit
    durable = True
    committed_files: list[str] = []
    latest_commit: str | None = None

    commit_paths_list = [hot_path, stub_path]
    msg = f"segment-history: rehydrate {family} {segment_id}"
    try:
        from app.git_locking import repository_mutation_lock

        with repository_mutation_lock(repo_root):
            success = gm.commit_paths(commit_paths_list, msg)
            if success:
                latest_commit = gm.latest_commit()
                committed_files = [
                    str(p.relative_to(repo_root)) for p in commit_paths_list if p.is_file()
                ]
            else:
                durable = False
                warnings.append(_make_warning(
                    "segment_history_git_commit_failed",
                    "Git commit failed for rehydrate",
                ))
    except Exception:
        durable = False
        warnings.append(_make_warning(
            "segment_history_git_commit_failed",
            "Git commit failed for rehydrate",
        ))

    # Emit audit event
    _emit_audit(audit, "segment_history_cold_rehydrate", {
        "family": family,
        "segment_id": segment_id,
        "source_path": source_path_str,
        "cold_payload_path": cold_payload_rel,
        "rehydrated_payload_path": hot_rel,
        "warning_count": len(warnings),
    })

    stub_rel = str(stub_path.relative_to(repo_root))

    return {
        "ok": True,
        "operation": "segment_history_cold_rehydrate",
        "family": family,
        "segment_id": segment_id,
        "stub_path": stub_rel,
        "cold_payload_path": cold_payload_rel,
        "rehydrated_payload_path": hot_rel,
        "removed_cold_payload_path": cold_payload_rel if cold_removed else None,
        "mutated_stub_path": stub_rel,
        "durable": durable,
        "committed_files": committed_files,
        "latest_commit": latest_commit,
        "warnings": warnings,
    }
