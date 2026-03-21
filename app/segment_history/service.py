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
    family: str,
    stream_key: str,
    rolled_at: datetime,
    target_dir: Path,
    *,
    reserved_ids: set[str] | None = None,
) -> str:
    """Allocate the next segment ID for a family+stream at the given timestamp.

    Format: ``{family}__{stream_key}__{YYYYMMDDTHHMMSSZ}__{seq:04d}``

    Scans *target_dir* for existing segment files to determine the next
    sequence number.  When *reserved_ids* is provided, also skips
    sequence numbers already claimed by other allocations in the same
    batch (prevents in-batch collisions when files haven't been written
    to disk yet).
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

    # Also account for IDs reserved by earlier allocations in the same
    # batch that haven't been written to disk yet.
    if reserved_ids:
        for rid in reserved_ids:
            if rid.startswith(prefix):
                tail = rid[len(prefix):]
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

    # Correct byte_size to reflect the actual payload (excluding any
    # carry-forward partial trailing line that the caller's summary
    # may have included).
    summary = dict(summary)
    summary["byte_size"] = _byte_size(rolled_content)

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

    The source file is **not** deleted by this function — the caller is
    responsible for deferring deletion until after a successful git commit
    so that a non-raising commit failure does not permanently lose the
    original source file.
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


def _first_nonempty_line_preview(content: str, max_len: int = 200) -> str:
    """Return the first non-empty line truncated to *max_len*, or empty string."""
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped:
            return stripped[:max_len]
    return ""


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
def _truncate_reconciled_sources(
    repo_root: Path,
    family: str,
    target_paths: list[str],
    source_paths: list[str],
    gm: Any,
) -> None:
    """Remove already-rolled data from source files after crash recovery.

    After manifest reconciliation commits orphaned payloads and stubs, the
    source files still contain the data that was rolled into those payloads.
    Without truncation the source would either be permanently blocked from
    re-rolling (duplicate-segment guard) or re-rolled into a duplicate
    segment.

    For each (payload, stub) pair in *target_paths*, reads the committed
    payload content, checks whether the corresponding source file starts
    with that content, and truncates the source to retain only the
    post-payload portion (new appends that arrived after the crash).

    For journal sources (family ``"journal"``), the source was fully
    consumed by the roll, so it is deleted (matching normal journal roll
    behaviour).
    """
    from app.storage import write_text_file as _write_text

    # Build a map: source_path -> list of payload_path for all committed
    # pairs.  We need to read stubs to find source_path associations.
    source_payload_map: dict[str, list[Path]] = {}
    for i in range(0, len(target_paths) - 1, 2):
        payload_rel = target_paths[i]
        stub_rel = target_paths[i + 1] if i + 1 < len(target_paths) else ""
        if not payload_rel or not stub_rel:
            continue
        stub_path = repo_root / stub_rel
        if not stub_path.is_file():
            continue
        try:
            stub = json.loads(stub_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        sp = stub.get("source_path", "")
        if sp:
            source_payload_map.setdefault(sp, []).append(repo_root / payload_rel)

    truncated_sources: list[Path] = []
    for sp_rel in source_paths:
        src = repo_root / sp_rel
        if not src.is_file():
            continue
        payloads = source_payload_map.get(sp_rel, [])
        if not payloads:
            continue

        if family == "journal":
            # Journal sources are fully consumed — delete the source
            # (matching _roll_journal_source behaviour).
            try:
                src.unlink()
                truncated_sources.append(src)
            except OSError:
                _log.warning(
                    "Could not delete reconciled journal source: %s",
                    sp_rel,
                )
            continue

        # JSONL sources: read the payload that was rolled from this source
        # and strip it from the source's prefix.
        try:
            source_content = src.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue

        for pp in payloads:
            if not pp.is_file():
                continue
            try:
                payload_content = pp.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            if source_content.startswith(payload_content):
                source_content = source_content[len(payload_content):]

        # Write the truncated source (only post-roll appends remain).
        try:
            _write_text(src, source_content)
            truncated_sources.append(src)
            _log.info(
                "Truncated reconciled source to remove already-rolled data: %s",
                sp_rel,
            )
        except OSError:
            _log.warning(
                "Could not truncate reconciled source: %s", sp_rel,
            )

    # Commit the truncated sources so git reflects the truncation.
    if truncated_sources and gm is not None:
        try:
            from app.git_locking import repository_mutation_lock

            with repository_mutation_lock(repo_root):
                gm.commit_paths(
                    truncated_sources,
                    f"segment-history: truncate reconciled sources for {family}",
                )
        except Exception:
            _log.warning(
                "Could not commit truncated sources for %s", family,
                exc_info=True,
            )



def _reconcile_manifest_residue(
    repo_root: Path,
    caller_family: str,
    caller_op: str,
    gm: Any,
    *,
    locked_source_paths: set[str] | None = None,
) -> dict[str, Any] | None:
    """Check for and reconcile a leftover manifest from a prior crash.

    When a manifest exists from a crashed operation, this function removes
    orphaned target files (payloads/stubs/cold archives) listed in the
    manifest's ``target_paths`` that are not yet committed to git.  This
    prevents phantom files from accumulating after mid-batch crashes.

    When *locked_source_paths* is provided, reconciliation is only attempted
    if the manifest's ``source_paths`` overlap with the caller's held lock
    set **and** the caller holds locks on **all** of the manifest's sources.
    This prevents concurrent operations with partial lock coverage from
    both attempting recovery and competing over the manifest.

    After a successful recovery commit for ``maintenance`` or
    ``write_time_rollover`` operations, source files are truncated to
    remove the already-rolled data prefix, preventing duplicate segments
    on subsequent rolls.

    Returns None if no manifest exists, or a dict with ``warning`` and
    ``reconciled_source_paths`` keys describing the reconciliation action.
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

    # If the caller provided its locked source set, only reconcile when
    # the caller holds locks covering ALL of the manifest's sources.
    # Partial overlap means another concurrent operation holds the
    # remaining locks and could also attempt reconciliation — skipping
    # prevents double-recovery commits and manifest clobber races.
    manifest_sources = set(manifest.get("source_paths", []))
    if locked_source_paths is not None:
        if not manifest_sources or not manifest_sources <= locked_source_paths:
            return None

    recorded_op = manifest.get("operation", "unknown")
    family = manifest.get("family", "unknown")
    segment_ids = manifest.get("segment_ids", [])
    target_paths = manifest.get("target_paths", [])

    if segment_ids:
        _log.warning(
            "Found stale segment-history manifest (op=%s family=%s segments=%d). "
            "Attempting to recover orphaned target files.",
            recorded_op, family, len(segment_ids),
        )

    # Recovery strategy: attempt to commit orphaned target files rather than
    # deleting them.  Deletion would cause data loss when source files have
    # already been mutated (truncated/deleted) by the crashed operation —
    # the targets may be the only remaining copy of rolled data.
    #
    # Target paths are stored as interleaved pairs [payload, stub, ...].
    # If a payload exists without its companion stub, the crash happened
    # between the payload write and the stub write — the source file is
    # still intact, so we remove the orphaned payload instead of
    # committing an unreferenced file.
    #
    # For cold-store manifests, the "payload" is a .gz cold archive and
    # the stub might not have been mutated yet (crash between writing the
    # .gz and mutating the stub).  If the stub exists but has not been
    # mutated (no cold_stored_at), the cold .gz is unreferenced — remove
    # it instead of committing semantically stale state.  The next
    # cold-store run will re-process the segment cleanly.
    is_cold_store = recorded_op == "cold_store"
    existing_targets: list[Path] = []
    orphaned_payloads: list[Path] = []
    for i in range(0, len(target_paths) - 1, 2):
        payload_rel = target_paths[i]
        stub_rel = target_paths[i + 1] if i + 1 < len(target_paths) else ""
        payload_exists = bool(payload_rel) and (repo_root / payload_rel).is_file()
        stub_exists = bool(stub_rel) and (repo_root / stub_rel).is_file()
        if payload_exists and stub_exists:
            # For cold-store: check whether the stub was actually mutated.
            # If the stub still lacks cold_stored_at, the crash happened
            # between writing the .gz and mutating the stub.  The cold .gz
            # is unreferenced — remove it; the segment will be re-processed.
            if is_cold_store:
                try:
                    stub_data = json.loads(
                        (repo_root / stub_rel).read_text(encoding="utf-8")
                    )
                    if not stub_data.get("cold_stored_at"):
                        orphaned_payloads.append(repo_root / payload_rel)
                        # Stub is unchanged — still valid, just commit it
                        existing_targets.append(repo_root / stub_rel)
                        continue
                except (json.JSONDecodeError, OSError):
                    pass  # Fall through to default handling
            existing_targets.append(repo_root / payload_rel)
            existing_targets.append(repo_root / stub_rel)
        elif payload_exists and not stub_exists:
            # Crash between payload write and stub write — source is
            # intact, so clean up the orphaned payload.
            # Safety: never delete a .json stub file as an "orphaned payload".
            if not payload_rel.endswith(".json"):
                orphaned_payloads.append(repo_root / payload_rel)
            else:
                _log.warning(
                    "Reconciliation: unexpected .json file in payload position, "
                    "skipping deletion: %s", payload_rel,
                )
                existing_targets.append(repo_root / payload_rel)
        elif stub_exists:
            existing_targets.append(repo_root / stub_rel)
    # Handle odd trailing entry
    if len(target_paths) % 2 == 1 and target_paths[-1]:
        p = repo_root / target_paths[-1]
        if p.is_file():
            existing_targets.append(p)

    # Remove orphaned payloads that have no companion stub
    for op in orphaned_payloads:
        try:
            op.unlink()
            _log.info("Removed orphaned payload without stub: %s", op)
        except OSError:
            _log.warning("Could not remove orphaned payload: %s", op)

    # Include source files so the recovery commit captures their current
    # state (possibly truncated by the crashed roll).
    source_paths_list = manifest.get("source_paths", [])
    source_files: list[Path] = []
    for sp in source_paths_list:
        if sp:
            sf = repo_root / sp
            if sf.is_file():
                source_files.append(sf)

    recovered = False
    reconciled_source_paths: set[str] = set()
    if existing_targets and gm is not None:
        try:
            from app.git_locking import repository_mutation_lock

            commit_paths = existing_targets + source_files
            with repository_mutation_lock(repo_root):
                gm.commit_paths(
                    commit_paths,
                    f"segment-history: recover crashed {recorded_op} for {family}",
                )
            recovered = True
            _log.info(
                "Recovered %d orphaned files from crashed %s for %s",
                len(existing_targets), recorded_op, family,
            )

            # After successfully committing orphaned targets for a
            # maintenance or write-time-rollover crash, truncate the
            # source files to remove the already-rolled data prefix.
            # Without this, the source retains stale data that would
            # either be permanently blocked from re-rolling (F1) or
            # duplicated by a subsequent write-time rollover (F2).
            if recorded_op in ("maintenance", "write_time_rollover"):
                _truncate_reconciled_sources(
                    repo_root, family, target_paths,
                    source_paths_list, gm,
                )
                reconciled_source_paths = set(source_paths_list)

            # Remove cleanup_paths — files that the operation intended
            # to delete after a successful commit (e.g. hot payloads
            # after cold-store, cold payloads after rehydrate).  These
            # files are not in target_paths (they're not being committed)
            # but lingered because the crash happened before deletion.
            for cp_rel in manifest.get("cleanup_paths", []):
                cp = repo_root / cp_rel
                if cp.is_file():
                    try:
                        cp.unlink()
                        _log.info(
                            "Removed cleanup path after recovery: %s",
                            cp_rel,
                        )
                    except OSError:
                        _log.warning(
                            "Could not remove cleanup path: %s", cp_rel,
                        )
        except Exception:
            _log.warning(
                "Could not commit recovered files from crashed %s for %s — "
                "preserving manifest and targets on disk for next retry",
                recorded_op, family,
                exc_info=True,
            )

    # Only remove the manifest when recovery succeeded or when there are
    # no target files to recover.  Preserving it on commit failure lets
    # the next reconciliation pass retry, matching the maintenance and
    # cold-store pattern (F5).
    if recovered or not existing_targets:
        remove_manifest(repo_root, caller_family)

    action = "recovered" if recovered else f"preserved={len(existing_targets)}"
    detail = (
        f"segment_history_manifest_residue:{recorded_op}:{family}:"
        f"{len(segment_ids)}:{action}"
    )
    return {
        "warning": detail,
        "reconciled_source_paths": reconciled_source_paths,
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
    batch_limit: int | None = None,
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
    from app.segment_history.locking import (
        SegmentHistoryLockTimeout,
        acquire_sorted_source_locks,
    )
    from app.segment_history.manifest import (
        ManifestOccupied,
        remove_manifest,
        write_manifest,
    )

    if family not in FAMILIES:
        return {
            "ok": False,
            "operation": "segment_history_maintenance",
            "family": family,
            "error": {
                "code": "segment_history_invalid_family",
                "detail": f"Unknown family: {family}",
            },
        }

    if now is None:
        now = datetime.now(timezone.utc)

    config = FAMILIES[family]
    warnings: list[dict[str, Any]] = []

    # 0. Manifest reconciliation is deferred until inside the source lock
    #    to prevent concurrent callers from both attempting recovery commits.

    # 1. Discover active sources
    sources = discover_active_sources(family, repo_root)

    # 2. Skip 0-byte sources; collect empty ack candidates for
    #    lock-guarded deletion (prevents TOCTOU where a concurrent write
    #    populates the file between the size check and the unlink).
    eligible: list[Path] = []
    deferred_empty_ack_deletes: list[Path] = []
    for src in sources:
        try:
            size = src.stat().st_size
        except OSError:
            continue
        if size == 0:
            if family == "message_stream" and src.parent == repo_root / "messages" / "acks":
                deferred_empty_ack_deletes.append(src)
            continue
        if check_rollover_eligible(src, family, settings, now, warnings=warnings):
            eligible.append(src)

    lock_dir = repo_root / ".locks" / "segment_history"

    # Delete empty ack files under their source lock to prevent deleting
    # a file that a concurrent writer just populated.
    if deferred_empty_ack_deletes:
        from app.segment_history.locking import segment_history_source_lock
        for ack_src in deferred_empty_ack_deletes:
            ack_rel = str(ack_src.relative_to(repo_root))
            ack_sk = _derive_stream_key(family, ack_rel)
            ack_lock_key = f"segment_history:{family}:{ack_sk}"
            try:
                with segment_history_source_lock(ack_lock_key, lock_dir=lock_dir):
                    # Re-check under lock: only delete if still empty.
                    try:
                        ack_size = ack_src.stat().st_size
                    except OSError:
                        continue
                    if ack_size == 0:
                        try:
                            ack_src.unlink()
                            warnings.append(_make_warning(
                                "segment_history_empty_ack_deleted",
                                f"Removed empty ack file: {ack_src.name}",
                                path=str(ack_src),
                            ))
                        except OSError:
                            pass
            except SegmentHistoryLockTimeout:
                pass  # Skip this ack file; it'll be cleaned up next run.

    # 3. Sort and apply batch limit
    eligible.sort()
    effective_batch_limit = getattr(settings, "segment_history_batch_limit", 500)
    if batch_limit is not None:
        effective_batch_limit = min(batch_limit, effective_batch_limit)
    batch_limit_reached = len(eligible) > effective_batch_limit
    eligible = eligible[:effective_batch_limit]

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
    rolled_sources: list[Path] = []  # sources actually rolled (for git commit)
    deferred_audit: list[dict[str, Any]] = []  # audit events emitted after lock release
    all_source_rels: list[str] = []
    lock_keys: list[str] = []

    for src in eligible:
        rel = str(src.relative_to(repo_root))
        stream_key = _derive_stream_key(family, rel)
        lock_keys.append(f"segment_history:{family}:{stream_key}")
        all_source_rels.append(rel)

    source_rollback: list[tuple[Path, bytes | None]] = []
    durable = True
    committed_files: list[str] = []
    latest_commit: str | None = None

    try:
        # 5. Acquire sorted source locks
        with acquire_sorted_source_locks(lock_keys, lock_dir=lock_dir):
            # 5a. Reconcile manifest residue inside lock scope to prevent
            #     concurrent callers from both attempting recovery commits.
            #     Only reconcile if the manifest's sources overlap with our
            #     locked set — prevents clobbering a concurrent operation's
            #     manifest on non-overlapping sources.
            residue = _reconcile_manifest_residue(
                repo_root, family, "maintenance", gm,
                locked_source_paths=set(all_source_rels),
            )
            if residue:
                warnings.append(_make_warning(
                    "segment_history_manifest_residue",
                    residue["warning"],
                ))

            # Post-lock: recount eligible for selection_count.
            # Re-check rollover eligibility under lock so that a source
            # already rolled by a concurrent write-time rollover becomes
            # a deterministic no-op (spec requirement).
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
                # Re-check rollover eligibility under lock to handle the
                # race where write-time rollover already rolled this source
                # (leaving only carry-forward content below threshold).
                if not check_rollover_eligible(src, family, settings, now, warnings=warnings):
                    continue
                locked_eligible.append(src)

            selection_count = len(locked_eligible)

            # 6. Pre-compute segment IDs and target paths for manifest.
            #    Track reserved IDs so that two sources mapping to the same
            #    (family, stream_key, timestamp) don't collide on seq number
            #    before any files are written to disk.
            planned: list[tuple[Path, str, str, Path, Path]] = []  # (src, rel, seg_id, payload, stub)
            planned_source_rels: list[str] = []
            planned_segment_ids: list[str] = []
            planned_target_paths: list[str] = []
            batch_reserved_ids: set[str] = set()
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
                seg_id = _next_segment_id(
                    family, sk, now, hist, reserved_ids=batch_reserved_ids,
                )
                batch_reserved_ids.add(seg_id)
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

            try:
                # 7. Roll each eligible source under lock
                for src, rel, segment_id, payload_path, _stub_path_plan in planned:
                    stream_key = _derive_stream_key(family, rel)
                    stub_dir = _stub_path_plan.parent

                    # Guard against duplicate segments after crash
                    # recovery.  When reconciliation committed orphaned
                    # targets for this source but truncation failed, an
                    # existing stub already references this source_path.
                    # The source still starts with the already-rolled
                    # data prefix.  Detect this by finding a matching
                    # stub AND verifying the source content starts with
                    # the existing payload — only then skip the re-roll.
                    #
                    # The content-prefix check prevents permanently
                    # blocking re-rolling after a normal successful roll
                    # where truncation succeeded (F1-R15): in that case
                    # the source contains only carry-forward + new data,
                    # which does NOT start with the old payload.
                    if stub_dir.is_dir():
                        already_rolled = False
                        for existing in stub_dir.iterdir():
                            if not existing.name.endswith(".json"):
                                continue
                            try:
                                es = json.loads(existing.read_text(encoding="utf-8"))
                                if es.get("source_path") != rel:
                                    continue
                                # Found a stub referencing this source.
                                # Check if the source still contains the
                                # already-rolled data prefix.
                                existing_payload_rel = es.get("payload_path", "")
                                if not existing_payload_rel:
                                    already_rolled = True
                                    break
                                existing_payload = repo_root / existing_payload_rel
                                if not existing_payload.is_file():
                                    continue
                                payload_content = existing_payload.read_text(
                                    encoding="utf-8", errors="replace",
                                )
                                source_content = src.read_text(
                                    encoding="utf-8", errors="replace",
                                )
                                if source_content.startswith(payload_content):
                                    already_rolled = True
                                    break
                            except (json.JSONDecodeError, OSError):
                                continue
                        if already_rolled:
                            selection_count -= 1
                            warnings.append(_make_warning(
                                "segment_history_already_rolled",
                                f"Source still contains already-rolled data "
                                f"prefix, skipping to prevent duplicate: {rel}",
                                path=rel,
                            ))
                            continue

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
                            selection_count -= 1
                            warnings.append(_make_warning(
                                "segment_history_only_partial_line",
                                f"Source has only a partial unterminated line, skipping roll: {rel}",
                                path=rel,
                            ))
                            continue
                        stub, created = result

                    rolled_segment_ids.append(segment_id)
                    all_created.extend(created)
                    rolled_sources.append(src)

                    # Defer audit event — emitting inside the source lock
                    # would self-deadlock when the audit callback triggers
                    # write-time rollover on the same source lock key.
                    deferred_audit.append({
                        "family": family,
                        "segment_id": segment_id,
                        "source_path": rel,
                        "payload_path": str(payload_path.relative_to(repo_root)),
                        "warning_count": len(warnings),
                    })

                # 8. Git commit — inside source lock scope per spec
                if all_created:
                    from app.git_locking import repository_mutation_lock

                    commit_paths = all_created + rolled_sources
                    commit_message = (
                        f"segment-history: roll {family} {selection_count}"
                    )
                    try:
                        with repository_mutation_lock(repo_root):
                            success = gm.commit_paths(commit_paths, commit_message)
                            if success:
                                latest_commit = gm.latest_commit()
                                committed_files = [str(p.relative_to(repo_root)) for p in commit_paths]
                            else:
                                durable = False
                                warnings.append(_make_warning(
                                    "segment_history_git_commit_failed",
                                    f"Git commit failed for maintenance roll; "
                                    f"at-risk segments: {rolled_segment_ids}",
                                ))
                    except Exception:
                        durable = False
                        warnings.append(_make_warning(
                            "segment_history_git_commit_failed",
                            f"Git commit failed for maintenance roll; "
                            f"at-risk segments: {rolled_segment_ids}",
                        ))

                    # Delete journal source files only after a durable
                    # commit ensures the payload is recoverable.  If the
                    # commit failed, journal sources remain on disk as
                    # fallback (matching the cold-store deferred-deletion
                    # pattern).
                    if durable and family == "journal":
                        journal_delete_paths: list[Path] = []
                        for src in rolled_sources:
                            if src.is_file():
                                try:
                                    src.unlink()
                                    journal_delete_paths.append(src)
                                except OSError:
                                    _log.warning(
                                        "Could not delete journal source after commit: %s",
                                        src,
                                    )
                        # Stage the source deletions in git.
                        if journal_delete_paths:
                            try:
                                with repository_mutation_lock(repo_root):
                                    gm.commit_paths(
                                        journal_delete_paths,
                                        "segment-history: remove rolled journal sources",
                                    )
                                    latest_commit = gm.latest_commit()
                            except Exception:
                                warnings.append(_make_warning(
                                    "segment_history_cleanup_commit_failed",
                                    "Git commit failed for journal source removal",
                                ))
            except Exception:
                # Rollback under lock: restore sources, remove created
                # files, AND remove any partially-written target files
                # listed in the manifest that were not tracked in
                # all_created (e.g. a payload written by _roll_jsonl_source
                # before the stub write failed).
                if source_rollback:
                    _restore_rollback_state(source_rollback)
                _remove_created_paths(all_created)
                # Clean up manifest target files not already in all_created
                created_set = {str(p) for p in all_created}
                for tp in planned_target_paths:
                    full = repo_root / tp
                    if str(full) not in created_set and full.is_file():
                        try:
                            full.unlink()
                        except OSError:
                            _log.warning(
                                "Could not remove orphaned target during rollback: %s", tp,
                            )
                remove_manifest(repo_root, family)
                raise

            # Remove manifest only when the commit succeeded; when durable
            # is False the manifest must survive so that the next
            # _reconcile_manifest_residue call can re-commit the orphaned
            # files (consistent with write-time rollover in audit.py).
            if durable:
                remove_manifest(repo_root, family)

    except ManifestOccupied as exc:
        return {
            "ok": False,
            "operation": "segment_history_maintenance",
            "family": family,
            "error": {
                "code": exc.code,
                "detail": str(exc),
            },
        }
    except SegmentHistoryLockTimeout as exc:
        return {
            "ok": False,
            "operation": "segment_history_maintenance",
            "family": family,
            "error": {
                "code": "segment_history_source_lock_timeout",
                "detail": str(exc),
            },
        }

    # 9. Emit deferred audit events (after source lock release to avoid
    #    self-deadlock when the audit callback triggers write-time rollover).
    for evt in deferred_audit:
        _emit_audit(audit, "segment_history_roll", evt)

    result = {
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
    if not durable:
        result["at_risk_segment_ids"] = rolled_segment_ids
    return result


# ===========================================================================
# Phase 6: Cold-store service
# ===========================================================================
def segment_history_cold_store_service(
    *,
    family: str,
    repo_root: Path,
    settings: Any,
    gm: Any,
    batch_limit: int | None = None,
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
    from app.segment_history.locking import (
        SegmentHistoryLockTimeout,
        acquire_sorted_source_locks,
    )
    from app.segment_history.manifest import (
        ManifestOccupied,
        remove_manifest,
        write_manifest,
    )

    if family not in FAMILIES:
        return {
            "ok": False,
            "operation": "segment_history_cold_store",
            "family": family,
            "error": {
                "code": "segment_history_invalid_family",
                "detail": f"Unknown family: {family}",
            },
        }

    if now is None:
        now = datetime.now(timezone.utc)

    config = FAMILIES[family]
    cold_after_days = _get_cold_after_days_setting(family, settings)
    warnings: list[dict[str, Any]] = []

    # 0a. Validate segment_ids format upfront per spec
    if segment_ids:
        for sid in segment_ids:
            parsed = _validate_segment_id(family, sid)
            if parsed is None:
                return {
                    "ok": False,
                    "operation": "segment_history_cold_store",
                    "family": family,
                    "error": {
                        "code": "segment_history_invalid_segment_id",
                        "detail": f"Invalid segment ID format: {sid}",
                    },
                }

    # 0b. Manifest reconciliation is deferred until inside the source lock
    #     to prevent concurrent callers from both attempting recovery commits.

    # 1. Discover candidates by scanning stub dir
    candidates: list[tuple[str, dict, Path]] = []  # (segment_id, stub, stub_path)
    seen_segment_ids: set[str] = set()  # Track IDs encountered for stub_not_found warnings

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

            seg_id = stub.get("segment_id", entry.stem)

            # Skip already cold-stored — with warning if explicitly requested
            if stub.get("cold_stored_at"):
                if segment_ids and seg_id in segment_ids:
                    warnings.append(_make_warning(
                        "segment_history_already_cold",
                        f"Segment is already cold-stored: {seg_id}",
                        segment_id=seg_id,
                    ))
                    seen_segment_ids.add(seg_id)
                continue

            # Filter by requested segment_ids
            if segment_ids:
                seen_segment_ids.add(seg_id)
                if seg_id not in segment_ids:
                    continue

            # Check cold eligibility
            elig_field = config.cold_eligibility_field
            summary = stub.get("summary", {})
            elig_value = summary.get(elig_field)
            if elig_value is None:
                warnings.append(_make_warning(
                    "segment_history_missing_cold_timestamp",
                    f"Stub summary field '{elig_field}' is null, skipping cold eligibility: {seg_id}",
                    segment_id=seg_id,
                ))
                continue

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
                if family == "journal":
                    warnings.append(_make_warning(
                        "segment_history_invalid_stub_summary",
                        f"Invalid '{elig_field}' value in stub summary: {seg_id}",
                        segment_id=seg_id,
                    ))
                continue

            candidates.append((seg_id, stub, entry))

    # Emit warnings for explicitly requested segment_ids that were not found
    if segment_ids:
        for req_id in segment_ids:
            if req_id not in seen_segment_ids:
                warnings.append(_make_warning(
                    "segment_history_stub_not_found",
                    f"No stub found for requested segment: {req_id}",
                    segment_id=req_id,
                ))

    # Sort by segment_id, apply batch limit
    candidates.sort(key=lambda x: x[0])
    effective_batch_limit = getattr(settings, "segment_history_batch_limit", 500)
    if batch_limit is not None:
        effective_batch_limit = min(batch_limit, effective_batch_limit)
    batch_limit_reached = len(candidates) > effective_batch_limit
    candidates = candidates[:effective_batch_limit]

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
    cold_store_source_rels: list[str] = []
    for c in candidates:
        src_path = c[1].get("source_path", c[0])
        cold_store_source_rels.append(src_path)
        sk = _derive_stream_key(family, src_path)
        lock_keys.append(f"segment_history:{family}:{sk}")

    lock_dir = repo_root / ".locks" / "segment_history"
    selection_count = 0
    cold_stored_ids: list[str] = []
    commit_paths: list[Path] = []
    created_cold_paths: list[Path] = []
    stub_rollback: list[tuple[Path, bytes | None]] = []
    hot_rollback: list[tuple[Path, bytes | None]] = []
    deferred_audit: list[dict[str, Any]] = []
    durable = True
    cleanup_durable = True
    committed_files: list[str] = []
    latest_commit: str | None = None

    try:
        with acquire_sorted_source_locks(lock_keys, lock_dir=lock_dir):
            # Reconcile manifest residue inside lock scope to prevent
            # concurrent callers from both attempting recovery commits.
            # Only reconcile if the manifest's sources overlap with our
            # locked set — prevents clobbering a concurrent operation's
            # manifest on non-overlapping sources.
            residue = _reconcile_manifest_residue(
                repo_root, family, "cold_store", gm,
                locked_source_paths=set(cold_store_source_rels),
            )
            if residue:
                warnings.append(_make_warning(
                    "segment_history_manifest_residue",
                    residue["warning"],
                ))

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
                    warnings.append(_make_warning(
                        "segment_history_missing_cold_timestamp",
                        f"Stub summary field '{elig_field}' is null under lock, skipping: {seg_id}",
                        segment_id=seg_id,
                    ))
                    continue
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
                v_cleanup_paths: list[str] = []
                for sid, s, sp in validated:
                    pp = s.get("payload_path", "")
                    cp = str(_cold_payload_path(repo_root / pp).relative_to(repo_root)) if pp else ""
                    v_target_paths.extend([cp, str(sp.relative_to(repo_root))])
                    # Hot payload should be removed after a successful
                    # commit — record it so reconciliation can clean up
                    # orphaned hot payloads after a crash.
                    if pp:
                        v_cleanup_paths.append(pp)
                write_manifest(
                    repo_root,
                    operation="cold_store",
                    family=family,
                    source_paths=v_source_paths,
                    segment_ids=v_seg_ids,
                    target_paths=v_target_paths,
                    cleanup_paths=v_cleanup_paths,
                )

            # Capture stub and hot-payload state for rollback
            rollback_stub_paths = [sp for _, _, sp in validated]
            rollback_hot_paths = [
                repo_root / s.get("payload_path", "")
                for _, s, _ in validated
                if s.get("payload_path")
            ]
            stub_rollback = _capture_rollback_state(rollback_stub_paths)
            hot_rollback = _capture_rollback_state(rollback_hot_paths)

            try:
                deferred_hot_deletes: list[tuple[Path, str]] = []

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
                    created_cold_paths.append(cold_path)

                    # Mutate stub: payload_path moves to cold location, add cold_stored_at
                    cold_rel = str(cold_path.relative_to(repo_root))
                    cold_stored_at = now.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                    updated_stub = _mutate_stub_cold(
                        stub, cold_rel, cold_stored_at
                    )
                    write_text_file(
                        stub_path,
                        json.dumps(updated_stub, ensure_ascii=False, indent=2),
                    )

                    cold_stored_ids.append(seg_id)
                    # Include hot_payload so git stages its deletion
                    commit_paths.extend([cold_path, stub_path, hot_payload])
                    # Defer hot payload deletion until all writes succeed
                    deferred_hot_deletes.append((hot_payload, seg_id))

                    # Defer audit — same reason as maintenance (F1 self-lock).
                    deferred_audit.append({
                        "family": family,
                        "segment_id": seg_id,
                        "source_path": stub.get("source_path", ""),
                        "cold_payload_path": cold_rel,
                        "warning_count": len(warnings),
                    })

                # Git commit cold payloads + mutated stubs first (without
                # deleting hot payloads) so that on crash the committed
                # state has both cold .gz and hot originals — no data loss.
                from app.git_locking import repository_mutation_lock

                if commit_paths:
                    msg = f"segment-history: cold-store {family} {selection_count}"
                    try:
                        with repository_mutation_lock(repo_root):
                            success = gm.commit_paths(commit_paths, msg)
                            if success:
                                latest_commit = gm.latest_commit()
                                committed_files = [
                                    str(p.relative_to(repo_root)) for p in commit_paths
                                ]
                            else:
                                durable = False
                                warnings.append(_make_warning(
                                    "segment_history_git_commit_failed",
                                    f"Git commit failed for cold-store; "
                                    f"at-risk segments: {cold_stored_ids}",
                                ))
                    except Exception:
                        durable = False
                        warnings.append(_make_warning(
                            "segment_history_git_commit_failed",
                            f"Git commit failed for cold-store; "
                            f"at-risk segments: {cold_stored_ids}",
                        ))

                # Delete hot payloads only after a durable commit ensures
                # the cold .gz and mutated stubs are recoverable.  If the
                # commit failed, hot payloads remain on disk as fallback.
                cleanup_durable = True
                if durable and deferred_hot_deletes:
                    hot_delete_paths: list[Path] = []
                    for hp, sid in deferred_hot_deletes:
                        try:
                            hp.unlink()
                            hot_delete_paths.append(hp)
                        except OSError:
                            cleanup_durable = False
                            warnings.append(_make_warning(
                                "segment_history_hot_payload_remove_failed",
                                f"Could not remove hot payload after cold-store: {sid}",
                                segment_id=sid,
                            ))
                    # Second commit to stage the hot-payload deletions.
                    if hot_delete_paths:
                        try:
                            with repository_mutation_lock(repo_root):
                                gm.commit_paths(
                                    hot_delete_paths,
                                    f"segment-history: remove hot payloads {family}",
                                )
                                latest_commit = gm.latest_commit()
                        except Exception:
                            cleanup_durable = False
                            warnings.append(_make_warning(
                                "segment_history_cleanup_commit_failed",
                                f"Git commit failed for hot-payload removal; "
                                f"hot files deleted from disk but not staged in git; "
                                f"affected segments: {cold_stored_ids}",
                            ))
            except Exception:
                # Rollback under lock: restore stubs and hot payloads, remove
                # cold .gz files, clean manifest.
                _restore_rollback_state(stub_rollback)
                _restore_rollback_state(hot_rollback)
                _remove_created_paths(created_cold_paths)
                remove_manifest(repo_root, family)
                raise

            # Remove manifest only when the commit succeeded; preserving
            # it on failure lets _reconcile_manifest_residue re-commit
            # orphaned files on the next invocation.
            if durable:
                remove_manifest(repo_root, family)

    except ManifestOccupied as exc:
        return {
            "ok": False,
            "operation": "segment_history_cold_store",
            "family": family,
            "error": {
                "code": exc.code,
                "detail": str(exc),
            },
        }
    except SegmentHistoryLockTimeout as exc:
        return {
            "ok": False,
            "operation": "segment_history_cold_store",
            "family": family,
            "error": {
                "code": "segment_history_source_lock_timeout",
                "detail": str(exc),
            },
        }

    # Emit deferred audit events after source lock release.
    for evt in deferred_audit:
        _emit_audit(audit, "segment_history_cold_store", evt)

    result = {
        "ok": True,
        "operation": "segment_history_cold_store",
        "family": family,
        "selection_count": selection_count,
        "cold_stored_count": len(cold_stored_ids),
        "batch_limit_reached": batch_limit_reached,
        "cold_segment_ids": cold_stored_ids,
        "durable": durable,
        "cleanup_durable": cleanup_durable,
        "committed_files": committed_files,
        "latest_commit": latest_commit,
        "warnings": warnings,
    }
    if not durable or not cleanup_durable:
        result["at_risk_segment_ids"] = cold_stored_ids
    return result


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
    from app.segment_history.locking import (
        SegmentHistoryLockTimeout,
        segment_history_source_lock,
    )

    if family not in FAMILIES:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "operation": "segment_history_cold_rehydrate",
                "family": family,
                "segment_id": segment_id,
                "error": {
                    "code": "segment_history_invalid_family",
                    "detail": f"Unknown family: {family}",
                },
            },
        )

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

    # Must be cold-stored — unless a rehydrate manifest exists for this
    # segment (non-durable prior attempt), in which case we proceed to
    # the under-lock reconciliation path.
    if not stub.get("cold_stored_at"):
        from app.segment_history.manifest import read_manifest as _pre_lock_read_mf
        try:
            _pre_mf = _pre_lock_read_mf(repo_root, family)
        except ValueError:
            _pre_mf = None
        has_rehydrate_manifest = (
            _pre_mf is not None
            and _pre_mf.get("operation") == "rehydrate"
            and segment_id in _pre_mf.get("segment_ids", [])
        )
        if not has_rehydrate_manifest:
            return _error_response(
                409,
                "segment_history_not_cold",
                "The requested segment is already hot and cannot be rehydrated.",
            )
        # Fall through to lock scope for manifest reconciliation.

    cold_payload_rel = stub.get("payload_path", "")
    cold_path = repo_root / cold_payload_rel

    if not cold_path.is_file() and stub.get("cold_stored_at"):
        return _error_response(
            409,
            "segment_history_cold_payload_missing",
            f"Cold payload file missing for segment: {segment_id}",
        )

    # Derive lock key from source_path via _derive_stream_key
    source_path_str = stub.get("source_path", "")
    sk = _derive_stream_key(family, source_path_str)
    lock_key = f"segment_history:{family}:{sk}"
    lock_dir = repo_root / ".locks" / "segment_history"

    try:
        with segment_history_source_lock(lock_key, lock_dir=lock_dir):
            # Re-read and re-validate stub under lock to prevent TOCTOU:
            # a concurrent rehydrate or cold-store could have mutated the
            # stub between the pre-lock read and lock acquisition.
            try:
                stub = json.loads(stub_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return _error_response(
                    409,
                    "segment_history_stub_unreadable",
                    f"Cannot read stub under lock for segment: {segment_id}",
                )

            if not stub.get("cold_stored_at"):
                # The stub says hot — but this may be a non-durable
                # rehydrate from a prior attempt where the commit failed
                # and the manifest was preserved.  If a rehydrate
                # manifest exists referencing this segment, attempt
                # reconciliation to commit the uncommitted state rather
                # than permanently blocking re-attempts with a 409.
                from app.segment_history.manifest import read_manifest as _read_mf_check
                try:
                    mf = _read_mf_check(repo_root, family)
                except ValueError:
                    mf = None
                if (
                    mf is not None
                    and mf.get("operation") == "rehydrate"
                    and segment_id in mf.get("segment_ids", [])
                ):
                    source_path_str_pre = stub.get("source_path", "")
                    recon = _reconcile_manifest_residue(
                        repo_root, family, "rehydrate", gm,
                        locked_source_paths={source_path_str_pre},
                    )
                    # Determine whether reconciliation actually committed
                    # the orphaned state.  If the commit failed, the
                    # manifest is preserved and the segment is NOT
                    # durably hot — report durable=False so the agent
                    # knows recovery is still needed.
                    recon_recovered = (
                        recon is not None
                        and "recovered" in recon.get("warning", "")
                    )
                    if recon:
                        warnings.append(_make_warning(
                            "segment_history_rehydrate_recovered"
                            if recon_recovered
                            else "segment_history_rehydrate_recovery_pending",
                            f"{'Recovered' if recon_recovered else 'Recovery pending for'} "
                            f"non-durable rehydrate for "
                            f"segment {segment_id} via manifest reconciliation",
                            segment_id=segment_id,
                        ))
                    hot_rel = stub.get("payload_path", "")
                    stub_rel = str(stub_path.relative_to(repo_root))
                    result_recon: dict[str, Any] = {
                        "ok": True,
                        "operation": "segment_history_cold_rehydrate",
                        "family": family,
                        "segment_id": segment_id,
                        "stub_path": stub_rel,
                        "cold_payload_path": None,
                        "rehydrated_payload_path": hot_rel,
                        "removed_cold_payload_path": None,
                        "mutated_stub_path": stub_rel,
                        "durable": recon_recovered,
                        "cleanup_durable": recon_recovered,
                        "committed_files": [],
                        "latest_commit": None,
                        "warnings": warnings,
                    }
                    if not recon_recovered:
                        result_recon["at_risk_segment_ids"] = [segment_id]
                    return result_recon
                return _error_response(
                    409,
                    "segment_history_not_cold",
                    "The requested segment is already hot and cannot be rehydrated.",
                )

            # Re-derive cold path from the authoritative under-lock stub
            cold_payload_rel = stub.get("payload_path", "")
            cold_path = repo_root / cold_payload_rel

            # Re-derive source_path from the authoritative under-lock stub
            source_path_str = stub.get("source_path", "")
            hot_path = _rehydrate_hot_path(family, segment_id, source_path_str, repo_root)

            # Check for pending batch residue under lock — prevents
            # TOCTOU where a manifest appears between pre-lock check
            # and lock acquisition.
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

            # Conflict check under lock: canonical hot target must not exist.
            # If the hot file exists but the stub still points to cold, this
            # is an orphaned hot file from a prior crash (crash between
            # hot-payload write and stub mutation).  Auto-clean the orphan
            # so the rehydrate can proceed instead of permanently blocking.
            if hot_path.is_file():
                if stub.get("cold_stored_at") and cold_path.is_file():
                    # Stub is still cold, cold payload intact — the orphan
                    # is safe to remove.
                    try:
                        hot_path.unlink()
                        _log.info(
                            "Removed orphaned hot file from prior crash: %s",
                            hot_path,
                        )
                        warnings.append(_make_warning(
                            "segment_history_orphaned_hot_removed",
                            f"Removed orphaned hot file from prior crash "
                            f"for segment: {segment_id}",
                            segment_id=segment_id,
                        ))
                    except OSError:
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
                                    "detail": "target rolled payload already exists "
                                              "and could not be removed",
                                },
                                "conflict_path": hot_rel,
                            },
                        )
                else:
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

            # Capture stub state for rollback before mutations begin.
            stub_rollback = _capture_rollback_state([stub_path])

            # Write crash-recovery manifest before mutations so that a
            # process crash mid-rehydrate leaves a signal for the next
            # rehydrate attempt to auto-clean the orphaned hot file
            # (see conflict check above).
            from app.segment_history.manifest import (
                ManifestOccupied as _ManifestOccupied,
                remove_manifest as _remove_rehydrate_manifest,
                write_manifest as _write_rehydrate_manifest,
            )

            hot_rel_planned = str(hot_path.relative_to(repo_root))
            try:
                _write_rehydrate_manifest(
                    repo_root,
                    operation="rehydrate",
                    family=family,
                    source_paths=[source_path_str],
                    segment_ids=[segment_id],
                    target_paths=[hot_rel_planned, str(stub_path.relative_to(repo_root))],
                    cleanup_paths=[cold_payload_rel],
                )
            except _ManifestOccupied as exc:
                return _error_response(
                    409, exc.code, str(exc),
                )

            # Write hot payload
            hot_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                write_bytes_file(hot_path, decompressed)

                # Mutate stub: payload_path back to hot, remove cold_stored_at
                hot_rel = str(hot_path.relative_to(repo_root))
                updated_stub = _mutate_stub_rehydrate(stub, hot_rel)
                write_text_file(
                    stub_path,
                    json.dumps(updated_stub, ensure_ascii=False, indent=2),
                )

                # Git commit hot payload + mutated stub first, keeping
                # the cold payload on disk so a crash after commit failure
                # leaves both cold and hot copies available for recovery.
                durable = True
                committed_files: list[str] = []
                latest_commit: str | None = None

                from app.git_locking import repository_mutation_lock

                commit_paths_list = [hot_path, stub_path]
                msg = f"segment-history: rehydrate {family} {segment_id}"
                try:
                    with repository_mutation_lock(repo_root):
                        success = gm.commit_paths(commit_paths_list, msg)
                        if success:
                            latest_commit = gm.latest_commit()
                            committed_files = [
                                str(p.relative_to(repo_root)) for p in commit_paths_list
                            ]
                        else:
                            durable = False
                            warnings.append(_make_warning(
                                "segment_history_git_commit_failed",
                                f"Git commit failed for rehydrate; "
                                f"at-risk segment: {segment_id}",
                                segment_id=segment_id,
                            ))
                except Exception:
                    durable = False
                    warnings.append(_make_warning(
                        "segment_history_git_commit_failed",
                        f"Git commit failed for rehydrate; "
                        f"at-risk segment: {segment_id}",
                        segment_id=segment_id,
                    ))

                # Remove cold payload only after durable commit ensures
                # the hot payload and mutated stub are recoverable.  If
                # the commit failed, cold payload remains as fallback.
                cold_removed = False
                cleanup_durable = True
                if durable:
                    try:
                        cold_path.unlink()
                        cold_removed = True
                    except OSError:
                        cleanup_durable = False
                        warnings.append(_make_warning(
                            "segment_history_cold_payload_remove_failed",
                            f"Could not remove cold payload after rehydrate: {segment_id}",
                            segment_id=segment_id,
                        ))
                    # Stage the cold-payload deletion in git.
                    if cold_removed:
                        try:
                            with repository_mutation_lock(repo_root):
                                gm.commit_paths(
                                    [cold_path],
                                    f"segment-history: remove cold payload {family} {segment_id}",
                                )
                                latest_commit = gm.latest_commit()
                        except Exception:
                            cleanup_durable = False
                            warnings.append(_make_warning(
                                "segment_history_cleanup_commit_failed",
                                f"Git commit failed for cold-payload removal; "
                                f"cold file deleted from disk but not staged in git; "
                                f"affected segment: {segment_id}",
                                segment_id=segment_id,
                            ))

                # Remove manifest after successful commit.  On commit
                # failure the manifest is preserved so the orphaned-hot
                # auto-cleanup fires on the next rehydrate attempt.
                if durable:
                    _remove_rehydrate_manifest(repo_root, family)
            except Exception:
                # Rollback: restore stub to pre-mutation state and remove
                # the hot payload that was created, so the segment doesn't
                # get stuck in an unrecoverable state.
                _restore_rollback_state(stub_rollback)
                _remove_created_paths([hot_path])
                _remove_rehydrate_manifest(repo_root, family)
                raise
    except SegmentHistoryLockTimeout as exc:
        return _error_response(
            409,
            "segment_history_source_lock_timeout",
            str(exc),
        )

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

    result = {
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
        "cleanup_durable": cleanup_durable,
        "committed_files": committed_files,
        "latest_commit": latest_commit,
        "warnings": warnings,
    }
    if not durable or not cleanup_durable:
        result["at_risk_segment_ids"] = [segment_id]
    return result
