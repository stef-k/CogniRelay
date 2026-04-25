"""SQLite-backed one-shot schedule and reminder service."""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar

from fastapi import HTTPException

from app.auth import AuthContext
from app.config import SCOPE_READ_FILES, SCOPE_WRITE_PROJECTS
from app.models import ContextRetrieveRequest, ContinuityReadRequest
from app.timestamps import format_iso, iso_now

SCHEDULE_DB_REL = "memory/schedule/schedule.db"
SCHEDULE_SCHEMA_VERSION = 1
SCHEDULE_SQLITE_BUSY_TIMEOUT_MS = 1000
SCHEDULE_SQLITE_LOCK_RETRIES = 2
SCHEDULE_SQLITE_LOCK_RETRY_DELAY_SECONDS = 0.1

_SCHEDULE_ID_RE = re.compile(r"^sched_[a-z0-9][a-z0-9_-]{0,57}$")
_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_KINDS = {"reminder", "task_nudge"}
_STATUSES = {"pending", "acknowledged", "done", "retired"}
_SUBJECT_KINDS = {"user", "peer", "thread", "task"}
_TERMINAL = {"acknowledged", "done", "retired"}
_LIST_QUERY_KEYS = {
    "status",
    "due",
    "task_id",
    "thread_id",
    "subject_kind",
    "subject_id",
    "include_retired",
    "limit",
    "offset",
}
_CREATE_KEYS = {"schedule_id", "idempotency_key", "kind", "title", "note", "due_at", "task_id", "thread_id", "subject_kind", "subject_id", "metadata"}
_PATCH_KEYS = {"expected_version", "kind", "title", "note", "due_at", "task_id", "thread_id", "subject_kind", "subject_id", "metadata"}
_ACK_KEYS = {"expected_version", "status", "reason"}
_RETIRE_KEYS = {"expected_version", "reason"}
_T = TypeVar("_T")


@dataclass(frozen=True)
class _ValidationFailure(Exception):
    detail: dict[str, str]


@dataclass(frozen=True)
class _StorageFailure(Exception):
    code: str


@dataclass(frozen=True)
class _Clock:
    now: datetime
    now_iso: str
    now_ts: int


def _detail(code: str, field: str, message: str) -> dict[str, str]:
    return {"code": code, "field": field, "message": message}


def _raise_validation(code: str, field: str, message: str) -> None:
    raise _ValidationFailure(_detail(code, field, message))


def _raise_http_validation(detail: dict[str, str]) -> None:
    raise HTTPException(status_code=422, detail=detail)


def _raise_http_storage(code: str) -> None:
    raise HTTPException(status_code=503, detail={"code": code, "warnings": [code]})


def _clock() -> _Clock:
    try:
        now = iso_now()
    except Exception as exc:
        raise _StorageFailure("schedule_clock_unavailable") from exc
    return _Clock(now=now, now_iso=format_iso(now), now_ts=int(now.timestamp()))


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _classify_sqlite_error(exc: sqlite3.Error, *, bootstrap: bool = False) -> str:
    msg = str(exc).lower()
    if "locked" in msg or "busy" in msg:
        return "schedule_db_locked"
    if "malformed" in msg or "corrupt" in msg or "not a database" in msg or isinstance(exc, sqlite3.DatabaseError) and "database disk image is malformed" in msg:
        return "schedule_db_corrupt"
    return "schedule_bootstrap_failed" if bootstrap else "schedule_db_unavailable"


def _rollback_quietly(conn: sqlite3.Connection | None) -> None:
    if conn is None:
        return
    try:
        conn.rollback()
    except sqlite3.Error:
        pass


def _run_db_operation(
    repo_root: Path,
    *,
    mutation: bool,
    operation: Callable[[sqlite3.Connection, list[str]], _T],
) -> _T:
    db_path = repo_root / SCHEDULE_DB_REL
    existed = db_path.exists()
    last_failure: _StorageFailure | None = None
    for attempt in range(SCHEDULE_SQLITE_LOCK_RETRIES + 1):
        conn: sqlite3.Connection | None = None
        try:
            conn = _connect_once(repo_root, existed=existed)
            warnings = ["schedule_db_missing"] if not existed else []
            return operation(conn, warnings)
        except _StorageFailure as exc:
            last_failure = exc
            if mutation:
                _rollback_quietly(conn)
            if exc.code == "schedule_db_locked" and attempt < SCHEDULE_SQLITE_LOCK_RETRIES:
                time.sleep(SCHEDULE_SQLITE_LOCK_RETRY_DELAY_SECONDS)
                continue
            raise
        except sqlite3.Error as exc:
            code = _classify_sqlite_error(exc)
            last_failure = _StorageFailure(code)
            if mutation:
                _rollback_quietly(conn)
            if code == "schedule_db_locked" and attempt < SCHEDULE_SQLITE_LOCK_RETRIES:
                time.sleep(SCHEDULE_SQLITE_LOCK_RETRY_DELAY_SECONDS)
                continue
            raise last_failure from exc
        finally:
            if conn is not None:
                conn.close()
    if last_failure is not None:
        raise last_failure
    raise _StorageFailure("schedule_db_unavailable")


def _parse_due_at(value: Any, field: str = "due_at") -> tuple[str, int]:
    if value is None:
        _raise_validation("invalid_schedule_payload", field, f"{field} cannot be null")
    if not isinstance(value, str):
        _raise_validation("invalid_schedule_due_at", field, "due_at must use YYYY-MM-DDTHH:MM:SSZ")
    raw = value.strip()
    if not _UTC_RE.fullmatch(raw):
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2})?", raw):
            _raise_validation("invalid_schedule_due_at", field, "due_at must be UTC in YYYY-MM-DDTHH:MM:SSZ form")
        _raise_validation("invalid_schedule_due_at", field, "due_at must use YYYY-MM-DDTHH:MM:SSZ")
    try:
        dt = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        _raise_validation("invalid_schedule_due_at", field, "due_at must use YYYY-MM-DDTHH:MM:SSZ")
    return raw, int(dt.timestamp())


def _trim_optional_string(
    payload: dict[str, Any],
    field: str,
    *,
    max_len: int,
    nullable: bool = True,
    min_len: int = 1,
    code: str = "invalid_schedule_payload",
    message_name: str | None = None,
    empty_to_null: bool = False,
) -> str | None:
    if field not in payload:
        return None
    value = payload[field]
    name = message_name or field
    if value is None:
        if nullable:
            return None
        _raise_validation("invalid_schedule_payload", field, f"{field} cannot be null")
    if not isinstance(value, str):
        _raise_validation(code, field, f"{name} must be {min_len}-{max_len} characters" if min_len else f"{name} must be at most {max_len} characters")
    out = value.strip()
    if empty_to_null and out == "":
        return None
    if len(out) < min_len or len(out) > max_len:
        if min_len == 1:
            _raise_validation(code, field, f"{name} must be 1-{max_len} characters")
        _raise_validation(code, field, f"{name} must be at most {max_len} characters")
    return out


def _trim_required_string(payload: dict[str, Any], field: str, *, max_len: int, code: str, message: str) -> str:
    if field not in payload:
        _raise_validation("invalid_schedule_payload", field, f"{field} is required")
    value = payload[field]
    if value is None:
        _raise_validation("invalid_schedule_payload", field, f"{field} cannot be null")
    if not isinstance(value, str):
        _raise_validation(code, field, message)
    out = value.strip()
    if not (1 <= len(out) <= max_len):
        _raise_validation(code, field, message)
    return out


def _validate_schedule_id(value: Any, *, required: bool = True) -> str | None:
    if value is None:
        if required:
            _raise_validation("invalid_schedule_payload", "schedule_id", "schedule_id is required")
        return None
    if not isinstance(value, str) or not _SCHEDULE_ID_RE.fullmatch(value.strip()):
        _raise_validation("invalid_schedule_id", "schedule_id", "schedule_id must match ^sched_[a-z0-9][a-z0-9_-]{0,57}$")
    return value.strip()


def _validate_metadata(value: Any, *, supplied: bool) -> dict[str, Any]:
    if not supplied or value is None:
        return {}
    if not isinstance(value, dict):
        _raise_validation("invalid_schedule_metadata", "metadata", "metadata must be a flat object with canonical JSON <= 2048 bytes")
    out: dict[str, Any] = {}
    for key, item in value.items():
        if not isinstance(key, str) or isinstance(item, (dict, list)):
            _raise_validation("invalid_schedule_metadata", "metadata", "metadata must be a flat object with canonical JSON <= 2048 bytes")
        if isinstance(item, float) and not math.isfinite(item):
            _raise_validation("invalid_schedule_metadata", "metadata", "metadata must be a flat object with canonical JSON <= 2048 bytes")
        if item is not None and not isinstance(item, (str, int, float, bool)):
            _raise_validation("invalid_schedule_metadata", "metadata", "metadata must be a flat object with canonical JSON <= 2048 bytes")
        out[key] = item
    try:
        raw = _canonical_json(out)
    except ValueError:
        _raise_validation("invalid_schedule_metadata", "metadata", "metadata must be a flat object with canonical JSON <= 2048 bytes")
    if len(raw.encode("utf-8")) > 2048:
        _raise_validation("invalid_schedule_metadata", "metadata", "metadata must be a flat object with canonical JSON <= 2048 bytes")
    return out


def _validate_materialized_metadata(metadata: Any) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        raise ValueError("metadata is not object")
    out: dict[str, Any] = {}
    for key, item in metadata.items():
        if not isinstance(key, str) or isinstance(item, (dict, list)):
            raise ValueError("metadata is not flat")
        if isinstance(item, float) and not math.isfinite(item):
            raise ValueError("metadata has non-finite number")
        if item is not None and not isinstance(item, (str, int, float, bool)):
            raise ValueError("metadata has invalid scalar")
        out[key] = item
    raw = _canonical_json(out)
    if len(raw.encode("utf-8")) > 2048:
        raise ValueError("metadata too large")
    return out


def _validate_subject(payload: dict[str, Any], *, prefix: str = "") -> tuple[str | None, str | None]:
    kind_field = f"{prefix}subject_kind" if prefix else "subject_kind"
    id_field = f"{prefix}subject_id" if prefix else "subject_id"
    has_kind = "subject_kind" in payload
    has_id = "subject_id" in payload
    kind = payload.get("subject_kind")
    subject_id = payload.get("subject_id")
    if kind is None:
        kind_out = None
    elif not isinstance(kind, str) or kind.strip() not in _SUBJECT_KINDS:
        _raise_validation("invalid_schedule_subject", kind_field, "subject_kind must be user, peer, thread, or task")
    else:
        kind_out = kind.strip()
    if subject_id is None:
        id_out = None
    elif not isinstance(subject_id, str):
        _raise_validation("invalid_schedule_payload", id_field, "subject_id must be 1-200 characters")
    else:
        id_out = subject_id.strip()
        if not (1 <= len(id_out) <= 200):
            _raise_validation("invalid_schedule_payload", id_field, "subject_id must be 1-200 characters")
    if (has_kind or has_id) and ((kind_out is None) != (id_out is None)):
        _raise_validation("invalid_schedule_subject", kind_field, "subject_kind and subject_id must be supplied or cleared together")
    return kind_out, id_out


def _reject_unknown_keys(payload: dict[str, Any], allowed: set[str]) -> None:
    for key in payload:
        if key not in allowed:
            _raise_validation("invalid_schedule_payload", key, f"unexpected field: {key}")


def _validate_actor(auth: AuthContext, field: str) -> str:
    actor = str(auth.peer_id or "").strip()
    if not (1 <= len(actor) <= 200):
        raise _StorageFailure("schedule_actor_invalid")
    return actor


def _connect_once(repo_root: Path, *, existed: bool) -> sqlite3.Connection:
    db_path = repo_root / SCHEDULE_DB_REL
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path), timeout=SCHEDULE_SQLITE_BUSY_TIMEOUT_MS / 1000)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={SCHEDULE_SQLITE_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        _bootstrap_schema(conn)
    except sqlite3.OperationalError as exc:
        code = _classify_sqlite_error(exc, bootstrap=not existed)
        if not existed and code == "schedule_db_unavailable":
            code = "schedule_bootstrap_failed"
        raise _StorageFailure(code) from exc
    except sqlite3.DatabaseError as exc:
        code = _classify_sqlite_error(exc, bootstrap=not existed)
        raise _StorageFailure(code) from exc
    except OSError as exc:
        raise _StorageFailure("schedule_bootstrap_failed" if not existed else "schedule_db_unavailable") from exc
    return conn


def _bootstrap_schema(conn: sqlite3.Connection) -> None:
    existing = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schedule_schema_migrations'"
    ).fetchone()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS schedule_schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scheduled_items (
            schedule_id TEXT PRIMARY KEY,
            kind TEXT NOT NULL CHECK (kind IN ('reminder', 'task_nudge')),
            status TEXT NOT NULL CHECK (status IN ('pending', 'acknowledged', 'done', 'retired')),
            title TEXT NOT NULL,
            note TEXT,
            due_at TEXT NOT NULL,
            due_at_ts INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            updated_by TEXT NOT NULL,
            terminal_at TEXT,
            terminal_by TEXT,
            terminal_reason TEXT,
            task_id TEXT,
            thread_id TEXT,
            subject_kind TEXT CHECK (subject_kind IS NULL OR subject_kind IN ('user', 'peer', 'thread', 'task')),
            subject_id TEXT,
            idempotency_key TEXT UNIQUE,
            create_identity_hash TEXT NOT NULL,
            create_identity_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            version INTEGER NOT NULL DEFAULT 1,
            CHECK (length(schedule_id) BETWEEN 7 AND 64),
            CHECK (length(title) BETWEEN 1 AND 160),
            CHECK (note IS NULL OR length(note) <= 1000),
            CHECK (length(created_by) BETWEEN 1 AND 200),
            CHECK (length(updated_by) BETWEEN 1 AND 200),
            CHECK (terminal_reason IS NULL OR length(terminal_reason) <= 500),
            CHECK (task_id IS NULL OR length(task_id) BETWEEN 1 AND 200),
            CHECK (thread_id IS NULL OR length(thread_id) BETWEEN 1 AND 200),
            CHECK ((subject_kind IS NULL AND subject_id IS NULL) OR (subject_kind IS NOT NULL AND subject_id IS NOT NULL)),
            CHECK (subject_id IS NULL OR length(subject_id) BETWEEN 1 AND 200),
            CHECK (idempotency_key IS NULL OR length(idempotency_key) BETWEEN 1 AND 200),
            CHECK (length(create_identity_hash) = 64),
            CHECK (length(create_identity_json) >= 2),
            CHECK (length(CAST(metadata_json AS BLOB)) <= 2048)
        );

        CREATE INDEX IF NOT EXISTS idx_scheduled_items_pending_due
            ON scheduled_items(due_at_ts, schedule_id)
            WHERE status = 'pending';
        CREATE INDEX IF NOT EXISTS idx_scheduled_items_status_due
            ON scheduled_items(status, due_at_ts, schedule_id);
        CREATE INDEX IF NOT EXISTS idx_scheduled_items_task_id
            ON scheduled_items(task_id)
            WHERE task_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_scheduled_items_thread_id
            ON scheduled_items(thread_id)
            WHERE thread_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_scheduled_items_subject
            ON scheduled_items(subject_kind, subject_id)
            WHERE subject_kind IS NOT NULL AND subject_id IS NOT NULL;
        """
    )
    max_row = conn.execute("SELECT MAX(version) AS version FROM schedule_schema_migrations").fetchone()
    max_version = int(max_row["version"] or 0)
    if max_version > SCHEDULE_SCHEMA_VERSION:
        raise _StorageFailure("schedule_schema_too_new")
    if max_version == 0:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO schedule_schema_migrations(version, applied_at) VALUES (?, ?)",
                (SCHEDULE_SCHEMA_VERSION, format_iso(iso_now())),
            )
            conn.commit()
        except sqlite3.Error as exc:
            raise _StorageFailure("schedule_migration_failed") from exc
    elif existing is None:
        conn.commit()


def _parse_stored_timestamp(value: Any, field: str) -> tuple[str, int]:
    if not isinstance(value, str) or not _UTC_RE.fullmatch(value):
        raise ValueError(f"{field} is not a UTC timestamp")
    try:
        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(f"{field} is invalid") from exc
    return value, int(dt.timestamp())


def _stored_optional_string(value: Any, field: str, *, max_len: int, min_len: int = 1) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} is not string")
    if not (min_len <= len(value.strip()) <= max_len):
        raise ValueError(f"{field} length invalid")
    return value


def _row_to_item(row: sqlite3.Row, now_ts: int) -> dict[str, Any]:
    schedule_id = row["schedule_id"]
    if not isinstance(schedule_id, str) or not _SCHEDULE_ID_RE.fullmatch(schedule_id):
        raise ValueError("schedule_id invalid")
    kind = row["kind"]
    if kind not in _KINDS:
        raise ValueError("kind invalid")
    status = row["status"]
    if status not in _STATUSES:
        raise ValueError("status invalid")
    due_at, due_at_ts = _parse_stored_timestamp(row["due_at"], "due_at")
    stored_due_at_ts = row["due_at_ts"]
    if isinstance(stored_due_at_ts, bool) or not isinstance(stored_due_at_ts, int) or stored_due_at_ts != due_at_ts:
        raise ValueError("due_at_ts invalid")
    created_at, _created_ts = _parse_stored_timestamp(row["created_at"], "created_at")
    updated_at, _updated_ts = _parse_stored_timestamp(row["updated_at"], "updated_at")
    title = row["title"]
    if not isinstance(title, str) or not (1 <= len(title.strip()) <= 160):
        raise ValueError("title invalid")
    note = _stored_optional_string(row["note"], "note", max_len=1000, min_len=0)
    created_by = _stored_optional_string(row["created_by"], "created_by", max_len=200)
    updated_by = _stored_optional_string(row["updated_by"], "updated_by", max_len=200)
    terminal_at = row["terminal_at"]
    terminal_by = _stored_optional_string(row["terminal_by"], "terminal_by", max_len=200)
    terminal_reason = _stored_optional_string(row["terminal_reason"], "terminal_reason", max_len=500, min_len=0)
    if status == "pending":
        if terminal_at is not None or terminal_by is not None or terminal_reason is not None:
            raise ValueError("pending row has terminal fields")
    else:
        if terminal_at is None or terminal_by is None:
            raise ValueError("terminal row missing terminal fields")
        terminal_at, _terminal_ts = _parse_stored_timestamp(terminal_at, "terminal_at")
    task_id = _stored_optional_string(row["task_id"], "task_id", max_len=200)
    thread_id = _stored_optional_string(row["thread_id"], "thread_id", max_len=200)
    subject_kind = row["subject_kind"]
    subject_id = _stored_optional_string(row["subject_id"], "subject_id", max_len=200)
    if subject_kind is not None and subject_kind not in _SUBJECT_KINDS:
        raise ValueError("subject_kind invalid")
    if (subject_kind is None) != (subject_id is None):
        raise ValueError("subject tuple invalid")
    idempotency_key = _stored_optional_string(row["idempotency_key"], "idempotency_key", max_len=200)
    try:
        metadata = _validate_materialized_metadata(json.loads(row["metadata_json"]))
    except (TypeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("metadata invalid") from exc
    version = row["version"]
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise ValueError("version invalid")
    if status in _TERMINAL:
        derived = "terminal"
    elif due_at_ts <= now_ts:
        derived = "due"
    else:
        derived = "scheduled"
    return {
        "schedule_id": schedule_id,
        "kind": kind,
        "status": status,
        "derived_state": derived,
        "title": row["title"],
        "note": note,
        "due_at": due_at,
        "created_at": created_at,
        "updated_at": updated_at,
        "created_by": created_by,
        "updated_by": updated_by,
        "terminal_at": terminal_at,
        "terminal_by": terminal_by,
        "terminal_reason": terminal_reason,
        "task_id": task_id,
        "thread_id": thread_id,
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "idempotency_key": idempotency_key,
        "metadata": metadata,
        "version": version,
    }


def _select_by_id(conn: sqlite3.Connection, schedule_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM scheduled_items WHERE schedule_id = ?", (schedule_id,)).fetchone()


def _create_identity(payload: dict[str, Any], created_by: str) -> tuple[str, str]:
    identity = {
        "kind": payload["kind"],
        "title": payload["title"],
        "note": payload.get("note"),
        "due_at": payload["due_at"],
        "task_id": payload.get("task_id"),
        "thread_id": payload.get("thread_id"),
        "subject_kind": payload.get("subject_kind"),
        "subject_id": payload.get("subject_id"),
        "idempotency_key": payload.get("idempotency_key"),
        "metadata": payload["metadata"],
        "created_by": created_by,
    }
    raw = _canonical_json(identity)
    return raw, hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _normalize_create(payload: dict[str, Any], actor: str) -> tuple[dict[str, Any], str, str]:
    _reject_unknown_keys(payload, _CREATE_KEYS)
    for field in ("kind", "title", "due_at"):
        if field not in payload:
            _raise_validation("invalid_schedule_payload", field, f"{field} is required")
    schedule_id = _validate_schedule_id(payload.get("schedule_id"), required=False)
    idem = _trim_optional_string(payload, "idempotency_key", max_len=200, code="invalid_schedule_payload", message_name="idempotency_key")
    kind = payload.get("kind")
    if kind is None:
        _raise_validation("invalid_schedule_payload", "kind", "kind cannot be null")
    if not isinstance(kind, str) or kind.strip() not in _KINDS:
        _raise_validation("invalid_schedule_kind", "kind", "kind must be reminder or task_nudge")
    title = _trim_required_string(payload, "title", max_len=160, code="invalid_schedule_title", message="title must be 1-160 characters")
    note = _trim_optional_string(payload, "note", max_len=1000, min_len=0, code="invalid_schedule_note", message_name="note")
    due_at, due_at_ts = _parse_due_at(payload.get("due_at"))
    task_id = _trim_optional_string(payload, "task_id", max_len=200, code="invalid_schedule_payload", message_name="task_id")
    thread_id = _trim_optional_string(payload, "thread_id", max_len=200, code="invalid_schedule_payload", message_name="thread_id")
    subject_kind, subject_id = _validate_subject(payload)
    metadata = _validate_metadata(payload.get("metadata"), supplied="metadata" in payload)
    if kind.strip() == "task_nudge" and not any([task_id, thread_id, subject_kind and subject_id]):
        _raise_validation("invalid_schedule_link", "kind", "task_nudge requires task_id, thread_id, or subject_kind plus subject_id")
    normalized = {
        "schedule_id": schedule_id,
        "idempotency_key": idem,
        "kind": kind.strip(),
        "title": title,
        "note": note,
        "due_at": due_at,
        "due_at_ts": due_at_ts,
        "task_id": task_id,
        "thread_id": thread_id,
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "metadata": metadata,
    }
    identity_json, identity_hash = _create_identity(normalized, actor)
    if schedule_id is None:
        normalized["schedule_id"] = "sched_" + identity_hash[:32]
    return normalized, identity_json, identity_hash


def _conflict(code: str, field: str, message: str) -> None:
    raise HTTPException(status_code=409, detail=_detail(code, field, message))


def schedule_create_service(*, repo_root: Path, auth: AuthContext, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """Create a one-shot scheduled item or return an exact idempotent replay."""
    auth.require(SCOPE_WRITE_PROJECTS)
    auth.require_write_path(SCHEDULE_DB_REL)
    try:
        clock = _clock()
        actor = _validate_actor(auth, "created_by")
        normalized, identity_json, identity_hash = _normalize_create(payload, actor)
    except _ValidationFailure as exc:
        _raise_http_validation(exc.detail)
    except _StorageFailure as exc:
        _raise_http_storage(exc.code)
    try:
        def _operation(conn: sqlite3.Connection, warnings: list[str]) -> tuple[int, dict[str, Any]]:
            schedule_id = normalized["schedule_id"]
            idempotency_key = normalized.get("idempotency_key")
            row_by_id = _select_by_id(conn, schedule_id)
            row_by_key = None
            if idempotency_key:
                row_by_key = conn.execute("SELECT * FROM scheduled_items WHERE idempotency_key = ?", (idempotency_key,)).fetchone()
            existing_rows = [row for row in (row_by_id, row_by_key) if row is not None]
            same_existing = existing_rows and all(row["schedule_id"] == existing_rows[0]["schedule_id"] for row in existing_rows)
            if same_existing and existing_rows[0]["create_identity_hash"] == identity_hash and existing_rows[0]["create_identity_json"] == identity_json:
                return 200, {"ok": True, "created": False, "item": _row_to_item(existing_rows[0], clock.now_ts), "warnings": warnings}
            if int(normalized["due_at_ts"]) <= clock.now_ts:
                _raise_http_validation(_detail("due_at_not_future", "due_at", "due_at must be in the future"))
            if row_by_id is not None and row_by_key is not None and row_by_id["schedule_id"] != row_by_key["schedule_id"]:
                _conflict("idempotency_key_conflict", "idempotency_key", "idempotency_key already exists for a different schedule item")
            if row_by_id is not None:
                _conflict("schedule_id_conflict", "schedule_id", "schedule_id already exists for a different schedule item")
            if row_by_key is not None:
                _conflict("idempotency_key_conflict", "idempotency_key", "idempotency_key already exists for a different schedule item")
            conn.execute(
                """
                INSERT INTO scheduled_items(
                    schedule_id, kind, status, title, note, due_at, due_at_ts,
                    created_at, updated_at, created_by, updated_by,
                    task_id, thread_id, subject_kind, subject_id, idempotency_key,
                    create_identity_hash, create_identity_json, metadata_json, version
                ) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    schedule_id,
                    normalized["kind"],
                    normalized["title"],
                    normalized["note"],
                    normalized["due_at"],
                    normalized["due_at_ts"],
                    clock.now_iso,
                    clock.now_iso,
                    actor,
                    actor,
                    normalized["task_id"],
                    normalized["thread_id"],
                    normalized["subject_kind"],
                    normalized["subject_id"],
                    idempotency_key,
                    identity_hash,
                    identity_json,
                    _canonical_json(normalized["metadata"]),
                ),
            )
            conn.commit()
            row = _select_by_id(conn, schedule_id)
            return 201, {"ok": True, "created": True, "item": _row_to_item(row, clock.now_ts), "warnings": warnings}

        return _run_db_operation(repo_root, mutation=True, operation=_operation)
    except _StorageFailure as exc:
        _raise_http_storage(exc.code)


def schedule_get_service(*, repo_root: Path, auth: AuthContext, schedule_id: str) -> dict[str, Any]:
    """Read one scheduled item by id, degrading for store failures."""
    auth.require(SCOPE_READ_FILES)
    auth.require_read_path(SCHEDULE_DB_REL)
    try:
        sid = _validate_schedule_id(schedule_id)
        clock = _clock()
    except _ValidationFailure as exc:
        _raise_http_validation(exc.detail)
    except _StorageFailure as exc:
        return {"ok": False, "item": None, "warnings": [exc.code]}
    try:
        def _operation(conn: sqlite3.Connection, warnings: list[str]) -> dict[str, Any]:
            row = _select_by_id(conn, sid)
            if row is None:
                raise HTTPException(status_code=404, detail={"code": "schedule_not_found", "schedule_id": sid, "warnings": warnings})
            try:
                return {"ok": True, "item": _row_to_item(row, clock.now_ts), "warnings": warnings}
            except Exception:
                return {"ok": False, "item": None, "warnings": [*warnings, f"schedule_row_invalid:{sid}"]}

        return _run_db_operation(repo_root, mutation=False, operation=_operation)
    except _StorageFailure as exc:
        return {"ok": False, "item": None, "warnings": [exc.code]}


def _parse_bool_query(value: Any, field: str, *, strict: bool = False) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and not strict:
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    _raise_validation("invalid_schedule_query", field, f"{field.split('.')[-1]} must be a boolean")


def _parse_int_query(value: Any, field: str, default: int, minimum: int, maximum: int, *, strict: bool = False) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        _raise_validation("invalid_schedule_query", field, f"{field.split('.')[-1]} must be an integer between {minimum} and {maximum}")
    if strict and not isinstance(value, int):
        _raise_validation("invalid_schedule_query", field, f"{field.split('.')[-1]} must be an integer between {minimum} and {maximum}")
    try:
        out = int(value)
    except Exception:
        _raise_validation("invalid_schedule_query", field, f"{field.split('.')[-1]} must be an integer between {minimum} and {maximum}")
    if not (minimum <= out <= maximum):
        _raise_validation("invalid_schedule_query", field, f"{field.split('.')[-1]} must be an integer between {minimum} and {maximum}")
    return out


def _normalize_list_query(query: dict[str, Any], *, strict_types: bool = False) -> dict[str, Any]:
    _reject_unknown_keys(query, _LIST_QUERY_KEYS)
    status = query.get("status")
    if status is not None and not isinstance(status, str):
        _raise_validation("invalid_schedule_status", "query.status", "status must be pending, acknowledged, done, or retired")
    if status is not None and status not in _STATUSES:
        _raise_validation("invalid_schedule_status", "query.status", "status must be pending, acknowledged, done, or retired")
    due = _parse_bool_query(query.get("due"), "query.due", strict=strict_types)
    task_id = query.get("task_id")
    if task_id is not None:
        if not isinstance(task_id, str):
            _raise_validation("invalid_schedule_query", "query.task_id", "task_id must be 1-200 characters")
        task_id = task_id.strip()
        if not (1 <= len(task_id) <= 200):
            _raise_validation("invalid_schedule_query", "query.task_id", "task_id must be 1-200 characters")
    thread_id = query.get("thread_id")
    if thread_id is not None:
        if not isinstance(thread_id, str):
            _raise_validation("invalid_schedule_query", "query.thread_id", "thread_id must be 1-200 characters")
        thread_id = thread_id.strip()
        if not (1 <= len(thread_id) <= 200):
            _raise_validation("invalid_schedule_query", "query.thread_id", "thread_id must be 1-200 characters")
    subject_kind = query.get("subject_kind")
    subject_id = query.get("subject_id")
    if subject_kind is not None and not isinstance(subject_kind, str):
        _raise_validation("invalid_schedule_subject", "query.subject_kind", "subject_kind must be user, peer, thread, or task")
    if subject_kind is not None and subject_kind not in _SUBJECT_KINDS:
        _raise_validation("invalid_schedule_subject", "query.subject_kind", "subject_kind must be user, peer, thread, or task")
    if subject_id is not None:
        if not isinstance(subject_id, str):
            _raise_validation("invalid_schedule_query", "query.subject_id", "subject_id must be 1-200 characters")
        subject_id = subject_id.strip()
        if not (1 <= len(subject_id) <= 200):
            _raise_validation("invalid_schedule_query", "query.subject_id", "subject_id must be 1-200 characters")
    if (subject_kind is None) != (subject_id is None):
        _raise_validation("invalid_schedule_subject", "query.subject_kind", "subject_kind and subject_id must be supplied or cleared together")
    include_retired = _parse_bool_query(query.get("include_retired"), "query.include_retired", strict=strict_types)
    limit = _parse_int_query(query.get("limit"), "query.limit", 50, 1, 200, strict=strict_types)
    offset = _parse_int_query(query.get("offset"), "query.offset", 0, 0, 10000, strict=strict_types)
    return {
        "status": status,
        "due": due,
        "task_id": task_id,
        "thread_id": thread_id,
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "include_retired": bool(include_retired) if include_retired is not None else False,
        "limit": limit,
        "offset": offset,
    }


def _filter_items(items: list[dict[str, Any]], query: dict[str, Any]) -> list[dict[str, Any]]:
    out = items
    if query["status"]:
        out = [item for item in out if item["status"] == query["status"]]
    elif not query["include_retired"]:
        out = [item for item in out if item["status"] != "retired"]
    if query["due"] is True:
        out = [item for item in out if item["status"] == "pending" and item["derived_state"] == "due"]
    if query["due"] is False:
        out = [item for item in out if item["status"] == "pending" and item["derived_state"] == "scheduled"]
    for field in ("task_id", "thread_id", "subject_kind", "subject_id"):
        if query[field] is not None:
            out = [item for item in out if item[field] == query[field]]
    return out


def schedule_list_service(*, repo_root: Path, auth: AuthContext, query: dict[str, Any]) -> dict[str, Any]:
    """List scheduled items with deterministic filtering and degraded reads."""
    auth.require(SCOPE_READ_FILES)
    auth.require_read_path(SCHEDULE_DB_REL)
    try:
        normalized = _normalize_list_query(query)
        clock = _clock()
    except _ValidationFailure as exc:
        _raise_http_validation(exc.detail)
    except _StorageFailure as exc:
        return {"ok": False, "count": 0, "total": 0, "limit": 50, "offset": 0, "items": [], "warnings": [exc.code]}
    try:
        def _operation(conn: sqlite3.Connection, warnings: list[str]) -> dict[str, Any]:
            rows = conn.execute("SELECT * FROM scheduled_items ORDER BY due_at_ts ASC, schedule_id ASC").fetchall()
            items: list[dict[str, Any]] = []
            skipped = False
            for row in rows:
                try:
                    items.append(_row_to_item(row, clock.now_ts))
                except Exception:
                    skipped = True
                    sid = row["schedule_id"] if "schedule_id" in row.keys() and row["schedule_id"] else None
                    warnings.append(f"schedule_row_invalid:{sid}" if sid else "schedule_row_invalid")
            if skipped:
                warnings.append("schedule_rows_skipped")
            filtered = _filter_items(items, normalized)
            total = len(filtered)
            returned = filtered[normalized["offset"] : normalized["offset"] + normalized["limit"]]
            return {
                "ok": True,
                "count": len(returned),
                "total": total,
                "limit": normalized["limit"],
                "offset": normalized["offset"],
                "items": returned,
                "warnings": warnings,
            }

        return _run_db_operation(repo_root, mutation=False, operation=_operation)
    except _StorageFailure as exc:
        return {"ok": False, "count": 0, "total": 0, "limit": normalized["limit"], "offset": normalized["offset"], "items": [], "warnings": [exc.code]}


def _normalize_expected_version(value: Any, *, required: bool) -> int | None:
    if value is None:
        if required:
            _raise_validation("invalid_schedule_version", "expected_version", "expected_version must be a positive integer")
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        _raise_validation("invalid_schedule_version", "expected_version", "expected_version must be a positive integer")
    return value


def _normalize_patch(payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    _reject_unknown_keys(payload, _PATCH_KEYS)
    expected = _normalize_expected_version(payload.get("expected_version"), required=True)
    mutable_keys = set(payload) - {"expected_version"}
    if not mutable_keys:
        _raise_validation("invalid_schedule_payload", "body", "patch must include at least one mutable field")
    normalized: dict[str, Any] = {}
    if "kind" in payload:
        if payload["kind"] is None:
            _raise_validation("invalid_schedule_payload", "kind", "kind cannot be null")
        if not isinstance(payload["kind"], str) or payload["kind"].strip() not in _KINDS:
            _raise_validation("invalid_schedule_kind", "kind", "kind must be reminder or task_nudge")
        normalized["kind"] = payload["kind"].strip()
    if "title" in payload:
        normalized["title"] = _trim_required_string(payload, "title", max_len=160, code="invalid_schedule_title", message="title must be 1-160 characters")
    if "note" in payload:
        normalized["note"] = _trim_optional_string(payload, "note", max_len=1000, min_len=0, code="invalid_schedule_note", message_name="note")
    if "due_at" in payload:
        due_at, due_at_ts = _parse_due_at(payload["due_at"])
        normalized["due_at"] = due_at
        normalized["due_at_ts"] = due_at_ts
    if "task_id" in payload:
        normalized["task_id"] = _trim_optional_string(payload, "task_id", max_len=200, code="invalid_schedule_payload", message_name="task_id")
    if "thread_id" in payload:
        normalized["thread_id"] = _trim_optional_string(payload, "thread_id", max_len=200, code="invalid_schedule_payload", message_name="thread_id")
    if "subject_kind" in payload or "subject_id" in payload:
        normalized["subject_kind"], normalized["subject_id"] = _validate_subject(payload)
    if "metadata" in payload:
        normalized["metadata_json"] = _canonical_json(_validate_metadata(payload.get("metadata"), supplied=True))
    return expected or 0, normalized


def _status_conflict() -> None:
    _conflict("schedule_status_conflict", "status", "schedule status does not allow this transition")


def schedule_update_service(*, repo_root: Path, auth: AuthContext, schedule_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Patch mutable fields on a pending scheduled item."""
    auth.require(SCOPE_WRITE_PROJECTS)
    auth.require_write_path(SCHEDULE_DB_REL)
    try:
        sid = _validate_schedule_id(schedule_id)
        clock = _clock()
        actor = _validate_actor(auth, "updated_by")
        expected, changes = _normalize_patch(payload)
    except _ValidationFailure as exc:
        _raise_http_validation(exc.detail)
    except _StorageFailure as exc:
        _raise_http_storage(exc.code)
    try:
        def _operation(conn: sqlite3.Connection, warnings: list[str]) -> dict[str, Any]:
            row = _select_by_id(conn, sid)
            if row is None:
                raise HTTPException(status_code=404, detail={"code": "schedule_not_found", "schedule_id": sid, "warnings": warnings})
            if int(row["version"]) != expected:
                _conflict("schedule_version_conflict", "expected_version", "expected_version does not match current version")
            if row["status"] != "pending":
                _status_conflict()
            if "due_at_ts" in changes and int(changes["due_at_ts"]) <= clock.now_ts:
                _raise_http_validation(_detail("due_at_not_future", "due_at", "due_at must be in the future"))
            candidate = {key: row[key] for key in row.keys()}
            candidate.update(changes)
            if candidate.get("kind") == "task_nudge" and not any([candidate.get("task_id"), candidate.get("thread_id"), candidate.get("subject_kind") and candidate.get("subject_id")]):
                _raise_http_validation(_detail("invalid_schedule_link", "kind", "task_nudge requires task_id, thread_id, or subject_kind plus subject_id"))
            changed = any(row[key] != value for key, value in changes.items())
            if not changed:
                return {"ok": True, "updated": False, "item": _row_to_item(row, clock.now_ts), "warnings": warnings}
            assignments = [f"{key} = ?" for key in changes]
            values = list(changes.values())
            assignments.extend(["updated_at = ?", "updated_by = ?", "version = version + 1"])
            values.extend([clock.now_iso, actor, sid])
            conn.execute(f"UPDATE scheduled_items SET {', '.join(assignments)} WHERE schedule_id = ?", values)
            conn.commit()
            return {"ok": True, "updated": True, "item": _row_to_item(_select_by_id(conn, sid), clock.now_ts), "warnings": warnings}

        return _run_db_operation(repo_root, mutation=True, operation=_operation)
    except _StorageFailure as exc:
        _raise_http_storage(exc.code)


def _normalize_reason(payload: dict[str, Any]) -> str | None:
    return _trim_optional_string(payload, "reason", max_len=500, min_len=0, code="invalid_schedule_reason", message_name="reason", empty_to_null=True)


def _normalize_ack(payload: dict[str, Any]) -> tuple[int | None, str, str | None]:
    _reject_unknown_keys(payload, _ACK_KEYS)
    expected = _normalize_expected_version(payload.get("expected_version"), required=False)
    status = payload.get("status", "acknowledged")
    if status is None:
        _raise_validation("invalid_schedule_payload", "status", "status cannot be null")
    if not isinstance(status, str) or status.strip() not in {"acknowledged", "done"}:
        _raise_validation("invalid_schedule_status", "status", "status must be acknowledged or done")
    return expected, status.strip(), _normalize_reason(payload)


def schedule_acknowledge_service(*, repo_root: Path, auth: AuthContext, schedule_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Mark a pending item acknowledged or done."""
    auth.require(SCOPE_WRITE_PROJECTS)
    auth.require_write_path(SCHEDULE_DB_REL)
    try:
        sid = _validate_schedule_id(schedule_id)
        clock = _clock()
        actor = _validate_actor(auth, "updated_by")
        expected, target_status, reason = _normalize_ack(payload)
    except _ValidationFailure as exc:
        _raise_http_validation(exc.detail)
    except _StorageFailure as exc:
        _raise_http_storage(exc.code)
    try:
        def _operation(conn: sqlite3.Connection, warnings: list[str]) -> dict[str, Any]:
            row = _select_by_id(conn, sid)
            if row is None:
                raise HTTPException(status_code=404, detail={"code": "schedule_not_found", "schedule_id": sid, "warnings": warnings})
            if row["status"] == "pending":
                if expected is None:
                    _raise_http_validation(_detail("invalid_schedule_version", "expected_version", "expected_version must be a positive integer"))
                if int(row["version"]) != expected:
                    _conflict("schedule_version_conflict", "expected_version", "expected_version does not match current version")
            else:
                if expected is not None and int(row["version"]) != expected:
                    _conflict("schedule_version_conflict", "expected_version", "expected_version does not match current version")
                if row["status"] == target_status and row["terminal_reason"] == reason:
                    return {"ok": True, "updated": False, "item": _row_to_item(row, clock.now_ts), "warnings": warnings}
                _status_conflict()
            conn.execute(
                """
                UPDATE scheduled_items
                SET status = ?, terminal_at = ?, terminal_by = ?, terminal_reason = ?,
                    updated_at = ?, updated_by = ?, version = version + 1
                WHERE schedule_id = ?
                """,
                (target_status, clock.now_iso, actor, reason, clock.now_iso, actor, sid),
            )
            conn.commit()
            return {"ok": True, "updated": True, "item": _row_to_item(_select_by_id(conn, sid), clock.now_ts), "warnings": warnings}

        return _run_db_operation(repo_root, mutation=True, operation=_operation)
    except _StorageFailure as exc:
        _raise_http_storage(exc.code)


def _normalize_retire(payload: dict[str, Any]) -> tuple[int | None, str | None]:
    _reject_unknown_keys(payload, _RETIRE_KEYS)
    return _normalize_expected_version(payload.get("expected_version"), required=False), _normalize_reason(payload)


def schedule_retire_service(*, repo_root: Path, auth: AuthContext, schedule_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Retire an item without deleting it."""
    auth.require(SCOPE_WRITE_PROJECTS)
    auth.require_write_path(SCHEDULE_DB_REL)
    try:
        sid = _validate_schedule_id(schedule_id)
        clock = _clock()
        actor = _validate_actor(auth, "updated_by")
        expected, reason = _normalize_retire(payload)
    except _ValidationFailure as exc:
        _raise_http_validation(exc.detail)
    except _StorageFailure as exc:
        _raise_http_storage(exc.code)
    try:
        def _operation(conn: sqlite3.Connection, warnings: list[str]) -> dict[str, Any]:
            row = _select_by_id(conn, sid)
            if row is None:
                raise HTTPException(status_code=404, detail={"code": "schedule_not_found", "schedule_id": sid, "warnings": warnings})
            if row["status"] == "retired":
                if expected is not None and int(row["version"]) != expected:
                    _conflict("schedule_version_conflict", "expected_version", "expected_version does not match current version")
                if row["terminal_reason"] == reason:
                    return {"ok": True, "updated": False, "item": _row_to_item(row, clock.now_ts), "warnings": warnings}
                _status_conflict()
            if expected is None:
                _raise_http_validation(_detail("invalid_schedule_version", "expected_version", "expected_version must be a positive integer"))
            if int(row["version"]) != expected:
                _conflict("schedule_version_conflict", "expected_version", "expected_version does not match current version")
            conn.execute(
                """
                UPDATE scheduled_items
                SET status = 'retired', terminal_at = ?, terminal_by = ?, terminal_reason = ?,
                    updated_at = ?, updated_by = ?, version = version + 1
                WHERE schedule_id = ?
                """,
                (clock.now_iso, actor, reason, clock.now_iso, actor, sid),
            )
            conn.commit()
            return {"ok": True, "updated": True, "item": _row_to_item(_select_by_id(conn, sid), clock.now_ts), "warnings": warnings}

        return _run_db_operation(repo_root, mutation=True, operation=_operation)
    except _StorageFailure as exc:
        _raise_http_storage(exc.code)


def _scope_matches(item: dict[str, Any], scopes: set[tuple[str, str]]) -> bool:
    for kind, subject_id in scopes:
        if item["subject_kind"] == kind and item["subject_id"] == subject_id:
            return True
        if kind == "thread" and item["thread_id"] == subject_id:
            return True
        if kind == "task" and item["task_id"] == subject_id:
            return True
    return False


def _empty_context(window_hours: int, warnings: list[str] | None = None) -> dict[str, Any]:
    return {
        "due": {"items": [], "count": 0, "truncated": False},
        "upcoming": {"window_hours": window_hours, "items": [], "count": 0, "truncated": False},
        "warnings": list(warnings or []),
    }


def _schedule_context(repo_root: Path, auth: AuthContext, scopes: set[tuple[str, str]], *, due_limit: int, upcoming_limit: int, upcoming_window_hours: int) -> dict[str, Any]:
    if not scopes:
        return _empty_context(upcoming_window_hours)
    try:
        auth.require_read_path(SCHEDULE_DB_REL)
        clock = _clock()
        def _operation(conn: sqlite3.Connection, warnings: list[str]) -> dict[str, Any]:
            rows = conn.execute("SELECT * FROM scheduled_items WHERE status = 'pending' ORDER BY due_at_ts ASC, schedule_id ASC").fetchall()
            due: list[dict[str, Any]] = []
            upcoming: list[dict[str, Any]] = []
            skipped = False
            end_ts = clock.now_ts + upcoming_window_hours * 3600
            for row in rows:
                try:
                    item = _row_to_item(row, clock.now_ts)
                except Exception:
                    skipped = True
                    sid = row["schedule_id"] if "schedule_id" in row.keys() and row["schedule_id"] else None
                    warnings.append(f"schedule_row_invalid:{sid}" if sid else "schedule_row_invalid")
                    continue
                if not _scope_matches(item, scopes):
                    continue
                if int(row["due_at_ts"]) <= clock.now_ts:
                    due.append(item)
                elif int(row["due_at_ts"]) <= end_ts:
                    upcoming.append(item)
            if skipped:
                warnings.append("schedule_rows_skipped")
            return {
                "due": {"items": due[:due_limit], "count": min(len(due), due_limit), "truncated": len(due) > due_limit},
                "upcoming": {
                    "window_hours": upcoming_window_hours,
                    "items": upcoming[:upcoming_limit],
                    "count": min(len(upcoming), upcoming_limit),
                    "truncated": len(upcoming) > upcoming_limit,
                },
                "warnings": warnings,
            }

        return _run_db_operation(repo_root, mutation=False, operation=_operation)
    except _StorageFailure as exc:
        return _empty_context(upcoming_window_hours, [exc.code])
    except Exception:
        return _empty_context(upcoming_window_hours, ["schedule_db_unavailable"])


def schedule_context_for_startup_read(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContinuityReadRequest,
    due_limit: int,
    upcoming_limit: int,
    upcoming_window_hours: int,
) -> dict[str, Any]:
    """Return scoped schedule context for startup continuity reads."""
    scopes = {(req.subject_kind, req.subject_id)} if req.subject_kind and req.subject_id else set()
    return _schedule_context(repo_root, auth, scopes, due_limit=due_limit, upcoming_limit=upcoming_limit, upcoming_window_hours=upcoming_window_hours)


def schedule_context_for_context_retrieve(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: ContextRetrieveRequest,
    due_limit: int,
    upcoming_limit: int,
    upcoming_window_hours: int,
) -> dict[str, Any]:
    """Return scoped schedule context for context retrieval."""
    scopes: set[tuple[str, str]] = set()
    if req.subject_kind and req.subject_id:
        scopes.add((req.subject_kind, req.subject_id))
    for selector in req.continuity_selectors:
        if selector.subject_kind and selector.subject_id:
            scopes.add((selector.subject_kind, selector.subject_id))
    return _schedule_context(repo_root, auth, scopes, due_limit=due_limit, upcoming_limit=upcoming_limit, upcoming_window_hours=upcoming_window_hours)


def validate_schedule_mcp_arguments(name: str, arguments: dict[str, Any]) -> dict[str, str] | None:
    """Return a schedule validation detail for MCP static argument failures."""
    try:
        if name == "schedule.create":
            _normalize_create(arguments, "mcp-validation")
        elif name == "schedule.get":
            _reject_unknown_keys(arguments, {"schedule_id"})
            _validate_schedule_id(arguments.get("schedule_id"))
        elif name == "schedule.list":
            _normalize_list_query(arguments, strict_types=True)
        elif name == "schedule.update":
            _reject_unknown_keys(arguments, {"schedule_id"} | _PATCH_KEYS)
            _validate_schedule_id(arguments.get("schedule_id"))
            body = dict(arguments)
            body.pop("schedule_id", None)
            _normalize_patch(body)
        elif name == "schedule.acknowledge":
            _reject_unknown_keys(arguments, {"schedule_id"} | _ACK_KEYS)
            _validate_schedule_id(arguments.get("schedule_id"))
            body = dict(arguments)
            body.pop("schedule_id", None)
            _normalize_ack(body)
        elif name == "schedule.retire":
            _reject_unknown_keys(arguments, {"schedule_id"} | _RETIRE_KEYS)
            _validate_schedule_id(arguments.get("schedule_id"))
            body = dict(arguments)
            body.pop("schedule_id", None)
            _normalize_retire(body)
    except _ValidationFailure as exc:
        return exc.detail
    return None
