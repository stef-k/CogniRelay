"""Messaging, relay, replay, and delivery-state business logic."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException

from app.auth import AuthContext
from app.config import DEFAULT_MAX_JSONL_READ_BYTES
from app.git_safety import try_commit_paths
from app.models import MessageAckRequest, MessageReplayRequest, MessageSendRequest, RelayForwardRequest
from app.segment_history.append import SegmentHistoryAppendError, locked_append_jsonl, locked_append_jsonl_multi
from app.segment_history.locking import LockInfrastructureError, SegmentHistoryLockTimeout, segment_history_source_lock
from app.storage import safe_path, write_bytes_file, write_text_file
from app.timestamps import format_iso, iso_now

DELIVERY_STATE_REL = "messages/state/delivery_index.json"

_logger = logging.getLogger(__name__)


def _empty_state() -> dict[str, Any]:
    """Return a fresh empty delivery state dict."""
    return {"version": "1", "records": {}, "idempotency": {}}


def _delivery_state_path(repo_root: Path) -> Path:
    """Return the repository path for delivery-state persistence."""
    return safe_path(repo_root, DELIVERY_STATE_REL)


def load_delivery_state(repo_root: Path) -> dict[str, Any]:
    """Load normalized delivery state from disk.

    Returns a dict with keys ``version``, ``records``, ``idempotency``, and
    optionally ``warnings`` when the on-disk state could not be loaded cleanly.

    Logs at WARNING for parse/structural issues and at ERROR with traceback
    for unexpected exceptions (e.g. PermissionError, OSError).
    """
    path = _delivery_state_path(repo_root)
    if not path.exists():
        return _empty_state()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, ValueError):
        _logger.warning("Corrupt delivery state at %s; returning empty state", path)
        state = _empty_state()
        state["warnings"] = ["delivery_state_corrupt: parse error; state reset to empty"]
        return state
    except Exception:  # noqa: BLE001 — intentionally broad for mission-critical degradation
        _logger.error("Unexpected error reading delivery state at %s", path, exc_info=True)
        state = _empty_state()
        state["warnings"] = ["delivery_state_unreadable: unexpected error; state reset to empty"]
        return state
    if not isinstance(data, dict):
        _logger.warning("Delivery state at %s is not a JSON object; returning empty state", path)
        state = _empty_state()
        state["warnings"] = ["delivery_state_corrupt: expected JSON object; state reset to empty"]
        return state
    warnings: list[str] = []
    records = data.get("records")
    idempotency = data.get("idempotency")
    if not isinstance(records, dict):
        _logger.warning("Delivery state at %s has non-dict 'records'; resetting to empty", path)
        warnings.append("delivery_state_partial_corrupt: 'records' was not a dict; reset to empty")
        records = {}
    if not isinstance(idempotency, dict):
        _logger.warning("Delivery state at %s has non-dict 'idempotency'; resetting to empty", path)
        warnings.append("delivery_state_partial_corrupt: 'idempotency' was not a dict; reset to empty")
        idempotency = {}
    result: dict[str, Any] = {"version": "1", "records": records, "idempotency": idempotency}
    if warnings:
        result["warnings"] = warnings
    return result


def _write_delivery_state(repo_root: Path, state: dict[str, Any]) -> Path:
    """Persist the delivery-state file, including only durable keys (version, records, idempotency)."""
    path = _delivery_state_path(repo_root)
    # Allowlist: update this tuple when adding new persistent keys to the schema.
    persist = {k: state[k] for k in ("version", "records", "idempotency") if k in state}
    write_text_file(path, json.dumps(persist, ensure_ascii=False, indent=2))
    return path


def _capture_rollback_plan(paths: list[Path]) -> list[tuple[Path, bytes | None]]:
    """Capture prior bytes for state-style files in first-seen order."""
    rollback_plan: list[tuple[Path, bytes | None]] = []
    seen: set[Path] = set()
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        rollback_plan.append((path, path.read_bytes() if path.exists() else None))
    return rollback_plan


def _restore_rollback_plan(rollback_plan: list[tuple[Path, bytes | None]]) -> None:
    """Best-effort restore for multi-step message mutations before commit."""
    for path, old_bytes in rollback_plan:
        try:
            if old_bytes is None:
                path.unlink(missing_ok=True)
            else:
                write_bytes_file(path, old_bytes)
        except Exception:  # noqa: BLE001 - preserve the original write failure
            _logger.exception("Failed to restore %s during message rollback", path)


def _capture_append_targets(paths: list[Path]) -> list[tuple[Path, int, bool]]:
    """Capture append rollback metadata without reading full file contents."""
    targets: list[tuple[Path, int, bool]] = []
    seen: set[Path] = set()
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        is_new = not path.exists()
        prior_size = 0 if is_new else path.stat().st_size
        targets.append((path, prior_size, is_new))
    return targets


def _restore_appends(targets: list[tuple[Path, int, bool]]) -> None:
    """Best-effort rollback for append-only files after a later write fails."""
    for path, prior_size, is_new in targets:
        try:
            if is_new:
                path.unlink(missing_ok=True)
            else:
                with path.open("r+b") as handle:
                    handle.truncate(prior_size)
                    handle.flush()
                    os.fsync(handle.fileno())
        except Exception:  # noqa: BLE001 - preserve original exception
            _logger.exception("Failed to restore appended file %s during message rollback", path)


def _non_durable_warning(operation: str) -> str:
    """Return the standard warning for degrade-safe git durability failures."""
    return f"{operation}_not_durable: data is on disk but not durably committed to git"


def effective_delivery_status(record: dict[str, Any], now: datetime, *, parse_iso: Callable[[str | None], datetime | None]) -> str:
    """Return the effective delivery status for a record at the given time."""
    status = str(record.get("status") or "pending_ack")
    if status != "pending_ack":
        return status
    ack_deadline = parse_iso(record.get("ack_deadline"))
    if not ack_deadline:
        return status
    if now > ack_deadline:
        return "dead_letter"
    return status


def delivery_record_view(record: dict[str, Any], now: datetime, *, parse_iso: Callable[[str | None], datetime | None]) -> dict[str, Any]:
    """Return a delivery record with the computed effective status attached."""
    out = dict(record)
    out["effective_status"] = effective_delivery_status(record, now, parse_iso=parse_iso)
    return out


def _idempotency_scope_key(sender: str, recipient: str, idempotency_key: str) -> str:
    """Build the stable idempotency key used for send deduplication."""
    return f"{sender}|{recipient}|{idempotency_key}"


def messages_send_service(
    *,
    settings: Any,
    gm: Any,
    auth: AuthContext,
    req: MessageSendRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    verify_signed_payload: Callable[..., dict[str, Any]],
    verification_failure_count: Callable[[Any, AuthContext], int],
    record_verification_failure: Callable[[Any, AuthContext, str], None],
    parse_iso: Callable[[str | None], datetime | None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """Persist a message, optional signature verification, and delivery tracking state."""
    enforce_rate_limit(settings, auth, "messages_send")
    enforce_payload_limit(settings, req.model_dump(), "messages_send")
    auth.require("write:messages")
    auth.require_write_path("messages/inbox/x.jsonl")
    auth.require_write_path(DELIVERY_STATE_REL)

    if settings.require_signed_ingress and req.signed_envelope is None:
        raise HTTPException(status_code=400, detail="signed_envelope is required when strict signed ingress is enabled")

    try:
        with segment_history_source_lock("registry:delivery_index", lock_dir=settings.repo_root / ".locks"):
            state = load_delivery_state(settings.repo_root)
            if req.idempotency_key:
                idem_key = _idempotency_scope_key(req.sender, req.recipient, req.idempotency_key)
                existing_id = state.get("idempotency", {}).get(idem_key)
                if existing_id:
                    existing = state.get("records", {}).get(existing_id)
                    if isinstance(existing, dict):
                        verification = None
                        if req.signed_envelope is not None:
                            signed_payload = {
                                "thread_id": req.thread_id,
                                "sender": req.sender,
                                "recipient": req.recipient,
                                "subject": req.subject,
                                "body_md": req.body_md,
                                "priority": req.priority,
                                "attachments": req.attachments,
                                "idempotency_key": req.idempotency_key,
                                "delivery": req.delivery.model_dump(),
                            }
                            verification = verify_signed_payload(
                                settings=settings,
                                gm=gm,
                                auth=auth,
                                payload=signed_payload,
                                key_id=req.signed_envelope.key_id,
                                nonce=req.signed_envelope.nonce,
                                expires_at=req.signed_envelope.expires_at,
                                signature=req.signed_envelope.signature,
                                algorithm=req.signed_envelope.algorithm,
                                consume_nonce=False,
                                audit_event="messages_send_signature",
                                verification_failure_count=verification_failure_count,
                                record_verification_failure=record_verification_failure,
                                audit=audit,
                            )
                            if not verification["valid"]:
                                raise HTTPException(status_code=401, detail=f"Invalid signed envelope: {verification['reason']}")

                        now = datetime.now(timezone.utc)
                        audit(auth, "message_send_idempotent_replay", {"idempotency_key": req.idempotency_key, "message_id": existing_id})
                        early_result: dict[str, Any] = {
                            "ok": True,
                            "idempotent_replay": True,
                            "message": existing.get("message"),
                            "delivery_state": delivery_record_view(existing, now, parse_iso=parse_iso),
                            "signature_verification": verification,
                            "committed_files": [],
                            "latest_commit": gm.latest_commit(),
                        }
                        if state.get("warnings"):
                            early_result["warnings"] = state["warnings"]
                        return early_result

            signature_verification = None
            committed_files: list[str] = []
            if req.signed_envelope is not None:
                signed_payload = {
                    "thread_id": req.thread_id,
                    "sender": req.sender,
                    "recipient": req.recipient,
                    "subject": req.subject,
                    "body_md": req.body_md,
                    "priority": req.priority,
                    "attachments": req.attachments,
                    "idempotency_key": req.idempotency_key,
                    "delivery": req.delivery.model_dump(),
                }
                signature_verification = verify_signed_payload(
                    settings=settings,
                    gm=gm,
                    auth=auth,
                    payload=signed_payload,
                    key_id=req.signed_envelope.key_id,
                    nonce=req.signed_envelope.nonce,
                    expires_at=req.signed_envelope.expires_at,
                    signature=req.signed_envelope.signature,
                    algorithm=req.signed_envelope.algorithm,
                    consume_nonce=req.signed_envelope.consume_nonce,
                    audit_event="messages_send_signature",
                    verification_failure_count=verification_failure_count,
                    record_verification_failure=record_verification_failure,
                    audit=audit,
                )
                if not signature_verification["valid"]:
                    raise HTTPException(status_code=401, detail=f"Invalid signed envelope: {signature_verification['reason']}")
                committed_files.extend(signature_verification.get("committed_files", []))

            now = iso_now()
            msg = {
                "id": f"msg_{uuid4().hex[:12]}",
                "thread_id": req.thread_id,
                "from": req.sender,
                "to": req.recipient,
                "sent_at": format_iso(now),
                "subject": req.subject,
                "body_md": req.body_md,
                "priority": req.priority,
                "attachments": req.attachments,
                "idempotency_key": req.idempotency_key,
                "delivery": req.delivery.model_dump(),
            }

            inbox_path_rel = f"messages/inbox/{req.recipient}.jsonl"
            outbox_path_rel = f"messages/outbox/{req.sender}.jsonl"
            thread_path_rel = f"messages/threads/{req.thread_id}.jsonl"

            rels = [inbox_path_rel, outbox_path_rel, thread_path_rel]
            paths = [safe_path(settings.repo_root, r) for r in rels]

            should_track_delivery = bool(req.idempotency_key or req.delivery.requires_ack)
            delivery_state = None
            warnings: list[str] = list(state.get("warnings") or [])
            if should_track_delivery:
                ack_deadline = None
                if req.delivery.requires_ack:
                    ack_deadline = format_iso(now + timedelta(seconds=req.delivery.ack_timeout_seconds))
                status = "pending_ack" if req.delivery.requires_ack else "delivered"
                record = {
                    "message_id": msg["id"],
                    "thread_id": req.thread_id,
                    "from": req.sender,
                    "to": req.recipient,
                    "subject": req.subject,
                    "idempotency_key": req.idempotency_key,
                    "status": status,
                    "requires_ack": req.delivery.requires_ack,
                    "ack_timeout_seconds": req.delivery.ack_timeout_seconds,
                    "max_retries": req.delivery.max_retries,
                    "retry_count": 0,
                    "sent_at": format_iso(now),
                    "ack_deadline": ack_deadline,
                    "acks": [],
                    "last_error": None,
                    "message": msg,
                }
                state.setdefault("records", {})[msg["id"]] = record
                if req.idempotency_key:
                    key = _idempotency_scope_key(req.sender, req.recipient, req.idempotency_key)
                    state.setdefault("idempotency", {})[key] = msg["id"]
                delivery_state = delivery_record_view(record, now, parse_iso=parse_iso)

            commit_paths = list(paths)
            commit_rels = list(rels)
            append_targets = _capture_append_targets(paths)
            state_rollback_plan = _capture_rollback_plan([_delivery_state_path(settings.repo_root)]) if should_track_delivery else []
            try:
                locked_append_jsonl_multi(paths, msg, repo_root=settings.repo_root, gm=gm, settings=settings)
                if should_track_delivery:
                    state_path = _write_delivery_state(settings.repo_root, state)
                    commit_paths.append(state_path)
                    commit_rels.append(DELIVERY_STATE_REL)
            except Exception:
                _restore_rollback_plan(state_rollback_plan)
                _restore_appends(append_targets)
                raise
    except (SegmentHistoryLockTimeout, LockInfrastructureError):
        raise HTTPException(status_code=503, detail="Delivery state lock unavailable; retry")

    if try_commit_paths(paths=commit_paths, gm=gm, commit_message=f"messages: send {msg['id']}"):
        committed_files.extend(commit_rels)
    else:
        warnings.append(_non_durable_warning("messages_send"))

    audit(auth, "message_send", {"thread_id": req.thread_id, "to": req.recipient})
    result: dict[str, Any] = {
        "ok": True,
        "idempotent_replay": False,
        "message": msg,
        "delivery_state": delivery_state,
        "signature_verification": signature_verification,
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }
    if warnings:
        result["warnings"] = warnings
    return result


def messages_ack_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: MessageAckRequest,
    parse_iso: Callable[[str | None], datetime | None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """Record an acknowledgement against a tracked delivery record."""
    auth.require("write:messages")
    auth.require_write_path(DELIVERY_STATE_REL)

    try:
        with segment_history_source_lock("registry:delivery_index", lock_dir=repo_root / ".locks"):
            state = load_delivery_state(repo_root)
            record = state.get("records", {}).get(req.message_id)
            if not isinstance(record, dict):
                raise HTTPException(status_code=404, detail="Tracked message not found")

            now = iso_now()
            ack_row = {
                "ack_id": req.ack_id or f"ack_{uuid4().hex[:12]}",
                "message_id": req.message_id,
                "status": req.status,
                "reason": req.reason,
                "ack_at": format_iso(now),
                "by": auth.peer_id,
            }
            record.setdefault("acks", []).append(ack_row)

            if req.status == "accepted":
                record["status"] = "acked"
            elif req.status == "rejected":
                record["status"] = "dead_letter"
                record["last_error"] = req.reason or "rejected"
            else:
                record["status"] = "pending_ack"
                timeout = int(record.get("ack_timeout_seconds") or 300)
                record["ack_deadline"] = format_iso(now + timedelta(seconds=timeout))

            committed_files = []
            ack_rel = f"messages/acks/{req.message_id}.jsonl"
            state_path = _delivery_state_path(repo_root)
            ack_path = safe_path(repo_root, ack_rel)
            state_rollback_plan = _capture_rollback_plan([state_path])
            ack_append_targets = _capture_append_targets([ack_path])
            try:
                _write_delivery_state(repo_root, state)
                locked_append_jsonl(ack_path, ack_row, repo_root=repo_root, gm=gm, settings=None, family="message_stream")
            except Exception:
                _restore_appends(ack_append_targets)
                _restore_rollback_plan(state_rollback_plan)
                raise
    except (SegmentHistoryLockTimeout, LockInfrastructureError):
        raise HTTPException(status_code=503, detail="Delivery state lock unavailable; retry")
    warnings: list[str] = list(state.get("warnings") or [])
    if try_commit_paths(paths=[state_path, ack_path], gm=gm, commit_message=f"messages: ack {req.message_id}"):
        committed_files.extend([DELIVERY_STATE_REL, ack_rel])
    else:
        warnings.append(_non_durable_warning("messages_ack"))

    audit(auth, "messages_ack", {"message_id": req.message_id, "status": req.status})
    result: dict[str, Any] = {
        "ok": True,
        "message_id": req.message_id,
        "ack": ack_row,
        "delivery_state": delivery_record_view(record, now, parse_iso=parse_iso),
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }
    if warnings:
        result["warnings"] = warnings
    return result


def messages_pending_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    recipient: str | None,
    status: str | None,
    include_terminal: bool,
    limit: int,
    parse_iso: Callable[[str | None], datetime | None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """List pending or dead-letter delivery records visible to the caller."""
    auth.require("read:files")
    auth.require_read_path(DELIVERY_STATE_REL)

    state = load_delivery_state(repo_root)
    warnings: list[str] = list(state.get("warnings") or [])
    now = datetime.now(timezone.utc)
    rows = []
    summary: dict[str, int] = {}
    skipped_records = 0
    for record in state.get("records", {}).values():
        if not isinstance(record, dict):
            skipped_records += 1
            continue
        view = delivery_record_view(record, now, parse_iso=parse_iso)
        eff = str(view.get("effective_status"))
        summary[eff] = summary.get(eff, 0) + 1
        if recipient and str(view.get("to")) != recipient:
            continue
        if status and eff != status:
            continue
        if not include_terminal and eff in {"acked", "dead_letter", "delivered"}:
            continue
        rows.append(view)
    if skipped_records:
        _logger.warning("Skipped %d non-dict record(s) in delivery state", skipped_records)
        warnings.append(f"delivery_state_partial_corrupt: {skipped_records} non-dict record(s) skipped")

    rows.sort(key=lambda x: str(x.get("sent_at", "")), reverse=True)
    out = rows[:limit]
    audit(auth, "messages_pending", {"count": len(out), "recipient": recipient, "status": status})
    result: dict[str, Any] = {"ok": True, "count": len(out), "summary": summary, "messages": out}
    if warnings:
        result["warnings"] = warnings
    return result


def messages_inbox_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    recipient: str,
    limit: int,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
    max_jsonl_read_bytes: int = DEFAULT_MAX_JSONL_READ_BYTES,
) -> dict:
    """Return recent inbox messages for a recipient."""
    auth.require("read:files")
    auth.require_read_path(f"messages/inbox/{recipient}.jsonl")
    path = safe_path(repo_root, f"messages/inbox/{recipient}.jsonl")
    if not path.exists():
        return {"ok": True, "recipient": recipient, "count": 0, "messages": []}

    try:
        file_size = path.stat().st_size
    except OSError:
        _logger.warning("stat() failed on inbox file for %s; returning degraded response", recipient, exc_info=True)
        audit(auth, "messages_inbox", {"recipient": recipient, "count": 0})
        return {
            "ok": True, "degraded": True, "recipient": recipient, "count": 0, "messages": [],
            "warnings": ["inbox_stat_failed: unable to determine file size; returning empty to avoid unbounded read"],
        }
    if file_size > max_jsonl_read_bytes:
        _logger.warning(
            "Inbox file for %s is %d bytes (limit %d); returning degraded response",
            recipient, file_size, max_jsonl_read_bytes,
        )
        audit(auth, "messages_inbox", {"recipient": recipient, "count": 0})
        return {
            "ok": True, "degraded": True, "recipient": recipient, "count": 0, "messages": [],
            "warnings": [
                f"inbox_too_large: file is {file_size} bytes, exceeds {max_jsonl_read_bytes} byte safety limit; "
                "all messages unavailable until file is compacted or truncated"
            ],
        }

    utf8_corrupted = False
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
        if "\ufffd" in raw:
            _logger.warning("file %s contains invalid UTF-8 bytes (replaced with U+FFFD)", path)
            utf8_corrupted = True
        all_lines = raw.splitlines()
    except MemoryError:
        _logger.critical("OOM while reading inbox file for %s", recipient, exc_info=True)
        raise
    except Exception:  # noqa: BLE001 — mission-critical degradation
        _logger.error("Failed to read inbox file for %s", recipient, exc_info=True)
        result: dict[str, Any] = {"ok": True, "degraded": True, "recipient": recipient, "count": 0, "messages": []}
        result["warnings"] = ["inbox_unreadable: I/O error reading inbox file"]
        audit(auth, "messages_inbox", {"recipient": recipient, "count": 0})
        return result
    tail = all_lines[-limit:]
    file_offset = len(all_lines) - len(tail)
    messages: list[dict[str, Any]] = []
    malformed = 0
    non_dict = 0
    for idx, line in enumerate(tail):
        try:
            row = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            malformed += 1
            preview = repr(line[:200]) + ("..." if len(line) > 200 else "") if line else "<empty>"
            _logger.warning("malformed JSONL in inbox %s (file line %d, %d chars): %s", recipient, file_offset + idx + 1, len(line), preview)
            continue
        if not isinstance(row, dict):
            non_dict += 1
            _logger.warning("non-dict JSON in inbox %s (file line %d), skipping", recipient, file_offset + idx + 1)
            continue
        messages.append(row)
    audit(auth, "messages_inbox", {"recipient": recipient, "count": len(messages)})
    result = {"ok": True, "recipient": recipient, "count": len(messages), "messages": messages}
    warnings: list[str] = []
    skipped = malformed + non_dict
    if skipped:
        parts = []
        if malformed:
            parts.append(f"{malformed} malformed")
        if non_dict:
            parts.append(f"{non_dict} non-dict")
        warnings.append(f"inbox_partial_corrupt: {', '.join(parts)} line(s) skipped")
    if utf8_corrupted:
        warnings.append("inbox_utf8_corrupted: file contains invalid UTF-8 bytes replaced with U+FFFD")
    if warnings:
        result["warnings"] = warnings
    return result


def messages_thread_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    thread_id: str,
    limit: int,
    audit: Callable[[AuthContext, str, dict[str, Any]], None] | None = None,
    max_jsonl_read_bytes: int = DEFAULT_MAX_JSONL_READ_BYTES,
) -> dict:
    """Return recent messages for a thread."""
    auth.require("read:files")
    rel = f"messages/threads/{thread_id}.jsonl"
    auth.require_read_path(rel)
    path = safe_path(repo_root, rel)
    if not path.exists():
        return {"ok": True, "thread_id": thread_id, "count": 0, "messages": []}

    try:
        file_size = path.stat().st_size
    except OSError:
        _logger.warning("stat() failed on thread file for %s; returning degraded response", thread_id, exc_info=True)
        if audit:
            audit(auth, "messages_thread", {"thread_id": thread_id, "count": 0})
        return {
            "ok": True, "degraded": True, "thread_id": thread_id, "count": 0, "messages": [],
            "warnings": ["thread_stat_failed: unable to determine file size; returning empty to avoid unbounded read"],
        }
    if file_size > max_jsonl_read_bytes:
        _logger.warning(
            "Thread file for %s is %d bytes (limit %d); returning degraded response",
            thread_id, file_size, max_jsonl_read_bytes,
        )
        if audit:
            audit(auth, "messages_thread", {"thread_id": thread_id, "count": 0})
        return {
            "ok": True, "degraded": True, "thread_id": thread_id, "count": 0, "messages": [],
            "warnings": [
                f"thread_too_large: file is {file_size} bytes, exceeds {max_jsonl_read_bytes} byte safety limit; "
                "all messages unavailable until file is compacted or truncated"
            ],
        }

    utf8_corrupted = False
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
        if "\ufffd" in raw:
            _logger.warning("file %s contains invalid UTF-8 bytes (replaced with U+FFFD)", path)
            utf8_corrupted = True
        all_lines = raw.splitlines()
    except MemoryError:
        _logger.critical("OOM while reading thread file for %s", thread_id, exc_info=True)
        raise
    except Exception:  # noqa: BLE001 — mission-critical degradation
        _logger.error("Failed to read thread file for %s", thread_id, exc_info=True)
        if audit:
            audit(auth, "messages_thread", {"thread_id": thread_id, "count": 0})
        return {
            "ok": True,
            "degraded": True,
            "thread_id": thread_id,
            "count": 0,
            "messages": [],
            "warnings": ["thread_unreadable: I/O error reading thread file"],
        }
    tail = all_lines[-limit:]
    file_offset = len(all_lines) - len(tail)
    messages: list[dict[str, Any]] = []
    malformed = 0
    non_dict = 0
    for idx, line in enumerate(tail):
        try:
            row = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            malformed += 1
            preview = repr(line[:200]) + ("..." if len(line) > 200 else "") if line else "<empty>"
            _logger.warning("malformed JSONL in thread %s (file line %d, %d chars): %s", thread_id, file_offset + idx + 1, len(line), preview)
            continue
        if not isinstance(row, dict):
            non_dict += 1
            _logger.warning("non-dict JSON in thread %s (file line %d), skipping", thread_id, file_offset + idx + 1)
            continue
        messages.append(row)
    result: dict[str, Any] = {"ok": True, "thread_id": thread_id, "count": len(messages), "messages": messages}
    warnings: list[str] = []
    skipped = malformed + non_dict
    if skipped:
        parts = []
        if malformed:
            parts.append(f"{malformed} malformed")
        if non_dict:
            parts.append(f"{non_dict} non-dict")
        warnings.append(f"thread_partial_corrupt: {', '.join(parts)} line(s) skipped")
    if utf8_corrupted:
        warnings.append("thread_utf8_corrupted: file contains invalid UTF-8 bytes replaced with U+FFFD")
    if audit:
        audit(auth, "messages_thread", {"thread_id": thread_id, "count": len(messages)})
    if warnings:
        result["warnings"] = warnings
    return result


def relay_forward_service(
    *,
    settings: Any,
    gm: Any,
    auth: AuthContext,
    req: RelayForwardRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    verify_signed_payload: Callable[..., dict[str, Any]],
    verification_failure_count: Callable[[Any, AuthContext], int],
    record_verification_failure: Callable[[Any, AuthContext, str], None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """Forward a relay envelope to the local message send flow after verification."""
    enforce_rate_limit(settings, auth, "relay_forward")
    enforce_payload_limit(settings, req.model_dump(), "relay_forward")
    auth.require("write:messages")

    if settings.require_signed_ingress and req.signed_envelope is None:
        raise HTTPException(status_code=400, detail="signed_envelope is required when strict signed ingress is enabled")

    committed_files: list[str] = []
    signature_verification = None
    if req.signed_envelope is not None:
        signed_payload = {
            "relay_id": req.relay_id,
            "target_recipient": req.target_recipient,
            "thread_id": req.thread_id,
            "sender": req.sender,
            "subject": req.subject,
            "body_md": req.body_md,
            "priority": req.priority,
            "attachments": req.attachments,
            "envelope": req.envelope,
        }
        signature_verification = verify_signed_payload(
            settings=settings,
            gm=gm,
            auth=auth,
            payload=signed_payload,
            key_id=req.signed_envelope.key_id,
            nonce=req.signed_envelope.nonce,
            expires_at=req.signed_envelope.expires_at,
            signature=req.signed_envelope.signature,
            algorithm=req.signed_envelope.algorithm,
            consume_nonce=req.signed_envelope.consume_nonce,
            audit_event="relay_forward_signature",
            verification_failure_count=verification_failure_count,
            record_verification_failure=record_verification_failure,
            audit=audit,
        )
        if not signature_verification["valid"]:
            raise HTTPException(status_code=401, detail=f"Invalid signed envelope: {signature_verification['reason']}")
        committed_files.extend(signature_verification.get("committed_files", []))

    now = iso_now()
    msg = {
        "id": f"msg_{uuid4().hex[:12]}",
        "thread_id": req.thread_id,
        "from": req.sender,
        "to": req.target_recipient,
        "via": req.relay_id,
        "sent_at": format_iso(now),
        "subject": req.subject,
        "body_md": req.body_md,
        "priority": req.priority,
        "attachments": req.attachments,
        "envelope": req.envelope,
    }
    relay_rel = f"messages/relay/{req.relay_id}.jsonl"
    inbox_rel = f"messages/inbox/{req.target_recipient}.jsonl"
    thread_rel = f"messages/threads/{req.thread_id}.jsonl"
    rels = [relay_rel, inbox_rel, thread_rel]
    paths = [safe_path(settings.repo_root, r) for r in rels]
    try:
        locked_append_jsonl_multi(paths, msg, repo_root=settings.repo_root, gm=gm, settings=settings)
    except SegmentHistoryAppendError as exc:
        raise HTTPException(
            status_code=503, detail=f"Relay forward append failed: {exc.detail}",
        ) from exc
    warnings: list[str] = []
    if try_commit_paths(paths=paths, gm=gm, commit_message=f"relay: forward {msg['id']}"):
        committed_files.extend(rels)
    else:
        warnings.append(_non_durable_warning("relay_forward"))
    audit(auth, "relay_forward", {"relay_id": req.relay_id, "to": req.target_recipient, "thread_id": req.thread_id})
    result = {
        "ok": True,
        "message": msg,
        "signature_verification": signature_verification,
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }
    if warnings:
        result["warnings"] = warnings
    return result


def replay_messages_service(
    *,
    settings: Any,
    gm: Any,
    auth: AuthContext,
    req: MessageReplayRequest,
    parse_iso: Callable[[str | None], datetime | None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """Retry one or more tracked dead-letter deliveries."""
    auth.require("write:messages")
    auth.require_write_path("messages/inbox/x.jsonl")
    auth.require_write_path(DELIVERY_STATE_REL)
    try:
        with segment_history_source_lock("registry:delivery_index", lock_dir=settings.repo_root / ".locks"):
            state = load_delivery_state(settings.repo_root)
            record = state.get("records", {}).get(req.message_id)
            if not isinstance(record, dict):
                raise HTTPException(status_code=404, detail="Tracked message not found")

            now = iso_now()
            effective = effective_delivery_status(record, now, parse_iso=parse_iso)
            if not req.force and effective != "dead_letter":
                raise HTTPException(status_code=409, detail=f"Replay requires dead_letter status; got {effective}")
            retry_count = int(record.get("retry_count") or 0)
            max_retries = int(record.get("max_retries") or 0)
            if not req.force and retry_count >= max_retries:
                raise HTTPException(status_code=409, detail=f"Replay retry limit reached ({retry_count}/{max_retries})")

            original = record.get("message")
            if not isinstance(original, dict):
                original = {
                    "id": req.message_id,
                    "thread_id": record.get("thread_id"),
                    "from": record.get("from"),
                    "to": record.get("to"),
                    "subject": record.get("subject"),
                    "body_md": "",
                    "attachments": [],
                    "priority": "normal",
                    "delivery": {
                        "requires_ack": bool(record.get("requires_ack")),
                        "ack_timeout_seconds": int(record.get("ack_timeout_seconds") or req.ack_timeout_seconds),
                        "max_retries": max_retries,
                    },
                }

            new_message_id = f"msg_{uuid4().hex[:12]}"
            replay_msg = dict(original)
            replay_msg["id"] = new_message_id
            replay_msg["replay_of"] = req.message_id
            replay_msg["sent_at"] = format_iso(now)

            sender = str(replay_msg.get("from") or record.get("from") or "unknown")
            recipient = str(replay_msg.get("to") or record.get("to") or "unknown")
            thread_id = str(replay_msg.get("thread_id") or record.get("thread_id") or "thread_unknown")
            inbox_rel = f"messages/inbox/{recipient}.jsonl"
            outbox_rel = f"messages/outbox/{sender}.jsonl"
            thread_rel = f"messages/threads/{thread_id}.jsonl"
            rels = [inbox_rel, outbox_rel, thread_rel]
            for rel in rels:
                auth.require_write_path(rel)
            paths = [safe_path(settings.repo_root, r) for r in rels]
            committed_files = []

            if req.requires_ack:
                ack_deadline = format_iso(now + timedelta(seconds=req.ack_timeout_seconds))
                new_status = "pending_ack"
            else:
                ack_deadline = None
                new_status = "delivered"

            new_record = dict(record)
            new_record.update(
                {
                    "message_id": new_message_id,
                    "thread_id": thread_id,
                    "from": sender,
                    "to": recipient,
                    "status": new_status,
                    "requires_ack": req.requires_ack,
                    "ack_timeout_seconds": req.ack_timeout_seconds,
                    "retry_count": retry_count + 1,
                    "sent_at": format_iso(now),
                    "ack_deadline": ack_deadline,
                    "acks": [],
                    "last_error": None,
                    "replay_of": req.message_id,
                    "message": replay_msg,
                }
            )
            state.setdefault("records", {})[new_message_id] = new_record
            record["status"] = "replayed"
            record["replayed_to"] = new_message_id
            record["updated_at"] = format_iso(now)
            if req.reason:
                record["replay_reason"] = req.reason

            state_path = _delivery_state_path(settings.repo_root)
            append_targets = _capture_append_targets(paths)
            state_rollback_plan = _capture_rollback_plan([state_path])
            try:
                locked_append_jsonl_multi(paths, replay_msg, repo_root=settings.repo_root, gm=gm, settings=settings)
                _write_delivery_state(settings.repo_root, state)
            except Exception:
                _restore_rollback_plan(state_rollback_plan)
                _restore_appends(append_targets)
                raise
    except (SegmentHistoryLockTimeout, LockInfrastructureError):
        raise HTTPException(status_code=503, detail="Delivery state lock unavailable; retry")
    warnings: list[str] = list(state.get("warnings") or [])
    commit_paths = paths + [state_path]
    commit_rels = rels + [DELIVERY_STATE_REL]
    if try_commit_paths(
        paths=commit_paths,
        gm=gm,
        commit_message=f"messages: replay {req.message_id} -> {new_message_id}",
    ):
        committed_files.extend(commit_rels)
    else:
        warnings.append(_non_durable_warning("messages_replay"))

    audit(auth, "messages_replay", {"message_id": req.message_id, "new_message_id": new_message_id, "reason": req.reason, "force": req.force})
    result: dict[str, Any] = {
        "ok": True,
        "message_id": req.message_id,
        "replayed_message_id": new_message_id,
        "delivery_state": delivery_record_view(new_record, now, parse_iso=parse_iso),
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }
    if warnings:
        result["warnings"] = warnings
    return result
