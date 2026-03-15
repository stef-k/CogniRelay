from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException

from app.auth import AuthContext
from app.models import MessageAckRequest, MessageReplayRequest, MessageSendRequest, RelayForwardRequest
from app.storage import append_jsonl, safe_path, write_text_file

DELIVERY_STATE_REL = "messages/state/delivery_index.json"


def _delivery_state_path(repo_root: Path) -> Path:
    return safe_path(repo_root, DELIVERY_STATE_REL)


def load_delivery_state(repo_root: Path) -> dict[str, Any]:
    path = _delivery_state_path(repo_root)
    if not path.exists():
        return {"version": "1", "records": {}, "idempotency": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": "1", "records": {}, "idempotency": {}}
    if not isinstance(data, dict):
        return {"version": "1", "records": {}, "idempotency": {}}
    records = data.get("records")
    idempotency = data.get("idempotency")
    if not isinstance(records, dict):
        records = {}
    if not isinstance(idempotency, dict):
        idempotency = {}
    return {"version": "1", "records": records, "idempotency": idempotency}


def _write_delivery_state(repo_root: Path, state: dict[str, Any]) -> Path:
    path = _delivery_state_path(repo_root)
    write_text_file(path, json.dumps(state, ensure_ascii=False, indent=2))
    return path


def effective_delivery_status(record: dict[str, Any], now: datetime, *, parse_iso: Callable[[str | None], datetime | None]) -> str:
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
    out = dict(record)
    out["effective_status"] = effective_delivery_status(record, now, parse_iso=parse_iso)
    return out


def _idempotency_scope_key(sender: str, recipient: str, idempotency_key: str) -> str:
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
    enforce_rate_limit(settings, auth, "messages_send")
    enforce_payload_limit(settings, req.model_dump(), "messages_send")
    auth.require("write:messages")
    auth.require_write_path("messages/inbox/x.jsonl")
    auth.require_write_path(DELIVERY_STATE_REL)

    if settings.require_signed_ingress and req.signed_envelope is None:
        raise HTTPException(status_code=400, detail="signed_envelope is required when strict signed ingress is enabled")

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
                return {
                    "ok": True,
                    "idempotent_replay": True,
                    "message": existing.get("message"),
                    "delivery_state": delivery_record_view(existing, now, parse_iso=parse_iso),
                    "signature_verification": verification,
                    "committed_files": [],
                    "latest_commit": gm.latest_commit(),
                }

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

    now = datetime.now(timezone.utc)
    msg = {
        "id": f"msg_{uuid4().hex[:12]}",
        "thread_id": req.thread_id,
        "from": req.sender,
        "to": req.recipient,
        "sent_at": now.isoformat(),
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

    for rel in (inbox_path_rel, outbox_path_rel, thread_path_rel):
        path = safe_path(settings.repo_root, rel)
        append_jsonl(path, msg)
        if gm.commit_file(path, f"messages: append {rel}"):
            committed_files.append(rel)

    should_track_delivery = bool(req.idempotency_key or req.delivery.requires_ack)
    delivery_state = None
    if should_track_delivery:
        ack_deadline = None
        if req.delivery.requires_ack:
            ack_deadline = (now + timedelta(seconds=req.delivery.ack_timeout_seconds)).isoformat()
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
            "sent_at": now.isoformat(),
            "ack_deadline": ack_deadline,
            "acks": [],
            "last_error": None,
            "message": msg,
        }
        state.setdefault("records", {})[msg["id"]] = record
        if req.idempotency_key:
            key = _idempotency_scope_key(req.sender, req.recipient, req.idempotency_key)
            state.setdefault("idempotency", {})[key] = msg["id"]
        state_path = _write_delivery_state(settings.repo_root, state)
        if gm.commit_file(state_path, f"messages: update delivery state {msg['id']}"):
            committed_files.append(DELIVERY_STATE_REL)
        delivery_state = delivery_record_view(record, now, parse_iso=parse_iso)

    audit(auth, "message_send", {"thread_id": req.thread_id, "to": req.recipient})
    return {
        "ok": True,
        "idempotent_replay": False,
        "message": msg,
        "delivery_state": delivery_state,
        "signature_verification": signature_verification,
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }


def messages_ack_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: MessageAckRequest,
    parse_iso: Callable[[str | None], datetime | None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    auth.require("write:messages")
    auth.require_write_path(DELIVERY_STATE_REL)

    state = load_delivery_state(repo_root)
    record = state.get("records", {}).get(req.message_id)
    if not isinstance(record, dict):
        raise HTTPException(status_code=404, detail="Tracked message not found")

    now = datetime.now(timezone.utc)
    ack_row = {
        "ack_id": req.ack_id or f"ack_{uuid4().hex[:12]}",
        "message_id": req.message_id,
        "status": req.status,
        "reason": req.reason,
        "ack_at": now.isoformat(),
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
        record["ack_deadline"] = (now + timedelta(seconds=timeout)).isoformat()

    state_path = _write_delivery_state(repo_root, state)
    committed_files = []
    if gm.commit_file(state_path, f"messages: ack {req.message_id}"):
        committed_files.append(DELIVERY_STATE_REL)

    ack_rel = f"messages/acks/{req.message_id}.jsonl"
    ack_path = safe_path(repo_root, ack_rel)
    append_jsonl(ack_path, ack_row)
    if gm.commit_file(ack_path, f"messages: ack log {req.message_id}"):
        committed_files.append(ack_rel)

    audit(auth, "messages_ack", {"message_id": req.message_id, "status": req.status})
    return {
        "ok": True,
        "message_id": req.message_id,
        "ack": ack_row,
        "delivery_state": delivery_record_view(record, now, parse_iso=parse_iso),
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }


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
    auth.require("read:files")
    auth.require_read_path(DELIVERY_STATE_REL)

    state = load_delivery_state(repo_root)
    now = datetime.now(timezone.utc)
    rows = []
    summary: dict[str, int] = {}
    for record in state.get("records", {}).values():
        if not isinstance(record, dict):
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

    rows.sort(key=lambda x: str(x.get("sent_at", "")), reverse=True)
    out = rows[:limit]
    audit(auth, "messages_pending", {"count": len(out), "recipient": recipient, "status": status})
    return {"ok": True, "count": len(out), "summary": summary, "messages": out}


def messages_inbox_service(*, repo_root: Path, auth: AuthContext, recipient: str, limit: int, audit: Callable[[AuthContext, str, dict[str, Any]], None]) -> dict:
    auth.require("read:files")
    auth.require_read_path(f"messages/inbox/{recipient}.jsonl")
    path = safe_path(repo_root, f"messages/inbox/{recipient}.jsonl")
    if not path.exists():
        return {"ok": True, "recipient": recipient, "count": 0, "messages": []}

    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    messages = []
    for line in lines[-limit:]:
        try:
            messages.append(json.loads(line))
        except Exception:
            continue
    audit(auth, "messages_inbox", {"recipient": recipient, "count": len(messages)})
    return {"ok": True, "recipient": recipient, "count": len(messages), "messages": messages}


def messages_thread_service(*, repo_root: Path, auth: AuthContext, thread_id: str, limit: int) -> dict:
    auth.require("read:files")
    rel = f"messages/threads/{thread_id}.jsonl"
    auth.require_read_path(rel)
    path = safe_path(repo_root, rel)
    if not path.exists():
        return {"ok": True, "thread_id": thread_id, "count": 0, "messages": []}
    messages = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines()[-limit:]:
        try:
            messages.append(json.loads(line))
        except Exception:
            continue
    return {"ok": True, "thread_id": thread_id, "count": len(messages), "messages": messages}


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

    now = datetime.now(timezone.utc)
    msg = {
        "id": f"msg_{uuid4().hex[:12]}",
        "thread_id": req.thread_id,
        "from": req.sender,
        "to": req.target_recipient,
        "via": req.relay_id,
        "sent_at": now.isoformat(),
        "subject": req.subject,
        "body_md": req.body_md,
        "priority": req.priority,
        "attachments": req.attachments,
        "envelope": req.envelope,
    }
    relay_rel = f"messages/relay/{req.relay_id}.jsonl"
    inbox_rel = f"messages/inbox/{req.target_recipient}.jsonl"
    thread_rel = f"messages/threads/{req.thread_id}.jsonl"
    for rel in (relay_rel, inbox_rel, thread_rel):
        path = safe_path(settings.repo_root, rel)
        append_jsonl(path, msg)
        if gm.commit_file(path, f"relay: forward {rel}"):
            committed_files.append(rel)
    audit(auth, "relay_forward", {"relay_id": req.relay_id, "to": req.target_recipient, "thread_id": req.thread_id})
    return {
        "ok": True,
        "message": msg,
        "signature_verification": signature_verification,
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }


def replay_messages_service(
    *,
    settings: Any,
    gm: Any,
    auth: AuthContext,
    req: MessageReplayRequest,
    parse_iso: Callable[[str | None], datetime | None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    auth.require("write:messages")
    auth.require_write_path("messages/inbox/x.jsonl")
    auth.require_write_path(DELIVERY_STATE_REL)
    state = load_delivery_state(settings.repo_root)
    record = state.get("records", {}).get(req.message_id)
    if not isinstance(record, dict):
        raise HTTPException(status_code=404, detail="Tracked message not found")

    now = datetime.now(timezone.utc)
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
    replay_msg["sent_at"] = now.isoformat()

    sender = str(replay_msg.get("from") or record.get("from") or "unknown")
    recipient = str(replay_msg.get("to") or record.get("to") or "unknown")
    thread_id = str(replay_msg.get("thread_id") or record.get("thread_id") or "thread_unknown")
    inbox_rel = f"messages/inbox/{recipient}.jsonl"
    outbox_rel = f"messages/outbox/{sender}.jsonl"
    thread_rel = f"messages/threads/{thread_id}.jsonl"
    committed_files = []
    for rel in (inbox_rel, outbox_rel, thread_rel):
        auth.require_write_path(rel)
        path = safe_path(settings.repo_root, rel)
        append_jsonl(path, replay_msg)
        if gm.commit_file(path, f"messages: replay append {rel}"):
            committed_files.append(rel)

    if req.requires_ack:
        ack_deadline = (now + timedelta(seconds=req.ack_timeout_seconds)).isoformat()
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
            "sent_at": now.isoformat(),
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
    record["updated_at"] = now.isoformat()
    if req.reason:
        record["replay_reason"] = req.reason

    state_path = _write_delivery_state(settings.repo_root, state)
    if gm.commit_file(state_path, f"messages: replay {req.message_id} -> {new_message_id}"):
        committed_files.append(DELIVERY_STATE_REL)

    audit(auth, "messages_replay", {"message_id": req.message_id, "new_message_id": new_message_id, "reason": req.reason, "force": req.force})
    return {
        "ok": True,
        "message_id": req.message_id,
        "replayed_message_id": new_message_id,
        "delivery_state": delivery_record_view(new_record, now, parse_iso=parse_iso),
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }
