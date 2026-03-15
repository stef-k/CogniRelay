from __future__ import annotations

import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from fastapi import HTTPException

from app.audit import append_audit
from app.config import sha256_token
from app.discovery import handle_mcp_rpc_request as discovery_handle_mcp_rpc_request
from app.storage import safe_path, write_text_file

RATE_LIMIT_STATE_REL = "logs/rate_limit_state.json"


def audit_event(settings: Any, auth: Any, event: str, detail: dict[str, Any]) -> None:
    if not settings.audit_log_enabled:
        return
    append_audit(settings.repo_root, event, auth.peer_id if auth else "anonymous", detail)


def scope_for_path(path: str) -> str:
    top = Path(path).parts[0] if Path(path).parts else ""
    if top == "journal":
        return "write:journal"
    if top == "messages":
        return "write:messages"
    if top in {"projects", "memory", "essays", "archive", "config", "logs"}:
        return "write:projects"
    return "write:projects"


def resolve_auth_context(
    require_auth_fn: Callable[..., Any],
    authorization: str | None,
    required: bool,
    *,
    x_forwarded_for: str | None = None,
    x_real_ip: str | None = None,
    request: Any = None,
) -> Any | None:
    if not authorization:
        if required:
            raise HTTPException(status_code=401, detail="Missing Authorization header")
        return None
    return require_auth_fn(
        authorization=authorization,
        x_forwarded_for=x_forwarded_for,
        x_real_ip=x_real_ip,
        request=request,
    )


def handle_mcp_request(
    request_payload: Any,
    *,
    authorization: str | None,
    x_forwarded_for: str | None,
    x_real_ip: str | None,
    request: Any,
    contract_version: str,
    tools: list[dict[str, Any]],
    resolve_auth_context_fn: Callable[..., Any | None],
    invoke_tool_by_name: Callable[[str, dict[str, Any], Any | None], dict[str, Any]],
) -> dict[str, Any] | None:
    return discovery_handle_mcp_rpc_request(
        request_payload,
        authorization=authorization,
        x_forwarded_for=x_forwarded_for,
        x_real_ip=x_real_ip,
        request=request,
        contract_version=contract_version,
        tools=tools,
        resolve_auth_context=resolve_auth_context_fn,
        invoke_tool_by_name=invoke_tool_by_name,
    )


def _estimate_payload_bytes(payload: Any) -> int:
    try:
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    except Exception:
        encoded = str(payload).encode("utf-8", errors="ignore")
    return len(encoded)


def enforce_payload_limit(settings: Any, payload: Any, label: str) -> None:
    size = _estimate_payload_bytes(payload)
    if size > int(settings.max_payload_bytes):
        raise HTTPException(
            status_code=413,
            detail=f"Payload too large for {label}: {size} bytes > limit {settings.max_payload_bytes}",
        )


def _rate_limit_path(repo_root: Path) -> Path:
    return safe_path(repo_root, RATE_LIMIT_STATE_REL)


def _load_rate_limit_state(repo_root: Path) -> dict[str, Any]:
    path = _rate_limit_path(repo_root)
    if not path.exists():
        return {"schema_version": "1.0", "events": [], "verification_failures": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "events": [], "verification_failures": []}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "events": [], "verification_failures": []}
    events = data.get("events")
    failures = data.get("verification_failures")
    if not isinstance(events, list):
        events = []
    if not isinstance(failures, list):
        failures = []
    return {"schema_version": "1.0", "events": events, "verification_failures": failures}


def load_rate_limit_state(repo_root: Path) -> dict[str, Any]:
    return _load_rate_limit_state(repo_root)


def _write_rate_limit_state(repo_root: Path, payload: dict[str, Any]) -> Path:
    path = _rate_limit_path(repo_root)
    write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
    return path


def _auth_refs(auth: Any) -> tuple[str, str]:
    raw_token = getattr(auth, "token", None)
    if isinstance(raw_token, str) and raw_token:
        token_ref = sha256_token(raw_token)[:24]
    else:
        peer_id = getattr(auth, "peer_id", None)
        token_ref = sha256_token(f"peer:{peer_id or 'unknown'}")[:24]
    client_ip = getattr(auth, "client_ip", None)
    ip_ref = (client_ip or "unknown").strip() or "unknown"
    return token_ref, ip_ref


def parse_iso(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _prune_rate_limit_state(payload: dict[str, Any], now: datetime, max_window_seconds: int) -> None:
    cutoff = now - timedelta(seconds=max_window_seconds)
    kept_events = []
    for row in payload.get("events", []):
        if not isinstance(row, dict):
            continue
        at = parse_iso(row.get("at"))
        if at is not None and at >= cutoff:
            kept_events.append(row)
    payload["events"] = kept_events

    kept_failures = []
    for row in payload.get("verification_failures", []):
        if not isinstance(row, dict):
            continue
        at = parse_iso(row.get("at"))
        if at is not None and at >= cutoff:
            kept_failures.append(row)
    payload["verification_failures"] = kept_failures


def enforce_rate_limit(settings: Any, auth: Any, bucket: str) -> None:
    now = datetime.now(timezone.utc)
    token_ref, ip_ref = _auth_refs(auth)
    payload = _load_rate_limit_state(settings.repo_root)
    max_window = max(60, int(settings.verify_failure_window_seconds))
    _prune_rate_limit_state(payload, now, max_window)

    events = payload.setdefault("events", [])
    token_count = 0
    ip_count = 0
    cutoff = now - timedelta(seconds=60)
    for row in events:
        if not isinstance(row, dict):
            continue
        if str(row.get("bucket") or "") != bucket:
            continue
        at = parse_iso(row.get("at"))
        if at is None or at < cutoff:
            continue
        if str(row.get("token_ref") or "") == token_ref:
            token_count += 1
        if str(row.get("ip_ref") or "") == ip_ref:
            ip_count += 1

    if token_count >= int(settings.token_rate_limit_per_minute):
        raise HTTPException(status_code=429, detail=f"Token rate limit exceeded for bucket {bucket}")
    if ip_count >= int(settings.ip_rate_limit_per_minute):
        raise HTTPException(status_code=429, detail=f"IP rate limit exceeded for bucket {bucket}")

    events.append(
        {
            "at": now.isoformat(),
            "bucket": bucket,
            "token_ref": token_ref,
            "ip_ref": ip_ref,
            "peer_id": auth.peer_id,
        }
    )
    _write_rate_limit_state(settings.repo_root, payload)


def record_verification_failure(settings: Any, auth: Any, reason: str) -> None:
    now = datetime.now(timezone.utc)
    token_ref, ip_ref = _auth_refs(auth)
    payload = _load_rate_limit_state(settings.repo_root)
    max_window = max(60, int(settings.verify_failure_window_seconds))
    _prune_rate_limit_state(payload, now, max_window)
    failures = payload.setdefault("verification_failures", [])
    failures.append(
        {
            "at": now.isoformat(),
            "token_ref": token_ref,
            "ip_ref": ip_ref,
            "peer_id": auth.peer_id,
            "reason": reason,
        }
    )
    _write_rate_limit_state(settings.repo_root, payload)


def verification_failure_count(settings: Any, auth: Any) -> int:
    now = datetime.now(timezone.utc)
    token_ref, _ = _auth_refs(auth)
    payload = _load_rate_limit_state(settings.repo_root)
    max_window = max(60, int(settings.verify_failure_window_seconds))
    _prune_rate_limit_state(payload, now, max_window)
    cutoff = now - timedelta(seconds=max_window)
    count = 0
    for row in payload.get("verification_failures", []):
        if not isinstance(row, dict):
            continue
        if str(row.get("token_ref") or "") != token_ref:
            continue
        at = parse_iso(row.get("at"))
        if at is None or at < cutoff:
            continue
        count += 1
    _write_rate_limit_state(settings.repo_root, payload)
    return count


def run_git(repo_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=repo_root, text=True, capture_output=True, check=False)


def read_commit_file(repo_root: Path, commit_ref: str, rel_path: str) -> str | None:
    cp = run_git(repo_root, "show", f"{commit_ref}:{rel_path}")
    if cp.returncode != 0:
        return None
    return cp.stdout
