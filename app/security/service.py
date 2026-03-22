"""Token, key, governance, and signed-message verification business logic."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException

from app.timestamps import parse_iso as _parse_iso

from app.auth import AuthContext
from app.config import ALL_SCOPES
from app.models import (
    MessageVerifyRequest,
    SecurityKeysRotateRequest,
    SecurityTokenIssueRequest,
    SecurityTokenRevokeRequest,
    SecurityTokenRotateRequest,
)
from app.git_safety import safe_commit_updated_file, try_commit_file
from app.segment_history.locking import segment_history_source_lock, SegmentHistoryLockTimeout, LockInfrastructureError
from app.storage import safe_path, write_text_file

TOKEN_CONFIG_REL = "config/peer_tokens.json"
SECURITY_KEYS_REL = "config/security_keys.json"
NONCE_INDEX_REL = "messages/security/nonce_index.json"
GOVERNANCE_POLICY_REL = "config/governance_policy.json"


def _sha256_text(content: str) -> str:
    """Return the SHA-256 digest for a text payload."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _message_signing_blob(payload: dict[str, Any], key_id: str, nonce: str, expires_at: str | None) -> bytes:
    """Build the canonical byte payload used for message signing and verification."""
    canonical = json.dumps(
        {"payload": payload, "key_id": key_id, "nonce": nonce, "expires_at": expires_at},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return canonical.encode("utf-8")


def _hmac_sha256(secret: str, blob: bytes) -> str:
    """Return the HMAC-SHA256 digest for the provided blob."""
    return hmac.new(secret.encode("utf-8"), blob, hashlib.sha256).hexdigest()


def _default_governance_policy() -> dict[str, Any]:
    """Return the default governance policy for token and key management."""
    return {
        "schema_version": "1.0",
        "authority_model": {
            "issuer": "hosting_agent",
            "description": "Hosting agent is sole token/key issuer for this CogniRelay instance.",
        },
        "scope_templates": {
            "collaboration_peer": {
                "scopes": ["read:files", "search", "write:messages"],
                "read_namespaces": ["memory", "messages"],
                "write_namespaces": ["messages"],
            },
            "replication_peer": {
                "scopes": ["admin:peers", "read:files", "write:messages"],
                "read_namespaces": ["*"],
                "write_namespaces": ["messages", "peers", "snapshots"],
            },
        },
        "incident_response": {
            "token_compromise": [
                "revoke impacted token(s)",
                "rotate key material",
                "review audit window",
                "issue replacement token(s)",
            ],
            "replication_conflict": [
                "set conflict_policy=error",
                "inspect drift + tombstones",
                "resume with explicit transition plan",
            ],
        },
        "audit_retention": {"api_audit_days": 90, "security_events_days": 180},
    }


def load_governance_policy(repo_root: Path) -> dict[str, Any]:
    """Load governance policy from disk with defaults merged in."""
    path = safe_path(repo_root, GOVERNANCE_POLICY_REL)
    default = _default_governance_policy()
    if not path.exists():
        return default
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    if not isinstance(data, dict):
        return default
    merged = dict(default)
    merged.update(data)
    return merged


def governance_policy_service(*, repo_root: Path) -> dict[str, Any]:
    """Return the effective governance policy payload."""
    return {"ok": True, "policy": load_governance_policy(repo_root)}


def _external_key_store_path(settings) -> Path:
    """Resolve the external key-store path from settings."""
    return Path(settings.key_store_path).expanduser().resolve()


def _load_external_key_store(settings) -> dict[str, Any]:
    """Load the external signing key store with normalized defaults."""
    path = _external_key_store_path(settings)
    if not path.exists():
        return {"schema_version": "1.0", "keys": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "keys": {}}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "keys": {}}
    keys = data.get("keys")
    if not isinstance(keys, dict):
        keys = {}
    return {"schema_version": "1.0", "keys": keys}


def _write_external_key_store(settings, payload: dict[str, Any]) -> Path:
    """Persist the external signing key store with restricted permissions."""
    path = _external_key_store_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass
    return path


def _resolve_signing_secret(settings, key_id: str, row: dict[str, Any]) -> str | None:
    """Resolve a signing secret from inline or external key-store data."""
    secret = row.get("secret")
    if isinstance(secret, str) and secret:
        return secret
    if not settings.use_external_key_store:
        return None
    key_store = _load_external_key_store(settings)
    entry = key_store.get("keys", {}).get(key_id)
    if not isinstance(entry, dict):
        return None
    ext_secret = entry.get("secret")
    if not isinstance(ext_secret, str) or not ext_secret:
        return None
    return ext_secret


def load_security_keys(repo_root: Path) -> dict[str, Any]:
    """Load normalized signing key metadata from disk."""
    path = safe_path(repo_root, SECURITY_KEYS_REL)
    if not path.exists():
        return {"schema_version": "1.0", "active_key_id": None, "keys": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "active_key_id": None, "keys": {}}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "active_key_id": None, "keys": {}}
    keys = data.get("keys")
    if not isinstance(keys, dict):
        keys = {}
    return {"schema_version": "1.0", "active_key_id": data.get("active_key_id"), "keys": keys}


def _write_security_keys(repo_root: Path, payload: dict[str, Any]) -> Path:
    """Persist signing key metadata to disk."""
    path = safe_path(repo_root, SECURITY_KEYS_REL)
    write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
    return path


def _load_token_config(repo_root: Path) -> dict[str, Any]:
    """Load normalized peer token configuration from disk."""
    path = safe_path(repo_root, TOKEN_CONFIG_REL)
    if not path.exists():
        return {"schema_version": "1.0", "tokens": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "tokens": []}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "tokens": []}
    tokens = data.get("tokens")
    if not isinstance(tokens, list):
        tokens = []
    return {"schema_version": "1.0", "tokens": tokens}


def load_token_config(repo_root: Path) -> dict[str, Any]:
    """Return the normalized token configuration payload."""
    return _load_token_config(repo_root)


def _write_token_config(repo_root: Path, payload: dict[str, Any]) -> Path:
    """Persist token configuration to disk."""
    path = safe_path(repo_root, TOKEN_CONFIG_REL)
    write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
    return path


def _resolve_token_expiry(expires_at: str | None, ttl_seconds: int | None) -> str | None:
    """Resolve token expiry from either an explicit timestamp or TTL seconds."""
    if expires_at and ttl_seconds:
        raise HTTPException(status_code=400, detail="Provide either expires_at or ttl_seconds, not both")
    if ttl_seconds:
        return (datetime.now(timezone.utc) + timedelta(seconds=int(ttl_seconds))).isoformat()
    if expires_at:
        dt = _parse_iso(expires_at)
        if dt is None:
            raise HTTPException(status_code=400, detail="Invalid expires_at format")
        return dt.isoformat()
    return None


def _token_effective_status(entry: dict[str, Any], now: datetime) -> str:
    """Return the effective token status at the provided time."""
    status = str(entry.get("status") or "active")
    if status != "active":
        return status
    exp = _parse_iso(entry.get("expires_at"))
    if exp is not None and now > exp:
        return "expired"
    return "active"


def _token_public_view(entry: dict[str, Any], now: datetime) -> dict[str, Any]:
    """Return the public-facing token representation used by API responses."""
    return {
        "token_id": entry.get("token_id"),
        "peer_id": entry.get("peer_id"),
        "scopes": entry.get("scopes", []),
        "read_namespaces": entry.get("read_namespaces", []),
        "write_namespaces": entry.get("write_namespaces", []),
        "status": entry.get("status", "active"),
        "effective_status": _token_effective_status(entry, now),
        "issued_at": entry.get("issued_at"),
        "expires_at": entry.get("expires_at"),
        "revoked_at": entry.get("revoked_at"),
        "revoked_reason": entry.get("revoked_reason"),
        "rotated_at": entry.get("rotated_at"),
        "rotated_to_token_id": entry.get("rotated_to_token_id"),
        "rotated_from_token_id": entry.get("rotated_from_token_id"),
        "description": entry.get("description"),
        "token_sha256": entry.get("token_sha256"),
    }


def _normalize_token_sha(value: str | None) -> str | None:
    """Normalize optional token digest input to a bare SHA-256 hex digest."""
    if not value:
        return None
    normalized = str(value).strip()
    if normalized.startswith("sha256:"):
        normalized = normalized.split(":", 1)[1]
    return normalized or None


def _load_nonce_index(repo_root: Path) -> dict[str, Any]:
    """Load the nonce replay-prevention index with normalized defaults."""
    path = safe_path(repo_root, NONCE_INDEX_REL)
    if not path.exists():
        return {"schema_version": "1.0", "entries": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "entries": {}}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "entries": {}}
    entries = data.get("entries")
    if not isinstance(entries, dict):
        entries = {}
    return {"schema_version": "1.0", "entries": entries}


def _write_nonce_index(repo_root: Path, payload: dict[str, Any]) -> Path:
    """Persist the nonce replay-prevention index."""
    path = safe_path(repo_root, NONCE_INDEX_REL)
    write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
    return path


def _prune_nonce_entries(payload: dict[str, Any], now: datetime) -> int:
    """Remove expired nonce entries and return the number removed."""
    entries = payload.setdefault("entries", {})
    if not isinstance(entries, dict):
        payload["entries"] = {}
        return 0
    remove_keys: list[str] = []
    for key, row in entries.items():
        if not isinstance(row, dict):
            remove_keys.append(key)
            continue
        exp = _parse_iso(row.get("expires_at"))
        if exp is not None and now > exp:
            remove_keys.append(key)
    for key in remove_keys:
        entries.pop(key, None)
    return len(remove_keys)


def security_tokens_list_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    peer_id: str | None,
    status: str | None,
    include_inactive: bool,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    settings: Any,
) -> dict:
    """List token metadata visible to an admin caller."""
    enforce_rate_limit(settings, auth, "security_tokens_list")
    auth.require("admin:peers")
    auth.require_read_path(TOKEN_CONFIG_REL)
    payload = _load_token_config(repo_root)
    now = datetime.now(timezone.utc)
    rows = []
    for row in payload.get("tokens", []):
        if not isinstance(row, dict):
            continue
        view = _token_public_view(row, now)
        if peer_id and str(view.get("peer_id") or "") != peer_id:
            continue
        effective = str(view.get("effective_status") or "")
        if status and effective != status:
            continue
        if not include_inactive and effective != "active":
            continue
        rows.append(view)
    rows.sort(key=lambda x: (str(x.get("issued_at") or ""), str(x.get("token_id") or "")), reverse=True)
    return {"ok": True, "count": len(rows), "tokens": rows}


def security_tokens_issue_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: SecurityTokenIssueRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
    refresh_settings: Callable[[], Any],
) -> dict:
    """Issue a new peer token and persist the updated token configuration."""
    enforce_rate_limit(settings, auth, "security_tokens_issue")
    enforce_payload_limit(settings, req.model_dump(), "security_tokens_issue")
    auth.require("admin:peers")
    auth.require_write_path(TOKEN_CONFIG_REL)

    payload = _load_token_config(repo_root)
    tokens = payload.setdefault("tokens", [])
    if not isinstance(tokens, list):
        tokens = []
        payload["tokens"] = tokens

    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    token_id = req.token_id or f"tok_{now_dt.strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    if any(isinstance(x, dict) and str(x.get("token_id") or "") == token_id for x in tokens):
        raise HTTPException(status_code=409, detail=f"Token id already exists: {token_id}")

    token_plain = f"cgr_{uuid4().hex}{uuid4().hex[:8]}"
    token_sha = hashlib.sha256(token_plain.encode("utf-8")).hexdigest()
    expires_at = _resolve_token_expiry(req.expires_at, req.ttl_seconds)
    scopes = sorted(set(str(s) for s in (req.scopes or []) if str(s))) or sorted(ALL_SCOPES)
    read_ns = sorted(set(str(s) for s in (req.read_namespaces or []) if str(s))) or ["*"]
    write_ns = sorted(set(str(s) for s in (req.write_namespaces or []) if str(s))) or ["*"]

    entry = {
        "token_id": token_id,
        "peer_id": req.peer_id,
        "token_sha256": token_sha,
        "scopes": scopes,
        "read_namespaces": read_ns,
        "write_namespaces": write_ns,
        "status": "active",
        "issued_at": now,
        "expires_at": expires_at,
        "revoked_at": None,
        "revoked_reason": None,
        "description": req.description,
    }
    tokens.append(entry)

    config_path = safe_path(repo_root, TOKEN_CONFIG_REL)
    token_old_bytes = config_path.read_bytes() if config_path.exists() else None
    path = _write_token_config(repo_root, payload)
    committed = safe_commit_updated_file(
        path=path, gm=gm,
        commit_message=f"security: issue token {token_id}",
        error_detail=f"Failed to commit token issuance for {token_id}",
        old_bytes=token_old_bytes,
    )
    refresh_settings()
    audit(auth, "security_tokens_issue", {"token_id": token_id, "peer_id": req.peer_id, "expires_at": expires_at})
    return {"ok": True, "token": token_plain, "token_meta": _token_public_view(entry, now_dt), "committed": committed, "latest_commit": gm.latest_commit()}


def security_tokens_revoke_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: SecurityTokenRevokeRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
    refresh_settings: Callable[[], Any],
) -> dict:
    """Revoke an existing peer token by token id or digest."""
    enforce_rate_limit(settings, auth, "security_tokens_revoke")
    enforce_payload_limit(settings, req.model_dump(), "security_tokens_revoke")
    auth.require("admin:peers")
    auth.require_write_path(TOKEN_CONFIG_REL)

    payload = _load_token_config(repo_root)
    tokens = payload.setdefault("tokens", [])
    if not isinstance(tokens, list):
        tokens = []
        payload["tokens"] = tokens

    if req.revoke_all_for_peer:
        if not req.peer_id:
            raise HTTPException(status_code=400, detail="peer_id is required when revoke_all_for_peer=true")
    else:
        norm_sha = _normalize_token_sha(req.token_sha256)
        if not req.token_id and not norm_sha:
            raise HTTPException(status_code=400, detail="Provide token_id or token_sha256")

    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    norm_sha = _normalize_token_sha(req.token_sha256)
    matched = 0
    revoked = 0
    revoked_rows = []
    for row in tokens:
        if not isinstance(row, dict):
            continue
        is_match = False
        if req.revoke_all_for_peer:
            is_match = str(row.get("peer_id") or "") == str(req.peer_id or "")
        else:
            if req.token_id and str(row.get("token_id") or "") == req.token_id:
                is_match = True
            elif norm_sha and str(row.get("token_sha256") or "") == norm_sha:
                is_match = True
        if not is_match:
            continue
        matched += 1
        if str(row.get("status") or "active") == "active":
            row["status"] = "revoked"
            row["revoked_at"] = now
            row["revoked_reason"] = req.reason
            revoked += 1
        revoked_rows.append(_token_public_view(row, now_dt))

    if matched == 0:
        raise HTTPException(status_code=404, detail="Token entry not found")

    committed = False
    if revoked > 0:
        revoke_config_path = safe_path(repo_root, TOKEN_CONFIG_REL)
        revoke_old_bytes = revoke_config_path.read_bytes() if revoke_config_path.exists() else None
        path = _write_token_config(repo_root, payload)
        committed = safe_commit_updated_file(
            path=path, gm=gm,
            commit_message="security: revoke token(s)",
            error_detail="Failed to commit token revocation",
            old_bytes=revoke_old_bytes,
        )
    refresh_settings()
    audit(auth, "security_tokens_revoke", {"matched": matched, "revoked": revoked, "reason": req.reason})
    return {"ok": True, "matched": matched, "revoked": revoked, "tokens": revoked_rows, "committed": committed, "latest_commit": gm.latest_commit()}


def security_tokens_rotate_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: SecurityTokenRotateRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
    refresh_settings: Callable[[], Any],
) -> dict:
    """Rotate a peer token by issuing a replacement and revoking the prior one."""
    enforce_rate_limit(settings, auth, "security_tokens_rotate")
    enforce_payload_limit(settings, req.model_dump(), "security_tokens_rotate")
    auth.require("admin:peers")
    auth.require_write_path(TOKEN_CONFIG_REL)

    norm_sha = _normalize_token_sha(req.token_sha256)
    if not req.token_id and not norm_sha:
        raise HTTPException(status_code=400, detail="Provide token_id or token_sha256")

    payload = _load_token_config(repo_root)
    tokens = payload.setdefault("tokens", [])
    if not isinstance(tokens, list):
        tokens = []
        payload["tokens"] = tokens

    matched: list[dict[str, Any]] = []
    for row in tokens:
        if not isinstance(row, dict):
            continue
        if req.token_id and str(row.get("token_id") or "") == req.token_id:
            matched.append(row)
            continue
        if norm_sha and str(row.get("token_sha256") or "") == norm_sha:
            matched.append(row)

    if not matched:
        raise HTTPException(status_code=404, detail="Token entry not found")
    if len(matched) > 1:
        raise HTTPException(status_code=409, detail="Multiple tokens matched; use token_id for deterministic rotate")

    src = matched[0]
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    src_effective = _token_effective_status(src, now_dt)
    if src_effective == "revoked":
        raise HTTPException(status_code=409, detail="Token is already revoked")

    source_token_id = str(src.get("token_id") or "")
    peer_id = str(src.get("peer_id") or "")
    if not peer_id:
        raise HTTPException(status_code=400, detail="Matched token is missing peer_id")

    new_token_id = req.new_token_id or f"tok_{now_dt.strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    if any(isinstance(x, dict) and str(x.get("token_id") or "") == new_token_id for x in tokens):
        raise HTTPException(status_code=409, detail=f"Token id already exists: {new_token_id}")

    def _normalize_scopes(values: Any, default_all: bool = True) -> list[str]:
        vals = sorted(set(str(v) for v in (values or []) if str(v)))
        if vals:
            return vals
        return sorted(ALL_SCOPES) if default_all else []

    def _normalize_namespaces(values: Any) -> list[str]:
        vals = sorted(set(str(v) for v in (values or []) if str(v)))
        return vals or ["*"]

    scopes = _normalize_scopes(req.scopes if req.scopes is not None else src.get("scopes"))
    read_ns = _normalize_namespaces(req.read_namespaces if req.read_namespaces is not None else src.get("read_namespaces"))
    write_ns = _normalize_namespaces(req.write_namespaces if req.write_namespaces is not None else src.get("write_namespaces"))
    expires_at = _resolve_token_expiry(req.expires_at, req.ttl_seconds)
    if expires_at is None:
        expires_at = src.get("expires_at")
    description = req.description if req.description is not None else src.get("description")

    token_plain = f"cgr_{uuid4().hex}{uuid4().hex[:8]}"
    token_sha = hashlib.sha256(token_plain.encode("utf-8")).hexdigest()

    src["status"] = "revoked"
    src["revoked_at"] = now
    src["rotated_at"] = now
    src["revoked_reason"] = req.reason or f"rotated_to:{new_token_id}"
    src["rotated_to_token_id"] = new_token_id

    entry = {
        "token_id": new_token_id,
        "peer_id": peer_id,
        "token_sha256": token_sha,
        "scopes": scopes,
        "read_namespaces": read_ns,
        "write_namespaces": write_ns,
        "status": "active",
        "issued_at": now,
        "expires_at": expires_at,
        "revoked_at": None,
        "revoked_reason": None,
        "rotated_at": None,
        "rotated_to_token_id": None,
        "rotated_from_token_id": source_token_id or None,
        "description": description,
    }
    tokens.append(entry)

    rotate_config_path = safe_path(repo_root, TOKEN_CONFIG_REL)
    rotate_old_bytes = rotate_config_path.read_bytes() if rotate_config_path.exists() else None
    path = _write_token_config(repo_root, payload)
    source_ref = source_token_id or "sha-match"
    committed = safe_commit_updated_file(
        path=path, gm=gm,
        commit_message=f"security: rotate token {source_ref} -> {new_token_id}",
        error_detail=f"Failed to commit token rotation {source_ref} -> {new_token_id}",
        old_bytes=rotate_old_bytes,
    )
    refresh_settings()
    audit(
        auth,
        "security_tokens_rotate",
        {
            "peer_id": peer_id,
            "from_token_id": source_token_id or None,
            "to_token_id": new_token_id,
            "expires_at": expires_at,
            "reason": req.reason,
            "source_effective_status": src_effective,
        },
    )
    return {
        "ok": True,
        "token": token_plain,
        "from_token": _token_public_view(src, now_dt),
        "token_meta": _token_public_view(entry, now_dt),
        "committed": committed,
        "latest_commit": gm.latest_commit(),
    }


def security_keys_rotate_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: SecurityKeysRotateRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """Rotate signing keys and update active key metadata."""
    enforce_rate_limit(settings, auth, "security_keys_rotate")
    enforce_payload_limit(settings, req.model_dump(), "security_keys_rotate")
    auth.require("admin:peers")
    auth.require_write_path(SECURITY_KEYS_REL)
    payload = load_security_keys(repo_root)
    now = datetime.now(timezone.utc).isoformat()
    key_id = req.key_id or f"key_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    secret = req.secret or f"{uuid4().hex}{uuid4().hex}"
    keys = payload.setdefault("keys", {})
    if not isinstance(keys, dict):
        keys = {}
        payload["keys"] = keys

    previous_active = str(payload.get("active_key_id") or "") or None
    if req.retire_previous and previous_active and previous_active in keys and previous_active != key_id:
        prev = keys.get(previous_active)
        if isinstance(prev, dict):
            prev["status"] = "retired"
            prev["retired_at"] = now

    prev_row = keys.get(key_id) if isinstance(keys.get(key_id), dict) else {}
    created_at = str(prev_row.get("created_at") or now)
    row = {
        "key_id": key_id,
        "algorithm": "hmac-sha256",
        "status": "active" if req.activate else "staged",
        "created_at": created_at,
        "rotated_at": now,
        "retired_at": None,
        "secret_sha256": _sha256_text(secret),
    }

    storage_mode = "external" if settings.use_external_key_store else "repo"
    # Prepare external key store payload but defer write until after repo commit
    ext_key_store_payload: dict[str, Any] | None = None
    if settings.use_external_key_store:
        key_store = _load_external_key_store(settings)
        ext_keys = key_store.setdefault("keys", {})
        if not isinstance(ext_keys, dict):
            ext_keys = {}
            key_store["keys"] = ext_keys
        ext_keys[key_id] = {"secret": secret, "updated_at": now}
        ext_key_store_payload = key_store
        row["secret_ref"] = f"external:{key_id}"
    else:
        row["secret"] = secret

    keys[key_id] = row
    if req.activate:
        payload["active_key_id"] = key_id

    keys_file_path = safe_path(repo_root, SECURITY_KEYS_REL)
    keys_old_bytes = keys_file_path.read_bytes() if keys_file_path.exists() else None
    path = _write_security_keys(repo_root, payload)
    committed = safe_commit_updated_file(
        path=path, gm=gm,
        commit_message=f"security: rotate key {key_id}",
        error_detail=f"Failed to commit key rotation for {key_id}",
        old_bytes=keys_old_bytes,
    )
    # Write external key store only after repo commit succeeds
    if ext_key_store_payload is not None:
        _write_external_key_store(settings, ext_key_store_payload)
    audit(auth, "security_keys_rotate", {"key_id": key_id, "activate": req.activate, "retire_previous": req.retire_previous, "storage_mode": storage_mode})

    key_view = {
        "key_id": key_id,
        "algorithm": "hmac-sha256",
        "status": row["status"],
        "created_at": created_at,
        "rotated_at": now,
        "storage_mode": storage_mode,
    }
    if req.return_secret:
        key_view["secret"] = secret
    return {"ok": True, "active_key_id": payload.get("active_key_id"), "key": key_view, "committed": committed, "latest_commit": gm.latest_commit()}


def verify_signed_payload_service(
    *,
    settings: Any,
    gm: Any,
    auth: AuthContext,
    payload: dict[str, Any],
    key_id: str,
    nonce: str,
    expires_at: str | None,
    signature: str,
    algorithm: str,
    consume_nonce: bool,
    audit_event: str,
    verification_failure_count: Callable[[Any, AuthContext], int],
    record_verification_failure: Callable[[Any, AuthContext, str], None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """Verify a signed payload against key material, expiry, and nonce rules."""
    if consume_nonce:
        auth.require_write_path(NONCE_INDEX_REL)

    prior_failures = verification_failure_count(settings, auth)
    if prior_failures >= int(settings.verify_failure_limit):
        detail = {
            "valid": False,
            "reason": "verification_throttled",
            "algorithm": algorithm,
            "key_id": key_id,
            "nonce": nonce,
            "expires_at": expires_at,
            "signature_valid": False,
            "expired": False,
            "replay_detected": False,
            "nonce_consumed": False,
            "failure_count": prior_failures,
            "committed_files": [],
        }
        audit(auth, audit_event, detail)
        return detail

    keys_payload = load_security_keys(settings.repo_root)
    keys = keys_payload.get("keys", {})
    key_row = keys.get(key_id) if isinstance(keys, dict) else None
    if not isinstance(key_row, dict):
        raise HTTPException(status_code=404, detail=f"Unknown key_id: {key_id}")
    if str(key_row.get("algorithm") or "hmac-sha256") != algorithm:
        raise HTTPException(status_code=400, detail=f"Algorithm mismatch for key {key_id}")

    secret = _resolve_signing_secret(settings, key_id, key_row)
    if not secret:
        raise HTTPException(status_code=500, detail=f"Key secret missing for {key_id}")

    now = datetime.now(timezone.utc)
    expires_dt = _parse_iso(expires_at)
    expired = expires_dt is not None and now > expires_dt

    blob = _message_signing_blob(payload, key_id, nonce, expires_at)
    expected_signature = _hmac_sha256(secret, blob)
    signature_valid = hmac.compare_digest(expected_signature, signature.strip())
    replay_detected = False
    nonce_consumed = False
    warnings: list[str] = []
    reason = "ok"
    committed_files: list[str] = []

    if expired:
        reason = "expired"
    elif not signature_valid:
        reason = "invalid_signature"
    elif consume_nonce:
        try:
            with segment_history_source_lock("registry:nonce_index", lock_dir=settings.repo_root / ".locks"):
                nonce_payload = _load_nonce_index(settings.repo_root)
                _prune_nonce_entries(nonce_payload, now)
                entries = nonce_payload.setdefault("entries", {})
                key = f"{key_id}|{nonce}"
                if key in entries:
                    replay_detected = True
                    reason = "replay_detected"
                else:
                    entries[key] = {"key_id": key_id, "nonce": nonce, "first_seen_at": now.isoformat(), "expires_at": expires_at}
                    nonce_path = _write_nonce_index(settings.repo_root, nonce_payload)
                    nonce_committed = try_commit_file(
                        path=nonce_path, gm=gm,
                        commit_message=f"messages: consume nonce {key_id}:{nonce}",
                    )
                    if nonce_committed:
                        committed_files.append(NONCE_INDEX_REL)
                    else:
                        warnings.append("nonce_commit_failed: nonce consumed on disk but not committed to git")
                    nonce_consumed = True
        except (SegmentHistoryLockTimeout, LockInfrastructureError):
            raise HTTPException(status_code=503, detail="Nonce index lock unavailable; retry")

    valid = reason == "ok"
    if not valid:
        record_verification_failure(settings, auth, reason)

    detail: dict[str, Any] = {
        "valid": valid,
        "reason": reason,
        "algorithm": algorithm,
        "key_id": key_id,
        "nonce": nonce,
        "expires_at": expires_at,
        "signature_valid": signature_valid,
        "expired": expired,
        "replay_detected": replay_detected,
        "consume_nonce": consume_nonce,
        "nonce_consumed": nonce_consumed,
        "failure_count": verification_failure_count(settings, auth),
        "committed_files": committed_files,
    }
    if warnings:
        detail["warnings"] = warnings
    audit(auth, audit_event, detail)
    return detail


def messages_verify_service(
    *,
    settings: Any,
    gm: Any,
    auth: AuthContext,
    req: MessageVerifyRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    verification_failure_count: Callable[[Any, AuthContext], int],
    record_verification_failure: Callable[[Any, AuthContext, str], None],
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict:
    """Verify a message signature without mutating delivery state."""
    enforce_rate_limit(settings, auth, "messages_verify")
    enforce_payload_limit(settings, req.model_dump(), "messages_verify")
    auth.require("write:messages")
    verification = verify_signed_payload_service(
        settings=settings,
        gm=gm,
        auth=auth,
        payload=req.payload,
        key_id=req.key_id,
        nonce=req.nonce,
        expires_at=req.expires_at,
        signature=req.signature,
        algorithm=req.algorithm,
        consume_nonce=req.consume_nonce,
        audit_event="messages_verify",
        verification_failure_count=verification_failure_count,
        record_verification_failure=record_verification_failure,
        audit=audit,
    )
    return {"ok": True, **verification, "latest_commit": gm.latest_commit()}
