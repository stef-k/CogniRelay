"""Peer registry, trust transitions, and remote manifest lookup logic."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin
from urllib.request import Request as UrlRequest, urlopen

from fastapi import HTTPException

from app.auth import AuthContext
from app.models import PeerRegisterRequest, PeerTrustTransitionRequest
from app.storage import safe_path, write_text_file

PEERS_REGISTRY_REL = "peers/registry.json"
TRUST_POLICIES_REL = "peers/trust_policies.json"


def load_peers_registry(repo_root: Path) -> dict[str, Any]:
    """Load normalized peer registry state from disk."""
    path = safe_path(repo_root, PEERS_REGISTRY_REL)
    if not path.exists():
        return {"schema_version": "1.0", "updated_at": None, "peers": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "updated_at": None, "peers": {}}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "updated_at": None, "peers": {}}
    peers = data.get("peers")
    if not isinstance(peers, dict):
        peers = {}
    return {"schema_version": "1.0", "updated_at": data.get("updated_at"), "peers": peers}


def _write_peers_registry(repo_root: Path, registry: dict[str, Any]) -> Path:
    """Persist the peer registry payload."""
    path = safe_path(repo_root, PEERS_REGISTRY_REL)
    write_text_file(path, json.dumps(registry, ensure_ascii=False, indent=2))
    return path


def _public_key_fingerprint(public_key: str) -> str:
    """Return the canonical fingerprint for a peer public key."""
    return "sha256:" + hashlib.sha256(public_key.encode("utf-8")).hexdigest()


def _normalize_fingerprint(value: str | None) -> str | None:
    """Normalize optional fingerprint input to the canonical sha256 form."""
    if not value:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if not normalized.startswith("sha256:"):
        normalized = "sha256:" + normalized
    return normalized


def _load_trust_policies(repo_root: Path, trust_policies_rel: str) -> dict[str, Any]:
    """Load trust transition policy with defaults applied."""
    path = safe_path(repo_root, trust_policies_rel)
    default = {
        "schema_version": "1.0",
        "allowed_transitions": {
            "untrusted": ["restricted"],
            "restricted": ["trusted", "untrusted"],
            "trusted": ["restricted"],
        },
        "require_reason_on_transition": True,
        "require_fingerprint_for_trusted": True,
    }
    if not path.exists():
        return default
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    if not isinstance(data, dict):
        return default
    transitions = data.get("allowed_transitions")
    if not isinstance(transitions, dict):
        data["allowed_transitions"] = default["allowed_transitions"]
    if "require_reason_on_transition" not in data:
        data["require_reason_on_transition"] = True
    if "require_fingerprint_for_trusted" not in data:
        data["require_fingerprint_for_trusted"] = True
    data.setdefault("schema_version", "1.0")
    return data


def _write_trust_policies(repo_root: Path, trust_policies_rel: str, payload: dict[str, Any]) -> Path:
    """Persist the trust policy payload."""
    path = safe_path(repo_root, trust_policies_rel)
    write_text_file(path, json.dumps(payload, ensure_ascii=False, indent=2))
    return path


def _assert_trust_transition_allowed(policies: dict[str, Any], current: str, target: str, reason: str | None) -> None:
    """Validate a requested trust transition against policy rules."""
    if current == target:
        return
    allowed = policies.get("allowed_transitions", {}).get(current)
    allowed_set = set(str(item) for item in allowed) if isinstance(allowed, list) else set()
    if target not in allowed_set:
        raise HTTPException(status_code=409, detail=f"Trust transition not allowed: {current} -> {target}")
    if bool(policies.get("require_reason_on_transition", True)) and not (reason and str(reason).strip()):
        raise HTTPException(status_code=400, detail="Trust transition reason is required")


def peers_list_service(*, repo_root: Path, auth: AuthContext, audit: Callable[[AuthContext, str, dict[str, Any]], None]) -> dict[str, Any]:
    """Return the visible peer registry list."""
    auth.require("read:files")
    auth.require_read_path(PEERS_REGISTRY_REL)
    registry = load_peers_registry(repo_root)
    peers: list[dict[str, Any]] = []
    for peer_id, record in sorted(registry.get("peers", {}).items(), key=lambda row: row[0]):
        if not isinstance(record, dict):
            continue
        peers.append({"peer_id": peer_id, **record})
    audit(auth, "peers_list", {"count": len(peers)})
    return {"ok": True, "count": len(peers), "peers": peers, "updated_at": registry.get("updated_at")}


def peers_register_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: PeerRegisterRequest,
    trust_policies_rel: str,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Create or update a peer registry entry under the trust policy rules."""
    enforce_rate_limit(settings, auth, "peers_register")
    enforce_payload_limit(settings, req.model_dump(), "peers_register")
    auth.require("admin:peers")
    auth.require_write_path(PEERS_REGISTRY_REL)

    registry = load_peers_registry(repo_root)
    peers = registry.setdefault("peers", {})
    now = datetime.now(timezone.utc).isoformat()
    prev = peers.get(req.peer_id) if isinstance(peers, dict) else None

    policies = _load_trust_policies(repo_root, trust_policies_rel)
    current_trust = str(prev.get("trust_level") or "untrusted") if isinstance(prev, dict) else "untrusted"
    target_trust = req.trust_level

    provided_fp = _public_key_fingerprint(req.public_key) if req.public_key else None
    expected_fp = _normalize_fingerprint(req.expected_public_key_fingerprint)
    prev_fp = str(prev.get("public_key_fingerprint") or "") if isinstance(prev, dict) else ""

    if expected_fp and provided_fp and expected_fp != provided_fp:
        raise HTTPException(status_code=400, detail="public_key fingerprint mismatch")
    if expected_fp and not provided_fp and prev_fp and expected_fp != prev_fp:
        raise HTTPException(status_code=400, detail="existing peer fingerprint does not match expected fingerprint")

    if isinstance(prev, dict):
        _assert_trust_transition_allowed(policies, current_trust, target_trust, req.transition_reason)

    if bool(policies.get("require_fingerprint_for_trusted", True)) and target_trust == "trusted":
        fingerprint_for_trusted = provided_fp or (prev_fp if prev_fp else None)
        if not fingerprint_for_trusted:
            raise HTTPException(status_code=400, detail="public_key fingerprint is required for trusted peers")

    created_at = prev.get("created_at") if isinstance(prev, dict) and prev.get("created_at") else now
    history = []
    if isinstance(prev, dict) and isinstance(prev.get("trust_history"), list):
        history = list(prev.get("trust_history"))
    if current_trust != target_trust:
        history.append(
            {
                "at": now,
                "from": current_trust,
                "to": target_trust,
                "reason": req.transition_reason or "register_update",
                "by": auth.peer_id,
            }
        )

    record = {
        "base_url": req.base_url,
        "public_key": req.public_key,
        "public_key_fingerprint": provided_fp or (prev_fp if prev_fp else None),
        "capabilities_url": req.capabilities_url,
        "trust_level": target_trust,
        "allowed_scopes": req.allowed_scopes,
        "created_at": created_at,
        "updated_at": now,
        "trust_history": history,
    }
    peers[req.peer_id] = record
    registry["updated_at"] = now
    path = _write_peers_registry(repo_root, registry)
    committed = gm.commit_file(path, f"peers: register {req.peer_id}")

    policy_path = _write_trust_policies(repo_root, trust_policies_rel, policies)
    gm.commit_file(policy_path, "peers: ensure trust policies")

    audit(
        auth,
        "peers_register",
        {
            "peer_id": req.peer_id,
            "created": prev is None,
            "trust_from": current_trust,
            "trust_to": target_trust,
            "fingerprint": record.get("public_key_fingerprint"),
        },
    )
    return {"ok": True, "peer": {"peer_id": req.peer_id, **record}, "created": prev is None, "committed": committed, "latest_commit": gm.latest_commit()}


def peers_trust_transition_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    peer_id: str,
    req: PeerTrustTransitionRequest,
    trust_policies_rel: str,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Apply a trust-level transition to an existing peer."""
    enforce_rate_limit(settings, auth, "peers_trust_transition")
    enforce_payload_limit(settings, req.model_dump(), "peers_trust_transition")
    auth.require("admin:peers")
    auth.require_write_path(PEERS_REGISTRY_REL)

    registry = load_peers_registry(repo_root)
    peers = registry.setdefault("peers", {})
    row = peers.get(peer_id) if isinstance(peers, dict) else None
    if not isinstance(row, dict):
        raise HTTPException(status_code=404, detail=f"Peer not found: {peer_id}")

    policies = _load_trust_policies(repo_root, trust_policies_rel)
    current = str(row.get("trust_level") or "untrusted")
    target = req.trust_level
    _assert_trust_transition_allowed(policies, current, target, req.reason)

    expected_fp = _normalize_fingerprint(req.expected_public_key_fingerprint)
    current_fp = _normalize_fingerprint(str(row.get("public_key_fingerprint") or ""))
    if expected_fp and expected_fp != current_fp:
        raise HTTPException(status_code=409, detail="Peer fingerprint mismatch for trust transition")

    now = datetime.now(timezone.utc).isoformat()
    history = row.get("trust_history")
    if not isinstance(history, list):
        history = []
    history.append({"at": now, "from": current, "to": target, "reason": req.reason, "by": auth.peer_id})
    row["trust_level"] = target
    row["updated_at"] = now
    row["trust_history"] = history
    peers[peer_id] = row
    registry["updated_at"] = now

    path = _write_peers_registry(repo_root, registry)
    committed = gm.commit_file(path, f"peers: trust transition {peer_id} {current}->{target}")
    audit(auth, "peers_trust_transition", {"peer_id": peer_id, "from": current, "to": target, "reason": req.reason})
    return {"ok": True, "peer": {"peer_id": peer_id, **row}, "committed": committed, "latest_commit": gm.latest_commit()}


def peer_manifest_service(*, repo_root: Path, auth: AuthContext, peer_id: str, audit: Callable[[AuthContext, str, dict[str, Any]], None]) -> dict[str, Any]:
    """Fetch and return the remote manifest for a registered peer."""
    auth.require("read:files")
    auth.require_read_path(PEERS_REGISTRY_REL)
    registry = load_peers_registry(repo_root)
    record = registry.get("peers", {}).get(peer_id)
    if not isinstance(record, dict):
        raise HTTPException(status_code=404, detail=f"Peer not found: {peer_id}")

    base_url = str(record.get("base_url") or "").strip()
    capabilities_url = str(record.get("capabilities_url") or "/v1/manifest").strip() or "/v1/manifest"
    if not base_url:
        raise HTTPException(status_code=400, detail=f"Peer {peer_id} has empty base_url")
    source_url = urljoin(base_url.rstrip("/") + "/", capabilities_url.lstrip("/"))

    try:
        with urlopen(UrlRequest(source_url, headers={"Accept": "application/json"}), timeout=10) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            manifest_payload = json.loads(body)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch peer manifest: {exc}") from exc

    audit(auth, "peer_manifest_fetch", {"peer_id": peer_id, "source_url": source_url})
    return {"ok": True, "peer_id": peer_id, "source_url": source_url, "manifest": manifest_payload}
