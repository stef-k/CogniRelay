"""Shared coordination artifact service logic."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException
from pydantic import ValidationError

from app.auth import AuthContext
from app.coordination.common import (
    is_admin,
    persist_new_artifact,
    persist_updated_artifact,
    query_identity_allowed,
    utc_now,
    validate_prefixed_hex_id,
)
from app.models import (
    CoordinationSharedArtifact,
    CoordinationSharedCreateRequest,
    CoordinationSharedQueryRequest,
    CoordinationSharedUpdateRequest,
)
from app.peers.service import load_peers_registry
from app.storage import read_text_file, safe_path

SHARED_DIR_REL = "memory/coordination/shared"
SHARED_SAMPLE_REL = f"{SHARED_DIR_REL}/x.json"
SHARED_INVALID_WARNING = "coordination_shared_artifact_skipped_invalid"
INVALID_SHARED_ID_DETAIL = "Invalid shared coordination artifact id"


def _shared_rel_path(shared_id: str) -> str:
    """Return the repository-relative storage path for one shared artifact."""
    return f"{SHARED_DIR_REL}/{shared_id}.json"


def _shared_path(repo_root: Path, shared_id: str) -> Path:
    """Resolve the repository path for one stored shared coordination artifact."""
    return safe_path(repo_root, _shared_rel_path(shared_id))


def _shared_query_sort_key(artifact: dict[str, Any]) -> tuple[float, str]:
    """Return the deterministic sort key for shared coordination query results."""
    updated_at = str(artifact.get("updated_at") or "")
    try:
        dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        updated_value = dt.timestamp()
    except Exception:
        updated_value = 0.0
    return (-updated_value, str(artifact.get("shared_id") or ""))


def _load_shared_artifact(repo_root: Path, shared_id: str) -> tuple[str, dict[str, Any]]:
    """Load and structurally validate one stored shared coordination artifact."""
    validate_prefixed_hex_id(shared_id, prefix="shared_", detail=INVALID_SHARED_ID_DETAIL)
    rel = _shared_rel_path(shared_id)
    path = _shared_path(repo_root, shared_id)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Shared coordination artifact not found")
    try:
        payload = json.loads(read_text_file(path))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid shared coordination artifact JSON: {exc}") from exc
    try:
        artifact = CoordinationSharedArtifact.model_validate(payload).model_dump(mode="json")
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid shared coordination artifact: {exc}") from exc
    return rel, artifact


def _ensure_shared_read_visibility(auth: AuthContext, artifact: dict[str, Any]) -> None:
    """Require owner, participant, or admin visibility for one shared artifact."""
    if is_admin(auth):
        return
    caller = getattr(auth, "peer_id", "")
    if caller == artifact.get("owner_peer"):
        return
    participants = artifact.get("participant_peers")
    if isinstance(participants, list) and caller in participants:
        return
    raise HTTPException(status_code=403, detail="Shared coordination artifact not visible to caller")


def _validate_shared_state_item_lengths(items: list[str], field_name: str) -> None:
    """Enforce per-item min/max length rules for shared-state arrays."""
    for item in items:
        if len(item) < 1:
            raise HTTPException(status_code=400, detail=f"Value too short in coordination_shared.{field_name}")
        if len(item) > 160:
            raise HTTPException(status_code=400, detail=f"Value too long in coordination_shared.{field_name}")


def _validate_shared_coordination_request(
    *,
    title: str,
    summary: str | None,
    constraints: list[str],
    drift_signals: list[str],
    coordination_alerts: list[str],
    commit_message: str | None,
) -> None:
    """Apply the deterministic 5B service-layer validation rules."""
    if len(title) < 1:
        raise HTTPException(status_code=400, detail="Value too short in coordination_shared.title")
    if len(title) > 120:
        raise HTTPException(status_code=400, detail="Value too long in coordination_shared.title")
    if summary is not None:
        if len(summary) < 1:
            raise HTTPException(status_code=400, detail="Value too short in coordination_shared.summary")
        if len(summary) > 240:
            raise HTTPException(status_code=400, detail="Value too long in coordination_shared.summary")
    _validate_shared_state_item_lengths(constraints, "constraints")
    _validate_shared_state_item_lengths(drift_signals, "drift_signals")
    _validate_shared_state_item_lengths(coordination_alerts, "coordination_alerts")
    if not constraints and not drift_signals and not coordination_alerts:
        raise HTTPException(status_code=400, detail="Shared coordination state must include at least one shared item")
    if commit_message is not None and len(commit_message) > 120:
        raise HTTPException(status_code=400, detail="Value too long in coordination_shared.commit_message")


def _persist_new_shared_artifact(
    *,
    repo_root: Path,
    gm: Any,
    artifact: dict[str, Any],
    commit_message: str,
) -> str:
    """Persist one newly created shared artifact and roll it back on commit failure."""
    shared_id = str(artifact["shared_id"])
    rel = _shared_rel_path(shared_id)
    return persist_new_artifact(
        path=_shared_path(repo_root, shared_id),
        rel=rel,
        gm=gm,
        artifact=artifact,
        commit_message=commit_message,
        error_detail="Failed to commit shared coordination artifact",
    )


def _persist_updated_shared_artifact(
    *,
    repo_root: Path,
    gm: Any,
    shared_id: str,
    artifact: dict[str, Any],
    commit_message: str,
) -> str:
    """Persist an updated shared artifact and restore prior bytes if commit fails."""
    rel = _shared_rel_path(shared_id)
    return persist_updated_artifact(
        path=_shared_path(repo_root, shared_id),
        rel=rel,
        gm=gm,
        artifact=artifact,
        commit_message=commit_message,
        error_detail="Failed to commit shared coordination update",
    )


def shared_create_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: CoordinationSharedCreateRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Create one owner-authored shared coordination artifact."""
    enforce_rate_limit(settings, auth, "coordination_shared_create")
    enforce_payload_limit(settings, req.model_dump(), "coordination_shared_create")
    auth.require("write:projects")
    auth.require_write_path(SHARED_SAMPLE_REL)

    registry = load_peers_registry(repo_root)
    seen: set[str] = set()
    for participant_peer in req.participant_peers:
        if participant_peer == auth.peer_id:
            raise HTTPException(status_code=400, detail="participant_peers must not include owner_peer")
        if participant_peer in seen:
            raise HTTPException(status_code=400, detail="participant_peers must not contain duplicates")
        seen.add(participant_peer)
        peer = registry.get("peers", {}).get(participant_peer)
        if not isinstance(peer, dict):
            raise HTTPException(status_code=404, detail=f"Peer not found: {participant_peer}")
        trust_level = str(peer.get("trust_level") or "untrusted")
        if trust_level == "untrusted":
            raise HTTPException(status_code=409, detail=f"Peer is untrusted: {participant_peer}")

    _validate_shared_coordination_request(
        title=req.title,
        summary=req.summary,
        constraints=req.constraints,
        drift_signals=req.drift_signals,
        coordination_alerts=req.coordination_alerts,
        commit_message=req.commit_message,
    )
    now = utc_now()
    artifact = CoordinationSharedArtifact(
        shared_id=f"shared_{uuid4().hex}",
        created_at=now,
        updated_at=now,
        created_by=auth.peer_id,
        owner_peer=auth.peer_id,
        participant_peers=list(req.participant_peers),
        task_id=req.task_id,
        thread_id=req.thread_id,
        title=req.title,
        summary=req.summary,
        shared_state={
            "constraints": list(req.constraints),
            "drift_signals": list(req.drift_signals),
            "coordination_alerts": list(req.coordination_alerts),
        },
        version=1,
        last_updated_by=auth.peer_id,
    ).model_dump(mode="json")

    commit_message = req.commit_message
    if commit_message is None or not commit_message.strip():
        commit_message = f"coordination: create {artifact['shared_id']}"
    rel = _persist_new_shared_artifact(repo_root=repo_root, gm=gm, artifact=artifact, commit_message=commit_message)
    audit(
        auth,
        "coordination_shared_create",
        {
            "shared_id": artifact["shared_id"],
            "owner_peer": artifact["owner_peer"],
            "participant_peers": artifact["participant_peers"],
            "task_id": artifact["task_id"],
            "thread_id": artifact["thread_id"],
            "path": rel,
        },
    )
    return {"ok": True, "shared": artifact, "path": rel, "created": True, "latest_commit": gm.latest_commit()}


def shared_read_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    shared_id: str,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Read one shared coordination artifact after artifact-based visibility checks."""
    enforce_rate_limit(settings, auth, "coordination_shared_read")
    rel, artifact = _load_shared_artifact(repo_root, shared_id)
    _ensure_shared_read_visibility(auth, artifact)
    audit(
        auth,
        "coordination_shared_read",
        {"shared_id": shared_id, "owner_peer": artifact["owner_peer"], "viewer_peer": auth.peer_id, "path": rel},
    )
    return {"ok": True, "shared": artifact, "path": rel}


def shared_query_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: CoordinationSharedQueryRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """List visible shared coordination artifacts for bounded owner/participant queries."""
    enforce_rate_limit(settings, auth, "coordination_shared_query")
    auth.require("read:files")
    if not is_admin(auth):
        # First require a self identity to block task-only/thread-only discovery for non-admin callers.
        if req.owner_peer != auth.peer_id and req.participant_peer != auth.peer_id:
            raise HTTPException(status_code=403, detail="Non-admin callers must include their own shared coordination identity")
    # Then reject mixed self/foreign identity combinations deterministically.
    if not query_identity_allowed(auth, req.owner_peer) or not query_identity_allowed(auth, req.participant_peer):
        raise HTTPException(status_code=403, detail="Non-admin callers may query only their own shared coordination identity")

    directory = safe_path(repo_root, SHARED_DIR_REL)
    warnings: list[str] = []
    visible: list[dict[str, Any]] = []
    if directory.exists() and directory.is_dir():
        invalid_seen = False
        for path in sorted(directory.iterdir(), key=lambda item: item.name):
            if path.is_dir() or path.suffix.lower() != ".json":
                continue
            try:
                payload = json.loads(read_text_file(path))
                artifact = CoordinationSharedArtifact.model_validate(payload).model_dump(mode="json")
            except (json.JSONDecodeError, ValidationError, OSError):
                invalid_seen = True
                continue
            if req.owner_peer is not None and artifact.get("owner_peer") != req.owner_peer:
                continue
            participants = artifact.get("participant_peers")
            if req.participant_peer is not None and (not isinstance(participants, list) or req.participant_peer not in participants):
                continue
            if req.task_id is not None and artifact.get("task_id") != req.task_id:
                continue
            if req.thread_id is not None and artifact.get("thread_id") != req.thread_id:
                continue
            try:
                _ensure_shared_read_visibility(auth, artifact)
            except HTTPException as exc:
                if exc.status_code == 403:
                    continue
                raise
            visible.append(artifact)
        if invalid_seen:
            warnings.append(SHARED_INVALID_WARNING)

    visible.sort(key=_shared_query_sort_key)
    total_matches = len(visible)
    shared_artifacts = visible[req.offset : req.offset + req.limit]
    audit(
        auth,
        "coordination_shared_query",
        {
            "owner_peer": req.owner_peer,
            "participant_peer": req.participant_peer,
            "task_id": req.task_id,
            "thread_id": req.thread_id,
            "count": len(shared_artifacts),
            "total_matches": total_matches,
        },
    )
    return {
        "ok": True,
        "count": len(shared_artifacts),
        "total_matches": total_matches,
        "warnings": warnings,
        "shared_artifacts": shared_artifacts,
    }


def shared_update_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    shared_id: str,
    req: CoordinationSharedUpdateRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Replace one shared coordination payload under owner-only version-checked semantics."""
    enforce_rate_limit(settings, auth, "coordination_shared_update")
    enforce_payload_limit(settings, req.model_dump(), "coordination_shared_update")
    auth.require("write:projects")
    auth.require_write_path(SHARED_SAMPLE_REL)
    _, artifact = _load_shared_artifact(repo_root, shared_id)
    if getattr(auth, "peer_id", "") != artifact.get("owner_peer"):
        raise HTTPException(status_code=403, detail="Only the owner may update this shared coordination artifact")

    _validate_shared_coordination_request(
        title=req.title,
        summary=req.summary,
        constraints=req.constraints,
        drift_signals=req.drift_signals,
        coordination_alerts=req.coordination_alerts,
        commit_message=req.commit_message,
    )
    current_version = int(artifact.get("version") or 0)
    if req.expected_version != current_version:
        raise HTTPException(status_code=409, detail="Shared coordination version conflict")

    updated = dict(artifact)
    updated["title"] = req.title
    updated["summary"] = req.summary
    updated["shared_state"] = {
        "constraints": list(req.constraints),
        "drift_signals": list(req.drift_signals),
        "coordination_alerts": list(req.coordination_alerts),
    }
    updated["updated_at"] = utc_now()
    updated["version"] = current_version + 1
    updated["last_updated_by"] = auth.peer_id
    commit_message = req.commit_message
    if commit_message is None or not commit_message.strip():
        commit_message = f"coordination: update {shared_id} v{updated['version']}"
    rel = _persist_updated_shared_artifact(
        repo_root=repo_root,
        gm=gm,
        shared_id=shared_id,
        artifact=updated,
        commit_message=commit_message,
    )
    audit(
        auth,
        "coordination_shared_update",
        {
            "shared_id": shared_id,
            "owner_peer": artifact["owner_peer"],
            "version": updated["version"],
            "updated_by": auth.peer_id,
            "path": rel,
        },
    )
    return {"ok": True, "shared": updated, "path": rel, "updated": True, "latest_commit": gm.latest_commit()}
