"""Inter-agent handoff and shared coordination artifact service logic."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException
from pydantic import ValidationError

from app.auth import AuthContext
from app.continuity.service import (
    _capsule_health_summary,
    _load_capsule,
    _verification_status,
    continuity_rel_path,
)
from app.models import (
    CoordinationHandoffArtifact,
    CoordinationHandoffConsumeRequest,
    CoordinationHandoffCreateRequest,
    CoordinationHandoffQueryRequest,
    CoordinationSharedArtifact,
    CoordinationSharedCreateRequest,
    CoordinationSharedQueryRequest,
    CoordinationSharedUpdateRequest,
)
from app.peers.service import load_peers_registry
from app.storage import canonical_json, read_text_file, safe_path, write_text_file

HANDOFFS_DIR_REL = "memory/coordination/handoffs"
HANDOFFS_SAMPLE_REL = f"{HANDOFFS_DIR_REL}/x.json"
HANDOFF_INVALID_WARNING = "handoff_artifact_skipped_invalid"
SHARED_DIR_REL = "memory/coordination/shared"
SHARED_SAMPLE_REL = f"{SHARED_DIR_REL}/x.json"
SHARED_INVALID_WARNING = "coordination_shared_artifact_skipped_invalid"


def _utc_now() -> str:
    """Return a normalized UTC timestamp for persisted handoff artifacts."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _is_admin(auth: AuthContext) -> bool:
    """Return whether the caller can bypass sender/recipient visibility checks."""
    return "admin:peers" in getattr(auth, "scopes", set())


def _handoff_rel_path(handoff_id: str) -> str:
    """Return the repository-relative storage path for one handoff artifact."""
    return f"{HANDOFFS_DIR_REL}/{handoff_id}.json"


def _handoff_path(repo_root: Path, handoff_id: str) -> Path:
    """Resolve the repository path for one stored handoff artifact."""
    return safe_path(repo_root, _handoff_rel_path(handoff_id))


def _shared_rel_path(shared_id: str) -> str:
    """Return the repository-relative storage path for one shared artifact."""
    return f"{SHARED_DIR_REL}/{shared_id}.json"


def _shared_path(repo_root: Path, shared_id: str) -> Path:
    """Resolve the repository path for one stored shared coordination artifact."""
    return safe_path(repo_root, _shared_rel_path(shared_id))


def _project_shared_continuity(capsule: dict[str, Any]) -> dict[str, list[str]]:
    """Project the 5A shareable continuity subset from one active capsule."""
    continuity = capsule.get("continuity") if isinstance(capsule.get("continuity"), dict) else {}
    active_constraints = continuity.get("active_constraints")
    drift_signals = continuity.get("drift_signals")
    return {
        "active_constraints": list(active_constraints) if isinstance(active_constraints, list) else [],
        "drift_signals": list(drift_signals) if isinstance(drift_signals, list) else [],
    }


def _source_summary(capsule: dict[str, Any], path: str) -> dict[str, str]:
    """Return the source-capsule summary embedded in a handoff artifact."""
    health_status, _reasons = _capsule_health_summary(capsule)
    return {
        "path": path,
        "updated_at": str(capsule.get("updated_at") or ""),
        "verified_at": str(capsule.get("verified_at") or ""),
        "verification_status": _verification_status(capsule),
        "health_status": health_status,
    }


def _load_handoff_artifact(repo_root: Path, handoff_id: str) -> tuple[str, dict[str, Any]]:
    """Load and structurally validate one stored handoff artifact."""
    rel = _handoff_rel_path(handoff_id)
    path = _handoff_path(repo_root, handoff_id)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Handoff artifact not found")
    try:
        payload = json.loads(read_text_file(path))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid handoff artifact JSON: {exc}") from exc
    try:
        artifact = CoordinationHandoffArtifact.model_validate(payload).model_dump(mode="json")
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid handoff artifact: {exc}") from exc
    return rel, artifact


def _ensure_read_visibility(auth: AuthContext, artifact: dict[str, Any]) -> None:
    """Require sender, recipient, or admin visibility for a loaded handoff artifact."""
    if _is_admin(auth):
        return
    caller = getattr(auth, "peer_id", "")
    if caller == artifact.get("sender_peer") or caller == artifact.get("recipient_peer"):
        return
    raise HTTPException(status_code=403, detail="Handoff artifact not visible to caller")


def _ensure_consume_visibility(auth: AuthContext, artifact: dict[str, Any]) -> None:
    """Require recipient-only visibility for handoff consume operations."""
    if getattr(auth, "peer_id", "") == artifact.get("recipient_peer"):
        return
    raise HTTPException(status_code=403, detail="Only the intended recipient may consume this handoff")


def _query_identity_allowed(auth: AuthContext, peer_id: str | None) -> bool:
    """Return whether the caller may query for the provided sender or recipient identity."""
    if peer_id is None:
        return True
    if _is_admin(auth):
        return True
    return getattr(auth, "peer_id", "") == peer_id


def _query_sort_key(artifact: dict[str, Any]) -> tuple[float, str]:
    """Return the deterministic sort key for handoff query results."""
    created_at = str(artifact.get("created_at") or "")
    try:
        dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        created_value = dt.timestamp()
    except Exception:
        created_value = 0.0
    return (-created_value, str(artifact.get("handoff_id") or ""))


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
    if _is_admin(auth):
        return
    caller = getattr(auth, "peer_id", "")
    if caller == artifact.get("owner_peer"):
        return
    participants = artifact.get("participant_peers")
    if isinstance(participants, list) and caller in participants:
        return
    raise HTTPException(status_code=403, detail="Shared coordination artifact not visible to caller")


def _shared_query_identity_allowed(auth: AuthContext, peer_id: str | None) -> bool:
    """Return whether the caller may query for the provided owner or participant identity."""
    if peer_id is None:
        return True
    if _is_admin(auth):
        return True
    return getattr(auth, "peer_id", "") == peer_id


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
    path = _shared_path(repo_root, shared_id)
    write_text_file(path, canonical_json(artifact))
    try:
        committed = gm.commit_file(path, commit_message)
        if not committed:
            raise RuntimeError("git commit produced no changes")
    except Exception as exc:
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Failed to commit shared coordination artifact") from exc
    return _shared_rel_path(shared_id)


def _persist_updated_shared_artifact(
    *,
    repo_root: Path,
    gm: Any,
    shared_id: str,
    artifact: dict[str, Any],
    commit_message: str,
) -> str:
    """Persist an updated shared artifact and restore prior bytes if commit fails."""
    path = _shared_path(repo_root, shared_id)
    old_bytes = path.read_bytes() if path.exists() else None
    write_text_file(path, canonical_json(artifact))
    try:
        committed = gm.commit_file(path, commit_message)
        if not committed:
            raise RuntimeError("git commit produced no changes")
    except Exception as exc:
        try:
            if old_bytes is None:
                if path.exists():
                    path.unlink()
            else:
                path.write_bytes(old_bytes)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Failed to commit shared coordination update") from exc
    return _shared_rel_path(shared_id)


def _persist_new_handoff(
    *,
    repo_root: Path,
    gm: Any,
    artifact: dict[str, Any],
    commit_message: str,
) -> tuple[str, str]:
    """Persist one newly created handoff artifact and roll it back on commit failure."""
    handoff_id = str(artifact["handoff_id"])
    rel = _handoff_rel_path(handoff_id)
    path = _handoff_path(repo_root, handoff_id)
    write_text_file(path, canonical_json(artifact))
    try:
        committed = gm.commit_file(path, commit_message)
        if not committed:
            raise RuntimeError("git commit produced no changes")
    except Exception as exc:
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Failed to commit handoff artifact") from exc
    return rel, canonical_json(artifact)


def _persist_updated_handoff(
    *,
    repo_root: Path,
    gm: Any,
    handoff_id: str,
    artifact: dict[str, Any],
    commit_message: str,
) -> str:
    """Persist an updated handoff artifact and restore prior bytes if commit fails."""
    path = _handoff_path(repo_root, handoff_id)
    old_bytes = path.read_bytes() if path.exists() else None
    write_text_file(path, canonical_json(artifact))
    try:
        committed = gm.commit_file(path, commit_message)
        if not committed:
            raise RuntimeError("git commit produced no changes")
    except Exception as exc:
        try:
            if old_bytes is None:
                if path.exists():
                    path.unlink()
            else:
                path.write_bytes(old_bytes)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Failed to commit handoff consume update") from exc
    return _handoff_rel_path(handoff_id)


def handoff_create_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: CoordinationHandoffCreateRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Create one bounded inter-agent handoff artifact from an active continuity capsule."""
    enforce_rate_limit(settings, auth, "coordination_handoff_create")
    enforce_payload_limit(settings, req.model_dump(), "coordination_handoff_create")
    auth.require("write:projects")
    auth.require_write_path(HANDOFFS_SAMPLE_REL)

    registry = load_peers_registry(repo_root)
    peer = registry.get("peers", {}).get(req.recipient_peer)
    if not isinstance(peer, dict):
        raise HTTPException(status_code=404, detail=f"Peer not found: {req.recipient_peer}")
    trust_level = str(peer.get("trust_level") or "untrusted")
    if trust_level == "untrusted":
        raise HTTPException(status_code=409, detail=f"Peer is untrusted: {req.recipient_peer}")

    source_path = continuity_rel_path(req.subject_kind, req.subject_id)
    capsule = _load_capsule(repo_root, source_path, expected_subject=(req.subject_kind, req.subject_id))
    artifact = CoordinationHandoffArtifact(
        handoff_id=f"handoff_{uuid4().hex}",
        created_at=_utc_now(),
        created_by=auth.peer_id,
        sender_peer=auth.peer_id,
        recipient_peer=req.recipient_peer,
        source_selector={"subject_kind": req.subject_kind, "subject_id": req.subject_id},
        source_summary=_source_summary(capsule, source_path),
        task_id=req.task_id,
        thread_id=req.thread_id,
        note=req.note,
        shared_continuity=_project_shared_continuity(capsule),
    ).model_dump(mode="json")

    commit_message = req.commit_message if req.commit_message is not None else None
    if commit_message is None or not commit_message.strip():
        commit_message = f"handoff: create {artifact['handoff_id']}"
    rel, _canonical = _persist_new_handoff(repo_root=repo_root, gm=gm, artifact=artifact, commit_message=commit_message)
    audit(
        auth,
        "handoff_create",
        {
            "handoff_id": artifact["handoff_id"],
            "sender_peer": artifact["sender_peer"],
            "recipient_peer": artifact["recipient_peer"],
            "source_selector": artifact["source_selector"],
            "path": rel,
        },
    )
    return {"ok": True, "handoff": artifact, "path": rel, "created": True, "latest_commit": gm.latest_commit()}


def handoff_read_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    handoff_id: str,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Read one handoff artifact after artifact-based sender/recipient visibility checks."""
    enforce_rate_limit(settings, auth, "coordination_handoff_read")
    rel, artifact = _load_handoff_artifact(repo_root, handoff_id)
    _ensure_read_visibility(auth, artifact)
    audit(auth, "handoff_read", {"handoff_id": handoff_id, "path": rel})
    return {"ok": True, "handoff": artifact, "path": rel}


def handoffs_query_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: CoordinationHandoffQueryRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """List visible handoff artifacts for one sender or recipient identity filter."""
    enforce_rate_limit(settings, auth, "coordination_handoff_query")
    auth.require("read:files")
    if req.recipient_peer is None and req.sender_peer is None:
        raise HTTPException(status_code=400, detail="recipient_peer or sender_peer is required")
    if not _query_identity_allowed(auth, req.recipient_peer) or not _query_identity_allowed(auth, req.sender_peer):
        raise HTTPException(status_code=403, detail="Non-admin callers may query only their own handoff identity")

    directory = safe_path(repo_root, HANDOFFS_DIR_REL)
    warnings: list[str] = []
    visible: list[dict[str, Any]] = []
    if directory.exists() and directory.is_dir():
        invalid_seen = False
        for path in sorted(directory.iterdir(), key=lambda item: item.name):
            if path.is_dir() or path.suffix.lower() != ".json":
                continue
            try:
                payload = json.loads(read_text_file(path))
                artifact = CoordinationHandoffArtifact.model_validate(payload).model_dump(mode="json")
            except (json.JSONDecodeError, ValidationError, OSError):
                invalid_seen = True
                continue
            if req.recipient_peer is not None and artifact.get("recipient_peer") != req.recipient_peer:
                continue
            if req.sender_peer is not None and artifact.get("sender_peer") != req.sender_peer:
                continue
            if req.status is not None and artifact.get("recipient_status") != req.status:
                continue
            try:
                _ensure_read_visibility(auth, artifact)
            except HTTPException as exc:
                if exc.status_code == 403:
                    continue
                raise
            visible.append(artifact)
        if invalid_seen:
            warnings.append(HANDOFF_INVALID_WARNING)

    visible.sort(key=_query_sort_key)
    total_matches = len(visible)
    handoffs = visible[req.offset : req.offset + req.limit]
    audit(
        auth,
        "handoff_query",
        {
            "sender_peer": req.sender_peer,
            "recipient_peer": req.recipient_peer,
            "status": req.status,
            "offset": req.offset,
            "limit": req.limit,
            "count": len(handoffs),
            "total_matches": total_matches,
            "warnings": warnings,
        },
    )
    return {
        "ok": True,
        "count": len(handoffs),
        "total_matches": total_matches,
        "warnings": warnings,
        "handoffs": handoffs,
    }


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
    now = _utc_now()
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

    commit_message = req.commit_message if req.commit_message is not None else None
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
    if not _is_admin(auth):
        if req.owner_peer != auth.peer_id and req.participant_peer != auth.peer_id:
            raise HTTPException(status_code=403, detail="Non-admin callers must include their own shared coordination identity")
    if not _shared_query_identity_allowed(auth, req.owner_peer) or not _shared_query_identity_allowed(auth, req.participant_peer):
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
    rel, artifact = _load_shared_artifact(repo_root, shared_id)
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
    updated["updated_at"] = _utc_now()
    updated["version"] = current_version + 1
    updated["last_updated_by"] = auth.peer_id
    commit_message = req.commit_message if req.commit_message is not None else None
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


def handoff_consume_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    handoff_id: str,
    req: CoordinationHandoffConsumeRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Record the recipient's deterministic consume outcome for one handoff artifact."""
    enforce_rate_limit(settings, auth, "coordination_handoff_consume")
    enforce_payload_limit(settings, req.model_dump(), "coordination_handoff_consume")
    rel, artifact = _load_handoff_artifact(repo_root, handoff_id)
    _ensure_consume_visibility(auth, artifact)

    current_status = str(artifact.get("recipient_status") or "pending")
    current_reason = artifact.get("recipient_reason")
    if current_status == req.status and current_reason == req.reason:
        return {
            "ok": True,
            "handoff": artifact,
            "path": rel,
            "updated": False,
            "latest_commit": gm.latest_commit(),
        }
    if current_status != "pending":
        raise HTTPException(status_code=409, detail="Handoff has already been consumed")

    updated = dict(artifact)
    updated["recipient_status"] = req.status
    updated["recipient_reason"] = req.reason
    updated["consumed_at"] = _utc_now()
    updated["consumed_by"] = auth.peer_id
    rel = _persist_updated_handoff(
        repo_root=repo_root,
        gm=gm,
        handoff_id=handoff_id,
        artifact=updated,
        commit_message=f"handoff: consume {handoff_id} {req.status}",
    )
    audit(
        auth,
        "handoff_consume",
        {
            "handoff_id": handoff_id,
            "recipient_status": req.status,
            "consumed_by": auth.peer_id,
            "path": rel,
        },
    )
    return {"ok": True, "handoff": updated, "path": rel, "updated": True, "latest_commit": gm.latest_commit()}
