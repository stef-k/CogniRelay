"""Inter-agent handoff artifact service logic."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException
from pydantic import ValidationError

from app.auth import AuthContext
from app.timestamps import iso_to_posix
from app.continuity.freshness import _capsule_health_summary, _verification_status
from app.continuity.persistence import _load_capsule
from app.continuity.paths import continuity_rel_path
from app.coordination.common import is_admin, persist_new_artifact, persist_updated_artifact, query_identity_allowed, utc_now, validate_prefixed_hex_id
from app.coordination.locking import ArtifactLockInfrastructureError, ArtifactLockTimeout, artifact_lock
from app.lifecycle_warnings import make_lock_error
from app.models import (
    CoordinationHandoffArtifact,
    CoordinationHandoffConsumeRequest,
    CoordinationHandoffCreateRequest,
    CoordinationHandoffQueryRequest,
)
from app.peers.service import load_peers_registry
from app.storage import read_text_file, safe_path

_log = logging.getLogger(__name__)

HANDOFFS_DIR_REL = "memory/coordination/handoffs"
INVALID_HANDOFF_ID_DETAIL = "Invalid handoff artifact id"
HANDOFFS_SAMPLE_REL = f"{HANDOFFS_DIR_REL}/x.json"
HANDOFF_INVALID_WARNING = "handoff_artifact_skipped_invalid"
SCAN_THRESHOLD_WARNING = "coordination_query_scan_threshold_exceeded"


def _handoff_rel_path(handoff_id: str) -> str:
    """Return the repository-relative storage path for one handoff artifact."""
    return f"{HANDOFFS_DIR_REL}/{handoff_id}.json"


def _handoff_path(repo_root: Path, handoff_id: str) -> Path:
    """Resolve the repository path for one stored handoff artifact."""
    return safe_path(repo_root, _handoff_rel_path(handoff_id))


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
    if is_admin(auth):
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


def _query_sort_key(artifact: dict[str, Any]) -> tuple[float, str]:
    """Return the deterministic sort key for handoff query results."""
    created_at = str(artifact.get("created_at") or "")
    return (-iso_to_posix(created_at), str(artifact.get("handoff_id") or ""))


def _persist_new_handoff(
    *,
    repo_root: Path,
    gm: Any,
    artifact: dict[str, Any],
    commit_message: str,
) -> str:
    """Persist one newly created handoff artifact and roll it back on commit failure."""
    handoff_id = str(artifact["handoff_id"])
    rel = _handoff_rel_path(handoff_id)
    return persist_new_artifact(
        path=_handoff_path(repo_root, handoff_id),
        rel=rel,
        gm=gm,
        artifact=artifact,
        commit_message=commit_message,
        error_detail="Failed to commit handoff artifact",
    )


def _persist_updated_handoff(
    *,
    repo_root: Path,
    gm: Any,
    handoff_id: str,
    artifact: dict[str, Any],
    commit_message: str,
) -> str:
    """Persist an updated handoff artifact and restore prior bytes if commit fails."""
    rel = _handoff_rel_path(handoff_id)
    return persist_updated_artifact(
        path=_handoff_path(repo_root, handoff_id),
        rel=rel,
        gm=gm,
        artifact=artifact,
        commit_message=commit_message,
        error_detail="Failed to commit handoff consume update",
    )


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
    if req.commit_message is not None and len(req.commit_message) > 120:
        raise HTTPException(status_code=400, detail="Value too long in coordination_handoff.commit_message")

    source_path = continuity_rel_path(req.subject_kind, req.subject_id)
    capsule = _load_capsule(repo_root, source_path, expected_subject=(req.subject_kind, req.subject_id))
    artifact = CoordinationHandoffArtifact(
        handoff_id=f"handoff_{uuid4().hex}",
        created_at=utc_now(),
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

    commit_message = req.commit_message
    if commit_message is None or not commit_message.strip():
        commit_message = f"handoff: create {artifact['handoff_id']}"
    rel = _persist_new_handoff(repo_root=repo_root, gm=gm, artifact=artifact, commit_message=commit_message)
    # Keep the SQLite sidecar index in sync after successful persist.
    from app.coordination.query_index import try_upsert_handoff

    try_upsert_handoff(artifact)
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
    """List visible handoff artifacts for one sender or recipient identity filter.

    Uses the SQLite sidecar index when available for O(log N) queries.
    Falls back to a full directory scan if the index is unavailable,
    adding a threshold warning when the file count is high.
    """
    enforce_rate_limit(settings, auth, "coordination_handoff_query")
    auth.require("read:files")
    if req.recipient_peer is None and req.sender_peer is None:
        raise HTTPException(status_code=400, detail="recipient_peer or sender_peer is required")
    if not query_identity_allowed(auth, req.recipient_peer) or not query_identity_allowed(auth, req.sender_peer):
        raise HTTPException(status_code=403, detail="Non-admin callers may query only their own handoff identity")

    warnings: list[str] = []
    visible: list[dict[str, Any]] = []
    total_matches = 0

    # --- Index path: O(log N) via SQLite sidecar ---
    from app.coordination.query_index import INDEX_UNAVAILABLE_WARNING, get_coordination_index

    idx = get_coordination_index()
    index_result = (
        idx.query_handoffs(
            sender_peer=req.sender_peer,
            recipient_peer=req.recipient_peer,
            status=req.status,
            offset=req.offset,
            limit=req.limit,
        )
        if idx is not None and idx.is_available
        else None
    )
    if index_result is not None:
        ids, total_matches = index_result
        for hid in ids:
            try:
                _, artifact = _load_handoff_artifact(repo_root, hid)
            except HTTPException as exc:
                if exc.status_code == 404:
                    # File removed between index and load — adjust count.
                    total_matches -= 1
                    continue
                _log.warning("Unexpected error loading indexed handoff %s: %s", hid, exc.detail)
                total_matches -= 1
                continue
            try:
                _ensure_read_visibility(auth, artifact)
            except HTTPException as exc:
                if exc.status_code == 403:
                    total_matches -= 1
                    continue
                raise
            visible.append(artifact)
    else:
        # --- Fallback: full directory scan ---
        warnings.append(INDEX_UNAVAILABLE_WARNING)
        directory = safe_path(repo_root, HANDOFFS_DIR_REL)
        if directory.exists() and directory.is_dir():
            invalid_seen = False
            file_count = 0
            for path in sorted(directory.iterdir(), key=lambda item: item.name):
                if path.is_dir() or path.suffix.lower() != ".json":
                    continue
                file_count += 1
                try:
                    payload = json.loads(read_text_file(path))
                    artifact = CoordinationHandoffArtifact.model_validate(payload).model_dump(mode="json")
                except (json.JSONDecodeError, ValidationError, OSError):
                    _log.warning("Skipping invalid handoff artifact: %s", path.name, exc_info=True)
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
            if file_count > getattr(settings, "coordination_query_scan_threshold", 5000):
                warnings.append(SCAN_THRESHOLD_WARNING)

        visible.sort(key=_query_sort_key)
        total_matches = len(visible)
        visible = visible[req.offset : req.offset + req.limit]

    handoffs = visible
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
    validate_prefixed_hex_id(handoff_id, prefix="handoff_", detail=INVALID_HANDOFF_ID_DETAIL)
    lock_dir = repo_root / ".locks"
    try:
        with artifact_lock(handoff_id, lock_dir=lock_dir):
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
            updated["consumed_at"] = utc_now()
            updated["consumed_by"] = auth.peer_id
            rel = _persist_updated_handoff(
                repo_root=repo_root,
                gm=gm,
                handoff_id=handoff_id,
                artifact=updated,
                commit_message=f"handoff: consume {handoff_id} {req.status}",
            )
            # Keep the SQLite sidecar index in sync after successful persist.
            from app.coordination.query_index import try_upsert_handoff

            try_upsert_handoff(updated)
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
    except ArtifactLockTimeout as exc:
        raise make_lock_error("coordination_handoff_consume", None, exc, is_timeout=True) from exc
    except ArtifactLockInfrastructureError as exc:
        raise make_lock_error("coordination_handoff_consume", None, exc, is_timeout=False) from exc
    return {"ok": True, "handoff": updated, "path": rel, "updated": True, "latest_commit": gm.latest_commit()}
