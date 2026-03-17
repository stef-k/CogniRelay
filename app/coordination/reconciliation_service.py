"""Reconciliation artifact service logic for bounded 5C disagreements.

Provides open, read, query, and resolve operations for coordination
reconciliation records.  Resolve uses first-write-wins version checking
with rollback-safe persistence and replay idempotency.
"""

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
    utc_now,
    validate_prefixed_hex_id,
)
from app.models import (
    CoordinationHandoffArtifact,
    CoordinationReconciliationArtifact,
    CoordinationReconciliationClaim,
    CoordinationReconciliationOpenRequest,
    CoordinationReconciliationQueryRequest,
    CoordinationReconciliationResolveRequest,
    CoordinationSharedArtifact,
)
from app.peers.service import load_peers_registry
from app.storage import read_text_file, safe_path

RECONCILIATIONS_DIR_REL = "memory/coordination/reconciliations"
RECONCILIATIONS_SAMPLE_REL = f"{RECONCILIATIONS_DIR_REL}/x.json"
RECONCILIATION_INVALID_WARNING = "coordination_reconciliation_artifact_skipped_invalid"
INVALID_RECONCILIATION_ID_DETAIL = "Invalid reconciliation artifact id"


def _reconciliation_rel_path(reconciliation_id: str) -> str:
    """Return the repository-relative storage path for one reconciliation artifact."""
    return f"{RECONCILIATIONS_DIR_REL}/{reconciliation_id}.json"


def _reconciliation_path(repo_root: Path, reconciliation_id: str) -> Path:
    """Resolve the repository path for one stored reconciliation artifact."""
    return safe_path(repo_root, _reconciliation_rel_path(reconciliation_id))


def _reconciliation_query_sort_key(artifact: dict[str, Any]) -> tuple[float, str]:
    """Return the deterministic sort key for reconciliation query results."""
    updated_at = str(artifact.get("updated_at") or "")
    try:
        dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        updated_value = dt.timestamp()
    except Exception:
        updated_value = 0.0
    return (-updated_value, str(artifact.get("reconciliation_id") or ""))


def _load_reconciliation_artifact(repo_root: Path, reconciliation_id: str) -> tuple[str, dict[str, Any]]:
    """Load and structurally validate one stored reconciliation artifact."""
    validate_prefixed_hex_id(reconciliation_id, prefix="recon_", detail=INVALID_RECONCILIATION_ID_DETAIL)
    rel = _reconciliation_rel_path(reconciliation_id)
    path = _reconciliation_path(repo_root, reconciliation_id)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Reconciliation artifact not found")
    try:
        payload = json.loads(read_text_file(path))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid reconciliation artifact JSON") from exc
    try:
        artifact = CoordinationReconciliationArtifact.model_validate(payload).model_dump(mode="json")
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail="Invalid reconciliation artifact structure") from exc
    return rel, artifact


def _ensure_reconciliation_visibility(auth: AuthContext, artifact: dict[str, Any]) -> None:
    """Require owner, participant, or admin visibility for one reconciliation artifact."""
    if is_admin(auth):
        return
    caller = getattr(auth, "peer_id", "")
    if caller == artifact.get("owner_peer"):
        return
    participants = artifact.get("participant_peers")
    if isinstance(participants, list) and caller in participants:
        return
    raise HTTPException(status_code=403, detail="Reconciliation artifact not visible to caller")


def _load_handoff_source(repo_root: Path, source_id: str) -> dict[str, Any]:
    """Load one referenced handoff source artifact for reconciliation open."""
    path = safe_path(repo_root, f"memory/coordination/handoffs/{source_id}.json")
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Referenced source artifact not found")
    try:
        payload = json.loads(read_text_file(path))
        return CoordinationHandoffArtifact.model_validate(payload).model_dump(mode="json")
    except (json.JSONDecodeError, ValidationError) as exc:
        raise HTTPException(status_code=400, detail=f"Referenced source artifact is invalid: {source_id}") from exc


def _load_shared_source(repo_root: Path, source_id: str) -> dict[str, Any]:
    """Load one referenced shared source artifact for reconciliation open."""
    path = safe_path(repo_root, f"memory/coordination/shared/{source_id}.json")
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Referenced source artifact not found")
    try:
        payload = json.loads(read_text_file(path))
        return CoordinationSharedArtifact.model_validate(payload).model_dump(mode="json")
    except (json.JSONDecodeError, ValidationError) as exc:
        raise HTTPException(status_code=400, detail=f"Referenced source artifact is invalid: {source_id}") from exc


def _load_source_artifact(repo_root: Path, claim: CoordinationReconciliationClaim) -> dict[str, Any]:
    """Load one referenced source artifact based on the claim source kind."""
    if claim.source_kind == "handoff":
        return _load_handoff_source(repo_root, claim.source_id)
    return _load_shared_source(repo_root, claim.source_id)


def _ensure_source_visibility(auth: AuthContext, claim: CoordinationReconciliationClaim, artifact: dict[str, Any]) -> None:
    """Require opener visibility to the claim's referenced source artifact."""
    if is_admin(auth):
        return
    caller = getattr(auth, "peer_id", "")
    if claim.source_kind == "handoff":
        if caller == artifact.get("sender_peer") or caller == artifact.get("recipient_peer"):
            return
    else:
        if caller == artifact.get("owner_peer"):
            return
        participants = artifact.get("participant_peers")
        if isinstance(participants, list) and caller in participants:
            return
    raise HTTPException(status_code=403, detail="Referenced source artifact not visible to caller")


def _validate_claim_membership(claim: CoordinationReconciliationClaim, artifact: dict[str, Any]) -> None:
    """Require the claimant peer to be recognized on the referenced source artifact."""
    if claim.source_kind == "handoff":
        if claim.claimant_peer == artifact.get("sender_peer") or claim.claimant_peer == artifact.get("recipient_peer"):
            return
        raise HTTPException(status_code=400, detail="Claimant peer is not recognized on referenced handoff artifact")
    if claim.claimant_peer == artifact.get("owner_peer"):
        return
    participants = artifact.get("participant_peers")
    if isinstance(participants, list) and claim.claimant_peer in participants:
        return
    raise HTTPException(status_code=400, detail="Claimant peer is not recognized on referenced shared artifact")


def _validate_open_text_bounds(req: CoordinationReconciliationOpenRequest) -> None:
    """Apply the deterministic 5C open text-bound validation rules."""
    if len(req.title) < 1:
        raise HTTPException(status_code=400, detail="Value too short in coordination_reconciliation.title")
    if len(req.title) > 120:
        raise HTTPException(status_code=400, detail="Value too long in coordination_reconciliation.title")
    if req.summary is not None:
        if len(req.summary) < 1:
            raise HTTPException(status_code=400, detail="Value too short in coordination_reconciliation.summary")
        if len(req.summary) > 240:
            raise HTTPException(status_code=400, detail="Value too long in coordination_reconciliation.summary")
    if req.commit_message is not None and len(req.commit_message) > 120:
        raise HTTPException(status_code=400, detail="Value too long in coordination_reconciliation.commit_message")
    for claim in req.claims:
        if len(claim.claim_summary) < 1:
            raise HTTPException(status_code=400, detail="Value too short in coordination_reconciliation.claim_summary")
        if len(claim.claim_summary) > 240:
            raise HTTPException(status_code=400, detail="Value too long in coordination_reconciliation.claim_summary")
        for evidence_ref in claim.evidence_refs:
            if len(evidence_ref) < 1:
                raise HTTPException(status_code=400, detail="Value too short in coordination_reconciliation.evidence_refs")
            if len(evidence_ref) > 160:
                raise HTTPException(status_code=400, detail="Value too long in coordination_reconciliation.evidence_refs")


def _validate_open_structure(req: CoordinationReconciliationOpenRequest) -> None:
    """Apply the deterministic 5C open structural and cross-field validation rules."""
    if len(req.claims) < 2:
        raise HTTPException(status_code=400, detail="Reconciliation must include at least two claims")
    if req.task_id is None and req.thread_id is None:
        raise HTTPException(status_code=400, detail="task_id or thread_id is required")
    seen: set[tuple[str, str, str]] = set()
    source_kinds: set[str] = set()
    for claim in req.claims:
        key = (claim.source_kind, claim.source_id, claim.claimant_peer)
        if key in seen:
            raise HTTPException(status_code=400, detail="Reconciliation claims must be unique")
        seen.add(key)
        source_kinds.add(claim.source_kind)
        if claim.source_kind == "shared" and claim.observed_version is None:
            raise HTTPException(status_code=400, detail="observed_version is required for shared claims")
        if claim.source_kind == "handoff" and claim.observed_version is not None:
            raise HTTPException(status_code=400, detail="observed_version is not allowed for handoff claims")
    trigger_valid = (
        (req.trigger == "handoff_vs_handoff" and source_kinds == {"handoff"})
        or (req.trigger == "shared_vs_shared" and source_kinds == {"shared"})
        or (req.trigger == "handoff_vs_shared" and source_kinds == {"handoff", "shared"})
        or (req.trigger == "concurrent_mutation_race" and source_kinds == {"shared"})
    )
    if not trigger_valid:
        raise HTTPException(status_code=400, detail="trigger does not match claim source kinds")


def _participant_peers(owner_peer: str, claims: list[CoordinationReconciliationClaim]) -> list[str]:
    """Derive participant peers in first-seen order from the bounded claim set."""
    participants: list[str] = []
    seen: set[str] = set()
    for claim in claims:
        if claim.claimant_peer == owner_peer or claim.claimant_peer in seen:
            continue
        seen.add(claim.claimant_peer)
        participants.append(claim.claimant_peer)
    return participants


def reconciliation_open_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    req: CoordinationReconciliationOpenRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Create one reconciliation artifact from a bounded, visible disagreement set."""
    enforce_rate_limit(settings, auth, "coordination_reconciliation_open")
    enforce_payload_limit(settings, req.model_dump(), "coordination_reconciliation_open")
    auth.require("write:projects")
    auth.require_write_path(RECONCILIATIONS_SAMPLE_REL)
    _validate_open_text_bounds(req)
    _validate_open_structure(req)

    # Steps 7-8: load peer registry, then validate all claimant peers in
    # caller-supplied claim order before loading any source artifacts.
    registry = load_peers_registry(repo_root)
    for claim in req.claims:
        peer = registry.get("peers", {}).get(claim.claimant_peer)
        if not isinstance(peer, dict):
            raise HTTPException(status_code=404, detail=f"Peer not found: {claim.claimant_peer}")
        trust_level = str(peer.get("trust_level") or "untrusted")
        if trust_level == "untrusted":
            raise HTTPException(status_code=409, detail=f"Peer is untrusted: {claim.claimant_peer}")

    # Step 9: load each referenced source artifact in caller-supplied claim order.
    loaded_sources: list[tuple[CoordinationReconciliationClaim, dict[str, Any]]] = []
    for claim in req.claims:
        source_artifact = _load_source_artifact(repo_root, claim)
        loaded_sources.append((claim, source_artifact))

    for claim, source_artifact in loaded_sources:
        _ensure_source_visibility(auth, claim, source_artifact)
        _validate_claim_membership(claim, source_artifact)
        if claim.source_kind == "shared":
            stored_version = int(source_artifact.get("version") or 0)
            if claim.observed_version is not None and claim.observed_version > stored_version:
                raise HTTPException(status_code=400, detail="observed_version exceeds stored shared version")

    now = utc_now()
    artifact = CoordinationReconciliationArtifact(
        reconciliation_id=f"recon_{uuid4().hex}",
        created_at=now,
        updated_at=now,
        opened_by=auth.peer_id,
        owner_peer=auth.peer_id,
        participant_peers=_participant_peers(auth.peer_id, req.claims),
        task_id=req.task_id,
        thread_id=req.thread_id,
        title=req.title,
        summary=req.summary,
        classification=req.classification,
        trigger=req.trigger,
        claims=req.claims,
        last_updated_by=auth.peer_id,
    ).model_dump(mode="json")

    commit_message = req.commit_message
    if commit_message is None or not commit_message.strip():
        commit_message = f"coordination: open {artifact['reconciliation_id']}"
    reconciliation_id = str(artifact["reconciliation_id"])
    rel = _reconciliation_rel_path(reconciliation_id)
    rel = persist_new_artifact(
        path=_reconciliation_path(repo_root, reconciliation_id),
        rel=rel,
        gm=gm,
        artifact=artifact,
        commit_message=commit_message,
        error_detail="Failed to commit reconciliation artifact",
    )
    audit(
        auth,
        "coordination_reconciliation_open",
        {
            "reconciliation_id": artifact["reconciliation_id"],
            "owner_peer": artifact["owner_peer"],
            "participant_peers": artifact["participant_peers"],
            "classification": artifact["classification"],
            "trigger": artifact["trigger"],
            "task_id": artifact["task_id"],
            "thread_id": artifact["thread_id"],
            "path": rel,
        },
    )
    return {"ok": True, "reconciliation": artifact, "path": rel, "created": True, "latest_commit": gm.latest_commit()}


def reconciliation_read_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    reconciliation_id: str,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Read one stored reconciliation artifact using owner/participant/admin visibility."""
    enforce_rate_limit(settings, auth, "coordination_reconciliation_read")
    rel, artifact = _load_reconciliation_artifact(repo_root, reconciliation_id)
    _ensure_reconciliation_visibility(auth, artifact)
    audit(
        auth,
        "coordination_reconciliation_read",
        {
            "reconciliation_id": reconciliation_id,
            "owner_peer": artifact["owner_peer"],
            "viewer_peer": auth.peer_id,
            "path": rel,
        },
    )
    return {"ok": True, "reconciliation": artifact, "path": rel}


def reconciliation_query_service(
    *,
    repo_root: Path,
    auth: AuthContext,
    req: CoordinationReconciliationQueryRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """List visible reconciliation artifacts for bounded owner/claimant queries."""
    enforce_rate_limit(settings, auth, "coordination_reconciliation_query")
    auth.require("read:files")
    if not is_admin(auth):
        has_self_owner = req.owner_peer == auth.peer_id
        has_self_claimant = req.claimant_peer == auth.peer_id
        if not has_self_owner and not has_self_claimant:
            raise HTTPException(status_code=403, detail="Non-admin callers may query only their own reconciliation identity")
        if (
            (req.owner_peer is not None and req.owner_peer != auth.peer_id)
            or (req.claimant_peer is not None and req.claimant_peer != auth.peer_id)
        ):
            raise HTTPException(status_code=403, detail="Non-admin callers may query only their own reconciliation identity")

    directory = safe_path(repo_root, RECONCILIATIONS_DIR_REL)
    warnings: list[str] = []
    visible: list[dict[str, Any]] = []
    if directory.exists() and directory.is_dir():
        invalid_seen = False
        for path in sorted(directory.iterdir(), key=lambda item: item.name):
            if path.is_dir() or path.suffix.lower() != ".json":
                continue
            try:
                payload = json.loads(read_text_file(path))
                artifact = CoordinationReconciliationArtifact.model_validate(payload).model_dump(mode="json")
            except (json.JSONDecodeError, ValidationError, OSError):
                invalid_seen = True
                continue
            if req.owner_peer is not None and artifact.get("owner_peer") != req.owner_peer:
                continue
            if req.claimant_peer is not None:
                claims = artifact.get("claims")
                if not isinstance(claims, list) or not any(
                    isinstance(item, dict) and item.get("claimant_peer") == req.claimant_peer for item in claims
                ):
                    continue
            if req.status is not None and artifact.get("status") != req.status:
                continue
            if req.classification is not None and artifact.get("classification") != req.classification:
                continue
            if req.task_id is not None and artifact.get("task_id") != req.task_id:
                continue
            if req.thread_id is not None and artifact.get("thread_id") != req.thread_id:
                continue
            try:
                _ensure_reconciliation_visibility(auth, artifact)
            except HTTPException as exc:
                if exc.status_code == 403:
                    continue
                raise
            visible.append(artifact)
        if invalid_seen:
            warnings.append(RECONCILIATION_INVALID_WARNING)

    visible.sort(key=_reconciliation_query_sort_key)
    total_matches = len(visible)
    reconciliations = visible[req.offset : req.offset + req.limit]
    audit(
        auth,
        "coordination_reconciliation_query",
        {
            "owner_peer": req.owner_peer,
            "claimant_peer": req.claimant_peer,
            "status": req.status,
            "classification": req.classification,
            "task_id": req.task_id,
            "thread_id": req.thread_id,
            "count": len(reconciliations),
            "total_matches": total_matches,
            "warnings": warnings,
        },
    )
    return {
        "ok": True,
        "count": len(reconciliations),
        "total_matches": total_matches,
        "warnings": warnings,
        "reconciliations": reconciliations,
    }


def reconciliation_resolve_service(
    *,
    repo_root: Path,
    gm: Any,
    auth: AuthContext,
    reconciliation_id: str,
    req: CoordinationReconciliationResolveRequest,
    enforce_rate_limit: Callable[[Any, AuthContext, str], None],
    enforce_payload_limit: Callable[[Any, Any, str], None],
    settings: Any,
    audit: Callable[[AuthContext, str, dict[str, Any]], None],
) -> dict[str, Any]:
    """Resolve one open reconciliation record under first-write-wins version checking.

    The resolve algorithm enforces owner-only (or admin) auth, requires a
    resolution_summary, checks the expected_version against the stored version,
    and writes bounded resolve fields (status, outcome, summary, resolved_at,
    resolved_by, version, last_updated_by).  If the artifact is already resolved
    with the same outcome and summary, the call returns updated=false without a
    new commit (replay idempotency).  A different outcome or summary after
    resolution returns HTTP 409.  On commit failure the prior artifact bytes are
    restored.
    """
    enforce_rate_limit(settings, auth, "coordination_reconciliation_resolve")
    enforce_payload_limit(settings, req.model_dump(), "coordination_reconciliation_resolve")
    auth.require("write:projects")
    auth.require_write_path(RECONCILIATIONS_SAMPLE_REL)
    validate_prefixed_hex_id(reconciliation_id, prefix="recon_", detail=INVALID_RECONCILIATION_ID_DETAIL)

    rel, artifact = _load_reconciliation_artifact(repo_root, reconciliation_id)

    # Owner-only resolve unless caller has admin:peers.
    caller = getattr(auth, "peer_id", "")
    if caller != artifact.get("owner_peer") and not is_admin(auth):
        raise HTTPException(status_code=403, detail="Only the owner may resolve this reconciliation artifact")

    # Require resolution_summary.
    if req.resolution_summary is None:
        raise HTTPException(status_code=400, detail="resolution_summary is required for reconciliation resolve")

    # Validate text bounds for resolution_summary and commit_message.
    if len(req.resolution_summary) < 1:
        raise HTTPException(status_code=400, detail="Value too short in coordination_reconciliation.resolution_summary")
    if len(req.resolution_summary) > 240:
        raise HTTPException(status_code=400, detail="Value too long in coordination_reconciliation.resolution_summary")
    if req.commit_message is not None and len(req.commit_message) > 120:
        raise HTTPException(status_code=400, detail="Value too long in coordination_reconciliation.commit_message")

    # Handle already-resolved artifacts: replay or conflict.
    if artifact.get("status") == "resolved":
        if artifact.get("resolution_outcome") == req.outcome and artifact.get("resolution_summary") == req.resolution_summary:
            return {"ok": True, "reconciliation": artifact, "path": rel, "updated": False, "latest_commit": gm.latest_commit()}
        raise HTTPException(status_code=409, detail="Reconciliation has already been resolved")

    # Version check: first-write-wins.
    stored_version = int(artifact.get("version") or 0)
    if req.expected_version != stored_version:
        raise HTTPException(status_code=409, detail="Reconciliation version conflict")

    # Build updated artifact.
    now = utc_now()
    updated = dict(artifact)
    updated["status"] = "resolved"
    updated["resolution_outcome"] = req.outcome
    updated["resolution_summary"] = req.resolution_summary
    updated["resolved_at"] = now
    updated["resolved_by"] = caller
    updated["updated_at"] = now
    updated["last_updated_by"] = caller
    updated["version"] = stored_version + 1

    commit_message = req.commit_message
    if commit_message is None or not commit_message.strip():
        commit_message = f"coordination: resolve {reconciliation_id} {req.outcome}"

    persist_updated_artifact(
        path=_reconciliation_path(repo_root, reconciliation_id),
        rel=rel,
        gm=gm,
        artifact=updated,
        commit_message=commit_message,
        error_detail="Failed to commit reconciliation resolve",
    )
    audit(
        auth,
        "coordination_reconciliation_resolve",
        {
            "reconciliation_id": reconciliation_id,
            "owner_peer": updated["owner_peer"],
            "outcome": updated["resolution_outcome"],
            "resolved_by": caller,
            "version": updated["version"],
            "path": rel,
        },
    )
    return {"ok": True, "reconciliation": updated, "path": rel, "updated": True, "latest_commit": gm.latest_commit()}
