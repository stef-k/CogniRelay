from __future__ import annotations

import hashlib
import hmac
import json
import os
import subprocess
import tarfile
from datetime import datetime, timedelta, timezone
import math
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from urllib.request import Request as UrlRequest, urlopen
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request as FastAPIRequest, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from .audit import append_audit
from .auth import AuthContext, require_auth
from .context import (
    context_retrieve_service,
    context_snapshot_create_service,
    context_snapshot_get_service,
    index_rebuild_incremental_service,
    index_rebuild_service,
    index_status_service,
    recent_list_service,
    search_service,
)
from .continuity import continuity_upsert_service
from .config import ALL_SCOPES, get_settings, sha256_token
from .discovery import (
    capabilities_payload,
    contracts_payload,
    discovery_payload,
    discovery_tools_payload,
    discovery_workflows_payload,
    handle_mcp_rpc_request,
    health_payload,
    manifest_payload,
    tool_catalog,
    well_known_cognirelay_payload,
    well_known_mcp_payload,
    workflow_catalog,
)
from .git_manager import GitManager
from .indexer import incremental_rebuild_index, list_recent_files, load_files_index, rebuild_index, search_index
from .models import (
    AppendRequest,
    CodeCheckRunRequest,
    CodeMergeRequest,
    CompactRequest,
    ContinuityUpsertRequest,
    ContextSnapshotRequest,
    ContextRetrieveRequest,
    MessageReplayRequest,
    MessageVerifyRequest,
    SecurityTokenIssueRequest,
    SecurityTokenRevokeRequest,
    SecurityTokenRotateRequest,
    MessageAckRequest,
    MessageSendRequest,
    PatchApplyRequest,
    PatchProposeRequest,
    PeerRegisterRequest,
    PeerTrustTransitionRequest,
    ReplicationPullRequest,
    ReplicationPushRequest,
    BackupCreateRequest,
    BackupRestoreTestRequest,
    OpsRunRequest,
    RecentRequest,
    RelayForwardRequest,
    SecurityKeysRotateRequest,
    SearchRequest,
    TaskCreateRequest,
    TaskUpdateRequest,
    WriteRequest,
)
from .ops import ops_catalog_service, ops_run_service, ops_schedule_export_service, ops_status_service
from .peers import (
    PEERS_REGISTRY_REL,
    load_peers_registry,
    peer_manifest_service,
    peers_list_service,
    peers_register_service,
    peers_trust_transition_service,
)
from .storage import StorageError, append_jsonl, read_text_file, safe_path, write_text_file


app = FastAPI(title="CogniRelay", version="0.3.0")


def _services() -> tuple:
    settings = get_settings()
    gm = GitManager(
        repo_root=settings.repo_root,
        author_name=settings.git_author_name,
        author_email=settings.git_author_email,
    )
    gm.ensure_repo(settings.auto_init_git)
    return settings, gm


def _audit(settings, auth: AuthContext | None, event: str, detail: dict) -> None:
    if not settings.audit_log_enabled:
        return
    append_audit(settings.repo_root, event, auth.peer_id if auth else "anonymous", detail)
def _scope_for_path(path: str) -> str:
    top = Path(path).parts[0] if Path(path).parts else ""
    if top == "journal":
        return "write:journal"
    if top == "messages":
        return "write:messages"
    if top in {"projects", "memory", "essays", "archive", "config", "logs"}:
        return "write:projects"
    return "write:projects"


def _schema_for_model(model_cls: Any) -> dict[str, Any]:
    return model_cls.model_json_schema()


def _tool_catalog() -> list[dict[str, Any]]:
    return tool_catalog(_schema_for_model)


def _workflow_catalog() -> list[dict[str, Any]]:
    return workflow_catalog()


@app.get("/v1/discovery")
def discovery() -> dict:
    settings = get_settings()
    tools = _tool_catalog()
    workflows = _workflow_catalog()
    return discovery_payload(settings.contract_version, tools=tools, workflows=workflows)


@app.get("/v1/discovery/tools")
def discovery_tools() -> dict:
    settings = get_settings()
    tools = _tool_catalog()
    return discovery_tools_payload(settings.contract_version, tools=tools)


@app.get("/v1/discovery/workflows")
def discovery_workflows() -> dict:
    workflows = _workflow_catalog()
    return discovery_workflows_payload(workflows=workflows)


@app.get("/.well-known/cognirelay.json")
def well_known_cognirelay() -> dict:
    return well_known_cognirelay_payload(discovery())


@app.get("/.well-known/mcp.json")
def well_known_mcp() -> dict:
    settings = get_settings()
    return well_known_mcp_payload(settings.contract_version)


def _resolve_auth_context(
    authorization: str | None,
    required: bool,
    x_forwarded_for: str | None = None,
    x_real_ip: str | None = None,
    request: FastAPIRequest | None = None,
) -> AuthContext | None:
    if not authorization:
        if required:
            raise HTTPException(status_code=401, detail="Missing Authorization header")
        return None
    return require_auth(
        authorization=authorization,
        x_forwarded_for=x_forwarded_for,
        x_real_ip=x_real_ip,
        request=request,
    )


def _invoke_tool_by_name(name: str, arguments: dict[str, Any], auth: AuthContext | None) -> dict[str, Any]:
    args = arguments or {}
    if not isinstance(args, dict):
        raise ValueError("arguments must be an object")

    if name == "system.health":
        return health()
    if name == "system.capabilities":
        return capabilities()
    if name == "system.manifest":
        return manifest()
    if name == "system.contracts":
        return contracts()
    if name == "system.governance_policy":
        return governance_policy()
    if name == "system.discovery":
        return discovery()
    if name == "system.discovery_tools":
        return discovery_tools()
    if name == "system.discovery_workflows":
        return discovery_workflows()
    if name == "memory.write":
        return write_file(WriteRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "memory.append_jsonl":
        return append_record(AppendRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "memory.read":
        return read_file(path=str(args["path"]), auth=auth)  # type: ignore[arg-type]
    if name == "index.rebuild_full":
        return index_rebuild(auth=auth)  # type: ignore[arg-type]
    if name == "index.rebuild_incremental":
        return index_rebuild_incremental(auth=auth)  # type: ignore[arg-type]
    if name == "index.status":
        return index_status(auth=auth)  # type: ignore[arg-type]
    if name == "peers.list":
        return peers_list(auth=auth)  # type: ignore[arg-type]
    if name == "peers.register":
        return peers_register(PeerRegisterRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "peers.trust_transition":
        req_args = dict(args)
        peer_id = str(req_args.pop("peer_id"))
        return peers_trust_transition(peer_id=peer_id, req=PeerTrustTransitionRequest(**req_args), auth=auth)  # type: ignore[arg-type]
    if name == "peers.fetch_manifest":
        return peer_manifest(peer_id=str(args["peer_id"]), auth=auth)  # type: ignore[arg-type]
    if name == "search.query":
        return search(SearchRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "recent.list":
        return recent_list(RecentRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "context.retrieve":
        return context_retrieve(ContextRetrieveRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "continuity.upsert":
        return continuity_upsert(ContinuityUpsertRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "context.snapshot_create":
        return context_snapshot_create(ContextSnapshotRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "context.snapshot_get":
        return context_snapshot_get(snapshot_id=str(args["snapshot_id"]), auth=auth)  # type: ignore[arg-type]
    if name == "tasks.create":
        return tasks_create(TaskCreateRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "tasks.update":
        req_args = dict(args)
        task_id = str(req_args.pop("task_id"))
        return tasks_update(task_id=task_id, req=TaskUpdateRequest(**req_args), auth=auth)  # type: ignore[arg-type]
    if name == "tasks.query":
        return tasks_query(
            status=args.get("status"),
            owner_peer=args.get("owner_peer"),
            collaborator=args.get("collaborator"),
            thread_id=args.get("thread_id"),
            limit=int(args.get("limit", 100)),
            auth=auth,  # type: ignore[arg-type]
        )
    if name == "docs.patch_propose":
        return docs_patch_propose(PatchProposeRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "docs.patch_apply":
        return docs_patch_apply(PatchApplyRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "code.patch_propose":
        return code_patch_propose(PatchProposeRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "code.checks_run":
        return code_checks_run(CodeCheckRunRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "code.merge":
        return code_merge(CodeMergeRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "security.tokens_list":
        return security_tokens_list(
            peer_id=args.get("peer_id"),
            status=args.get("status"),
            include_inactive=bool(args.get("include_inactive", False)),
            auth=auth,  # type: ignore[arg-type]
        )
    if name == "security.tokens_issue":
        return security_tokens_issue(SecurityTokenIssueRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "security.tokens_revoke":
        return security_tokens_revoke(SecurityTokenRevokeRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "security.tokens_rotate":
        return security_tokens_rotate(SecurityTokenRotateRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "security.keys_rotate":
        return security_keys_rotate(SecurityKeysRotateRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "messages.verify":
        return messages_verify(MessageVerifyRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "metrics.get":
        return metrics(auth=auth)  # type: ignore[arg-type]
    if name == "messages.replay":
        return replay_messages(MessageReplayRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "replication.pull":
        return replication_pull(ReplicationPullRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "replication.push":
        return replication_push(ReplicationPushRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "messages.send":
        return messages_send(MessageSendRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "messages.ack":
        return messages_ack(MessageAckRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "messages.pending":
        return messages_pending(
            recipient=args.get("recipient"),
            status=args.get("status"),
            include_terminal=bool(args.get("include_terminal", False)),
            limit=int(args.get("limit", 50)),
            auth=auth,  # type: ignore[arg-type]
        )
    if name == "messages.inbox":
        return messages_inbox(
            recipient=str(args["recipient"]),
            limit=int(args.get("limit", 20)),
            auth=auth,  # type: ignore[arg-type]
        )
    if name == "messages.thread":
        return messages_thread(
            thread_id=str(args["thread_id"]),
            limit=int(args.get("limit", 100)),
            auth=auth,  # type: ignore[arg-type]
        )
    if name == "messages.relay_forward":
        return relay_forward(RelayForwardRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "memory.compaction_plan":
        return compact_run(CompactRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "backup.create":
        return backup_create(BackupCreateRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "backup.restore_test":
        return backup_restore_test(BackupRestoreTestRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "ops.catalog":
        return ops_catalog(auth=auth)  # type: ignore[arg-type]
    if name == "ops.status":
        return ops_status(limit=int(args.get("limit", 50)), auth=auth)  # type: ignore[arg-type]
    if name == "ops.run":
        return ops_run(OpsRunRequest(**args), auth=auth)  # type: ignore[arg-type]
    if name == "ops.schedule_export":
        return ops_schedule_export(format=str(args.get("format", "systemd")), auth=auth)  # type: ignore[arg-type]
    raise ValueError(f"Unknown tool: {name}")


def _handle_mcp_rpc_request(
    request_payload: Any,
    authorization: str | None,
    x_forwarded_for: str | None = None,
    x_real_ip: str | None = None,
    request: FastAPIRequest | None = None,
) -> dict[str, Any] | None:
    return handle_mcp_rpc_request(
        request_payload,
        authorization=authorization,
        x_forwarded_for=x_forwarded_for,
        x_real_ip=x_real_ip,
        request=request,
        contract_version=get_settings().contract_version,
        tools=_tool_catalog(),
        resolve_auth_context=_resolve_auth_context,
        invoke_tool_by_name=_invoke_tool_by_name,
    )


@app.post("/v1/mcp")
def mcp_rpc(
    payload: Any,
    authorization: str | None = Header(default=None),
    x_forwarded_for: str | None = Header(default=None, alias="X-Forwarded-For"),
    x_real_ip: str | None = Header(default=None, alias="X-Real-IP"),
    http_request: FastAPIRequest = None,  # type: ignore[assignment]
) -> Any:
    # When called directly in unit tests, FastAPI's Header sentinel can appear here.
    if authorization is not None and not isinstance(authorization, str):
        authorization = None
    if isinstance(payload, list):
        if not payload:
            return _rpc_error(None, -32600, "Invalid Request: empty batch")
        out = []
        for item in payload:
            result = _handle_mcp_rpc_request(
                item,
                authorization,
                x_forwarded_for=x_forwarded_for,
                x_real_ip=x_real_ip,
                request=http_request,
            )
            if result is not None:
                out.append(result)
        if not out:
            return Response(status_code=204)
        return out
    result = _handle_mcp_rpc_request(
        payload,
        authorization,
        x_forwarded_for=x_forwarded_for,
        x_real_ip=x_real_ip,
        request=http_request,
    )
    if result is None:
        return Response(status_code=204)
    return result


@app.get("/health")
def health() -> dict:
    settings, gm = _services()
    return health_payload(
        app_version=app.version,
        contract_version=settings.contract_version,
        repo_root=str(settings.repo_root),
        git_initialized=gm.is_repo(),
        latest_commit=gm.latest_commit(),
        signed_ingress_required=bool(settings.require_signed_ingress),
    )


@app.get("/capabilities")
def capabilities() -> dict:
    return capabilities_payload()


@app.get("/v1/manifest")
def manifest() -> dict:
    """Machine-first endpoint map for autonomous clients."""
    return manifest_payload(app_version=app.version)



@app.get("/v1/contracts")
def contracts() -> dict:
    settings = get_settings()
    return contracts_payload(contract_version=settings.contract_version, tools=_tool_catalog())


@app.get("/v1/governance/policy")
def governance_policy() -> dict:
    settings, _ = _services()
    policy = _load_governance_policy(settings.repo_root)
    return {"ok": True, "policy": policy}
def _latest_backup_archive_rel(repo_root: Path) -> str | None:
    d = safe_path(repo_root, BACKUPS_DIR_REL)
    if not d.exists() or not d.is_dir():
        return None
    candidates = sorted(d.glob("backup_*.tar.gz"), key=lambda x: x.stat().st_mtime, reverse=True)
    if not candidates:
        return None
    return f"{BACKUPS_DIR_REL}/{candidates[0].name}"


@app.get("/v1/ops/catalog")
def ops_catalog(auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return ops_catalog_service(settings=settings, auth=auth, audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail))


@app.get("/v1/ops/status")
def ops_status(limit: int = Query(default=50, ge=1, le=500), auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return ops_status_service(
        repo_root=settings.repo_root,
        auth=auth,
        limit=limit,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.get("/v1/ops/schedule/export")
def ops_schedule_export(format: str = Query(default="systemd"), auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return ops_schedule_export_service(
        settings=settings,
        auth=auth,
        format=format,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/ops/run")
def ops_run(req: OpsRunRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return ops_run_service(
        settings=settings,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
        index_rebuild_incremental=index_rebuild_incremental,
        metrics=metrics,
        backup_create=backup_create,
        backup_create_request_factory=BackupCreateRequest,
        backup_restore_test=backup_restore_test,
        backup_restore_test_request_factory=BackupRestoreTestRequest,
        replication_pull=replication_pull,
        replication_pull_request_factory=ReplicationPullRequest,
        replication_push=replication_push,
        replication_push_request_factory=ReplicationPushRequest,
        compact_run=compact_run,
        compact_request_factory=CompactRequest,
        load_token_config=_load_token_config,
        parse_iso=_parse_iso,
        load_security_keys=_load_security_keys,
        load_delivery_state=_load_delivery_state,
        effective_delivery_status=_effective_delivery_status,
        replay_messages=replay_messages,
        replay_request_factory=MessageReplayRequest,
        backups_dir_rel=BACKUPS_DIR_REL,
    )


@app.post("/v1/write")
def write_file(req: WriteRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    _enforce_rate_limit(settings, auth, "write")
    _enforce_payload_limit(settings, {"path": req.path, "content": req.content}, "write")
    auth.require(_scope_for_path(req.path))
    auth.require_write_path(req.path)
    try:
        path = safe_path(settings.repo_root, req.path)
    except StorageError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    write_text_file(path, req.content)
    committed = gm.commit_file(path, req.commit_message or f"write: {req.path}")
    _audit(settings, auth, "write", {"path": req.path, "committed": committed})
    return {
        "ok": True,
        "path": req.path,
        "committed": committed,
        "latest_commit": gm.latest_commit(),
    }


@app.get("/v1/read")
def read_file(path: str = Query(...), auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    auth.require("read:files")
    auth.require_read_path(path)
    try:
        p = safe_path(settings.repo_root, path)
    except StorageError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not p.exists() or not p.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    _audit(settings, auth, "read", {"path": path})
    return {"ok": True, "path": path, "content": read_text_file(p)}


@app.post("/v1/append")
def append_record(req: AppendRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    _enforce_rate_limit(settings, auth, "append")
    _enforce_payload_limit(settings, {"path": req.path, "record": req.record}, "append")
    auth.require(_scope_for_path(req.path))
    auth.require_write_path(req.path)
    try:
        path = safe_path(settings.repo_root, req.path)
    except StorageError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    append_jsonl(path, req.record)
    committed = gm.commit_file(path, req.commit_message or f"append: {req.path}")
    _audit(settings, auth, "append", {"path": req.path, "committed": committed})
    return {"ok": True, "path": req.path, "committed": committed, "latest_commit": gm.latest_commit()}


@app.post("/v1/index/rebuild")
def index_rebuild(auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return index_rebuild_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/index/rebuild-incremental")
def index_rebuild_incremental(auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return index_rebuild_incremental_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.get("/v1/index/status")
def index_status(auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return index_status_service(repo_root=settings.repo_root, auth=auth)


@app.post("/v1/search")
def search(req: SearchRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return search_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/recent")
def recent_list(req: RecentRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return recent_list_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/continuity/upsert")
def continuity_upsert(req: ContinuityUpsertRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return continuity_upsert_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/context/retrieve")
def context_retrieve(req: ContextRetrieveRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return context_retrieve_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        now=datetime.now(timezone.utc),
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )
def _run_git(repo_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=repo_root, text=True, capture_output=True, check=False)


def _read_commit_file(repo_root: Path, commit_ref: str, rel_path: str) -> str | None:
    cp = _run_git(repo_root, "show", f"{commit_ref}:{rel_path}")
    if cp.returncode != 0:
        return None
    return cp.stdout
@app.get("/v1/peers")
def peers_list(auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return peers_list_service(
        repo_root=settings.repo_root,
        auth=auth,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/peers/register")
def peers_register(req: PeerRegisterRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return peers_register_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        trust_policies_rel=TRUST_POLICIES_REL,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/peers/{peer_id}/trust")
def peers_trust_transition(peer_id: str, req: PeerTrustTransitionRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return peers_trust_transition_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        peer_id=peer_id,
        req=req,
        trust_policies_rel=TRUST_POLICIES_REL,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.get("/v1/peers/{peer_id}/manifest")
def peer_manifest(peer_id: str, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return peer_manifest_service(
        repo_root=settings.repo_root,
        auth=auth,
        peer_id=peer_id,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/context/snapshot")
def context_snapshot_create(req: ContextSnapshotRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return context_snapshot_create_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        now=datetime.now(timezone.utc),
        service_version=app.version,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.get("/v1/context/snapshot/{snapshot_id}")
def context_snapshot_get(snapshot_id: str, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return context_snapshot_get_service(
        repo_root=settings.repo_root,
        auth=auth,
        snapshot_id=snapshot_id,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


TASKS_OPEN_DIR_REL = "tasks/open"
TASKS_DONE_DIR_REL = "tasks/done"
PATCH_PROPOSALS_DIR_REL = "patches/proposals"
PATCH_APPLIED_DIR_REL = "patches/applied"
RUN_CHECKS_DIR_REL = "runs/checks"
TASK_STATUS_TRANSITIONS = {
    "open": {"open", "in_progress", "blocked", "done"},
    "in_progress": {"in_progress", "open", "blocked", "done"},
    "blocked": {"blocked", "open", "in_progress", "done"},
    "done": {"done"},
}
CHECK_PROFILE_COMMANDS = {
    "lint": ["python3", "-m", "compileall", "-q", "."],
    "test": ["python3", "-m", "unittest", "discover", "-s", "tests", "-v"],
    "build": ["python3", "-m", "compileall", "."],
}


def _resolve_commit_ref(repo_root: Path, ref: str) -> str:
    cp = _run_git(repo_root, "rev-parse", "--verify", f"{ref}^{{commit}}")
    if cp.returncode != 0:
        raise HTTPException(status_code=400, detail=f"Invalid git ref: {ref}")
    return cp.stdout.strip()


def _task_rel(task_id: str, status: str) -> str:
    base = TASKS_DONE_DIR_REL if status == "done" else TASKS_OPEN_DIR_REL
    return f"{base}/{task_id}.json"


def _find_task(repo_root: Path, task_id: str) -> tuple[str, Path, dict[str, Any]] | tuple[None, None, None]:
    for rel in (f"{TASKS_OPEN_DIR_REL}/{task_id}.json", f"{TASKS_DONE_DIR_REL}/{task_id}.json"):
        p = safe_path(repo_root, rel)
        if not p.exists():
            continue
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        return rel, p, payload
    return None, None, None


def _iter_task_files(repo_root: Path) -> list[tuple[str, Path]]:
    out = []
    for base in (TASKS_OPEN_DIR_REL, TASKS_DONE_DIR_REL):
        d = safe_path(repo_root, base)
        if not d.exists() or not d.is_dir():
            continue
        for p in sorted(d.glob("*.json")):
            out.append((f"{base}/{p.name}", p))
    return out


def _extract_patch_paths(diff: str) -> set[str]:
    paths: set[str] = set()
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            parts = line.split()
            if len(parts) >= 4:
                for raw in (parts[2], parts[3]):
                    if raw == "/dev/null":
                        continue
                    norm = raw[2:] if raw.startswith("a/") or raw.startswith("b/") else raw
                    if norm:
                        paths.add(norm)
        elif line.startswith("--- ") or line.startswith("+++ "):
            raw = line.split(" ", 1)[1].strip()
            if raw == "/dev/null":
                continue
            norm = raw[2:] if raw.startswith("a/") or raw.startswith("b/") else raw
            if norm:
                paths.add(norm)
    return paths


def _run_check_command(repo_root: Path, ref_resolved: str, profile: str) -> tuple[int, str, str]:
    cmd = CHECK_PROFILE_COMMANDS[profile]
    tmp_dir = tempfile.mkdtemp(prefix="amr-check-")
    try:
        add_cp = _run_git(repo_root, "worktree", "add", "--detach", tmp_dir, ref_resolved)
        if add_cp.returncode != 0:
            return (1, "", f"failed to create worktree: {add_cp.stderr.strip()}")
        cp = subprocess.run(cmd, cwd=tmp_dir, text=True, capture_output=True, check=False)
        return (cp.returncode, cp.stdout, cp.stderr)
    finally:
        _run_git(repo_root, "worktree", "remove", "--force", tmp_dir)
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _load_check_artifacts(repo_root: Path) -> list[dict[str, Any]]:
    d = safe_path(repo_root, RUN_CHECKS_DIR_REL)
    if not d.exists() or not d.is_dir():
        return []
    out = []
    for p in sorted(d.glob("*.json")):
        try:
            row = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


@app.post("/v1/tasks")
def tasks_create(req: TaskCreateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    auth.require("write:projects")
    existing_rel, _, _ = _find_task(settings.repo_root, req.task_id)
    if existing_rel:
        raise HTTPException(status_code=409, detail=f"Task already exists: {req.task_id}")

    now = datetime.now(timezone.utc).isoformat()
    payload = req.model_dump()
    payload["created_at"] = now
    payload["updated_at"] = now
    payload["task_id"] = req.task_id
    rel = _task_rel(req.task_id, req.status)
    auth.require_write_path(rel)
    p = safe_path(settings.repo_root, rel)
    write_text_file(p, json.dumps(payload, ensure_ascii=False, indent=2))
    committed = gm.commit_file(p, f"tasks: create {req.task_id}")
    _audit(settings, auth, "tasks_create", {"task_id": req.task_id, "status": req.status})
    return {"ok": True, "task": payload, "path": rel, "committed": committed, "latest_commit": gm.latest_commit()}


@app.patch("/v1/tasks/{task_id}")
def tasks_update(task_id: str, req: TaskUpdateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    auth.require("write:projects")
    rel, path, task = _find_task(settings.repo_root, task_id)
    if not rel or not path or not isinstance(task, dict):
        raise HTTPException(status_code=404, detail=f"Task not found: {task_id}")

    current_status = str(task.get("status") or "open")
    new_status = req.status or current_status
    allowed = TASK_STATUS_TRANSITIONS.get(current_status, {current_status})
    if new_status not in allowed:
        raise HTTPException(status_code=409, detail=f"Invalid task status transition: {current_status} -> {new_status}")

    updates = req.model_dump(exclude_unset=True)
    task.update({k: v for k, v in updates.items() if v is not None})
    task["status"] = new_status
    task["updated_at"] = datetime.now(timezone.utc).isoformat()
    task["task_id"] = task_id

    next_rel = _task_rel(task_id, new_status)
    auth.require_write_path(next_rel)
    next_path = safe_path(settings.repo_root, next_rel)
    write_text_file(next_path, json.dumps(task, ensure_ascii=False, indent=2))
    committed_files = []
    if gm.commit_file(next_path, f"tasks: update {task_id}"):
        committed_files.append(next_rel)

    if path != next_path and path.exists():
        path.unlink()
        if gm.commit_file(path, f"tasks: move {task_id}"):
            committed_files.append(rel)

    _audit(settings, auth, "tasks_update", {"task_id": task_id, "status": new_status})
    return {"ok": True, "task": task, "path": next_rel, "committed_files": committed_files, "latest_commit": gm.latest_commit()}


@app.get("/v1/tasks/query")
def tasks_query(
    status: str | None = Query(default=None),
    owner_peer: str | None = Query(default=None),
    collaborator: str | None = Query(default=None),
    thread_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    auth: AuthContext = Depends(require_auth),
) -> dict:
    if status is not None and not isinstance(status, str):
        status = None
    if owner_peer is not None and not isinstance(owner_peer, str):
        owner_peer = None
    if collaborator is not None and not isinstance(collaborator, str):
        collaborator = None
    if thread_id is not None and not isinstance(thread_id, str):
        thread_id = None
    if not isinstance(limit, int):
        limit = 100

    settings, _ = _services()
    auth.require("read:files")
    auth.require_read_path(f"{TASKS_OPEN_DIR_REL}/x.json")
    auth.require_read_path(f"{TASKS_DONE_DIR_REL}/x.json")

    tasks = []
    for rel, p in _iter_task_files(settings.repo_root):
        try:
            row = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        row.setdefault("task_id", p.stem)
        row.setdefault("status", "done" if rel.startswith(TASKS_DONE_DIR_REL + "/") else "open")
        if status and str(row.get("status")) != status:
            continue
        if owner_peer and str(row.get("owner_peer")) != owner_peer:
            continue
        if collaborator and collaborator not in set(str(x) for x in row.get("collaborators", []) if x):
            continue
        if thread_id and str(row.get("thread_id") or "") != thread_id:
            continue
        tasks.append(row)

    tasks.sort(key=lambda x: (str(x.get("updated_at", "")), str(x.get("task_id", ""))), reverse=True)
    out = tasks[:limit]
    _audit(settings, auth, "tasks_query", {"count": len(out)})
    return {"ok": True, "count": len(out), "tasks": out}


def _patch_propose(kind: str, req: PatchProposeRequest, auth: AuthContext) -> dict:
    settings, gm = _services()
    auth.require("write:projects")
    auth.require_write_path(req.target_path)
    safe_path(settings.repo_root, req.target_path)
    if req.format != "unified_diff":
        raise HTTPException(status_code=400, detail=f"Unsupported patch format: {req.format}")

    if not req.diff.strip():
        raise HTTPException(status_code=400, detail="Patch diff must not be empty")
    diff_paths = _extract_patch_paths(req.diff)
    if diff_paths and diff_paths != {req.target_path}:
        raise HTTPException(status_code=400, detail=f"Patch must only target {req.target_path}; got {sorted(diff_paths)}")

    base_ref_resolved = _resolve_commit_ref(settings.repo_root, req.base_ref)
    patch_id = req.patch_id or f"patch_{uuid4().hex[:12]}"
    rel = f"{PATCH_PROPOSALS_DIR_REL}/{patch_id}.json"
    auth.require_write_path(rel)
    p = safe_path(settings.repo_root, rel)
    if p.exists():
        raise HTTPException(status_code=409, detail=f"Patch already exists: {patch_id}")

    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "schema_version": "1.0",
        "patch_id": patch_id,
        "patch_type": kind,
        "status": "proposed",
        "target_path": req.target_path,
        "base_ref": req.base_ref,
        "base_ref_resolved": base_ref_resolved,
        "format": req.format,
        "diff": req.diff,
        "reason": req.reason,
        "thread_id": req.thread_id,
        "created_at": now,
        "created_by": auth.peer_id,
        "updated_at": now,
    }
    write_text_file(p, json.dumps(payload, ensure_ascii=False, indent=2))
    committed = gm.commit_file(p, f"patches: propose {patch_id}")
    _audit(settings, auth, "patch_propose", {"patch_id": patch_id, "patch_type": kind, "target_path": req.target_path})
    return {"ok": True, "patch": payload, "path": rel, "committed": committed, "latest_commit": gm.latest_commit()}


@app.post("/v1/docs/patch/propose")
def docs_patch_propose(req: PatchProposeRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    return _patch_propose("doc_patch", req, auth)


@app.post("/v1/code/patch/propose")
def code_patch_propose(req: PatchProposeRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    return _patch_propose("code_patch", req, auth)


@app.post("/v1/docs/patch/apply")
def docs_patch_apply(req: PatchApplyRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    auth.require("write:projects")

    proposal_rel = f"{PATCH_PROPOSALS_DIR_REL}/{req.patch_id}.json"
    auth.require_write_path(proposal_rel)
    proposal_path = safe_path(settings.repo_root, proposal_rel)
    if not proposal_path.exists():
        raise HTTPException(status_code=404, detail=f"Patch not found: {req.patch_id}")
    try:
        proposal = json.loads(proposal_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Invalid patch proposal file: {e}") from e
    if not isinstance(proposal, dict):
        raise HTTPException(status_code=500, detail="Invalid patch proposal payload")
    if proposal.get("status") != "proposed":
        raise HTTPException(status_code=409, detail=f"Patch is not in proposed state: {proposal.get('status')}")

    target_path = str(proposal.get("target_path") or "")
    if not target_path:
        raise HTTPException(status_code=500, detail="Patch proposal missing target_path")
    auth.require_write_path(target_path)
    target_abs = safe_path(settings.repo_root, target_path)

    expected_ref = str(proposal.get("base_ref_resolved") or "")
    if expected_ref:
        # Compare target file state, not raw HEAD hash: proposal/check artifacts are allowed to
        # create commits as long as the target file content stayed aligned with expected base ref.
        expected_target = _read_commit_file(settings.repo_root, expected_ref, target_path)
        current_target = _read_commit_file(settings.repo_root, "HEAD", target_path)
        if expected_target != current_target:
            head = _resolve_commit_ref(settings.repo_root, "HEAD")
            raise HTTPException(
                status_code=409,
                detail=f"Patch base_ref mismatch: expected {expected_ref}, current {head}",
            )

    status_cp = _run_git(settings.repo_root, "status", "--porcelain")
    if status_cp.stdout.strip():
        raise HTTPException(status_code=409, detail="Working tree must be clean before applying patch")

    diff_text = str(proposal.get("diff") or "")
    if not diff_text.strip():
        raise HTTPException(status_code=400, detail="Patch diff is empty")
    tmp_fd, tmp_path = tempfile.mkstemp(prefix="amr-patch-", suffix=".diff")
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            f.write(diff_text)
        check_cp = _run_git(settings.repo_root, "apply", "--check", tmp_path)
        if check_cp.returncode != 0:
            raise HTTPException(status_code=409, detail=f"Patch apply check failed: {check_cp.stderr.strip()}")
        apply_cp = _run_git(settings.repo_root, "apply", tmp_path)
        if apply_cp.returncode != 0:
            raise HTTPException(status_code=409, detail=f"Patch apply failed: {apply_cp.stderr.strip()}")
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    committed_files = []
    commit_msg = req.commit_message or f"patches: apply {req.patch_id}"
    if gm.commit_file(target_abs, commit_msg):
        committed_files.append(target_path)

    now = datetime.now(timezone.utc).isoformat()
    proposal["status"] = "applied"
    proposal["applied_at"] = now
    proposal["applied_by"] = auth.peer_id
    proposal["updated_at"] = now
    proposal["applied_commit"] = gm.latest_commit()
    write_text_file(proposal_path, json.dumps(proposal, ensure_ascii=False, indent=2))
    if gm.commit_file(proposal_path, f"patches: mark applied {req.patch_id}"):
        committed_files.append(proposal_rel)

    applied_rel = f"{PATCH_APPLIED_DIR_REL}/{req.patch_id}.json"
    auth.require_write_path(applied_rel)
    applied_path = safe_path(settings.repo_root, applied_rel)
    write_text_file(applied_path, json.dumps(proposal, ensure_ascii=False, indent=2))
    if gm.commit_file(applied_path, f"patches: archive applied {req.patch_id}"):
        committed_files.append(applied_rel)

    _audit(settings, auth, "patch_apply", {"patch_id": req.patch_id, "target_path": target_path})
    return {
        "ok": True,
        "patch_id": req.patch_id,
        "target_path": target_path,
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }


@app.post("/v1/code/checks/run")
def code_checks_run(req: CodeCheckRunRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    auth.require("write:projects")
    ref_resolved = _resolve_commit_ref(settings.repo_root, req.ref)

    rc, stdout_text, stderr_text = _run_check_command(settings.repo_root, ref_resolved, req.profile)
    now = datetime.now(timezone.utc)
    run_id = f"run_{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    status = "passed" if rc == 0 else "failed"
    payload = {
        "schema_version": "1.0",
        "run_id": run_id,
        "profile": req.profile,
        "ref": req.ref,
        "ref_resolved": ref_resolved,
        "status": status,
        "return_code": rc,
        "started_at": now.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "command": CHECK_PROFILE_COMMANDS[req.profile],
        "stdout": stdout_text[-12000:],
        "stderr": stderr_text[-12000:],
    }
    rel = f"{RUN_CHECKS_DIR_REL}/{run_id}.json"
    auth.require_write_path(rel)
    p = safe_path(settings.repo_root, rel)
    write_text_file(p, json.dumps(payload, ensure_ascii=False, indent=2))
    committed = gm.commit_file(p, f"runs: check {run_id}")
    _audit(settings, auth, "code_checks_run", {"run_id": run_id, "profile": req.profile, "status": status, "ref": ref_resolved})
    return {"ok": True, "run": payload, "path": rel, "committed": committed, "latest_commit": gm.latest_commit()}


@app.post("/v1/code/merge")
def code_merge(req: CodeMergeRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    auth.require("write:projects")
    if req.target_ref != "HEAD":
        raise HTTPException(status_code=400, detail="Only target_ref=HEAD is currently supported")

    source_resolved = _resolve_commit_ref(settings.repo_root, req.source_ref)
    required = [str(p) for p in req.required_checks]
    artifacts = _load_check_artifacts(settings.repo_root)
    missing = []
    for profile in required:
        ok = any(
            isinstance(a, dict)
            and str(a.get("profile")) == profile
            and str(a.get("ref_resolved")) == source_resolved
            and str(a.get("status")) == "passed"
            for a in artifacts
        )
        if not ok:
            missing.append(profile)
    if missing:
        raise HTTPException(status_code=409, detail=f"Required checks not passed for {source_resolved}: {missing}")

    status_cp = _run_git(settings.repo_root, "status", "--porcelain")
    if status_cp.stdout.strip():
        raise HTTPException(status_code=409, detail="Working tree must be clean before merge")

    head_before = _resolve_commit_ref(settings.repo_root, "HEAD")
    merge_cp = _run_git(settings.repo_root, "merge", "--ff-only", source_resolved)
    if merge_cp.returncode != 0:
        raise HTTPException(status_code=409, detail=f"Merge failed: {merge_cp.stderr.strip()}")
    head_after = _resolve_commit_ref(settings.repo_root, "HEAD")
    merged = head_before != head_after
    _audit(settings, auth, "code_merge", {"source_ref": source_resolved, "merged": merged, "required_checks": required})
    return {
        "ok": True,
        "merged": merged,
        "source_ref": source_resolved,
        "target_ref": "HEAD",
        "head_before": head_before,
        "head_after": head_after,
        "required_checks": required,
    }


DELIVERY_STATE_REL = "messages/state/delivery_index.json"
TOKEN_CONFIG_REL = "config/peer_tokens.json"
SECURITY_KEYS_REL = "config/security_keys.json"
NONCE_INDEX_REL = "messages/security/nonce_index.json"
REPLICATION_STATE_REL = "peers/replication_state.json"
REPLICATION_ALLOWED_PREFIXES = {"journal", "essays", "projects", "memory", "messages", "tasks", "patches", "runs", "snapshots", "archive"}

RATE_LIMIT_STATE_REL = "logs/rate_limit_state.json"
TRUST_POLICIES_REL = "peers/trust_policies.json"
REPLICATION_TOMBSTONES_REL = "peers/replication_tombstones.json"
BACKUPS_DIR_REL = "backups"
GOVERNANCE_POLICY_REL = "config/governance_policy.json"
def _estimate_payload_bytes(payload: Any) -> int:
    try:
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    except Exception:
        encoded = str(payload).encode("utf-8", errors="ignore")
    return len(encoded)


def _enforce_payload_limit(settings, payload: Any, label: str) -> None:
    size = _estimate_payload_bytes(payload)
    if size > int(settings.max_payload_bytes):
        raise HTTPException(
            status_code=413,
            detail=f"Payload too large for {label}: {size} bytes > limit {settings.max_payload_bytes}",
        )


def _rate_limit_path(repo_root: Path) -> Path:
    return safe_path(repo_root, RATE_LIMIT_STATE_REL)


def _load_rate_limit_state(repo_root: Path) -> dict[str, Any]:
    p = _rate_limit_path(repo_root)
    if not p.exists():
        return {"schema_version": "1.0", "events": [], "verification_failures": []}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "events": [], "verification_failures": []}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "events": [], "verification_failures": []}
    events = data.get("events")
    fails = data.get("verification_failures")
    if not isinstance(events, list):
        events = []
    if not isinstance(fails, list):
        fails = []
    return {"schema_version": "1.0", "events": events, "verification_failures": fails}


def _write_rate_limit_state(repo_root: Path, payload: dict[str, Any]) -> Path:
    p = _rate_limit_path(repo_root)
    write_text_file(p, json.dumps(payload, ensure_ascii=False, indent=2))
    return p


def _auth_refs(auth: Any) -> tuple[str, str]:
    raw_token = getattr(auth, "token", None)
    if isinstance(raw_token, str) and raw_token:
        token_ref = sha256_token(raw_token)[:24]
    else:
        # Backward-compatible fallback for internal/test auth stubs.
        peer_id = getattr(auth, "peer_id", None)
        token_ref = sha256_token(f"peer:{peer_id or 'unknown'}")[:24]
    client_ip = getattr(auth, "client_ip", None)
    ip_ref = (client_ip or "unknown").strip() or "unknown"
    return token_ref, ip_ref


def _prune_rate_limit_state(payload: dict[str, Any], now: datetime, max_window_seconds: int) -> None:
    cutoff = now - timedelta(seconds=max_window_seconds)
    kept_events = []
    for row in payload.get("events", []):
        if not isinstance(row, dict):
            continue
        at = _parse_iso(row.get("at"))
        if at is not None and at >= cutoff:
            kept_events.append(row)
    payload["events"] = kept_events

    kept_fails = []
    for row in payload.get("verification_failures", []):
        if not isinstance(row, dict):
            continue
        at = _parse_iso(row.get("at"))
        if at is not None and at >= cutoff:
            kept_fails.append(row)
    payload["verification_failures"] = kept_fails


def _enforce_rate_limit(settings, auth: AuthContext, bucket: str) -> None:
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
        at = _parse_iso(row.get("at"))
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


def _record_verification_failure(settings, auth: AuthContext, reason: str) -> None:
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


def _verification_failure_count(settings, auth: AuthContext) -> int:
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
        at = _parse_iso(row.get("at"))
        if at is None or at < cutoff:
            continue
        count += 1
    _write_rate_limit_state(settings.repo_root, payload)
    return count


def _external_key_store_path(settings) -> Path:
    return Path(settings.key_store_path).expanduser().resolve()


def _load_external_key_store(settings) -> dict[str, Any]:
    p = _external_key_store_path(settings)
    if not p.exists():
        return {"schema_version": "1.0", "keys": {}}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "keys": {}}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "keys": {}}
    keys = data.get("keys")
    if not isinstance(keys, dict):
        keys = {}
    return {"schema_version": "1.0", "keys": keys}


def _write_external_key_store(settings, payload: dict[str, Any]) -> Path:
    p = _external_key_store_path(settings)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        os.chmod(p, 0o600)
    except Exception:
        # Best-effort permission hardening for environments without chmod support.
        pass
    return p


def _resolve_signing_secret(settings, key_id: str, row: dict[str, Any]) -> str | None:
    # Legacy in-repo fallback
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


def _default_governance_policy() -> dict[str, Any]:
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


def _load_governance_policy(repo_root: Path) -> dict[str, Any]:
    p = safe_path(repo_root, GOVERNANCE_POLICY_REL)
    default = _default_governance_policy()
    if not p.exists():
        return default
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return default
    if not isinstance(data, dict):
        return default
    merged = dict(default)
    merged.update(data)
    return merged

def _load_replication_tombstones(repo_root: Path) -> dict[str, Any]:
    p = safe_path(repo_root, REPLICATION_TOMBSTONES_REL)
    if not p.exists():
        return {"schema_version": "1.0", "entries": {}}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "entries": {}}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "entries": {}}
    entries = data.get("entries")
    if not isinstance(entries, dict):
        entries = {}
    return {"schema_version": "1.0", "entries": entries}


def _write_replication_tombstones(repo_root: Path, payload: dict[str, Any]) -> Path:
    p = safe_path(repo_root, REPLICATION_TOMBSTONES_REL)
    write_text_file(p, json.dumps(payload, ensure_ascii=False, indent=2))
    return p


def _parse_dt_or_epoch(iso_value: str | None, fallback_epoch: float) -> float:
    dt = _parse_iso(iso_value)
    if dt is None:
        return fallback_epoch
    return dt.timestamp()

def _delivery_state_path(repo_root: Path) -> Path:
    return safe_path(repo_root, DELIVERY_STATE_REL)


def _load_delivery_state(repo_root: Path) -> dict[str, Any]:
    p = _delivery_state_path(repo_root)
    if not p.exists():
        return {"version": "1", "records": {}, "idempotency": {}}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
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
    p = _delivery_state_path(repo_root)
    write_text_file(p, json.dumps(state, ensure_ascii=False, indent=2))
    return p


def _effective_delivery_status(record: dict[str, Any], now: datetime) -> str:
    status = str(record.get("status") or "pending_ack")
    if status != "pending_ack":
        return status
    ack_deadline = _parse_iso(record.get("ack_deadline"))
    if not ack_deadline:
        return status
    if now > ack_deadline:
        return "dead_letter"
    return status


def _delivery_record_view(record: dict[str, Any], now: datetime) -> dict[str, Any]:
    out = dict(record)
    out["effective_status"] = _effective_delivery_status(record, now)
    return out


def _idempotency_scope_key(sender: str, recipient: str, idempotency_key: str) -> str:
    return f"{sender}|{recipient}|{idempotency_key}"


def _canonical_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _sha256_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _message_signing_blob(payload: dict[str, Any], key_id: str, nonce: str, expires_at: str | None) -> bytes:
    canonical = _canonical_json({"payload": payload, "key_id": key_id, "nonce": nonce, "expires_at": expires_at})
    return canonical.encode("utf-8")


def _hmac_sha256(secret: str, blob: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), blob, hashlib.sha256).hexdigest()


def _load_security_keys(repo_root: Path) -> dict[str, Any]:
    p = safe_path(repo_root, SECURITY_KEYS_REL)
    if not p.exists():
        return {"schema_version": "1.0", "active_key_id": None, "keys": {}}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "active_key_id": None, "keys": {}}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "active_key_id": None, "keys": {}}
    keys = data.get("keys")
    if not isinstance(keys, dict):
        keys = {}
    return {"schema_version": "1.0", "active_key_id": data.get("active_key_id"), "keys": keys}


def _write_security_keys(repo_root: Path, payload: dict[str, Any]) -> Path:
    p = safe_path(repo_root, SECURITY_KEYS_REL)
    write_text_file(p, json.dumps(payload, ensure_ascii=False, indent=2))
    return p


def _load_token_config(repo_root: Path) -> dict[str, Any]:
    p = safe_path(repo_root, TOKEN_CONFIG_REL)
    if not p.exists():
        return {"schema_version": "1.0", "tokens": []}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "tokens": []}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "tokens": []}
    tokens = data.get("tokens")
    if not isinstance(tokens, list):
        tokens = []
    return {"schema_version": "1.0", "tokens": tokens}


def _write_token_config(repo_root: Path, payload: dict[str, Any]) -> Path:
    p = safe_path(repo_root, TOKEN_CONFIG_REL)
    write_text_file(p, json.dumps(payload, ensure_ascii=False, indent=2))
    return p


def _resolve_token_expiry(expires_at: str | None, ttl_seconds: int | None) -> str | None:
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
    status = str(entry.get("status") or "active")
    if status != "active":
        return status
    exp = _parse_iso(entry.get("expires_at"))
    if exp is not None and now > exp:
        return "expired"
    return "active"


def _token_public_view(entry: dict[str, Any], now: datetime) -> dict[str, Any]:
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
    if not value:
        return None
    v = str(value).strip()
    if v.startswith("sha256:"):
        v = v.split(":", 1)[1]
    return v or None


def _load_nonce_index(repo_root: Path) -> dict[str, Any]:
    p = safe_path(repo_root, NONCE_INDEX_REL)
    if not p.exists():
        return {"schema_version": "1.0", "entries": {}}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "entries": {}}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "entries": {}}
    entries = data.get("entries")
    if not isinstance(entries, dict):
        entries = {}
    return {"schema_version": "1.0", "entries": entries}


def _write_nonce_index(repo_root: Path, payload: dict[str, Any]) -> Path:
    p = safe_path(repo_root, NONCE_INDEX_REL)
    write_text_file(p, json.dumps(payload, ensure_ascii=False, indent=2))
    return p


def _prune_nonce_entries(payload: dict[str, Any], now: datetime) -> int:
    entries = payload.setdefault("entries", {})
    if not isinstance(entries, dict):
        payload["entries"] = {}
        return 0
    remove_keys = []
    for k, row in entries.items():
        if not isinstance(row, dict):
            remove_keys.append(k)
            continue
        exp = _parse_iso(row.get("expires_at"))
        if exp is not None and now > exp:
            remove_keys.append(k)
    for k in remove_keys:
        entries.pop(k, None)
    return len(remove_keys)


def _load_replication_state(repo_root: Path) -> dict[str, Any]:
    p = safe_path(repo_root, REPLICATION_STATE_REL)
    if not p.exists():
        return {"schema_version": "1.0", "last_pull_by_source": {}, "last_push": None, "pull_idempotency": {}}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"schema_version": "1.0", "last_pull_by_source": {}, "last_push": None, "pull_idempotency": {}}
    if not isinstance(data, dict):
        return {"schema_version": "1.0", "last_pull_by_source": {}, "last_push": None, "pull_idempotency": {}}
    if not isinstance(data.get("last_pull_by_source"), dict):
        data["last_pull_by_source"] = {}
    if not isinstance(data.get("pull_idempotency"), dict):
        data["pull_idempotency"] = {}
    return data


def _write_replication_state(repo_root: Path, payload: dict[str, Any]) -> Path:
    p = safe_path(repo_root, REPLICATION_STATE_REL)
    write_text_file(p, json.dumps(payload, ensure_ascii=False, indent=2))
    return p


def _iter_replication_files(repo_root: Path, include_prefixes: list[str], max_files: int, include_deleted: bool = True) -> list[dict[str, Any]]:
    prefixes = []
    for raw in include_prefixes:
        rel = str(raw or "").strip().strip("/")
        if not rel:
            continue
        top = Path(rel).parts[0] if Path(rel).parts else ""
        if top not in REPLICATION_ALLOWED_PREFIXES:
            continue
        prefixes.append(rel)
    if not prefixes:
        prefixes = ["memory", "messages", "projects", "essays", "journal", "tasks", "patches", "runs", "snapshots"]

    items = []
    for prefix in prefixes:
        base = safe_path(repo_root, prefix)
        if not base.exists():
            continue
        for p in sorted(base.rglob("*")):
            if not p.is_file() or ".git" in p.parts:
                continue
            rel = str(p.relative_to(repo_root))
            try:
                content = p.read_text(encoding="utf-8")
            except Exception:
                continue
            stat = p.stat()
            items.append(
                {
                    "path": rel,
                    "content": content,
                    "sha256": _sha256_text(content),
                    "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                    "deleted": False,
                    "tombstone_at": None,
                }
            )
            if len(items) >= max_files:
                return items

    if include_deleted and len(items) < max_files:
        tombstones = _load_replication_tombstones(repo_root)
        entries = tombstones.get("entries", {})
        if isinstance(entries, dict):
            for path, row in sorted(entries.items(), key=lambda x: x[0]):
                top = Path(str(path)).parts[0] if Path(str(path)).parts else ""
                if top not in REPLICATION_ALLOWED_PREFIXES:
                    continue
                if prefixes and not any(str(path).startswith(f"{p}/") or str(path) == p for p in prefixes):
                    continue
                if not isinstance(row, dict):
                    continue
                items.append(
                    {
                        "path": str(path),
                        "content": None,
                        "sha256": None,
                        "modified_at": row.get("tombstone_at"),
                        "deleted": True,
                        "tombstone_at": row.get("tombstone_at"),
                    }
                )
                if len(items) >= max_files:
                    return items

    return items


@app.post("/v1/messages/send")
def messages_send(req: MessageSendRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    _enforce_rate_limit(settings, auth, "messages_send")
    _enforce_payload_limit(settings, req.model_dump(), "messages_send")
    auth.require("write:messages")
    auth.require_write_path("messages/inbox/x.jsonl")
    auth.require_write_path(DELIVERY_STATE_REL)

    if settings.require_signed_ingress and req.signed_envelope is None:
        raise HTTPException(status_code=400, detail="signed_envelope is required when strict signed ingress is enabled")

    state = _load_delivery_state(settings.repo_root)
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
                    verification = _verify_signed_payload(
                        settings,
                        gm,
                        auth,
                        payload=signed_payload,
                        key_id=req.signed_envelope.key_id,
                        nonce=req.signed_envelope.nonce,
                        expires_at=req.signed_envelope.expires_at,
                        signature=req.signed_envelope.signature,
                        algorithm=req.signed_envelope.algorithm,
                        consume_nonce=False,
                        audit_event="messages_send_signature",
                    )
                    if not verification["valid"]:
                        raise HTTPException(status_code=401, detail=f"Invalid signed envelope: {verification['reason']}")

                now = datetime.now(timezone.utc)
                _audit(
                    settings,
                    auth,
                    "message_send_idempotent_replay",
                    {"idempotency_key": req.idempotency_key, "message_id": existing_id},
                )
                return {
                    "ok": True,
                    "idempotent_replay": True,
                    "message": existing.get("message"),
                    "delivery_state": _delivery_record_view(existing, now),
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
        signature_verification = _verify_signed_payload(
            settings,
            gm,
            auth,
            payload=signed_payload,
            key_id=req.signed_envelope.key_id,
            nonce=req.signed_envelope.nonce,
            expires_at=req.signed_envelope.expires_at,
            signature=req.signed_envelope.signature,
            algorithm=req.signed_envelope.algorithm,
            consume_nonce=req.signed_envelope.consume_nonce,
            audit_event="messages_send_signature",
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
        p = safe_path(settings.repo_root, rel)
        append_jsonl(p, msg)
        if gm.commit_file(p, f"messages: append {rel}"):
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
        delivery_state = _delivery_record_view(record, now)

    _audit(settings, auth, "message_send", {"thread_id": req.thread_id, "to": req.recipient})
    return {
        "ok": True,
        "idempotent_replay": False,
        "message": msg,
        "delivery_state": delivery_state,
        "signature_verification": signature_verification,
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }


@app.post("/v1/messages/ack")
def messages_ack(req: MessageAckRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    auth.require("write:messages")
    auth.require_write_path(DELIVERY_STATE_REL)

    state = _load_delivery_state(settings.repo_root)
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
    else:  # deferred
        record["status"] = "pending_ack"
        timeout = int(record.get("ack_timeout_seconds") or 300)
        record["ack_deadline"] = (now + timedelta(seconds=timeout)).isoformat()

    state_path = _write_delivery_state(settings.repo_root, state)
    committed_files = []
    if gm.commit_file(state_path, f"messages: ack {req.message_id}"):
        committed_files.append(DELIVERY_STATE_REL)

    ack_rel = f"messages/acks/{req.message_id}.jsonl"
    ack_path = safe_path(settings.repo_root, ack_rel)
    append_jsonl(ack_path, ack_row)
    if gm.commit_file(ack_path, f"messages: ack log {req.message_id}"):
        committed_files.append(ack_rel)

    _audit(settings, auth, "messages_ack", {"message_id": req.message_id, "status": req.status})
    return {
        "ok": True,
        "message_id": req.message_id,
        "ack": ack_row,
        "delivery_state": _delivery_record_view(record, now),
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }


@app.get("/v1/messages/pending")
def messages_pending(
    recipient: str | None = Query(default=None),
    status: str | None = Query(default=None),
    include_terminal: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=500),
    auth: AuthContext = Depends(require_auth),
) -> dict:
    if recipient is not None and not isinstance(recipient, str):
        recipient = None
    if status is not None and not isinstance(status, str):
        status = None
    if not isinstance(include_terminal, bool):
        include_terminal = False
    if not isinstance(limit, int):
        limit = 50

    settings, _ = _services()
    auth.require("read:files")
    auth.require_read_path(DELIVERY_STATE_REL)

    state = _load_delivery_state(settings.repo_root)
    now = datetime.now(timezone.utc)
    rows = []
    summary: dict[str, int] = {}
    for record in state.get("records", {}).values():
        if not isinstance(record, dict):
            continue
        view = _delivery_record_view(record, now)
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
    _audit(settings, auth, "messages_pending", {"count": len(out), "recipient": recipient, "status": status})
    return {"ok": True, "count": len(out), "summary": summary, "messages": out}


@app.get("/v1/messages/inbox")
def messages_inbox(recipient: str = Query(...), limit: int = Query(default=20, ge=1, le=200), auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    auth.require("read:files")
    auth.require_read_path(f"messages/inbox/{recipient}.jsonl")
    p = safe_path(settings.repo_root, f"messages/inbox/{recipient}.jsonl")
    if not p.exists():
        return {"ok": True, "recipient": recipient, "count": 0, "messages": []}

    lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    messages = []
    for line in lines[-limit:]:
        try:
            messages.append(json.loads(line))
        except Exception:
            continue
    _audit(settings, auth, "messages_inbox", {"recipient": recipient, "count": len(messages)})
    return {"ok": True, "recipient": recipient, "count": len(messages), "messages": messages}


@app.get("/v1/messages/thread")
def messages_thread(thread_id: str = Query(...), limit: int = Query(default=100, ge=1, le=1000), auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    auth.require("read:files")
    rel = f"messages/threads/{thread_id}.jsonl"
    auth.require_read_path(rel)
    p = safe_path(settings.repo_root, rel)
    if not p.exists():
        return {"ok": True, "thread_id": thread_id, "count": 0, "messages": []}
    messages = []
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines()[-limit:]:
        try:
            messages.append(json.loads(line))
        except Exception:
            continue
    return {"ok": True, "thread_id": thread_id, "count": len(messages), "messages": messages}


@app.post("/v1/relay/forward")
def relay_forward(req: RelayForwardRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    _enforce_rate_limit(settings, auth, "relay_forward")
    _enforce_payload_limit(settings, req.model_dump(), "relay_forward")
    auth.require("write:messages")
    # relay writes immutable relay log + recipient inbox + thread file

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
        signature_verification = _verify_signed_payload(
            settings,
            gm,
            auth,
            payload=signed_payload,
            key_id=req.signed_envelope.key_id,
            nonce=req.signed_envelope.nonce,
            expires_at=req.signed_envelope.expires_at,
            signature=req.signed_envelope.signature,
            algorithm=req.signed_envelope.algorithm,
            consume_nonce=req.signed_envelope.consume_nonce,
            audit_event="relay_forward_signature",
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
        p = safe_path(settings.repo_root, rel)
        append_jsonl(p, msg)
        if gm.commit_file(p, f"relay: forward {rel}"):
            committed_files.append(rel)
    _audit(settings, auth, "relay_forward", {"relay_id": req.relay_id, "to": req.target_recipient, "thread_id": req.thread_id})
    return {
        "ok": True,
        "message": msg,
        "signature_verification": signature_verification,
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }


@app.get("/v1/security/tokens")
def security_tokens_list(
    peer_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    include_inactive: bool = Query(default=False),
    auth: AuthContext = Depends(require_auth),
) -> dict:
    settings, _ = _services()
    _enforce_rate_limit(settings, auth, "security_tokens_list")
    auth.require("admin:peers")
    auth.require_read_path(TOKEN_CONFIG_REL)
    if peer_id is not None and not isinstance(peer_id, str):
        peer_id = None
    if status is not None and not isinstance(status, str):
        status = None
    if not isinstance(include_inactive, bool):
        include_inactive = False

    payload = _load_token_config(settings.repo_root)
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


@app.post("/v1/security/tokens/issue")
def security_tokens_issue(req: SecurityTokenIssueRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    _enforce_rate_limit(settings, auth, "security_tokens_issue")
    _enforce_payload_limit(settings, req.model_dump(), "security_tokens_issue")
    auth.require("admin:peers")
    auth.require_write_path(TOKEN_CONFIG_REL)

    payload = _load_token_config(settings.repo_root)
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

    p = _write_token_config(settings.repo_root, payload)
    committed = gm.commit_file(p, f"security: issue token {token_id}")
    get_settings(force_reload=True)

    _audit(settings, auth, "security_tokens_issue", {"token_id": token_id, "peer_id": req.peer_id, "expires_at": expires_at})
    return {
        "ok": True,
        "token": token_plain,
        "token_meta": _token_public_view(entry, now_dt),
        "committed": committed,
        "latest_commit": gm.latest_commit(),
    }


@app.post("/v1/security/tokens/revoke")
def security_tokens_revoke(req: SecurityTokenRevokeRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    _enforce_rate_limit(settings, auth, "security_tokens_revoke")
    _enforce_payload_limit(settings, req.model_dump(), "security_tokens_revoke")
    auth.require("admin:peers")
    auth.require_write_path(TOKEN_CONFIG_REL)

    payload = _load_token_config(settings.repo_root)
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
        p = _write_token_config(settings.repo_root, payload)
        committed = gm.commit_file(p, "security: revoke token(s)")
    get_settings(force_reload=True)

    _audit(settings, auth, "security_tokens_revoke", {"matched": matched, "revoked": revoked, "reason": req.reason})
    return {
        "ok": True,
        "matched": matched,
        "revoked": revoked,
        "tokens": revoked_rows,
        "committed": committed,
        "latest_commit": gm.latest_commit(),
    }



@app.post("/v1/security/tokens/rotate")
def security_tokens_rotate(req: SecurityTokenRotateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    _enforce_rate_limit(settings, auth, "security_tokens_rotate")
    _enforce_payload_limit(settings, req.model_dump(), "security_tokens_rotate")
    auth.require("admin:peers")
    auth.require_write_path(TOKEN_CONFIG_REL)

    norm_sha = _normalize_token_sha(req.token_sha256)
    if not req.token_id and not norm_sha:
        raise HTTPException(status_code=400, detail="Provide token_id or token_sha256")

    payload = _load_token_config(settings.repo_root)
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

    p = _write_token_config(settings.repo_root, payload)
    source_ref = source_token_id or "sha-match"
    committed = gm.commit_file(p, f"security: rotate token {source_ref} -> {new_token_id}")
    get_settings(force_reload=True)

    _audit(
        settings,
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
@app.post("/v1/security/keys/rotate")
def security_keys_rotate(req: SecurityKeysRotateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    _enforce_rate_limit(settings, auth, "security_keys_rotate")
    _enforce_payload_limit(settings, req.model_dump(), "security_keys_rotate")
    auth.require("admin:peers")
    auth.require_write_path(SECURITY_KEYS_REL)
    payload = _load_security_keys(settings.repo_root)
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
    if settings.use_external_key_store:
        key_store = _load_external_key_store(settings)
        ext_keys = key_store.setdefault("keys", {})
        if not isinstance(ext_keys, dict):
            ext_keys = {}
            key_store["keys"] = ext_keys
        ext_keys[key_id] = {"secret": secret, "updated_at": now}
        _write_external_key_store(settings, key_store)
        row["secret_ref"] = f"external:{key_id}"
    else:
        row["secret"] = secret

    keys[key_id] = row
    if req.activate:
        payload["active_key_id"] = key_id

    p = _write_security_keys(settings.repo_root, payload)
    committed = gm.commit_file(p, f"security: rotate key {key_id}")
    _audit(settings, auth, "security_keys_rotate", {"key_id": key_id, "activate": req.activate, "retire_previous": req.retire_previous, "storage_mode": storage_mode})

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

    return {
        "ok": True,
        "active_key_id": payload.get("active_key_id"),
        "key": key_view,
        "committed": committed,
        "latest_commit": gm.latest_commit(),
    }


def _verify_signed_payload(
    settings,
    gm,
    auth: AuthContext,
    payload: dict[str, Any],
    key_id: str,
    nonce: str,
    expires_at: str | None,
    signature: str,
    algorithm: str = "hmac-sha256",
    consume_nonce: bool = True,
    audit_event: str = "messages_verify",
) -> dict[str, Any]:
    if consume_nonce:
        auth.require_write_path(NONCE_INDEX_REL)

    prior_failures = _verification_failure_count(settings, auth)
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
        _audit(settings, auth, audit_event, detail)
        return detail

    keys_payload = _load_security_keys(settings.repo_root)
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
    reason = "ok"

    committed_files: list[str] = []
    if expired:
        reason = "expired"
    elif not signature_valid:
        reason = "invalid_signature"
    elif consume_nonce:
        nonce_payload = _load_nonce_index(settings.repo_root)
        _prune_nonce_entries(nonce_payload, now)
        entries = nonce_payload.setdefault("entries", {})
        key = f"{key_id}|{nonce}"
        if key in entries:
            replay_detected = True
            reason = "replay_detected"
        else:
            entries[key] = {
                "key_id": key_id,
                "nonce": nonce,
                "first_seen_at": now.isoformat(),
                "expires_at": expires_at,
            }
            nonce_path = _write_nonce_index(settings.repo_root, nonce_payload)
            if gm.commit_file(nonce_path, f"messages: consume nonce {key_id}:{nonce}"):
                committed_files.append(NONCE_INDEX_REL)
            nonce_consumed = True

    valid = reason == "ok"
    if not valid:
        _record_verification_failure(settings, auth, reason)

    detail = {
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
        "failure_count": _verification_failure_count(settings, auth),
        "committed_files": committed_files,
    }
    _audit(settings, auth, audit_event, detail)
    return detail


@app.post("/v1/messages/verify")
def messages_verify(req: MessageVerifyRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    _enforce_rate_limit(settings, auth, "messages_verify")
    _enforce_payload_limit(settings, req.model_dump(), "messages_verify")
    auth.require("write:messages")
    verification = _verify_signed_payload(
        settings,
        gm,
        auth,
        payload=req.payload,
        key_id=req.key_id,
        nonce=req.nonce,
        expires_at=req.expires_at,
        signature=req.signature,
        algorithm=req.algorithm,
        consume_nonce=req.consume_nonce,
        audit_event="messages_verify",
    )
    return {
        "ok": True,
        **verification,
        "latest_commit": gm.latest_commit(),
    }


@app.get("/v1/metrics")
def metrics(auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    auth.require("read:index")
    auth.require_read_path(DELIVERY_STATE_REL)
    auth.require_read_path("logs/api_audit.jsonl")
    now = datetime.now(timezone.utc)

    state = _load_delivery_state(settings.repo_root)
    delivery_summary: dict[str, int] = {}
    by_recipient: dict[str, dict[str, int]] = {}
    for row in state.get("records", {}).values():
        if not isinstance(row, dict):
            continue
        view = _delivery_record_view(row, now)
        eff = str(view.get("effective_status") or "unknown")
        delivery_summary[eff] = delivery_summary.get(eff, 0) + 1
        recipient = str(view.get("to") or "unknown")
        rec = by_recipient.setdefault(recipient, {"total": 0, "pending": 0, "acked": 0, "dead_letter": 0})
        rec["total"] += 1
        if eff == "pending_ack":
            rec["pending"] += 1
        elif eff == "acked":
            rec["acked"] += 1
        elif eff == "dead_letter":
            rec["dead_letter"] += 1

    acked = delivery_summary.get("acked", 0)
    dead_letter = delivery_summary.get("dead_letter", 0)
    ack_denom = acked + dead_letter
    ack_success_ratio = (acked / ack_denom) if ack_denom > 0 else 1.0

    event_counts: dict[str, int] = {}
    peer_counts: dict[str, int] = {}
    audit_path = settings.repo_root / "logs" / "api_audit.jsonl"
    if audit_path.exists():
        for line in audit_path.read_text(encoding="utf-8", errors="ignore").splitlines()[-10000:]:
            try:
                item = json.loads(line)
            except Exception:
                continue
            ev = str(item.get("event") or "unknown")
            event_counts[ev] = event_counts.get(ev, 0) + 1
            peer = str(item.get("peer_id") or "unknown")
            peer_counts[peer] = peer_counts.get(peer, 0) + 1

    check_artifacts = _load_check_artifacts(settings.repo_root)
    check_summary: dict[str, int] = {}
    for row in check_artifacts:
        profile = str(row.get("profile") or "unknown")
        status = str(row.get("status") or "unknown")
        key = f"{profile}:{status}"
        check_summary[key] = check_summary.get(key, 0) + 1

    replication_state = _load_replication_state(settings.repo_root)

    rate_state = _load_rate_limit_state(settings.repo_root)
    verification_failures_recent = 0
    fail_cutoff = now - timedelta(seconds=int(settings.verify_failure_window_seconds))
    for row in rate_state.get("verification_failures", []):
        if not isinstance(row, dict):
            continue
        at = _parse_iso(row.get("at"))
        if at is not None and at >= fail_cutoff:
            verification_failures_recent += 1

    alarms: list[dict[str, Any]] = []
    backlog_depth = delivery_summary.get("pending_ack", 0)
    if backlog_depth > int(settings.backlog_alarm_threshold):
        alarms.append(
            {
                "type": "delivery_backlog_growth",
                "severity": "warning",
                "message": f"Pending backlog depth {backlog_depth} exceeds threshold {settings.backlog_alarm_threshold}",
                "metric": "delivery.backlog_depth",
            }
        )

    if verification_failures_recent > int(settings.verification_alarm_threshold):
        alarms.append(
            {
                "type": "verification_failures",
                "severity": "warning",
                "message": (
                    f"Verification failures in last {settings.verify_failure_window_seconds}s: "
                    f"{verification_failures_recent} (threshold {settings.verification_alarm_threshold})"
                ),
                "metric": "security.verification_failures_recent",
            }
        )

    drift_threshold = int(settings.replication_drift_max_age_seconds)
    last_push = replication_state.get("last_push")
    if isinstance(last_push, dict):
        pushed_at = _parse_iso(last_push.get("pushed_at"))
        if pushed_at is not None and (now - pushed_at).total_seconds() > drift_threshold:
            alarms.append(
                {
                    "type": "replication_drift",
                    "severity": "warning",
                    "message": f"Last replication push is stale (> {drift_threshold}s)",
                    "metric": "replication.last_push",
                }
            )

    pulls = replication_state.get("last_pull_by_source", {})
    if isinstance(pulls, dict):
        for source, row in pulls.items():
            if not isinstance(row, dict):
                continue
            pulled_at = _parse_iso(row.get("pulled_at"))
            if pulled_at is not None and (now - pulled_at).total_seconds() > drift_threshold:
                alarms.append(
                    {
                        "type": "replication_drift",
                        "severity": "warning",
                        "message": f"Replication pull from {source} is stale (> {drift_threshold}s)",
                        "metric": "replication.last_pull_by_source",
                        "source_peer": source,
                    }
                )

    return {
        "ok": True,
        "generated_at": now.isoformat(),
        "delivery": {
            "summary": delivery_summary,
            "backlog_depth": backlog_depth,
            "ack_success_ratio": round(ack_success_ratio, 4),
            "by_recipient": by_recipient,
        },
        "checks": {"summary": check_summary, "artifact_count": len(check_artifacts)},
        "audit": {"event_counts": event_counts, "peer_counts": peer_counts},
        "security": {
            "verification_failures_recent": verification_failures_recent,
            "verification_failure_window_seconds": int(settings.verify_failure_window_seconds),
        },
        "replication": {
            "last_push": replication_state.get("last_push"),
            "last_pull_by_source": replication_state.get("last_pull_by_source", {}),
        },
        "alarms": alarms,
    }


@app.post("/v1/replay/messages")
def replay_messages(req: MessageReplayRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    auth.require("write:messages")
    auth.require_write_path("messages/inbox/x.jsonl")
    auth.require_write_path(DELIVERY_STATE_REL)
    state = _load_delivery_state(settings.repo_root)
    record = state.get("records", {}).get(req.message_id)
    if not isinstance(record, dict):
        raise HTTPException(status_code=404, detail="Tracked message not found")

    now = datetime.now(timezone.utc)
    effective = _effective_delivery_status(record, now)
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
        p = safe_path(settings.repo_root, rel)
        append_jsonl(p, replay_msg)
        if gm.commit_file(p, f"messages: replay append {rel}"):
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

    _audit(
        settings,
        auth,
        "messages_replay",
        {"message_id": req.message_id, "new_message_id": new_message_id, "reason": req.reason, "force": req.force},
    )
    return {
        "ok": True,
        "message_id": req.message_id,
        "replayed_message_id": new_message_id,
        "delivery_state": _delivery_record_view(new_record, now),
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }


@app.post("/v1/replication/pull")
def replication_pull(req: ReplicationPullRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    _enforce_rate_limit(settings, auth, "replication_pull")
    _enforce_payload_limit(settings, req.model_dump(), "replication_pull")
    auth.require("admin:peers")

    state = _load_replication_state(settings.repo_root)
    idempotency_key = (req.idempotency_key or "").strip() or None
    idem_ref = f"{req.source_peer}|{idempotency_key}" if idempotency_key else None
    if idem_ref:
        previous = state.get("pull_idempotency", {}).get(idem_ref)
        if isinstance(previous, dict):
            return {
                "ok": True,
                "idempotent_replay": True,
                "source_peer": req.source_peer,
                "received_count": int(previous.get("received_count") or 0),
                "changed_count": int(previous.get("changed_count") or 0),
                "deleted_count": int(previous.get("deleted_count") or 0),
                "conflict_count": int(previous.get("conflict_count") or 0),
                "skipped_count": int(previous.get("skipped_count") or 0),
                "committed_files": [],
                "latest_commit": gm.latest_commit(),
            }

    committed_files: list[str] = []
    changed = 0
    deleted = 0
    skipped = 0
    conflicts = 0
    tombstones = _load_replication_tombstones(settings.repo_root)
    tomb_entries = tombstones.setdefault("entries", {})
    if not isinstance(tomb_entries, dict):
        tomb_entries = {}
        tombstones["entries"] = tomb_entries

    now = datetime.now(timezone.utc)
    for file_row in req.files:
        top = Path(file_row.path).parts[0] if Path(file_row.path).parts else ""
        if top not in REPLICATION_ALLOWED_PREFIXES:
            raise HTTPException(status_code=400, detail=f"Replication path namespace not allowed: {file_row.path}")
        auth.require_write_path(file_row.path)

        p = safe_path(settings.repo_root, file_row.path)
        local_exists = p.exists() and p.is_file()
        local_content = read_text_file(p) if local_exists else None
        local_epoch = p.stat().st_mtime if local_exists else 0.0
        remote_epoch = _parse_dt_or_epoch(file_row.modified_at, now.timestamp())

        if file_row.deleted:
            if req.conflict_policy == "target_wins" and local_exists:
                conflicts += 1
                skipped += 1
                continue
            if req.conflict_policy == "error" and local_exists:
                raise HTTPException(status_code=409, detail=f"Replication conflict on delete: {file_row.path}")

            if local_exists:
                try:
                    p.unlink()
                    deleted += 1
                    changed += 1
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"Failed to delete replicated path {file_row.path}: {e}") from e
            else:
                skipped += 1

            tomb_entries[file_row.path] = {
                "tombstone_at": file_row.tombstone_at or now.isoformat(),
                "source_peer": req.source_peer,
                "idempotency_key": idempotency_key,
            }
            continue

        if file_row.content is None or file_row.sha256 is None:
            raise HTTPException(status_code=400, detail=f"Replication file payload requires content+sha256 for upsert: {file_row.path}")
        if _sha256_text(file_row.content) != file_row.sha256:
            raise HTTPException(status_code=400, detail=f"Replication sha256 mismatch for {file_row.path}")

        if req.mode == "upsert" and local_exists and local_content == file_row.content:
            skipped += 1
            continue

        should_write = True
        if local_exists and local_content != file_row.content:
            if req.conflict_policy == "target_wins":
                should_write = False
                conflicts += 1
            elif req.conflict_policy == "error":
                raise HTTPException(status_code=409, detail=f"Replication conflict on path: {file_row.path}")
            elif req.conflict_policy == "last_write_wins" and remote_epoch < local_epoch:
                should_write = False
                conflicts += 1

        if not should_write:
            skipped += 1
            continue

        write_text_file(p, file_row.content)
        changed += 1
        tomb_entries.pop(file_row.path, None)
        msg = req.commit_message or f"replication: pull {req.source_peer} {file_row.path}"
        if gm.commit_file(p, msg):
            committed_files.append(file_row.path)

    tomb_path = _write_replication_tombstones(settings.repo_root, tombstones)
    if gm.commit_file(tomb_path, f"replication: update tombstones {req.source_peer}"):
        committed_files.append(REPLICATION_TOMBSTONES_REL)

    state.setdefault("last_pull_by_source", {})[req.source_peer] = {
        "pulled_at": now.isoformat(),
        "received_count": len(req.files),
        "changed_count": changed,
        "deleted_count": deleted,
        "conflict_count": conflicts,
        "mode": req.mode,
        "conflict_policy": req.conflict_policy,
        "idempotency_key": idempotency_key,
    }
    if idem_ref:
        pull_map = state.setdefault("pull_idempotency", {})
        if not isinstance(pull_map, dict):
            pull_map = {}
            state["pull_idempotency"] = pull_map
        pull_map[idem_ref] = {
            "at": now.isoformat(),
            "received_count": len(req.files),
            "changed_count": changed,
            "deleted_count": deleted,
            "conflict_count": conflicts,
            "skipped_count": skipped,
        }

    state_path = _write_replication_state(settings.repo_root, state)
    if gm.commit_file(state_path, f"replication: update pull state {req.source_peer}"):
        committed_files.append(REPLICATION_STATE_REL)

    _audit(
        settings,
        auth,
        "replication_pull",
        {
            "source_peer": req.source_peer,
            "received": len(req.files),
            "changed": changed,
            "deleted": deleted,
            "conflicts": conflicts,
            "mode": req.mode,
            "conflict_policy": req.conflict_policy,
            "idempotency_key": idempotency_key,
        },
    )
    return {
        "ok": True,
        "idempotent_replay": False,
        "source_peer": req.source_peer,
        "received_count": len(req.files),
        "changed_count": changed,
        "deleted_count": deleted,
        "conflict_count": conflicts,
        "skipped_count": skipped,
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }


@app.post("/v1/replication/push")
def replication_push(req: ReplicationPushRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    _enforce_rate_limit(settings, auth, "replication_push")
    auth.require("admin:peers")

    files = _iter_replication_files(
        settings.repo_root,
        req.include_prefixes,
        req.max_files,
        include_deleted=req.include_deleted,
    )
    for row in files:
        auth.require_read_path(str(row.get("path", "")))

    by_prefix: dict[str, int] = {}
    for row in files:
        top = Path(str(row["path"])).parts[0] if Path(str(row["path"])).parts else ""
        by_prefix[top] = by_prefix.get(top, 0) + 1

    target_base = req.base_url
    if not target_base and req.peer_id:
        registry = load_peers_registry(settings.repo_root)
        peer = registry.get("peers", {}).get(req.peer_id)
        if isinstance(peer, dict):
            target_base = str(peer.get("base_url") or "").strip() or None

    push_id_source = req.idempotency_key or _canonical_json(
        {
            "peer": auth.peer_id,
            "target": target_base,
            "path": req.target_path,
            "policy": req.conflict_policy,
            "files": [{"path": f.get("path"), "sha256": f.get("sha256"), "deleted": bool(f.get("deleted"))} for f in files],
        }
    )
    push_id = "push_" + hashlib.sha256(push_id_source.encode("utf-8")).hexdigest()[:24]

    if req.dry_run or not target_base:
        return {
            "ok": True,
            "dry_run": True,
            "idempotency_key": push_id,
            "file_count": len(files),
            "by_prefix": by_prefix,
            "target_base_url": target_base,
            "target_path": req.target_path,
            "sample_paths": [row["path"] for row in files[:20]],
            "include_deleted": req.include_deleted,
            "conflict_policy": req.conflict_policy,
        }

    target_url = urljoin(target_base.rstrip("/") + "/", req.target_path.lstrip("/"))
    request_payload = {
        "source_peer": auth.peer_id,
        "files": files,
        "mode": "upsert",
        "conflict_policy": req.conflict_policy,
        "idempotency_key": push_id,
    }
    _enforce_payload_limit(settings, request_payload, "replication_push")

    body = _canonical_json(request_payload).encode("utf-8")
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if req.target_token:
        headers["Authorization"] = f"Bearer {req.target_token}"
    try:
        with urlopen(UrlRequest(target_url, data=body, headers=headers, method="POST"), timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
            remote_payload = json.loads(raw) if raw else {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed replication push: {e}") from e

    auth.require_write_path(REPLICATION_STATE_REL)
    state = _load_replication_state(settings.repo_root)
    state["last_push"] = {
        "pushed_at": datetime.now(timezone.utc).isoformat(),
        "target_url": target_url,
        "file_count": len(files),
        "by_prefix": by_prefix,
        "idempotency_key": push_id,
        "conflict_policy": req.conflict_policy,
        "include_deleted": req.include_deleted,
    }
    committed_files = []
    state_path = _write_replication_state(settings.repo_root, state)
    if gm.commit_file(state_path, "replication: update push state"):
        committed_files.append(REPLICATION_STATE_REL)

    _audit(
        settings,
        auth,
        "replication_push",
        {
            "target_url": target_url,
            "file_count": len(files),
            "idempotency_key": push_id,
            "conflict_policy": req.conflict_policy,
            "include_deleted": req.include_deleted,
        },
    )
    return {
        "ok": True,
        "dry_run": False,
        "idempotency_key": push_id,
        "target_url": target_url,
        "file_count": len(files),
        "by_prefix": by_prefix,
        "remote": remote_payload,
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }



@app.post("/v1/backup/create")
def backup_create(req: BackupCreateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    _enforce_rate_limit(settings, auth, "backup_create")
    _enforce_payload_limit(settings, req.model_dump(), "backup_create")
    auth.require("admin:peers")

    allowed = set(REPLICATION_ALLOWED_PREFIXES) | {"config", "logs", "peers"}
    include = []
    for raw in req.include_prefixes:
        rel = str(raw or "").strip().strip("/")
        if not rel:
            continue
        top = Path(rel).parts[0] if Path(rel).parts else ""
        if top in allowed:
            include.append(rel)
    if not include:
        include = ["memory", "messages", "tasks", "patches", "runs", "projects", "essays", "journal", "snapshots", "peers", "config", "logs"]

    now = datetime.now(timezone.utc)
    backup_id = f"backup_{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    backup_rel = f"{BACKUPS_DIR_REL}/{backup_id}.tar.gz"
    manifest_rel = f"{BACKUPS_DIR_REL}/{backup_id}.json"
    backup_path = safe_path(settings.repo_root, backup_rel)
    manifest_path = safe_path(settings.repo_root, manifest_rel)
    backup_path.parent.mkdir(parents=True, exist_ok=True)

    included_paths: list[str] = []
    with tarfile.open(backup_path, mode="w:gz") as tf:
        for prefix in include:
            p = safe_path(settings.repo_root, prefix)
            if not p.exists():
                continue
            tf.add(p, arcname=prefix)
            included_paths.append(prefix)

    manifest_payload = {
        "schema_version": "1.0",
        "backup_id": backup_id,
        "created_at": now.isoformat(),
        "created_by": auth.peer_id,
        "include_prefixes": included_paths,
        "note": req.note,
        "contract_version": settings.contract_version,
    }
    write_text_file(manifest_path, json.dumps(manifest_payload, ensure_ascii=False, indent=2))

    committed_files = []
    if gm.commit_file(backup_path, f"backup: create {backup_id}"):
        committed_files.append(backup_rel)
    if gm.commit_file(manifest_path, f"backup: manifest {backup_id}"):
        committed_files.append(manifest_rel)

    _audit(settings, auth, "backup_create", {"backup_id": backup_id, "include_prefixes": included_paths})
    return {
        "ok": True,
        "backup_id": backup_id,
        "backup_path": backup_rel,
        "manifest_path": manifest_rel,
        "committed_files": committed_files,
        "latest_commit": gm.latest_commit(),
    }


@app.post("/v1/backup/restore-test")
def backup_restore_test(req: BackupRestoreTestRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    _enforce_rate_limit(settings, auth, "backup_restore_test")
    _enforce_payload_limit(settings, req.model_dump(), "backup_restore_test")
    auth.require("admin:peers")

    rel = str(req.backup_path or "").strip()
    if not rel:
        raise HTTPException(status_code=400, detail="backup_path is required")
    if Path(rel).is_absolute():
        raise HTTPException(status_code=400, detail="backup_path must be repo-relative")
    if not rel.startswith(f"{BACKUPS_DIR_REL}/"):
        raise HTTPException(status_code=400, detail="backup_path must be under backups/")

    backup_path = safe_path(settings.repo_root, rel)
    if not backup_path.exists() or not backup_path.is_file():
        raise HTTPException(status_code=404, detail="Backup file not found")

    extracted_files = 0
    extracted_prefixes: set[str] = set()
    with tempfile.TemporaryDirectory() as td:
        restore_root = Path(td) / "restore"
        restore_root.mkdir(parents=True, exist_ok=True)
        try:
            with tarfile.open(backup_path, mode="r:gz") as tf:
                members = tf.getmembers()
                restore_root_resolved = restore_root.resolve()
                for m in members:
                    if m.issym() or m.islnk():
                        raise HTTPException(status_code=400, detail=f"Invalid backup archive: symbolic links are not allowed ({m.name})")
                    target = (restore_root / m.name).resolve()
                    if target != restore_root_resolved and restore_root_resolved not in target.parents:
                        raise HTTPException(status_code=400, detail=f"Invalid backup archive: unsafe path ({m.name})")
                tf.extractall(path=restore_root, filter="data")
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid backup archive: {e}") from e

        for m in members:
            if not m.isfile():
                continue
            extracted_files += 1
            top = Path(m.name).parts[0] if Path(m.name).parts else ""
            if top:
                extracted_prefixes.add(top)

        index_validation = None
        if req.verify_index_rebuild:
            try:
                payload = rebuild_index(restore_root)
                index_validation = {"ok": True, "file_count": int(payload.get("file_count") or 0)}
            except Exception as e:
                index_validation = {"ok": False, "error": str(e)}

    ok = extracted_files > 0 and (index_validation is None or bool(index_validation.get("ok")))
    _audit(settings, auth, "backup_restore_test", {"backup_path": rel, "ok": ok, "extracted_files": extracted_files})
    return {
        "ok": ok,
        "backup_path": rel,
        "extracted_files": extracted_files,
        "extracted_prefixes": sorted(extracted_prefixes),
        "index_validation": index_validation,
    }


def _load_access_stats(repo_root: Path) -> dict[str, dict]:
    """Very small access stats from audit log for compaction heuristics."""
    out: dict[str, dict] = {}
    p = repo_root / "logs" / "api_audit.jsonl"
    if not p.exists():
        return out
    now = datetime.now(timezone.utc)
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines()[-5000:]:
        try:
            row = json.loads(line)
        except Exception:
            continue
        if row.get("event") not in {"read", "messages_inbox", "search", "context_retrieve"}:
            continue
        detail = row.get("detail") or {}
        path = detail.get("path")
        if not path:
            continue
        stat = out.setdefault(path, {"access_count": 0, "last_access_at": None})
        stat["access_count"] += 1
        ts = row.get("ts")
        if ts and (stat["last_access_at"] is None or ts > stat["last_access_at"]):
            stat["last_access_at"] = ts
    return out


def _memory_class_for_path(rel: str) -> str:
    if rel.startswith("memory/core/"):
        return "core"
    if rel.startswith("memory/summaries/") or rel.startswith("messages/threads/") or rel.startswith("projects/"):
        return "durable" if rel.startswith("memory/summaries/") else "working"
    if rel.startswith("journal/") or rel.startswith("messages/inbox/") or rel.startswith("messages/outbox/") or rel.startswith("logs/"):
        return "ephemeral"
    if rel.startswith("memory/episodic/"):
        return "ephemeral"
    return "working"


def _parse_iso(sv: str | None):
    if not sv:
        return None
    try:
        return datetime.fromisoformat(str(sv).replace("Z", "+00:00"))
    except Exception:
        return None


def _candidate_policy(repo_root: Path, path: Path, access_stats: dict[str, dict]) -> dict | None:
    rel = str(path.relative_to(repo_root))
    if rel.startswith("index/") or ".git" in path.parts:
        return None
    try:
        st = path.stat()
    except Exception:
        return None
    now = datetime.now(timezone.utc)
    age_days = max(0.0, (now - datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)).total_seconds() / 86400.0)
    size_bytes = int(st.st_size)
    ns = Path(rel).parts[0] if Path(rel).parts else ""
    mem_class = _memory_class_for_path(rel)
    importance = 0.0
    snippet = ""
    text = ""
    if path.suffix.lower() in {".md", ".json", ".jsonl", ".txt"}:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
            snippet = " ".join(text.split())[:240]
        except Exception:
            text = ""
        if path.suffix.lower() == ".md":
            m = re.match(r"^---\n(.*?)\n---\n", text, re.DOTALL)
            if m:
                for line in m.group(1).splitlines():
                    if line.strip().startswith("importance:"):
                        try:
                            importance = float(line.split(":",1)[1].strip())
                        except Exception:
                            pass
    a = access_stats.get(rel, {})
    access_count = int(a.get("access_count") or 0)
    last_access_dt = _parse_iso(a.get("last_access_at"))
    last_access_days = 9999.0 if not last_access_dt else max(0.0, (now - last_access_dt).total_seconds() / 86400.0)

    type_weight = {"ephemeral": 1.0, "working": 0.35, "durable": 0.12, "core": -5.0}.get(mem_class, 0.2)
    age_pressure = min(1.5, age_days / 14.0)
    size_pressure = min(1.0, math.log10(max(10, size_bytes)) / 8.0)
    recency_relief = 0.9 if last_access_days < 3 else (0.35 if last_access_days < 14 else 0.0)
    frequency_relief = min(1.0, access_count / 12.0) * 0.75
    importance_relief = min(1.0, max(0.0, importance)) * 1.2
    active_link_relief = 0.6 if rel.startswith("messages/threads/") or rel.startswith("projects/") else 0.0

    candidate_score = round(type_weight + age_pressure + size_pressure - recency_relief - frequency_relief - importance_relief - active_link_relief, 4)

    promote_signals = []
    low = (text or "").lower()
    for kw in ["identity", "relationship", "trusted", "values", "preference", "decision"]:
        if kw in low:
            promote_signals.append(kw)
    if access_count >= 5:
        promote_signals.append("reused")
    if importance >= 0.7:
        promote_signals.append("high_importance")
    if mem_class in {"core"}:
        promote_signals.append("core_namespace")

    return {
        "path": rel,
        "namespace": ns,
        "memory_class": mem_class,
        "age_days": round(age_days, 2),
        "size_bytes": size_bytes,
        "importance": importance if importance else None,
        "access_count": access_count,
        "last_access_days": None if last_access_days >= 9999 else round(last_access_days, 2),
        "candidate_score": candidate_score,
        "promote_signals": sorted(set(promote_signals)),
        "snippet": snippet,
    }


@app.post("/v1/compact/run")
def compact_run(req: CompactRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    auth.require("compact:trigger")
    auth.require_write_path("memory/summaries/weekly/x.md")

    now = datetime.now(timezone.utc)
    report_id = f"compact_{now.strftime('%Y%m%dT%H%M%SZ')}"
    source_rel = req.source_path or "(policy-scan)"

    access_stats = _load_access_stats(settings.repo_root)
    candidates = []
    for p in settings.repo_root.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in {".md", ".json", ".jsonl", ".txt"}:
            continue
        c = _candidate_policy(settings.repo_root, p, access_stats)
        if c:
            candidates.append(c)

    # class-aware categorization (compaction is not deletion; this only proposes actions)
    summarize_now = []
    archive_after_summary = []
    promote_to_core_candidates = []
    keep_hot = []
    review_manually = []

    for c in sorted(candidates, key=lambda x: (-float(x["candidate_score"]), x["path"])):
        cls = c["memory_class"]
        score = float(c["candidate_score"])
        if cls == "core":
            keep_hot.append(c)
            continue
        if c.get("promote_signals") and cls in {"working", "durable"}:
            promote_to_core_candidates.append(c)
            if score > 0.4:
                summarize_now.append(c)
            else:
                keep_hot.append(c)
            continue
        if cls == "ephemeral":
            if score >= 0.4:
                summarize_now.append(c)
                if score >= 0.9:
                    archive_after_summary.append(c)
            else:
                keep_hot.append(c)
        elif cls == "working":
            if score >= 0.8:
                summarize_now.append(c)
            elif score >= 0.4:
                review_manually.append(c)
            else:
                keep_hot.append(c)
        elif cls == "durable":
            if score >= 1.0:
                review_manually.append(c)
            else:
                keep_hot.append(c)
        else:
            review_manually.append(c)

    summary_paths = [x["path"] for x in summarize_now[:20]]
    promote_paths = [x["path"] for x in promote_to_core_candidates[:10]]
    archive_paths = [x["path"] for x in archive_after_summary[:20]]
    keep_hot_paths = [x["path"] for x in keep_hot[:20]]

    report_md_rel = f"memory/summaries/weekly/{report_id}.md"
    report_json_rel = f"memory/summaries/weekly/{report_id}.json"
    report_path = safe_path(settings.repo_root, report_md_rel)
    report_json_path = safe_path(settings.repo_root, report_json_rel)
    body = f"""---
id: {report_id}
type: compaction_report
created_at: {now.isoformat()}
source: {source_rel}
---

# Compaction Report

This endpoint is an **orchestrator/planner**, not an LLM summarizer. It proposes candidates and categories.

## Policy (class-aware decay + promotion)
- Inputs: age, size, namespace/class, declared importance, access count, access recency
- Classes: ephemeral / working / durable / core
- Core is kept hot; durable is rarely compacted; ephemeral decays fastest
- Promotion candidates can *increase* in importance over time (identity/relationship/decision facts)

## Summary counts
- Candidates scanned: {len(candidates)}
- summarize_now: {len(summarize_now)}
- archive_after_summary: {len(archive_after_summary)}
- promote_to_core_candidates: {len(promote_to_core_candidates)}
- keep_hot: {len(keep_hot)}
- review_manually: {len(review_manually)}

## Summarize now (top)
{chr(10).join(f"- `{p}`" for p in summary_paths) if summary_paths else "- None"}

## Promote to core candidates (top)
{chr(10).join(f"- `{p}`" for p in promote_paths) if promote_paths else "- None"}

## Archive after summary (top)
{chr(10).join(f"- `{p}`" for p in archive_paths) if archive_paths else "- None"}

## Keep hot (sample)
{chr(10).join(f"- `{p}`" for p in keep_hot_paths) if keep_hot_paths else "- None"}

## Operator note
{req.note or 'N/A'}
"""

    payload = {
        "id": report_id,
        "type": "compaction_report",
        "created_at": now.isoformat(),
        "source": source_rel,
        "planner_only": True,
        "compaction_semantics": {
            "summarizes_content": False,
            "expected_ai_action": "Read candidate lists, generate summaries, then POST /v1/write or /v1/append",
        },
        "indexing_note": {
            "incremental_index_default": "working_tree",
            "can_include_uncommitted_changes": True,
            "future_mode": ["working_tree", "head_commit"],
        },
        "policy": {
            "inputs": ["age_days", "size_bytes", "memory_class", "importance", "access_count", "last_access_days"],
            "classes": ["ephemeral", "working", "durable", "core"],
            "decay": {
                "ephemeral": "fast",
                "working": "slow-while-active, faster-after-inactive",
                "durable": "very_slow",
                "core": "no_age_decay_retrieval_only",
            },
            "promotion_principle": "some memories gain importance over time via reuse/identity/relationship signals",
        },
        "summary_counts": {
            "candidates_scanned": len(candidates),
            "summarize_now": len(summarize_now),
            "archive_after_summary": len(archive_after_summary),
            "promote_to_core_candidates": len(promote_to_core_candidates),
            "keep_hot": len(keep_hot),
            "review_manually": len(review_manually),
        },
        "actions": {
            "summarize_now": summarize_now[:20],
            "archive_after_summary": archive_after_summary[:20],
            "promote_to_core_candidates": promote_to_core_candidates[:15],
            "keep_hot": keep_hot[:20],
            "review_manually": review_manually[:20],
        },
        "operator_note": req.note,
    }

    write_text_file(report_path, body)
    write_text_file(report_json_path, json.dumps(payload, ensure_ascii=False, indent=2))

    committed = []
    for rel in [report_md_rel, report_json_rel]:
        p = safe_path(settings.repo_root, rel)
        if gm.commit_file(p, f"memory: add compaction {report_id}"):
            committed.append(rel)

    _audit(settings, auth, "compact_run", {"report_id": report_id, "source": source_rel, "candidates": len(candidates)})
    return {"ok": True, "report_id": report_id, "paths": [report_md_rel, report_json_rel], "committed_files": committed, "latest_commit": gm.latest_commit(), "planner_only": True, "summary_counts": payload["summary_counts"]}


@app.exception_handler(Exception)
async def unhandled_exception_handler(_, exc: Exception):
    return JSONResponse(status_code=500, content={"ok": False, "error": type(exc).__name__, "detail": str(exc)})
