from __future__ import annotations

import hashlib
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
from .messages import (
    DELIVERY_STATE_REL,
    delivery_record_view,
    effective_delivery_status,
    load_delivery_state,
    messages_ack_service,
    messages_inbox_service,
    messages_pending_service,
    messages_send_service,
    messages_thread_service,
    relay_forward_service,
    replay_messages_service,
)
from .storage import StorageError, append_jsonl, read_text_file, safe_path, write_text_file
from .security import (
    GOVERNANCE_POLICY_REL,
    NONCE_INDEX_REL,
    SECURITY_KEYS_REL,
    TOKEN_CONFIG_REL,
    governance_policy_service,
    load_token_config,
    load_security_keys,
    messages_verify_service,
    security_keys_rotate_service,
    security_tokens_issue_service,
    security_tokens_list_service,
    security_tokens_revoke_service,
    security_tokens_rotate_service,
    verify_signed_payload_service,
)
from .tasks import (
    RUN_CHECKS_DIR_REL,
    code_checks_run_service,
    code_merge_service,
    code_patch_propose_service,
    docs_patch_apply_service,
    docs_patch_propose_service,
    load_check_artifacts,
    tasks_create_service,
    tasks_query_service,
    tasks_update_service,
)


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
    return governance_policy_service(repo_root=settings.repo_root)
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
        load_token_config=load_token_config,
        parse_iso=_parse_iso,
        load_security_keys=load_security_keys,
        load_delivery_state=load_delivery_state,
        effective_delivery_status=lambda record, now: effective_delivery_status(record, now, parse_iso=_parse_iso),
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


@app.post("/v1/tasks")
def tasks_create(req: TaskCreateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return tasks_create_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.patch("/v1/tasks/{task_id}")
def tasks_update(task_id: str, req: TaskUpdateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return tasks_update_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        task_id=task_id,
        req=req,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.get("/v1/tasks/query")
def tasks_query(
    status: str | None = Query(default=None),
    owner_peer: str | None = Query(default=None),
    collaborator: str | None = Query(default=None),
    thread_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    auth: AuthContext = Depends(require_auth),
) -> dict:
    settings, _ = _services()
    return tasks_query_service(
        repo_root=settings.repo_root,
        auth=auth,
        status=status if isinstance(status, str) else None,
        owner_peer=owner_peer if isinstance(owner_peer, str) else None,
        collaborator=collaborator if isinstance(collaborator, str) else None,
        thread_id=thread_id if isinstance(thread_id, str) else None,
        limit=limit if isinstance(limit, int) else 100,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/docs/patch/propose")
def docs_patch_propose(req: PatchProposeRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return docs_patch_propose_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        run_git=_run_git,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/code/patch/propose")
def code_patch_propose(req: PatchProposeRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return code_patch_propose_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        run_git=_run_git,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/docs/patch/apply")
def docs_patch_apply(req: PatchApplyRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return docs_patch_apply_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        run_git=_run_git,
        read_commit_file=_read_commit_file,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/code/checks/run")
def code_checks_run(req: CodeCheckRunRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return code_checks_run_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        run_git=_run_git,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/code/merge")
def code_merge(req: CodeMergeRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return code_merge_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        run_git=_run_git,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


DELIVERY_STATE_REL = "messages/state/delivery_index.json"
REPLICATION_STATE_REL = "peers/replication_state.json"
REPLICATION_ALLOWED_PREFIXES = {"journal", "essays", "projects", "memory", "messages", "tasks", "patches", "runs", "snapshots", "archive"}

RATE_LIMIT_STATE_REL = "logs/rate_limit_state.json"
TRUST_POLICIES_REL = "peers/trust_policies.json"
REPLICATION_TOMBSTONES_REL = "peers/replication_tombstones.json"
BACKUPS_DIR_REL = "backups"
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

def _canonical_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _sha256_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


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
    return messages_send_service(
        settings=settings,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        verify_signed_payload=verify_signed_payload_service,
        verification_failure_count=_verification_failure_count,
        record_verification_failure=_record_verification_failure,
        parse_iso=_parse_iso,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/messages/ack")
def messages_ack(req: MessageAckRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return messages_ack_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        parse_iso=_parse_iso,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.get("/v1/messages/pending")
def messages_pending(
    recipient: str | None = Query(default=None),
    status: str | None = Query(default=None),
    include_terminal: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=500),
    auth: AuthContext = Depends(require_auth),
) -> dict:
    settings, _ = _services()
    return messages_pending_service(
        repo_root=settings.repo_root,
        auth=auth,
        recipient=recipient if isinstance(recipient, str) else None,
        status=status if isinstance(status, str) else None,
        include_terminal=include_terminal if isinstance(include_terminal, bool) else False,
        limit=limit if isinstance(limit, int) else 50,
        parse_iso=_parse_iso,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.get("/v1/messages/inbox")
def messages_inbox(recipient: str = Query(...), limit: int = Query(default=20, ge=1, le=200), auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return messages_inbox_service(
        repo_root=settings.repo_root,
        auth=auth,
        recipient=recipient,
        limit=limit,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.get("/v1/messages/thread")
def messages_thread(thread_id: str = Query(...), limit: int = Query(default=100, ge=1, le=1000), auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return messages_thread_service(repo_root=settings.repo_root, auth=auth, thread_id=thread_id, limit=limit)


@app.post("/v1/relay/forward")
def relay_forward(req: RelayForwardRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return relay_forward_service(
        settings=settings,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        verify_signed_payload=verify_signed_payload_service,
        verification_failure_count=_verification_failure_count,
        record_verification_failure=_record_verification_failure,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.get("/v1/security/tokens")
def security_tokens_list(
    peer_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    include_inactive: bool = Query(default=False),
    auth: AuthContext = Depends(require_auth),
) -> dict:
    settings, _ = _services()
    return security_tokens_list_service(
        repo_root=settings.repo_root,
        auth=auth,
        peer_id=peer_id if isinstance(peer_id, str) else None,
        status=status if isinstance(status, str) else None,
        include_inactive=include_inactive if isinstance(include_inactive, bool) else False,
        enforce_rate_limit=_enforce_rate_limit,
        settings=settings,
    )


@app.post("/v1/security/tokens/issue")
def security_tokens_issue(req: SecurityTokenIssueRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return security_tokens_issue_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
        refresh_settings=lambda: get_settings(force_reload=True),
    )


@app.post("/v1/security/tokens/revoke")
def security_tokens_revoke(req: SecurityTokenRevokeRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return security_tokens_revoke_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
        refresh_settings=lambda: get_settings(force_reload=True),
    )



@app.post("/v1/security/tokens/rotate")
def security_tokens_rotate(req: SecurityTokenRotateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return security_tokens_rotate_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
        refresh_settings=lambda: get_settings(force_reload=True),
    )
@app.post("/v1/security/keys/rotate")
def security_keys_rotate(req: SecurityKeysRotateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return security_keys_rotate_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/messages/verify")
def messages_verify(req: MessageVerifyRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return messages_verify_service(
        settings=settings,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        verification_failure_count=_verification_failure_count,
        record_verification_failure=_record_verification_failure,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.get("/v1/metrics")
def metrics(auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    auth.require("read:index")
    auth.require_read_path(DELIVERY_STATE_REL)
    auth.require_read_path("logs/api_audit.jsonl")
    now = datetime.now(timezone.utc)

    state = load_delivery_state(settings.repo_root)
    delivery_summary: dict[str, int] = {}
    by_recipient: dict[str, dict[str, int]] = {}
    for row in state.get("records", {}).values():
        if not isinstance(row, dict):
            continue
        view = delivery_record_view(row, now, parse_iso=_parse_iso)
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

    check_artifacts = load_check_artifacts(settings.repo_root)
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
    return replay_messages_service(
        settings=settings,
        gm=gm,
        auth=auth,
        req=req,
        parse_iso=_parse_iso,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


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
