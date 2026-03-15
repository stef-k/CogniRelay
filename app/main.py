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
from .maintenance import (
    BACKUPS_DIR_REL,
    REPLICATION_ALLOWED_PREFIXES,
    REPLICATION_STATE_REL,
    REPLICATION_TOMBSTONES_REL,
    backup_create_service,
    backup_restore_test_service,
    compact_run_service,
    iter_replication_files,
    load_replication_state,
    metrics_service,
    replication_pull_service,
    replication_push_service,
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
RATE_LIMIT_STATE_REL = "logs/rate_limit_state.json"
TRUST_POLICIES_REL = "peers/trust_policies.json"
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


def _canonical_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _sha256_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


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
    return metrics_service(
        settings=settings,
        auth=auth,
        load_delivery_state=load_delivery_state,
        delivery_record_view=lambda row, now: delivery_record_view(row, now, parse_iso=_parse_iso),
        load_check_artifacts=load_check_artifacts,
        load_rate_limit_state=_load_rate_limit_state,
        parse_iso=_parse_iso,
    )


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
    return replication_pull_service(
        settings=settings,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        parse_iso=_parse_iso,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/replication/push")
def replication_push(req: ReplicationPushRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return replication_push_service(
        settings=settings,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        load_peers_registry=load_peers_registry,
        urlopen_fn=urlopen,
        url_request_factory=UrlRequest,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )



@app.post("/v1/backup/create")
def backup_create(req: BackupCreateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return backup_create_service(
        settings=settings,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.post("/v1/backup/restore-test")
def backup_restore_test(req: BackupRestoreTestRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, _ = _services()
    return backup_restore_test_service(
        settings=settings,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        rebuild_index=rebuild_index,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


def _parse_iso(sv: str | None):
    if not sv:
        return None
    try:
        return datetime.fromisoformat(str(sv).replace("Z", "+00:00"))
    except Exception:
        return None


@app.post("/v1/compact/run")
def compact_run(req: CompactRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    settings, gm = _services()
    return compact_run_service(
        settings=settings,
        gm=gm,
        auth=auth,
        req=req,
        parse_iso=_parse_iso,
        audit=lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail),
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(_, exc: Exception):
    return JSONResponse(status_code=500, content={"ok": False, "error": type(exc).__name__, "detail": str(exc)})
