"""FastAPI route composition for the CogniRelay service."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, Any, Callable, Literal

from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query, Request as FastAPIRequest, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from .auth import AuthContext, require_auth
from .git_locking import GitLockInfrastructureError, GitLockTimeout
from .lifecycle_warnings import make_error_detail
from .timestamps import parse_iso as _parse_iso
from .artifact_lifecycle.service import artifact_history_cold_rehydrate_service, artifact_history_cold_store_service
from .registry_lifecycle.service import registry_history_cold_rehydrate_service, registry_history_cold_store_service
from .segment_history.service import (
    segment_history_cold_rehydrate_service,
    segment_history_cold_store_service,
    segment_history_maintenance_service,
)
from .context import (
    append_record_service,
    context_retrieve_service,
    context_snapshot_create_service,
    context_snapshot_get_service,
    index_rebuild_incremental_service,
    index_rebuild_service,
    index_status_service,
    read_file_service,
    recent_list_service,
    search_service,
    write_file_service,
)
from .continuity import (
    continuity_archive_service,
    continuity_cold_rehydrate_service,
    continuity_cold_store_service,
    continuity_compare_service,
    continuity_delete_service,
    continuity_lifecycle_service,
    continuity_list_service,
    continuity_patch_service,
    continuity_read_service,
    continuity_retention_apply_service,
    continuity_retention_plan_service,
    continuity_refresh_plan_service,
    continuity_revalidate_service,
    continuity_upsert_service,
)
from .coordination import (
    handoff_consume_service,
    handoff_create_service,
    handoff_read_service,
    handoffs_query_service,
    reconciliation_open_service,
    reconciliation_query_service,
    reconciliation_read_service,
    reconciliation_resolve_service,
    shared_create_service,
    shared_query_service,
    shared_read_service,
    shared_update_service,
)
from .config import get_settings
from .coordination.locking import purge_stale_lockfiles
from .discovery import (
    capabilities_payload,
    capabilities_v1_payload,
    contracts_payload,
    discovery_payload,
    discovery_tools_payload,
    discovery_workflows_payload,
    health_payload,
    invoke_tool_by_name,
    manifest_payload,
    rpc_error_payload,
    tool_catalog,
    well_known_cognirelay_payload,
    well_known_mcp_payload,
    workflow_catalog,
)
from .git_manager import GitManager
from .indexer import rebuild_index
from .models import (
    AppendRequest,
    ArtifactHistoryColdRehydrateRequest,
    ArtifactHistoryColdStoreRequest,
    CodeCheckRunRequest,
    CodeMergeRequest,
    CompactRequest,
    CoordinationHandoffConsumeRequest,
    CoordinationHandoffCreateRequest,
    CoordinationHandoffQueryRequest,
    CoordinationReconciliationOpenRequest,
    CoordinationReconciliationQueryRequest,
    CoordinationReconciliationResolveRequest,
    CoordinationSharedCreateRequest,
    CoordinationSharedQueryRequest,
    CoordinationSharedUpdateRequest,
    ContinuityArchiveRequest,
    ContinuityColdRehydrateRequest,
    ContinuityColdStoreRequest,
    ContinuityCompareRequest,
    ContinuityDeleteRequest,
    ContinuityLifecycleRequest,
    ContinuityListRequest,
    ContinuityPatchRequest,
    ContinuityReadRequest,
    ContinuityRetentionApplyRequest,
    ContinuityRetentionPlanRequest,
    ContinuityRefreshPlanRequest,
    ContinuityRevalidateRequest,
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
    RegistryHistoryColdRehydrateRequest,
    RegistryHistoryColdStoreRequest,
    SegmentHistoryColdRehydrateRequest,
    SegmentHistoryColdStoreRequest,
    SegmentHistoryMaintenanceRequest,
    RelayForwardRequest,
    SecurityKeysRotateRequest,
    SearchRequest,
    TaskCreateRequest,
    TaskUpdateRequest,
    WriteRequest,
)
from .ops import ops_catalog_service, ops_run_service, ops_schedule_export_service, ops_status_service
from .peers import TRUST_POLICIES_REL, load_peers_registry, peer_manifest_service, peers_list_service, peers_register_service, peers_trust_transition_service
from .messages import (
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
    backup_create_service,
    backup_restore_test_service,
    compact_run_service,
    metrics_service,
    replication_pull_service,
    replication_push_service,
)
from .runtime import (
    audit_event as _audit,
    enforce_payload_limit as _enforce_payload_limit,
    enforce_rate_limit as _enforce_rate_limit,
    handle_mcp_request as _handle_mcp_rpc_request,
    load_rate_limit_state as _load_rate_limit_state,
    read_commit_file as _read_commit_file,
    record_verification_failure as _record_verification_failure,
    resolve_auth_context as _resolve_auth_context,
    run_git as _run_git,
    scope_for_path as _scope_for_path,
    verification_failure_count as _verification_failure_count,
)
from .security import (
    governance_policy_service,
    load_security_keys,
    load_token_config,
    messages_verify_service,
    security_keys_rotate_service,
    security_tokens_issue_service,
    security_tokens_list_service,
    security_tokens_revoke_service,
    security_tokens_rotate_service,
    verify_signed_payload_service,
)
from .tasks import (
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
from .help import (
    help_error_payload,
    help_hooks_payload,
    help_root_payload,
    help_tool_payload,
    help_topic_payload,
    is_forbidden_help_alias_path,
)
from .ui import build_ui_router

_log = logging.getLogger(__name__)


def _rebuild_coordination_index(settings: Any) -> None:
    """Build the SQLite sidecar index for O(log N) coordination queries.

    Each rebuild is wrapped individually so that a failure in one artifact
    type does not prevent the others from being indexed.  A partially built
    index is still set as the singleton — the query services fall back to
    full scan for any type whose rebuild failed.
    """
    from app.coordination.query_index import CoordinationQueryIndex, set_coordination_index

    db_path = settings.repo_root / "memory" / "coordination" / ".query_index.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    idx = CoordinationQueryIndex(db_path)

    for label, method, directory in [
        ("handoffs", idx.rebuild_handoffs, settings.repo_root / "memory" / "coordination" / "handoffs"),
        ("shared", idx.rebuild_shared, settings.repo_root / "memory" / "coordination" / "shared"),
        ("reconciliations", idx.rebuild_reconciliations, settings.repo_root / "memory" / "coordination" / "reconciliations"),
    ]:
        try:
            method(directory)
        except Exception:
            _log.error("Coordination index rebuild failed for %s — queries will use fallback scan", label, exc_info=True)

    # Always register the index, even if partially rebuilt.
    set_coordination_index(idx)


@asynccontextmanager
async def _lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    """Run startup housekeeping before the app begins serving requests."""
    import asyncio

    settings = get_settings()
    purge_stale_lockfiles(settings.repo_root / ".locks")

    # Run synchronous index rebuild off the event loop to avoid blocking startup.
    await asyncio.to_thread(_rebuild_coordination_index, settings)

    yield

    # Shutdown: close the SQLite connection to flush WAL checkpoint.
    from app.coordination.query_index import get_coordination_index, set_coordination_index

    idx = get_coordination_index()
    if idx is not None:
        idx.close()
        set_coordination_index(None)


app = FastAPI(title="CogniRelay", version="1.3.1", lifespan=_lifespan)

if get_settings().ui_enabled:
    app.include_router(build_ui_router(app_version=app.version))


@app.middleware("http")
async def _cache_raw_json_body(request: FastAPIRequest, call_next):  # type: ignore[no-untyped-def]
    """Reject forbidden help aliases and cache preserve-mode upsert bodies.

    Returns a direct 404 for forbidden trailing-slash help aliases. For
    POST /v1/continuity/upsert, caches the raw JSON body only when
    ``merge_mode`` is ``"preserve"``.
    """
    if is_forbidden_help_alias_path(request.url.path):
        return JSONResponse(status_code=404, content={"detail": "Not Found"})

    if request.url.path.endswith("/v1/continuity/upsert") and request.method == "POST":
        body_bytes = await request.body()
        try:
            raw = json.loads(body_bytes)
            if isinstance(raw, dict) and raw.get("merge_mode") == "preserve":
                request.state.raw_json_body = raw
        except (json.JSONDecodeError, UnicodeDecodeError):
            _log.warning("Failed to parse raw JSON body for preserve-mode upsert")  # let downstream validation handle malformed JSON
    return await call_next(request)


def _services() -> tuple:
    """Load settings and ensure the repository-backed git manager is ready."""
    settings = get_settings()
    gm = GitManager(
        repo_root=settings.repo_root,
        author_name=settings.git_author_name,
        author_email=settings.git_author_email,
    )
    gm.ensure_repo(settings.auto_init_git)
    return settings, gm


def _make_audit(settings: Any, gm: Any) -> Callable:
    """Build an audit callback that forwards the git manager for write-time rollover."""
    return lambda auth_ctx, event, detail: _audit(settings, auth_ctx, event, detail, gm=gm)


def _make_segment_audit(settings: Any, auth: Any, gm: Any) -> Callable:
    """Build a 2-arg audit callback for segment_history services.

    Segment-history services call ``_emit_audit(audit, event, detail)``
    which invokes ``audit(event, detail)`` — a 2-arg form that bakes in
    the ``auth`` context from the enclosing ops_run handler.
    """
    return lambda event, detail: _audit(settings, auth, event, detail, gm=gm)


def _schema_for_model(model_cls: Any) -> dict[str, Any]:
    """Return the JSON schema for a Pydantic model class."""
    return model_cls.model_json_schema()


def _tool_catalog() -> list[dict[str, Any]]:
    """Build the current tool catalog from the registered models."""
    return tool_catalog(_schema_for_model)


def _workflow_catalog() -> list[dict[str, Any]]:
    """Build the current workflow catalog."""
    return workflow_catalog()


@app.get("/v1/discovery")
def discovery() -> dict:
    """Return the top-level machine-readable discovery payload."""
    settings = get_settings()
    tools = _tool_catalog()
    workflows = _workflow_catalog()
    return discovery_payload(settings.contract_version, tools=tools, workflows=workflows)


@app.get("/v1/discovery/tools")
def discovery_tools() -> dict:
    """Return the machine-readable tool catalog payload."""
    settings = get_settings()
    tools = _tool_catalog()
    return discovery_tools_payload(settings.contract_version, tools=tools)


@app.get("/v1/discovery/workflows")
def discovery_workflows() -> dict:
    """Return the machine-readable workflow catalog payload."""
    workflows = _workflow_catalog()
    return discovery_workflows_payload(workflows=workflows)


@app.get("/.well-known/cognirelay.json")
def well_known_cognirelay() -> dict:
    """Serve the well-known CogniRelay discovery document."""
    return well_known_cognirelay_payload(discovery())


@app.get("/.well-known/mcp.json")
def well_known_mcp() -> dict:
    """Serve the well-known MCP-compatible descriptor."""
    settings = get_settings()
    return well_known_mcp_payload(settings.contract_version)


def _invoke_tool_by_name(name: str, arguments: dict[str, Any], auth: AuthContext | None) -> dict[str, Any]:
    """Compose main-route callbacks into the discovery tool dispatcher."""
    return invoke_tool_by_name(
        name,
        arguments,
        auth,
        health=health,
        capabilities=capabilities,
        capabilities_v1=capabilities_v1,
        manifest=manifest,
        contracts=contracts,
        governance_policy=governance_policy,
        discovery=discovery,
        discovery_tools=discovery_tools,
        discovery_workflows=discovery_workflows,
        write_file=lambda req, auth_ctx: write_file(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        append_record=lambda req, auth_ctx: append_record(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        read_file=lambda path, auth_ctx: read_file(path=path, auth=auth_ctx),  # type: ignore[arg-type]
        index_rebuild=lambda auth_ctx: index_rebuild(auth=auth_ctx),  # type: ignore[arg-type]
        index_rebuild_incremental=lambda auth_ctx: index_rebuild_incremental(auth=auth_ctx),  # type: ignore[arg-type]
        index_status=lambda auth_ctx: index_status(auth=auth_ctx),  # type: ignore[arg-type]
        peers_list=lambda auth_ctx: peers_list(auth=auth_ctx),  # type: ignore[arg-type]
        peers_register=lambda req, auth_ctx: peers_register(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        peers_trust_transition=lambda peer_id, req, auth_ctx: peers_trust_transition(peer_id=peer_id, req=req, auth=auth_ctx),  # type: ignore[arg-type]
        peer_manifest=lambda peer_id, auth_ctx: peer_manifest(peer_id=peer_id, auth=auth_ctx),  # type: ignore[arg-type]
        search=lambda req, auth_ctx: search(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        recent_list=lambda req, auth_ctx: recent_list(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        context_retrieve=lambda req, auth_ctx: context_retrieve(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        continuity_upsert=lambda req, auth_ctx: continuity_upsert(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        continuity_read=lambda req, auth_ctx: continuity_read(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        continuity_compare=lambda req, auth_ctx: continuity_compare(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        continuity_revalidate=lambda req, auth_ctx: continuity_revalidate(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        continuity_refresh_plan=lambda req, auth_ctx: continuity_refresh_plan(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        continuity_retention_plan=lambda req, auth_ctx: continuity_retention_plan(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        continuity_list=lambda req, auth_ctx: continuity_list(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        continuity_archive=lambda req, auth_ctx: continuity_archive(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        continuity_delete=lambda req, auth_ctx: continuity_delete(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        continuity_patch=lambda req, auth_ctx: continuity_patch(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        continuity_lifecycle=lambda req, auth_ctx: continuity_lifecycle(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        handoff_create=lambda req, auth_ctx: coordination_handoff_create(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        handoff_read=lambda handoff_id, auth_ctx: coordination_handoff_read(handoff_id=handoff_id, auth=auth_ctx),  # type: ignore[arg-type]
        handoff_query=lambda req, auth_ctx: coordination_handoffs_query(
            recipient_peer=req.recipient_peer,
            sender_peer=req.sender_peer,
            status=req.status,
            offset=req.offset,
            limit=req.limit,
            auth=auth_ctx,
        ),
        handoff_consume=lambda handoff_id, req, auth_ctx: coordination_handoff_consume(handoff_id=handoff_id, req=req, auth=auth_ctx),  # type: ignore[arg-type]
        shared_create=lambda req, auth_ctx: coordination_shared_create(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        shared_read=lambda shared_id, auth_ctx: coordination_shared_read(shared_id=shared_id, auth=auth_ctx),  # type: ignore[arg-type]
        shared_query=lambda req, auth_ctx: coordination_shared_query(
            owner_peer=req.owner_peer,
            participant_peer=req.participant_peer,
            task_id=req.task_id,
            thread_id=req.thread_id,
            offset=req.offset,
            limit=req.limit,
            auth=auth_ctx,
        ),
        shared_update=lambda shared_id, req, auth_ctx: coordination_shared_update(shared_id=shared_id, req=req, auth=auth_ctx),  # type: ignore[arg-type]
        reconciliation_open=lambda req, auth_ctx: coordination_reconciliation_open(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        reconciliation_read=lambda reconciliation_id, auth_ctx: coordination_reconciliation_read(reconciliation_id=reconciliation_id, auth=auth_ctx),  # type: ignore[arg-type]
        reconciliation_query=lambda req, auth_ctx: coordination_reconciliations_query(
            owner_peer=req.owner_peer,
            claimant_peer=req.claimant_peer,
            status=req.status,
            classification=req.classification,
            task_id=req.task_id,
            thread_id=req.thread_id,
            offset=req.offset,
            limit=req.limit,
            auth=auth_ctx,
        ),
        reconciliation_resolve=lambda reconciliation_id, req, auth_ctx: coordination_reconciliation_resolve(reconciliation_id=reconciliation_id, req=req, auth=auth_ctx),  # type: ignore[arg-type]
        context_snapshot_create=lambda req, auth_ctx: context_snapshot_create(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        context_snapshot_get=lambda snapshot_id, auth_ctx: context_snapshot_get(snapshot_id=snapshot_id, auth=auth_ctx),  # type: ignore[arg-type]
        tasks_create=lambda req, auth_ctx: tasks_create(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        tasks_update=lambda task_id, req, auth_ctx: tasks_update(task_id=task_id, req=req, auth=auth_ctx),  # type: ignore[arg-type]
        tasks_query=tasks_query,
        docs_patch_propose=lambda req, auth_ctx: docs_patch_propose(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        docs_patch_apply=lambda req, auth_ctx: docs_patch_apply(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        code_patch_propose=lambda req, auth_ctx: code_patch_propose(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        code_checks_run=lambda req, auth_ctx: code_checks_run(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        code_merge=lambda req, auth_ctx: code_merge(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        security_tokens_list=security_tokens_list,
        security_tokens_issue=lambda req, auth_ctx: security_tokens_issue(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        security_tokens_revoke=lambda req, auth_ctx: security_tokens_revoke(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        security_tokens_rotate=lambda req, auth_ctx: security_tokens_rotate(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        security_keys_rotate=lambda req, auth_ctx: security_keys_rotate(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        messages_verify=lambda req, auth_ctx: messages_verify(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        metrics=lambda auth_ctx: metrics(auth=auth_ctx),  # type: ignore[arg-type]
        replay_messages=lambda req, auth_ctx: replay_messages(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        replication_pull=lambda req, auth_ctx: replication_pull(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        replication_push=lambda req, auth_ctx: replication_push(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        messages_send=lambda req, auth_ctx: messages_send(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        messages_ack=lambda req, auth_ctx: messages_ack(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        messages_pending=messages_pending,
        messages_inbox=lambda recipient, limit, auth_ctx: messages_inbox(recipient=recipient, limit=limit, auth=auth_ctx),  # type: ignore[arg-type]
        messages_thread=lambda thread_id, limit, auth_ctx: messages_thread(thread_id=thread_id, limit=limit, auth=auth_ctx),  # type: ignore[arg-type]
        relay_forward=lambda req, auth_ctx: relay_forward(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        compact_run=lambda req, auth_ctx: compact_run(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        backup_create=lambda req, auth_ctx: backup_create(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        backup_restore_test=lambda req, auth_ctx: backup_restore_test(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        ops_catalog=lambda auth_ctx: ops_catalog(auth=auth_ctx),  # type: ignore[arg-type]
        ops_status=lambda limit, auth_ctx: ops_status(limit=limit, auth=auth_ctx),  # type: ignore[arg-type]
        ops_run=lambda req, auth_ctx: ops_run(req=req, auth=auth_ctx),  # type: ignore[arg-type]
        ops_schedule_export=lambda format, auth_ctx: ops_schedule_export(format=format, auth=auth_ctx),  # type: ignore[arg-type]
    )


@app.post("/v1/mcp")
def mcp_rpc(
    payload: Any = Body(...),
    authorization: str | None = Header(default=None),
    x_forwarded_for: str | None = Header(default=None, alias="X-Forwarded-For"),
    x_real_ip: str | None = Header(default=None, alias="X-Real-IP"),
    http_request: FastAPIRequest = None,  # type: ignore[assignment]
) -> Any:
    """Handle MCP-compatible JSON-RPC requests over HTTP."""
    # When called directly in unit tests, FastAPI's Header sentinel can appear here.
    if authorization is not None and not isinstance(authorization, str):
        authorization = None
    if isinstance(payload, list):
        if not payload:
            return rpc_error_payload(None, -32600, "Invalid Request: empty batch")
        out = []
        for item in payload:
            result = _handle_mcp_rpc_request(
                item,
                authorization=authorization,
                x_forwarded_for=x_forwarded_for,
                x_real_ip=x_real_ip,
                request=http_request,
                contract_version=get_settings().contract_version,
                tools=_tool_catalog(),
                resolve_auth_context_fn=lambda authz, required, **kwargs: _resolve_auth_context(
                    require_auth,
                    authz,
                    required,
                    **kwargs,
                ),
                invoke_tool_by_name=_invoke_tool_by_name,
            )
            if result is not None:
                out.append(result)
        if not out:
            return Response(status_code=204)
        return out
    result = _handle_mcp_rpc_request(
        payload,
        authorization=authorization,
        x_forwarded_for=x_forwarded_for,
        x_real_ip=x_real_ip,
        request=http_request,
        contract_version=get_settings().contract_version,
        tools=_tool_catalog(),
        resolve_auth_context_fn=lambda authz, required, **kwargs: _resolve_auth_context(
            require_auth,
            authz,
            required,
            **kwargs,
        ),
        invoke_tool_by_name=_invoke_tool_by_name,
    )
    if result is None:
        return Response(status_code=204)
    return result


@app.get("/health")
def health() -> dict:
    """Return service liveness, repo state, and contract metadata."""
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
    """Return the service feature flag payload."""
    return capabilities_payload()


@app.get("/v1/capabilities")
def capabilities_v1() -> dict:
    """Return the versioned, machine-readable v1 feature map."""
    return capabilities_v1_payload()


@app.get("/v1/manifest")
def manifest() -> dict:
    """Machine-first endpoint map for autonomous clients."""
    return manifest_payload(app_version=app.version)


@app.get("/v1/contracts")
def contracts() -> dict:
    """Return contract version metadata and compatibility policy."""
    settings = get_settings()
    return contracts_payload(contract_version=settings.contract_version, tools=_tool_catalog())


@app.get("/v1/help")
def help_root() -> dict:
    """Return the exact machine-facing HTTP help root body for issue #214 slice 1."""
    return help_root_payload()


@app.get("/v1/help/tools/{name}")
def help_tool(name: str) -> Any:
    """Return the exact supported tool-help body or the exact slice-1 validation error."""
    return help_tool_payload(name)


@app.get("/v1/help/topics/{id}")
def help_topic(id: str) -> Any:
    """Return the exact supported topic-help body or the exact slice-1 validation error."""
    return help_topic_payload(id)


@app.get("/v1/help/hooks")
def help_hooks() -> dict:
    """Return the exact hook guidance body for issue #214 slice 1."""
    return help_hooks_payload()


@app.get("/v1/help/errors/{code}")
def help_error(code: str) -> Any:
    """Return the exact supported error-help body or the exact slice-1 validation error."""
    return help_error_payload(code)


@app.get("/v1/governance/policy")
def governance_policy() -> dict:
    """Return the machine-readable governance policy pack."""
    settings, gm = _services()
    return governance_policy_service(repo_root=settings.repo_root)


@app.get("/v1/ops/catalog")
def ops_catalog(auth: AuthContext = Depends(require_auth)) -> dict:
    """Return the host-local operations catalog."""
    settings, gm = _services()
    return ops_catalog_service(settings=settings, auth=auth, audit=_make_audit(settings, gm))


@app.get("/v1/ops/status")
def ops_status(limit: int = Query(default=50, ge=1, le=500), auth: AuthContext = Depends(require_auth)) -> dict:
    """Return recent host-local operations status entries."""
    settings, gm = _services()
    return ops_status_service(
        repo_root=settings.repo_root,
        auth=auth,
        limit=limit,
        audit=_make_audit(settings, gm),
        max_jsonl_read_bytes=settings.max_jsonl_read_bytes,
    )


@app.get("/v1/ops/schedule/export")
def ops_schedule_export(format: str = Query(default="systemd"), auth: AuthContext = Depends(require_auth)) -> dict:
    """Export scheduler snippets for host-local maintenance jobs."""
    settings, gm = _services()
    return ops_schedule_export_service(
        settings=settings,
        auth=auth,
        format=format,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/ops/run")
def ops_run(req: OpsRunRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Execute one host-local maintenance operation."""
    settings, gm = _services()
    return ops_run_service(
        settings=settings,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        audit=_make_audit(settings, gm),
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
        continuity_cold_store=lambda req, auth: continuity_cold_store_service(
            repo_root=settings.repo_root,
            gm=gm,
            auth=auth,
            req=req,
            audit=_make_audit(settings, gm),
        ),
        continuity_cold_store_request_factory=ContinuityColdStoreRequest,
        continuity_cold_rehydrate=lambda req, auth: continuity_cold_rehydrate_service(
            repo_root=settings.repo_root,
            gm=gm,
            auth=auth,
            req=req,
            audit=_make_audit(settings, gm),
        ),
        continuity_cold_rehydrate_request_factory=ContinuityColdRehydrateRequest,
        continuity_retention_apply=lambda req, auth: continuity_retention_apply_service(
            repo_root=settings.repo_root,
            gm=gm,
            auth=auth,
            req=req,
            now=datetime.now(timezone.utc),
            retention_archive_days=settings.continuity_retention_archive_days,
            audit=_make_audit(settings, gm),
        ),
        continuity_retention_apply_request_factory=ContinuityRetentionApplyRequest,
        artifact_history_cold_store=lambda req, auth: artifact_history_cold_store_service(
            repo_root=settings.repo_root,
            gm=gm,
            auth=auth,
            req=req,
            audit=_make_audit(settings, gm),
        ),
        artifact_history_cold_store_request_factory=ArtifactHistoryColdStoreRequest,
        artifact_history_cold_rehydrate=lambda req, auth: artifact_history_cold_rehydrate_service(
            repo_root=settings.repo_root,
            gm=gm,
            auth=auth,
            req=req,
            audit=_make_audit(settings, gm),
        ),
        artifact_history_cold_rehydrate_request_factory=ArtifactHistoryColdRehydrateRequest,
        registry_history_cold_store=lambda req, auth: registry_history_cold_store_service(
            repo_root=settings.repo_root,
            gm=gm,
            auth=auth,
            req=req,
            audit=_make_audit(settings, gm),
        ),
        registry_history_cold_store_request_factory=RegistryHistoryColdStoreRequest,
        registry_history_cold_rehydrate=lambda req, auth: registry_history_cold_rehydrate_service(
            repo_root=settings.repo_root,
            gm=gm,
            auth=auth,
            req=req,
            audit=_make_audit(settings, gm),
        ),
        registry_history_cold_rehydrate_request_factory=RegistryHistoryColdRehydrateRequest,
        segment_history_maintenance=lambda req, auth: segment_history_maintenance_service(
            family=req.family,
            repo_root=settings.repo_root,
            settings=settings,
            gm=gm,
            batch_limit=req.batch_limit,
            audit=_make_segment_audit(settings, auth, gm),
        ),
        segment_history_maintenance_request_factory=SegmentHistoryMaintenanceRequest,
        segment_history_cold_store=lambda req, auth: segment_history_cold_store_service(
            family=req.family,
            repo_root=settings.repo_root,
            settings=settings,
            gm=gm,
            batch_limit=req.batch_limit,
            segment_ids=req.segment_ids,
            audit=_make_segment_audit(settings, auth, gm),
        ),
        segment_history_cold_store_request_factory=SegmentHistoryColdStoreRequest,
        segment_history_cold_rehydrate=lambda req, auth: segment_history_cold_rehydrate_service(
            family=req.family,
            segment_id=req.segment_id,
            repo_root=settings.repo_root,
            gm=gm,
            audit=_make_segment_audit(settings, auth, gm),
        ),
        segment_history_cold_rehydrate_request_factory=SegmentHistoryColdRehydrateRequest,
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
    """Write a text file into the repository and commit it if changed."""
    settings, gm = _services()
    return write_file_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        scope_for_path=_scope_for_path,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/read")
def read_file(path: str = Query(...), auth: AuthContext = Depends(require_auth)) -> dict:
    """Read a repository file by path after auth and path checks."""
    settings, gm = _services()
    return read_file_service(
        repo_root=settings.repo_root,
        auth=auth,
        path=path,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/append")
def append_record(req: AppendRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Append one JSONL record to a repository file and commit it if changed."""
    settings, gm = _services()
    return append_record_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        scope_for_path=_scope_for_path,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/index/rebuild")
def index_rebuild(auth: AuthContext = Depends(require_auth)) -> dict:
    """Rebuild the full derived search index set."""
    settings, gm = _services()
    return index_rebuild_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/index/rebuild-incremental")
def index_rebuild_incremental(auth: AuthContext = Depends(require_auth)) -> dict:
    """Incrementally rebuild derived indexes from repository changes."""
    settings, gm = _services()
    return index_rebuild_incremental_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/index/status")
def index_status(auth: AuthContext = Depends(require_auth)) -> dict:
    """Return the status of generated search index artifacts."""
    settings, gm = _services()
    return index_status_service(repo_root=settings.repo_root, auth=auth)


@app.post("/v1/search")
def search(req: SearchRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Search indexed repository content."""
    settings, gm = _services()
    return search_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/recent")
def recent_list(req: RecentRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """List recent repository files without a search query."""
    settings, gm = _services()
    return recent_list_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        audit=_make_audit(settings, gm),
    )


def _extract_cached_raw_body(request: FastAPIRequest) -> dict | None:
    """FastAPI dependency that extracts the cached raw JSON body for preserve-mode upsert."""
    return getattr(request.state, "raw_json_body", None)


@app.post("/v1/continuity/upsert")
def continuity_upsert(
    req: ContinuityUpsertRequest,
    auth: AuthContext = Depends(require_auth),
    raw_body: dict | None = Depends(_extract_cached_raw_body),
) -> dict:
    """Store or replace a continuity capsule."""
    settings, gm = _services()
    return continuity_upsert_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        raw_body=raw_body,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/continuity/patch")
def continuity_patch(req: ContinuityPatchRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Apply partial list-field patch operations to an existing continuity capsule."""
    settings, gm = _services()
    return continuity_patch_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/continuity/lifecycle")
def continuity_lifecycle(req: ContinuityLifecycleRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Apply a standalone lifecycle transition to a thread or task capsule."""
    settings, gm = _services()
    return continuity_lifecycle_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/continuity/read")
def continuity_read(req: ContinuityReadRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Read one continuity capsule by exact selector with optional fallback handling.

    Pass ``view="startup"`` to include a ``startup_summary`` extraction alongside the full capsule.
    """
    settings, gm = _services()
    return continuity_read_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        now=datetime.now(timezone.utc),
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/continuity/refresh/plan")
def continuity_refresh_plan(req: ContinuityRefreshPlanRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Build and persist a deterministic continuity refresh plan."""
    settings, gm = _services()
    return continuity_refresh_plan_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        now=datetime.now(timezone.utc),
        retention_archive_days=settings.continuity_retention_archive_days,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/continuity/retention/plan")
def continuity_retention_plan(req: ContinuityRetentionPlanRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Build and persist a deterministic continuity retention plan."""
    settings, gm = _services()
    return continuity_retention_plan_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        now=datetime.now(timezone.utc),
        retention_archive_days=settings.continuity_retention_archive_days,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/continuity/compare")
def continuity_compare(req: ContinuityCompareRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Compare one active continuity capsule to a candidate capsule."""
    settings, gm = _services()
    return continuity_compare_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/continuity/revalidate")
def continuity_revalidate(req: ContinuityRevalidateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Confirm, correct, degrade, or conflict-mark one active continuity capsule."""
    settings, gm = _services()
    return continuity_revalidate_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/continuity/list")
def continuity_list(req: ContinuityListRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """List active, fallback, and archived continuity capsule summaries."""
    settings, gm = _services()
    return continuity_list_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        now=datetime.now(timezone.utc),
        retention_archive_days=settings.continuity_retention_archive_days,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/continuity/archive")
def continuity_archive(req: ContinuityArchiveRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Archive one active continuity capsule and remove its active file."""
    settings, gm = _services()
    return continuity_archive_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        now=datetime.now(timezone.utc),
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/continuity/delete")
def continuity_delete(req: ContinuityDeleteRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Delete selected continuity artifacts for one exact selector."""
    settings, gm = _services()
    return continuity_delete_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/coordination/handoff/create")
def coordination_handoff_create(req: CoordinationHandoffCreateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Create one local-first inter-agent handoff artifact from an active capsule."""
    settings, gm = _services()
    return handoff_create_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/coordination/handoff/{handoff_id}")
def coordination_handoff_read(handoff_id: str, auth: AuthContext = Depends(require_auth)) -> dict:
    """Read one stored handoff artifact using sender/recipient/admin visibility."""
    settings, gm = _services()
    return handoff_read_service(
        repo_root=settings.repo_root,
        auth=auth,
        handoff_id=handoff_id,
        enforce_rate_limit=_enforce_rate_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/coordination/handoffs/query")
def coordination_handoffs_query(
    recipient_peer: str | None = Query(default=None),
    sender_peer: str | None = Query(default=None),
    status: Literal["pending", "accepted_advisory", "deferred", "rejected"] | None = Query(default=None),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=20, ge=1, le=100),
    auth: AuthContext = Depends(require_auth),
) -> dict:
    """Query visible handoff artifacts for one sender and/or recipient identity."""
    settings, gm = _services()
    try:
        req = CoordinationHandoffQueryRequest(
            recipient_peer=recipient_peer,
            sender_peer=sender_peer,
            status=status,
            offset=offset,
            limit=limit,
        )
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid coordination handoff query: {exc}") from exc
    return handoffs_query_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/coordination/handoff/{handoff_id}/consume")
def coordination_handoff_consume(
    handoff_id: str,
    req: CoordinationHandoffConsumeRequest,
    auth: AuthContext = Depends(require_auth),
) -> dict:
    """Record the intended recipient's consume outcome for one handoff artifact."""
    settings, gm = _services()
    return handoff_consume_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        handoff_id=handoff_id,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/coordination/shared/create")
def coordination_shared_create(req: CoordinationSharedCreateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Create one owner-authored shared coordination artifact."""
    settings, gm = _services()
    return shared_create_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/coordination/shared/query")
def coordination_shared_query(
    owner_peer: Annotated[str | None, Query()] = None,
    participant_peer: Annotated[str | None, Query()] = None,
    task_id: Annotated[str | None, Query()] = None,
    thread_id: Annotated[str | None, Query()] = None,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    auth: AuthContext = Depends(require_auth),
) -> dict:
    """Query visible shared coordination artifacts for bounded identity filters."""
    settings, gm = _services()
    try:
        req = CoordinationSharedQueryRequest(
            owner_peer=owner_peer,
            participant_peer=participant_peer,
            task_id=task_id,
            thread_id=thread_id,
            offset=offset,
            limit=limit,
        )
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid shared coordination query: {exc}") from exc
    return shared_query_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/coordination/shared/{shared_id}")
def coordination_shared_read(shared_id: str, auth: AuthContext = Depends(require_auth)) -> dict:
    """Read one shared coordination artifact using owner/participant/admin visibility."""
    settings, gm = _services()
    return shared_read_service(
        repo_root=settings.repo_root,
        auth=auth,
        shared_id=shared_id,
        enforce_rate_limit=_enforce_rate_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/coordination/shared/{shared_id}/update")
def coordination_shared_update(
    shared_id: str,
    req: CoordinationSharedUpdateRequest,
    auth: AuthContext = Depends(require_auth),
) -> dict:
    """Replace one shared coordination artifact payload under owner-only version checking."""
    settings, gm = _services()
    return shared_update_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        shared_id=shared_id,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/coordination/reconciliation/open")
def coordination_reconciliation_open(req: CoordinationReconciliationOpenRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Open one bounded reconciliation record from visible coordination artifacts."""
    settings, gm = _services()
    return reconciliation_open_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/coordination/reconciliations/query")
def coordination_reconciliations_query(
    owner_peer: Annotated[str | None, Query()] = None,
    claimant_peer: Annotated[str | None, Query()] = None,
    status: Annotated[Literal["open", "resolved"] | None, Query()] = None,
    classification: Annotated[Literal["contradictory", "stale_observation", "frame_conflict", "concurrent_race"] | None, Query()] = None,
    task_id: Annotated[str | None, Query()] = None,
    thread_id: Annotated[str | None, Query()] = None,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    auth: AuthContext = Depends(require_auth),
) -> dict:
    """Query visible reconciliation artifacts for bounded owner or claimant filters."""
    settings, gm = _services()
    try:
        req = CoordinationReconciliationQueryRequest(
            owner_peer=owner_peer,
            claimant_peer=claimant_peer,
            status=status,
            classification=classification,
            task_id=task_id,
            thread_id=thread_id,
            offset=offset,
            limit=limit,
        )
    except ValidationError as exc:
        if "At least one reconciliation query filter is required" in str(exc):
            raise HTTPException(status_code=400, detail="At least one reconciliation query filter is required") from exc
        raise HTTPException(status_code=400, detail=f"Invalid reconciliation query: {exc}") from exc
    return reconciliation_query_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/coordination/reconciliation/{reconciliation_id}")
def coordination_reconciliation_read(reconciliation_id: str, auth: AuthContext = Depends(require_auth)) -> dict:
    """Read one stored reconciliation artifact using owner/participant/admin visibility."""
    settings, gm = _services()
    return reconciliation_read_service(
        repo_root=settings.repo_root,
        auth=auth,
        reconciliation_id=reconciliation_id,
        enforce_rate_limit=_enforce_rate_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/coordination/reconciliation/{reconciliation_id}/resolve")
def coordination_reconciliation_resolve(
    reconciliation_id: str,
    req: CoordinationReconciliationResolveRequest,
    auth: AuthContext = Depends(require_auth),
) -> dict:
    """Resolve one open reconciliation record under first-write-wins version checking."""
    settings, gm = _services()
    return reconciliation_resolve_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        reconciliation_id=reconciliation_id,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/context/retrieve")
def context_retrieve(req: ContextRetrieveRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Build a continuation bundle for a task or subject."""
    settings, gm = _services()
    return context_retrieve_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        now=datetime.now(timezone.utc),
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/peers")
def peers_list(auth: AuthContext = Depends(require_auth)) -> dict:
    """List known peers from the repository registry."""
    settings, gm = _services()
    return peers_list_service(
        repo_root=settings.repo_root,
        auth=auth,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/peers/register")
def peers_register(req: PeerRegisterRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Create or update a peer registry record."""
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
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/peers/{peer_id}/trust")
def peers_trust_transition(peer_id: str, req: PeerTrustTransitionRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Apply a peer trust-level transition with policy enforcement."""
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
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/peers/{peer_id}/manifest")
def peer_manifest(peer_id: str, auth: AuthContext = Depends(require_auth)) -> dict:
    """Fetch and return a peer's advertised manifest."""
    settings, gm = _services()
    return peer_manifest_service(
        repo_root=settings.repo_root,
        auth=auth,
        peer_id=peer_id,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/context/snapshot")
def context_snapshot_create(req: ContextSnapshotRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Create and persist a deterministic context snapshot."""
    settings, gm = _services()
    return context_snapshot_create_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        now=datetime.now(timezone.utc),
        service_version=app.version,
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/context/snapshot/{snapshot_id}")
def context_snapshot_get(snapshot_id: str, auth: AuthContext = Depends(require_auth)) -> dict:
    """Load a stored context snapshot by id."""
    settings, gm = _services()
    return context_snapshot_get_service(
        repo_root=settings.repo_root,
        auth=auth,
        snapshot_id=snapshot_id,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/tasks")
def tasks_create(req: TaskCreateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Create a shared task record."""
    settings, gm = _services()
    return tasks_create_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        audit=_make_audit(settings, gm),
    )


@app.patch("/v1/tasks/{task_id}")
def tasks_update(task_id: str, req: TaskUpdateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Update an existing shared task record."""
    settings, gm = _services()
    return tasks_update_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        task_id=task_id,
        req=req,
        audit=_make_audit(settings, gm),
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
    """Query shared task records with optional filters."""
    settings, gm = _services()
    return tasks_query_service(
        repo_root=settings.repo_root,
        auth=auth,
        status=status if isinstance(status, str) else None,
        owner_peer=owner_peer if isinstance(owner_peer, str) else None,
        collaborator=collaborator if isinstance(collaborator, str) else None,
        thread_id=thread_id if isinstance(thread_id, str) else None,
        limit=limit if isinstance(limit, int) else 100,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/docs/patch/propose")
def docs_patch_propose(req: PatchProposeRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Propose a documentation patch against the repository."""
    settings, gm = _services()
    return docs_patch_propose_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        run_git=_run_git,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/code/patch/propose")
def code_patch_propose(req: PatchProposeRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Propose a code patch against the repository."""
    settings, gm = _services()
    return code_patch_propose_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        run_git=_run_git,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/docs/patch/apply")
def docs_patch_apply(req: PatchApplyRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Apply a previously proposed documentation patch."""
    settings, gm = _services()
    return docs_patch_apply_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        run_git=_run_git,
        read_commit_file=_read_commit_file,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/code/checks/run")
def code_checks_run(req: CodeCheckRunRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Run one configured code-check profile against a ref."""
    settings, gm = _services()
    return code_checks_run_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        run_git=_run_git,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/code/merge")
def code_merge(req: CodeMergeRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Merge one source ref into a target ref after required checks."""
    settings, gm = _services()
    return code_merge_service(
        repo_root=settings.repo_root,
        auth=auth,
        req=req,
        run_git=_run_git,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/messages/send")
def messages_send(req: MessageSendRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Send a direct message with optional signed-ingress enforcement."""
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
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/messages/ack")
def messages_ack(req: MessageAckRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Acknowledge a previously delivered message."""
    settings, gm = _services()
    return messages_ack_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        parse_iso=_parse_iso,
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/messages/pending")
def messages_pending(
    recipient: str | None = Query(default=None),
    status: str | None = Query(default=None),
    include_terminal: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=500),
    auth: AuthContext = Depends(require_auth),
) -> dict:
    """List pending or terminal messages for a recipient."""
    settings, gm = _services()
    return messages_pending_service(
        repo_root=settings.repo_root,
        auth=auth,
        recipient=recipient if isinstance(recipient, str) else None,
        status=status if isinstance(status, str) else None,
        include_terminal=include_terminal if isinstance(include_terminal, bool) else False,
        limit=limit if isinstance(limit, int) else 50,
        parse_iso=_parse_iso,
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/messages/inbox")
def messages_inbox(recipient: str = Query(...), limit: int = Query(default=20, ge=1, le=200), auth: AuthContext = Depends(require_auth)) -> dict:
    """Return inbox messages for a recipient."""
    settings, gm = _services()
    return messages_inbox_service(
        repo_root=settings.repo_root,
        auth=auth,
        recipient=recipient,
        limit=limit,
        audit=_make_audit(settings, gm),
        max_jsonl_read_bytes=settings.max_jsonl_read_bytes,
    )


@app.get("/v1/messages/thread")
def messages_thread(thread_id: str = Query(...), limit: int = Query(default=100, ge=1, le=1000), auth: AuthContext = Depends(require_auth)) -> dict:
    """Return messages for one thread."""
    settings, gm = _services()
    return messages_thread_service(
        repo_root=settings.repo_root,
        auth=auth,
        thread_id=thread_id,
        limit=limit,
        audit=_make_audit(settings, gm),
        max_jsonl_read_bytes=settings.max_jsonl_read_bytes,
    )


@app.post("/v1/relay/forward")
def relay_forward(req: RelayForwardRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Forward a message through the relay pipeline."""
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
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/security/tokens")
def security_tokens_list(
    peer_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
    include_inactive: bool = Query(default=False),
    auth: AuthContext = Depends(require_auth),
) -> dict:
    """List issued tokens with optional status and peer filters."""
    settings, gm = _services()
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
    """Issue a new peer token."""
    settings, gm = _services()
    return security_tokens_issue_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
        refresh_settings=lambda: get_settings(force_reload=True),
    )


@app.post("/v1/security/tokens/revoke")
def security_tokens_revoke(req: SecurityTokenRevokeRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Revoke one or more peer tokens."""
    settings, gm = _services()
    return security_tokens_revoke_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
        refresh_settings=lambda: get_settings(force_reload=True),
    )


@app.post("/v1/security/tokens/rotate")
def security_tokens_rotate(req: SecurityTokenRotateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Rotate an existing peer token and optionally update its metadata."""
    settings, gm = _services()
    return security_tokens_rotate_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
        refresh_settings=lambda: get_settings(force_reload=True),
    )


@app.post("/v1/security/keys/rotate")
def security_keys_rotate(req: SecurityKeysRotateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Rotate signing keys used for signed ingress and verification."""
    settings, gm = _services()
    return security_keys_rotate_service(
        repo_root=settings.repo_root,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        settings=settings,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/messages/verify")
def messages_verify(req: MessageVerifyRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Verify a signed payload without sending or relaying it."""
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
        audit=_make_audit(settings, gm),
    )


@app.get("/v1/metrics")
def metrics(auth: AuthContext = Depends(require_auth)) -> dict:
    """Return aggregated service metrics and alarm indicators."""
    settings, gm = _services()
    return metrics_service(
        settings=settings,
        auth=auth,
        load_delivery_state=load_delivery_state,
        delivery_record_view=lambda row, now: delivery_record_view(row, now, parse_iso=_parse_iso),
        load_check_artifacts=load_check_artifacts,
        load_rate_limit_state=_load_rate_limit_state,
        parse_iso=_parse_iso,
        max_jsonl_read_bytes=settings.max_jsonl_read_bytes,
    )


@app.post("/v1/replay/messages")
def replay_messages(req: MessageReplayRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Replay a previously failed or dead-lettered message."""
    settings, gm = _services()
    return replay_messages_service(
        settings=settings,
        gm=gm,
        auth=auth,
        req=req,
        parse_iso=_parse_iso,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/replication/pull")
def replication_pull(req: ReplicationPullRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Apply inbound replicated file state to the local repository."""
    settings, gm = _services()
    return replication_pull_service(
        settings=settings,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        parse_iso=_parse_iso,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/replication/push")
def replication_push(req: ReplicationPushRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Push local replicated file state to another peer."""
    settings, gm = _services()
    return replication_push_service(
        settings=settings,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        load_peers_registry=load_peers_registry,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/backup/create")
def backup_create(req: BackupCreateRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Create a repository backup archive."""
    settings, gm = _services()
    return backup_create_service(
        settings=settings,
        gm=gm,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/backup/restore-test")
def backup_restore_test(req: BackupRestoreTestRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Validate that a backup archive can be restored safely."""
    settings, gm = _services()
    return backup_restore_test_service(
        settings=settings,
        auth=auth,
        req=req,
        enforce_rate_limit=_enforce_rate_limit,
        enforce_payload_limit=_enforce_payload_limit,
        rebuild_index=rebuild_index,
        audit=_make_audit(settings, gm),
    )


@app.post("/v1/compact/run")
def compact_run(req: CompactRequest, auth: AuthContext = Depends(require_auth)) -> dict:
    """Generate a compaction plan for memory and summary candidates."""
    settings, gm = _services()
    return compact_run_service(
        settings=settings,
        gm=gm,
        auth=auth,
        req=req,
        parse_iso=_parse_iso,
        audit=_make_audit(settings, gm),
    )


@app.exception_handler(GitLockTimeout)
async def git_lock_timeout_handler(request: FastAPIRequest, exc: GitLockTimeout):
    """Convert uncaught git lock timeouts to 409 Conflict."""
    _log.warning(
        "Git lock timeout reached global handler: %s %s: %s",
        request.method, request.url.path, exc, exc_info=True,
    )
    return JSONResponse(
        status_code=409,
        content=make_error_detail(
            operation="git_lock",
            error_code="git_lock_timeout",
            error_detail=str(exc),
        ),
    )


@app.exception_handler(GitLockInfrastructureError)
async def git_lock_infra_handler(request: FastAPIRequest, exc: GitLockInfrastructureError):
    """Convert uncaught git lock infrastructure errors to 503 Service Unavailable."""
    _log.error(
        "Git lock infrastructure error reached global handler: %s %s: %s",
        request.method, request.url.path, exc, exc_info=True,
    )
    return JSONResponse(
        status_code=503,
        content=make_error_detail(
            operation="git_lock",
            error_code="git_lock_infrastructure_unavailable",
            error_detail=str(exc),
        ),
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: FastAPIRequest, exc: Exception):
    """Return a normalized JSON error payload for uncaught exceptions."""
    _log.error(
        "Unhandled exception reached global handler: %s %s",
        request.method, request.url.path, exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content=make_error_detail(
            operation="unhandled",
            error_code=type(exc).__name__,
            error_detail=str(exc),
        ),
    )
