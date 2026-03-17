"""Discovery catalogs, manifest payloads, and MCP-compatible request handling."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import HTTPException
from pydantic import ValidationError

from app.auth import AuthContext
from app.models import (
    AppendRequest,
    BackupCreateRequest,
    BackupRestoreTestRequest,
    CodeCheckRunRequest,
    CodeMergeRequest,
    CompactRequest,
    CoordinationHandoffConsumeRequest,
    CoordinationHandoffCreateRequest,
    CoordinationHandoffQueryRequest,
    ContinuityArchiveRequest,
    ContinuityCompareRequest,
    ContinuityDeleteRequest,
    ContinuityListRequest,
    ContinuityRefreshPlanRequest,
    ContinuityReadRequest,
    ContinuityRevalidateRequest,
    ContinuityUpsertRequest,
    ContextRetrieveRequest,
    ContextSnapshotRequest,
    MessageAckRequest,
    MessageReplayRequest,
    MessageSendRequest,
    MessageVerifyRequest,
    OpsRunRequest,
    PatchApplyRequest,
    PatchProposeRequest,
    PeerRegisterRequest,
    PeerTrustTransitionRequest,
    RecentRequest,
    RelayForwardRequest,
    ReplicationPullRequest,
    ReplicationPushRequest,
    SearchRequest,
    SecurityKeysRotateRequest,
    SecurityTokenIssueRequest,
    SecurityTokenRevokeRequest,
    SecurityTokenRotateRequest,
    TaskCreateRequest,
    TaskUpdateRequest,
    WriteRequest,
)
from app.storage import canonical_json


def tool_catalog(schema_for_model: Callable[[Any], dict[str, Any]]) -> list[dict[str, Any]]:
    """Return the machine-readable tool catalog exposed by the service."""
    return [
        {
            "name": "system.health",
            "description": "Check service liveness and git state.",
            "method": "GET",
            "path": "/health",
            "scopes": [],
            "idempotent": True,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "system.capabilities",
            "description": "Return high-level feature flags.",
            "method": "GET",
            "path": "/capabilities",
            "scopes": [],
            "idempotent": True,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "system.manifest",
            "description": "Return endpoint map and auth expectations.",
            "method": "GET",
            "path": "/v1/manifest",
            "scopes": [],
            "idempotent": True,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "system.contracts",
            "description": "Return frozen API/tool contract version and compatibility policy.",
            "method": "GET",
            "path": "/v1/contracts",
            "scopes": [],
            "idempotent": True,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "system.governance_policy",
            "description": "Return machine-readable governance policy pack.",
            "method": "GET",
            "path": "/v1/governance/policy",
            "scopes": [],
            "idempotent": True,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "system.discovery",
            "description": "Return machine guidance and entrypoints (MCP-like metadata).",
            "method": "GET",
            "path": "/v1/discovery",
            "scopes": [],
            "idempotent": True,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "system.discovery_tools",
            "description": "Return machine-usable tool catalog with schemas.",
            "method": "GET",
            "path": "/v1/discovery/tools",
            "scopes": [],
            "idempotent": True,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "system.discovery_workflows",
            "description": "Return recommended autonomous workflows.",
            "method": "GET",
            "path": "/v1/discovery/workflows",
            "scopes": [],
            "idempotent": True,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "memory.write",
            "description": "Write text content to a repo-relative path and commit.",
            "method": "POST",
            "path": "/v1/write",
            "scopes": ["write:journal|write:messages|write:projects", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(WriteRequest),
        },
        {
            "name": "memory.append_jsonl",
            "description": "Append one JSON object as a JSONL record and commit.",
            "method": "POST",
            "path": "/v1/append",
            "scopes": ["write:journal|write:messages|write:projects", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(AppendRequest),
        },
        {
            "name": "memory.read",
            "description": "Read a file by path.",
            "method": "GET",
            "path": "/v1/read",
            "scopes": ["read:files", "read_namespaces"],
            "idempotent": True,
            "input_schema": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
                "additionalProperties": False,
            },
        },
        {
            "name": "index.rebuild_full",
            "description": "Rebuild derived indexes and SQLite FTS from repo files.",
            "method": "POST",
            "path": "/v1/index/rebuild",
            "scopes": ["read:index"],
            "idempotent": False,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "index.rebuild_incremental",
            "description": "Incrementally rebuild indexes from mtime state.",
            "method": "POST",
            "path": "/v1/index/rebuild-incremental",
            "scopes": ["read:index"],
            "idempotent": False,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "index.status",
            "description": "Inspect current index status and derived artifacts.",
            "method": "GET",
            "path": "/v1/index/status",
            "scopes": ["read:index"],
            "idempotent": True,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "peers.list",
            "description": "List known peer records from registry.",
            "method": "GET",
            "path": "/v1/peers",
            "scopes": ["read:files", "read_namespaces"],
            "idempotent": True,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "peers.register",
            "description": "Create or update a peer registry record.",
            "method": "POST",
            "path": "/v1/peers/register",
            "scopes": ["admin:peers", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(PeerRegisterRequest),
        },
        {
            "name": "peers.trust_transition",
            "description": "Apply explicit peer trust-level transition with reason and fingerprint guard.",
            "method": "POST",
            "path": "/v1/peers/{peer_id}/trust",
            "scopes": ["admin:peers", "write_namespaces"],
            "idempotent": False,
            "input_schema": {
                "type": "object",
                "properties": {
                    "peer_id": {"type": "string"},
                    **schema_for_model(PeerTrustTransitionRequest).get("properties", {}),
                },
                "required": ["peer_id", "trust_level", "reason"],
                "additionalProperties": False,
            },
        },
        {
            "name": "peers.fetch_manifest",
            "description": "Fetch and return a peer's advertised manifest.",
            "method": "GET",
            "path": "/v1/peers/{peer_id}/manifest",
            "scopes": ["read:files", "read_namespaces"],
            "idempotent": True,
            "input_schema": {
                "type": "object",
                "properties": {"peer_id": {"type": "string"}},
                "required": ["peer_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "search.query",
            "description": "Search indexed repo content.",
            "method": "POST",
            "path": "/v1/search",
            "scopes": ["search", "read_namespaces"],
            "idempotent": True,
            "input_schema": schema_for_model(SearchRequest),
        },
        {
            "name": "recent.list",
            "description": "List recently modified indexed content without a query string.",
            "method": "POST",
            "path": "/v1/recent",
            "scopes": ["search", "read_namespaces"],
            "idempotent": True,
            "input_schema": schema_for_model(RecentRequest),
        },
        {
            "name": "context.retrieve",
            "description": "Build a compact context bundle for task continuation with continuity resilience and degraded index fallback.",
            "method": "POST",
            "path": "/v1/context/retrieve",
            "scopes": ["search", "read_namespaces"],
            "idempotent": True,
            "input_schema": schema_for_model(ContextRetrieveRequest),
        },
        {
            "name": "continuity.upsert",
            "description": "Create or replace one continuity capsule atomically.",
            "method": "POST",
            "path": "/v1/continuity/upsert",
            "scopes": ["write:projects", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(ContinuityUpsertRequest),
        },
        {
            "name": "continuity.read",
            "description": "Read one continuity capsule by exact selector with active, fallback, or structured missing-state output.",
            "method": "POST",
            "path": "/v1/continuity/read",
            "scopes": ["read:files", "read_namespaces"],
            "idempotent": True,
            "input_schema": schema_for_model(ContinuityReadRequest),
        },
        {
            "name": "continuity.compare",
            "description": "Compare one active continuity capsule against a candidate capsule without mutating storage.",
            "method": "POST",
            "path": "/v1/continuity/compare",
            "scopes": ["read:files", "read_namespaces"],
            "idempotent": True,
            "input_schema": schema_for_model(ContinuityCompareRequest),
        },
        {
            "name": "continuity.revalidate",
            "description": "Confirm, correct, degrade, or conflict-mark one active continuity capsule through an auditable git-backed write.",
            "method": "POST",
            "path": "/v1/continuity/revalidate",
            "scopes": ["write:projects", "write_namespaces", "read_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(ContinuityRevalidateRequest),
        },
        {
            "name": "continuity.refresh_plan",
            "description": "Build and persist the latest deterministic continuity refresh plan.",
            "method": "POST",
            "path": "/v1/continuity/refresh/plan",
            "scopes": ["read:files", "write:projects", "read_namespaces", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(ContinuityRefreshPlanRequest),
        },
        {
            "name": "continuity.list",
            "description": "List active, fallback, and archived continuity summaries with retention and recovery metadata.",
            "method": "POST",
            "path": "/v1/continuity/list",
            "scopes": ["read:files", "read_namespaces"],
            "idempotent": True,
            "input_schema": schema_for_model(ContinuityListRequest),
        },
        {
            "name": "continuity.archive",
            "description": "Archive one active continuity capsule and remove the active file.",
            "method": "POST",
            "path": "/v1/continuity/archive",
            "scopes": ["write:projects", "write_namespaces", "read_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(ContinuityArchiveRequest),
        },
        {
            "name": "continuity.delete",
            "description": "Hard-delete exact-selector active, fallback, and archive continuity artifacts.",
            "method": "POST",
            "path": "/v1/continuity/delete",
            "scopes": ["write:projects", "write_namespaces", "read_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(ContinuityDeleteRequest),
        },
        {
            "name": "coordination.handoff_create",
            "description": "Create one local-first inter-agent handoff artifact from an active continuity capsule.",
            "method": "POST",
            "path": "/v1/coordination/handoff/create",
            "scopes": ["write:projects", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(CoordinationHandoffCreateRequest),
        },
        {
            "name": "coordination.handoff_read",
            "description": "Read one stored handoff artifact using sender, recipient, or admin visibility.",
            "method": "GET",
            "path": "/v1/coordination/handoff/{handoff_id}",
            "scopes": ["authenticated"],
            "idempotent": True,
            "input_schema": {
                "type": "object",
                "properties": {"handoff_id": {"type": "string"}},
                "required": ["handoff_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "coordination.handoffs_query",
            "description": "Query visible handoff artifacts for one sender and/or recipient identity without shared-state mutation.",
            "method": "GET",
            "path": "/v1/coordination/handoffs/query",
            "scopes": ["read:files"],
            "idempotent": True,
            "input_schema": schema_for_model(CoordinationHandoffQueryRequest),
        },
        {
            "name": "coordination.handoff_consume",
            "description": "Record the recipient's consume outcome for one handoff artifact without mutating local continuity.",
            "method": "POST",
            "path": "/v1/coordination/handoff/{handoff_id}/consume",
            "scopes": ["authenticated"],
            "idempotent": False,
            "input_schema": {
                "type": "object",
                "properties": {
                    "handoff_id": {"type": "string"},
                    **schema_for_model(CoordinationHandoffConsumeRequest).get("properties", {}),
                },
                "required": ["handoff_id", "status"],
                "additionalProperties": False,
            },
        },
        {
            "name": "context.snapshot_create",
            "description": "Create deterministic context snapshot and persist it.",
            "method": "POST",
            "path": "/v1/context/snapshot",
            "scopes": ["search", "write:projects", "write_namespaces", "read_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(ContextSnapshotRequest),
        },
        {
            "name": "context.snapshot_get",
            "description": "Load persisted context snapshot by id.",
            "method": "GET",
            "path": "/v1/context/snapshot/{snapshot_id}",
            "scopes": ["read:files", "read_namespaces"],
            "idempotent": True,
            "input_schema": {
                "type": "object",
                "properties": {"snapshot_id": {"type": "string"}},
                "required": ["snapshot_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "tasks.create",
            "description": "Create a collaborative task record.",
            "method": "POST",
            "path": "/v1/tasks",
            "scopes": ["write:projects", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(TaskCreateRequest),
        },
        {
            "name": "tasks.update",
            "description": "Update a task and enforce status transitions.",
            "method": "PATCH",
            "path": "/v1/tasks/{task_id}",
            "scopes": ["write:projects", "write_namespaces"],
            "idempotent": False,
            "input_schema": {
                "type": "object",
                "properties": {"task_id": {"type": "string"}, **schema_for_model(TaskUpdateRequest).get("properties", {})},
                "required": ["task_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "tasks.query",
            "description": "Query task records by status/owner/collaborator/thread.",
            "method": "GET",
            "path": "/v1/tasks/query",
            "scopes": ["read:files", "read_namespaces"],
            "idempotent": True,
            "input_schema": {
                "type": "object",
                "properties": {
                    "status": {"type": "string"},
                    "owner_peer": {"type": "string"},
                    "collaborator": {"type": "string"},
                    "thread_id": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500},
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "docs.patch_propose",
            "description": "Propose unified diff patch for docs/content target.",
            "method": "POST",
            "path": "/v1/docs/patch/propose",
            "scopes": ["write:projects", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(PatchProposeRequest),
        },
        {
            "name": "docs.patch_apply",
            "description": "Apply proposed docs/content patch with base-ref checks.",
            "method": "POST",
            "path": "/v1/docs/patch/apply",
            "scopes": ["write:projects", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(PatchApplyRequest),
        },
        {
            "name": "code.patch_propose",
            "description": "Propose unified diff patch for code target.",
            "method": "POST",
            "path": "/v1/code/patch/propose",
            "scopes": ["write:projects", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(PatchProposeRequest),
        },
        {
            "name": "code.checks_run",
            "description": "Run configured check profile on a git ref and persist artifact.",
            "method": "POST",
            "path": "/v1/code/checks/run",
            "scopes": ["write:projects", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(CodeCheckRunRequest),
        },
        {
            "name": "code.merge",
            "description": "Fast-forward merge a source ref into HEAD after required checks pass.",
            "method": "POST",
            "path": "/v1/code/merge",
            "scopes": ["write:projects", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(CodeMergeRequest),
        },
        {
            "name": "security.tokens_list",
            "description": "List token entries with status and expiry metadata.",
            "method": "GET",
            "path": "/v1/security/tokens",
            "scopes": ["admin:peers", "read_namespaces"],
            "idempotent": True,
            "input_schema": {
                "type": "object",
                "properties": {
                    "peer_id": {"type": "string"},
                    "status": {"type": "string"},
                    "include_inactive": {"type": "boolean"},
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "security.tokens_issue",
            "description": "Issue a new peer token with scopes/namespaces and optional expiry.",
            "method": "POST",
            "path": "/v1/security/tokens/issue",
            "scopes": ["admin:peers", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(SecurityTokenIssueRequest),
        },
        {
            "name": "security.tokens_revoke",
            "description": "Revoke a token by id/sha or revoke all tokens for a peer.",
            "method": "POST",
            "path": "/v1/security/tokens/revoke",
            "scopes": ["admin:peers", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(SecurityTokenRevokeRequest),
        },
        {
            "name": "security.tokens_rotate",
            "description": "Rotate token credentials by revoking old token and issuing a replacement atomically.",
            "method": "POST",
            "path": "/v1/security/tokens/rotate",
            "scopes": ["admin:peers", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(SecurityTokenRotateRequest),
        },
        {
            "name": "security.keys_rotate",
            "description": "Rotate message verification key material.",
            "method": "POST",
            "path": "/v1/security/keys/rotate",
            "scopes": ["admin:peers", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(SecurityKeysRotateRequest),
        },
        {
            "name": "messages.verify",
            "description": "Verify signed message envelope and optionally consume nonce.",
            "method": "POST",
            "path": "/v1/messages/verify",
            "scopes": ["write:messages", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(MessageVerifyRequest),
        },
        {
            "name": "metrics.get",
            "description": "Return operational metrics summary.",
            "method": "GET",
            "path": "/v1/metrics",
            "scopes": ["read:index"],
            "idempotent": True,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "messages.replay",
            "description": "Replay a tracked dead-letter message back into delivery flow.",
            "method": "POST",
            "path": "/v1/replay/messages",
            "scopes": ["write:messages", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(MessageReplayRequest),
        },
        {
            "name": "replication.pull",
            "description": "Ingest replicated file bundle from peer.",
            "method": "POST",
            "path": "/v1/replication/pull",
            "scopes": ["admin:peers", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(ReplicationPullRequest),
        },
        {
            "name": "replication.push",
            "description": "Build and optionally push replication bundle to peer endpoint.",
            "method": "POST",
            "path": "/v1/replication/push",
            "scopes": ["admin:peers", "read_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(ReplicationPushRequest),
        },
        {
            "name": "messages.send",
            "description": "Write message to recipient inbox/outbox/thread.",
            "method": "POST",
            "path": "/v1/messages/send",
            "scopes": ["write:messages", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(MessageSendRequest),
        },
        {
            "name": "messages.ack",
            "description": "Acknowledge/defer/reject tracked message delivery.",
            "method": "POST",
            "path": "/v1/messages/ack",
            "scopes": ["write:messages", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(MessageAckRequest),
        },
        {
            "name": "messages.pending",
            "description": "Inspect pending and terminal delivery states.",
            "method": "GET",
            "path": "/v1/messages/pending",
            "scopes": ["read:files", "read_namespaces"],
            "idempotent": True,
            "input_schema": {
                "type": "object",
                "properties": {
                    "recipient": {"type": "string"},
                    "status": {"type": "string"},
                    "include_terminal": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500},
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "messages.inbox",
            "description": "Read recipient inbox messages.",
            "method": "GET",
            "path": "/v1/messages/inbox",
            "scopes": ["read:files", "read_namespaces"],
            "idempotent": True,
            "input_schema": {
                "type": "object",
                "properties": {
                    "recipient": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                },
                "required": ["recipient"],
                "additionalProperties": False,
            },
        },
        {
            "name": "messages.thread",
            "description": "Read thread messages by thread_id.",
            "method": "GET",
            "path": "/v1/messages/thread",
            "scopes": ["read:files", "read_namespaces"],
            "idempotent": True,
            "input_schema": {
                "type": "object",
                "properties": {
                    "thread_id": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
                },
                "required": ["thread_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "messages.relay_forward",
            "description": "Forward a message via relay log + inbox + thread write.",
            "method": "POST",
            "path": "/v1/relay/forward",
            "scopes": ["write:messages", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(RelayForwardRequest),
        },
        {
            "name": "memory.compaction_plan",
            "description": "Generate compaction planning report (planner only).",
            "method": "POST",
            "path": "/v1/compact/run",
            "scopes": ["compact:trigger", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(CompactRequest),
        },
        {
            "name": "backup.create",
            "description": "Create deterministic tar.gz backup bundle and manifest.",
            "method": "POST",
            "path": "/v1/backup/create",
            "scopes": ["admin:peers", "write_namespaces"],
            "idempotent": False,
            "input_schema": schema_for_model(BackupCreateRequest),
        },
        {
            "name": "backup.restore_test",
            "description": "Run backup restore validation in temporary directory with index and continuity checks.",
            "method": "POST",
            "path": "/v1/backup/restore-test",
            "scopes": ["admin:peers", "read_namespaces"],
            "idempotent": True,
            "input_schema": schema_for_model(BackupRestoreTestRequest),
        },
        {
            "name": "ops.catalog",
            "description": "List host-ops automation jobs and local security constraints.",
            "method": "GET",
            "path": "/v1/ops/catalog",
            "scopes": ["admin:peers"],
            "idempotent": True,
            "local_only": True,
            "input_schema": {"type": "object", "properties": {}, "additionalProperties": False},
        },
        {
            "name": "ops.status",
            "description": "Inspect recent host-ops runs and active job locks.",
            "method": "GET",
            "path": "/v1/ops/status",
            "scopes": ["admin:peers"],
            "idempotent": True,
            "local_only": True,
            "input_schema": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 500}},
                "additionalProperties": False,
            },
        },
        {
            "name": "ops.run",
            "description": "Run one host-ops job locally with lock/audit controls.",
            "method": "POST",
            "path": "/v1/ops/run",
            "scopes": ["admin:peers"],
            "idempotent": False,
            "local_only": True,
            "input_schema": schema_for_model(OpsRunRequest),
        },
        {
            "name": "ops.schedule_export",
            "description": "Export suggested systemd/cron schedule for host-ops jobs.",
            "method": "GET",
            "path": "/v1/ops/schedule/export",
            "scopes": ["admin:peers"],
            "idempotent": True,
            "local_only": True,
            "input_schema": {
                "type": "object",
                "properties": {"format": {"type": "string", "enum": ["systemd", "cron"]}},
                "additionalProperties": False,
            },
        },
    ]


def workflow_catalog() -> list[dict[str, Any]]:
    """Return recommended autonomous workflows built from the tool surface."""
    return [
        {
            "name": "bootstrap_cycle",
            "description": "Recommended autonomous startup/loop sequence.",
            "steps": [
                {"order": 1, "tool": "system.discovery"},
                {"order": 2, "tool": "system.manifest"},
                {"order": 3, "tool": "system.health"},
                {"order": 4, "tool": "index.rebuild_incremental"},
                {"order": 5, "tool": "context.retrieve"},
            ],
        },
        {
            "name": "collaborative_writing",
            "description": "Message exchange workflow with delivery acknowledgement.",
            "steps": [
                {"order": 1, "tool": "messages.send"},
                {"order": 2, "tool": "messages.pending"},
                {"order": 3, "tool": "messages.ack"},
                {"order": 4, "tool": "messages.thread"},
                {"order": 5, "tool": "memory.append_jsonl"},
                {"order": 6, "tool": "context.retrieve"},
            ],
        },
        {
            "name": "collaborative_tasks_and_code",
            "description": "Task graph + patch proposal + checks/merge sequence.",
            "steps": [
                {"order": 1, "tool": "tasks.query"},
                {"order": 2, "tool": "tasks.create"},
                {"order": 3, "tool": "tasks.update"},
                {"order": 4, "tool": "docs.patch_propose"},
                {"order": 5, "tool": "docs.patch_apply"},
                {"order": 6, "tool": "code.patch_propose"},
                {"order": 7, "tool": "code.checks_run"},
                {"order": 8, "tool": "code.merge"},
            ],
        },
        {
            "name": "federation_hardening",
            "description": "Signed envelope verification and replication operations.",
            "steps": [
                {"order": 1, "tool": "security.tokens_list"},
                {"order": 2, "tool": "security.tokens_issue"},
                {"order": 3, "tool": "security.tokens_rotate"},
                {"order": 4, "tool": "security.tokens_revoke"},
                {"order": 5, "tool": "peers.trust_transition"},
                {"order": 6, "tool": "security.keys_rotate"},
                {"order": 7, "tool": "messages.verify"},
                {"order": 8, "tool": "metrics.get"},
                {"order": 9, "tool": "messages.replay"},
                {"order": 10, "tool": "replication.push"},
                {"order": 11, "tool": "replication.pull"},
            ],
        },
        {
            "name": "maintenance_compaction",
            "description": "Periodic maintenance for continuity refresh visibility, backups, and compaction planning.",
            "steps": [
                {"order": 1, "tool": "index.rebuild_incremental"},
                {"order": 2, "tool": "continuity.refresh_plan"},
                {"order": 3, "tool": "backup.create"},
                {"order": 4, "tool": "backup.restore_test"},
                {"order": 5, "tool": "memory.compaction_plan"},
                {"order": 6, "tool": "memory.write"},
            ],
        },
        {
            "name": "host_ops_automation",
            "description": "Local-only host automation runner for maintenance and safety jobs.",
            "steps": [
                {"order": 1, "tool": "ops.catalog"},
                {"order": 2, "tool": "ops.status"},
                {"order": 3, "tool": "ops.run"},
                {"order": 4, "tool": "ops.schedule_export"},
            ],
        },
    ]


def discovery_payload(contract_version: str, *, tools: list[dict[str, Any]], workflows: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the top-level discovery payload used by autonomous clients."""
    return {
        "ok": True,
        "protocol": {
            "name": "cognirelay-http",
            "style": "mcp-like",
            "version": contract_version,
            "transport": "http+json",
        },
        "auth": {
            "type": "bearer",
            "header": "Authorization: Bearer <token>",
            "notes": [
                "Scopes and namespace restrictions are enforced per token.",
                "Use /v1/manifest for endpoint-level scope requirements.",
                "If strict signed ingress is enabled, message ingress requires signed_envelope.",
                "Rate limits and payload caps are enforced per token/IP.",
                "Host ops endpoints are local-only and intended for daemon/scheduler use on hosting machine.",
            ],
        },
        "entrypoints": {
            "manifest": "/v1/manifest",
            "contracts": "/v1/contracts",
            "governance_policy": "/v1/governance/policy",
            "ops_catalog": "/v1/ops/catalog",
            "ops_status": "/v1/ops/status",
            "tools": "/v1/discovery/tools",
            "workflows": "/v1/discovery/workflows",
            "mcp_rpc": "/v1/mcp",
            "mcp_well_known": "/.well-known/mcp.json",
        },
        "agent_guidance": {
            "first_calls": ["GET /v1/discovery", "GET /v1/manifest", "GET /health"],
            "mcp_first_calls": [
                "POST /v1/mcp {\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}",
                "POST /v1/mcp {\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\",\"params\":{}}",
                "POST /v1/mcp {\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}",
            ],
            "loop_hint": "Prefer /v1/index/rebuild-incremental over full rebuild in frequent loops.",
            "write_hint": "Prefer append-only JSONL for events/messages and frequent commits.",
        },
        "counts": {"tools": len(tools), "workflows": len(workflows)},
    }


def discovery_tools_payload(contract_version: str, *, tools: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the discovery payload containing only tool definitions."""
    return {
        "ok": True,
        "protocol": {"name": "cognirelay-http", "style": "mcp-like", "version": contract_version},
        "count": len(tools),
        "tools": tools,
    }


def discovery_workflows_payload(*, workflows: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the discovery payload containing only workflow definitions."""
    return {"ok": True, "count": len(workflows), "workflows": workflows}


def well_known_cognirelay_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the HTTP-native well-known discovery payload unchanged."""
    return payload


def well_known_mcp_payload(contract_version: str) -> dict[str, Any]:
    """Build the well-known MCP-compatible descriptor for the service."""
    return {
        "ok": True,
        "protocol": "jsonrpc-2.0",
        "style": "mcp-compatible",
        "transport": "http+json",
        "endpoint": "/v1/mcp",
        "contract_version": contract_version,
        "methods": ["initialize", "notifications/initialized", "ping", "tools/list", "tools/call"],
        "auth": {"type": "bearer", "header": "Authorization: Bearer <token>"},
    }


def _rpc_ok(request_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    """Build a JSON-RPC success response."""
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _rpc_error(request_id: Any, code: int, message: str, data: Any | None = None) -> dict[str, Any]:
    """Build a JSON-RPC error response."""
    err: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": err}


def rpc_error_payload(request_id: Any, code: int, message: str, data: Any | None = None) -> dict[str, Any]:
    """Public wrapper for building a JSON-RPC error response."""
    return _rpc_error(request_id, code, message, data)


def invoke_tool_by_name(
    name: str,
    arguments: dict[str, Any],
    auth: AuthContext | None,
    *,
    health: Callable[[], dict[str, Any]],
    capabilities: Callable[[], dict[str, Any]],
    manifest: Callable[[], dict[str, Any]],
    contracts: Callable[[], dict[str, Any]],
    governance_policy: Callable[[], dict[str, Any]],
    discovery: Callable[[], dict[str, Any]],
    discovery_tools: Callable[[], dict[str, Any]],
    discovery_workflows: Callable[[], dict[str, Any]],
    write_file: Callable[[WriteRequest, AuthContext | None], dict[str, Any]],
    append_record: Callable[[AppendRequest, AuthContext | None], dict[str, Any]],
    read_file: Callable[[str, AuthContext | None], dict[str, Any]],
    index_rebuild: Callable[[AuthContext | None], dict[str, Any]],
    index_rebuild_incremental: Callable[[AuthContext | None], dict[str, Any]],
    index_status: Callable[[AuthContext | None], dict[str, Any]],
    peers_list: Callable[[AuthContext | None], dict[str, Any]],
    peers_register: Callable[[PeerRegisterRequest, AuthContext | None], dict[str, Any]],
    peers_trust_transition: Callable[[str, PeerTrustTransitionRequest, AuthContext | None], dict[str, Any]],
    peer_manifest: Callable[[str, AuthContext | None], dict[str, Any]],
    search: Callable[[SearchRequest, AuthContext | None], dict[str, Any]],
    recent_list: Callable[[RecentRequest, AuthContext | None], dict[str, Any]],
    context_retrieve: Callable[[ContextRetrieveRequest, AuthContext | None], dict[str, Any]],
    continuity_upsert: Callable[[ContinuityUpsertRequest, AuthContext | None], dict[str, Any]],
    continuity_read: Callable[[ContinuityReadRequest, AuthContext | None], dict[str, Any]],
    continuity_compare: Callable[[ContinuityCompareRequest, AuthContext | None], dict[str, Any]],
    continuity_revalidate: Callable[[ContinuityRevalidateRequest, AuthContext | None], dict[str, Any]],
    continuity_refresh_plan: Callable[[ContinuityRefreshPlanRequest, AuthContext | None], dict[str, Any]],
    continuity_list: Callable[[ContinuityListRequest, AuthContext | None], dict[str, Any]],
    continuity_archive: Callable[[ContinuityArchiveRequest, AuthContext | None], dict[str, Any]],
    continuity_delete: Callable[[ContinuityDeleteRequest, AuthContext | None], dict[str, Any]],
    handoff_create: Callable[[CoordinationHandoffCreateRequest, AuthContext | None], dict[str, Any]],
    handoff_read: Callable[[str, AuthContext | None], dict[str, Any]],
    handoff_query: Callable[[CoordinationHandoffQueryRequest, AuthContext | None], dict[str, Any]],
    handoff_consume: Callable[[str, CoordinationHandoffConsumeRequest, AuthContext | None], dict[str, Any]],
    context_snapshot_create: Callable[[ContextSnapshotRequest, AuthContext | None], dict[str, Any]],
    context_snapshot_get: Callable[[str, AuthContext | None], dict[str, Any]],
    tasks_create: Callable[[TaskCreateRequest, AuthContext | None], dict[str, Any]],
    tasks_update: Callable[[str, TaskUpdateRequest, AuthContext | None], dict[str, Any]],
    tasks_query: Callable[..., dict[str, Any]],
    docs_patch_propose: Callable[[PatchProposeRequest, AuthContext | None], dict[str, Any]],
    docs_patch_apply: Callable[[PatchApplyRequest, AuthContext | None], dict[str, Any]],
    code_patch_propose: Callable[[PatchProposeRequest, AuthContext | None], dict[str, Any]],
    code_checks_run: Callable[[CodeCheckRunRequest, AuthContext | None], dict[str, Any]],
    code_merge: Callable[[CodeMergeRequest, AuthContext | None], dict[str, Any]],
    security_tokens_list: Callable[..., dict[str, Any]],
    security_tokens_issue: Callable[[SecurityTokenIssueRequest, AuthContext | None], dict[str, Any]],
    security_tokens_revoke: Callable[[SecurityTokenRevokeRequest, AuthContext | None], dict[str, Any]],
    security_tokens_rotate: Callable[[SecurityTokenRotateRequest, AuthContext | None], dict[str, Any]],
    security_keys_rotate: Callable[[SecurityKeysRotateRequest, AuthContext | None], dict[str, Any]],
    messages_verify: Callable[[MessageVerifyRequest, AuthContext | None], dict[str, Any]],
    metrics: Callable[[AuthContext | None], dict[str, Any]],
    replay_messages: Callable[[MessageReplayRequest, AuthContext | None], dict[str, Any]],
    replication_pull: Callable[[ReplicationPullRequest, AuthContext | None], dict[str, Any]],
    replication_push: Callable[[ReplicationPushRequest, AuthContext | None], dict[str, Any]],
    messages_send: Callable[[MessageSendRequest, AuthContext | None], dict[str, Any]],
    messages_ack: Callable[[MessageAckRequest, AuthContext | None], dict[str, Any]],
    messages_pending: Callable[..., dict[str, Any]],
    messages_inbox: Callable[[str, int, AuthContext | None], dict[str, Any]],
    messages_thread: Callable[[str, int, AuthContext | None], dict[str, Any]],
    relay_forward: Callable[[RelayForwardRequest, AuthContext | None], dict[str, Any]],
    compact_run: Callable[[CompactRequest, AuthContext | None], dict[str, Any]],
    backup_create: Callable[[BackupCreateRequest, AuthContext | None], dict[str, Any]],
    backup_restore_test: Callable[[BackupRestoreTestRequest, AuthContext | None], dict[str, Any]],
    ops_catalog: Callable[[AuthContext | None], dict[str, Any]],
    ops_status: Callable[[int, AuthContext | None], dict[str, Any]],
    ops_run: Callable[[OpsRunRequest, AuthContext | None], dict[str, Any]],
    ops_schedule_export: Callable[[str, AuthContext | None], dict[str, Any]],
) -> dict[str, Any]:
    """Dispatch one discovery tool invocation onto the provided route callbacks."""
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
        return write_file(WriteRequest(**args), auth)
    if name == "memory.append_jsonl":
        return append_record(AppendRequest(**args), auth)
    if name == "memory.read":
        return read_file(str(args["path"]), auth)
    if name == "index.rebuild_full":
        return index_rebuild(auth)
    if name == "index.rebuild_incremental":
        return index_rebuild_incremental(auth)
    if name == "index.status":
        return index_status(auth)
    if name == "peers.list":
        return peers_list(auth)
    if name == "peers.register":
        return peers_register(PeerRegisterRequest(**args), auth)
    if name == "peers.trust_transition":
        req_args = dict(args)
        peer_id = str(req_args.pop("peer_id"))
        return peers_trust_transition(peer_id, PeerTrustTransitionRequest(**req_args), auth)
    if name == "peers.fetch_manifest":
        return peer_manifest(str(args["peer_id"]), auth)
    if name == "search.query":
        return search(SearchRequest(**args), auth)
    if name == "recent.list":
        return recent_list(RecentRequest(**args), auth)
    if name == "context.retrieve":
        return context_retrieve(ContextRetrieveRequest(**args), auth)
    if name == "continuity.upsert":
        return continuity_upsert(ContinuityUpsertRequest(**args), auth)
    if name == "continuity.read":
        return continuity_read(ContinuityReadRequest(**args), auth)
    if name == "continuity.compare":
        return continuity_compare(ContinuityCompareRequest(**args), auth)
    if name == "continuity.revalidate":
        return continuity_revalidate(ContinuityRevalidateRequest(**args), auth)
    if name == "continuity.refresh_plan":
        return continuity_refresh_plan(ContinuityRefreshPlanRequest(**args), auth)
    if name == "continuity.list":
        return continuity_list(ContinuityListRequest(**args), auth)
    if name == "continuity.archive":
        return continuity_archive(ContinuityArchiveRequest(**args), auth)
    if name == "continuity.delete":
        return continuity_delete(ContinuityDeleteRequest(**args), auth)
    if name == "coordination.handoff_create":
        return handoff_create(CoordinationHandoffCreateRequest(**args), auth)
    if name == "coordination.handoff_read":
        return handoff_read(str(args["handoff_id"]), auth)
    if name == "coordination.handoffs_query":
        return handoff_query(CoordinationHandoffQueryRequest(**args), auth)
    if name == "coordination.handoff_consume":
        req_args = dict(args)
        handoff_id = str(req_args.pop("handoff_id"))
        return handoff_consume(handoff_id, CoordinationHandoffConsumeRequest(**req_args), auth)
    if name == "context.snapshot_create":
        return context_snapshot_create(ContextSnapshotRequest(**args), auth)
    if name == "context.snapshot_get":
        return context_snapshot_get(str(args["snapshot_id"]), auth)
    if name == "tasks.create":
        return tasks_create(TaskCreateRequest(**args), auth)
    if name == "tasks.update":
        req_args = dict(args)
        task_id = str(req_args.pop("task_id"))
        return tasks_update(task_id, TaskUpdateRequest(**req_args), auth)
    if name == "tasks.query":
        return tasks_query(
            status=args.get("status"),
            owner_peer=args.get("owner_peer"),
            collaborator=args.get("collaborator"),
            thread_id=args.get("thread_id"),
            limit=int(args.get("limit", 100)),
            auth=auth,
        )
    if name == "docs.patch_propose":
        return docs_patch_propose(PatchProposeRequest(**args), auth)
    if name == "docs.patch_apply":
        return docs_patch_apply(PatchApplyRequest(**args), auth)
    if name == "code.patch_propose":
        return code_patch_propose(PatchProposeRequest(**args), auth)
    if name == "code.checks_run":
        return code_checks_run(CodeCheckRunRequest(**args), auth)
    if name == "code.merge":
        return code_merge(CodeMergeRequest(**args), auth)
    if name == "security.tokens_list":
        return security_tokens_list(
            peer_id=args.get("peer_id"),
            status=args.get("status"),
            include_inactive=bool(args.get("include_inactive", False)),
            auth=auth,
        )
    if name == "security.tokens_issue":
        return security_tokens_issue(SecurityTokenIssueRequest(**args), auth)
    if name == "security.tokens_revoke":
        return security_tokens_revoke(SecurityTokenRevokeRequest(**args), auth)
    if name == "security.tokens_rotate":
        return security_tokens_rotate(SecurityTokenRotateRequest(**args), auth)
    if name == "security.keys_rotate":
        return security_keys_rotate(SecurityKeysRotateRequest(**args), auth)
    if name == "messages.verify":
        return messages_verify(MessageVerifyRequest(**args), auth)
    if name == "metrics.get":
        return metrics(auth)
    if name == "messages.replay":
        return replay_messages(MessageReplayRequest(**args), auth)
    if name == "replication.pull":
        return replication_pull(ReplicationPullRequest(**args), auth)
    if name == "replication.push":
        return replication_push(ReplicationPushRequest(**args), auth)
    if name == "messages.send":
        return messages_send(MessageSendRequest(**args), auth)
    if name == "messages.ack":
        return messages_ack(MessageAckRequest(**args), auth)
    if name == "messages.pending":
        return messages_pending(
            recipient=args.get("recipient"),
            status=args.get("status"),
            include_terminal=bool(args.get("include_terminal", False)),
            limit=int(args.get("limit", 50)),
            auth=auth,
        )
    if name == "messages.inbox":
        return messages_inbox(str(args["recipient"]), int(args.get("limit", 20)), auth)
    if name == "messages.thread":
        return messages_thread(str(args["thread_id"]), int(args.get("limit", 100)), auth)
    if name == "messages.relay_forward":
        return relay_forward(RelayForwardRequest(**args), auth)
    if name == "memory.compaction_plan":
        return compact_run(CompactRequest(**args), auth)
    if name == "backup.create":
        return backup_create(BackupCreateRequest(**args), auth)
    if name == "backup.restore_test":
        return backup_restore_test(BackupRestoreTestRequest(**args), auth)
    if name == "ops.catalog":
        return ops_catalog(auth)
    if name == "ops.status":
        return ops_status(int(args.get("limit", 50)), auth)
    if name == "ops.run":
        return ops_run(OpsRunRequest(**args), auth)
    if name == "ops.schedule_export":
        return ops_schedule_export(str(args.get("format", "systemd")), auth)
    raise ValueError(f"Unknown tool: {name}")


def _mcp_list_tools_result(tools: list[dict[str, Any]]) -> dict[str, Any]:
    """Convert the internal tool catalog into the MCP tools/list result shape."""
    rows = []
    for t in tools:
        rows.append(
            {
                "name": t["name"],
                "description": t["description"],
                "inputSchema": t["input_schema"],
                "metadata": {
                    "method": t["method"],
                    "path": t["path"],
                    "scopes": t["scopes"],
                    "idempotent": t["idempotent"],
                    "local_only": bool(t.get("local_only", False)),
                },
            }
        )
    return {"tools": rows}


def _mcp_initialize_result(contract_version: str, params: dict[str, Any]) -> dict[str, Any]:
    """Build the MCP initialize result payload."""
    requested_protocol = params.get("protocolVersion", contract_version)
    return {
        "protocolVersion": str(requested_protocol),
        "capabilities": {
            "tools": {"listChanged": False},
            "sampling": {},
        },
        "serverInfo": {
            "name": "cognirelay",
            "version": contract_version,
        },
        "instructions": (
            "Use tools/list to discover tool schemas, then tools/call for execution. "
            "Prefer /v1/discovery for supplemental HTTP-native guidance."
        ),
    }


def tool_schema_lookup(name: str, tools: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Look up one tool definition by name from the catalog."""
    for t in tools:
        if t["name"] == name:
            return t
    return None


def handle_mcp_rpc_request(
    request_payload: Any,
    *,
    authorization: str | None,
    x_forwarded_for: str | None,
    x_real_ip: str | None,
    request: Any,
    contract_version: str,
    tools: list[dict[str, Any]],
    resolve_auth_context: Callable[..., AuthContext | None],
    invoke_tool_by_name: Callable[[str, dict[str, Any], AuthContext | None], dict[str, Any]],
) -> dict[str, Any] | None:
    """Handle one MCP-compatible JSON-RPC request payload."""
    if not isinstance(request_payload, dict):
        return _rpc_error(None, -32600, "Invalid Request")

    request_id = request_payload.get("id")
    is_notification = "id" not in request_payload
    if request_payload.get("jsonrpc") != "2.0":
        return _rpc_error(request_id, -32600, "Invalid Request: jsonrpc must be '2.0'")

    method = request_payload.get("method")
    params = request_payload.get("params", {})
    if not isinstance(params, dict):
        return _rpc_error(request_id, -32602, "Invalid params: params must be an object")

    if method == "initialize":
        return _rpc_ok(request_id, _mcp_initialize_result(contract_version, params))

    if method == "notifications/initialized":
        if is_notification:
            return None
        return _rpc_ok(request_id, {"acknowledged": True})

    if method == "ping":
        return _rpc_ok(request_id, {"ok": True, "ts": datetime.now(timezone.utc).isoformat()})

    if method == "tools/list":
        return _rpc_ok(request_id, _mcp_list_tools_result(tools))
    if method != "tools/call":
        return _rpc_error(request_id, -32601, f"Method not found: {method}")

    name = params.get("name")
    arguments = params.get("arguments", {})
    if not isinstance(name, str) or not name:
        return _rpc_error(request_id, -32602, "Invalid params: 'name' is required")

    tool = tool_schema_lookup(name, tools)
    if tool is None:
        return _rpc_error(request_id, -32602, f"Unknown tool: {name}")

    auth_required = bool(tool.get("scopes"))
    try:
        auth = resolve_auth_context(
            authorization,
            required=auth_required,
            x_forwarded_for=x_forwarded_for,
            x_real_ip=x_real_ip,
            request=request,
        )
        result = invoke_tool_by_name(name, arguments, auth)
    except ValidationError as e:
        return _rpc_error(request_id, -32602, "Invalid params", data=e.errors())
    except HTTPException as e:
        if e.status_code == 401:
            return _rpc_error(request_id, -32001, "Unauthorized", data=e.detail)
        if e.status_code == 403:
            return _rpc_error(request_id, -32002, "Forbidden", data=e.detail)
        if e.status_code == 404:
            return _rpc_error(request_id, -32004, "Not Found", data=e.detail)
        return _rpc_error(request_id, -32003, "Tool execution failed", data=e.detail)
    except (KeyError, TypeError, ValueError) as e:
        return _rpc_error(request_id, -32602, "Invalid params", data=str(e))
    except Exception as e:
        return _rpc_error(request_id, -32003, "Tool execution failed", data=str(e))

    return _rpc_ok(
        request_id,
        {
            "toolName": name,
            "content": [{"type": "text", "text": f"Executed {name}"}],
            "structuredContent": result,
        },
    )


def health_payload(*, app_version: str, contract_version: str, repo_root: str, git_initialized: bool, latest_commit: str | None, signed_ingress_required: bool) -> dict:
    """Build the public health payload for the service."""
    return {
        "ok": True,
        "service": "cognirelay",
        "version": app_version,
        "contract_version": contract_version,
        "repo_root": repo_root,
        "git_initialized": git_initialized,
        "latest_commit": latest_commit,
        "signed_ingress_required": signed_ingress_required,
        "time": datetime.now(timezone.utc).isoformat(),
    }


def capabilities_payload() -> dict[str, Any]:
    """Return the high-level feature flags exposed by the service."""
    return {
        "features": [
            "write",
            "read",
            "append_jsonl",
            "index_rebuild",
            "search",
            "context_retrieve",
            "continuity_state",
            "context_snapshot",
            "messages_send_inbox",
            "compact_summary",
            "audit_log",
            "peer_tokens_json",
            "peers_registry",
            "sqlite_fts_search",
            "index_incremental",
            "relay_forward",
            "messages_reliable_delivery",
            "discovery_manifest",
            "discovery_tools",
            "discovery_workflows",
            "mcp_jsonrpc",
            "tasks_workflows",
            "patch_proposals",
            "checks_and_merge_policy",
            "security_keys_rotation",
            "security_token_lifecycle",
            "messages_signature_verify",
            "strict_signed_ingress",
            "metrics_endpoint",
            "messages_replay",
            "replication_push_pull",
            "trust_policy_transitions",
            "backup_restore_validation",
            "api_contract_versioning",
            "governance_policy_pack",
            "abuse_controls",
            "host_ops_orchestration",
        ]
    }


def manifest_payload(*, app_version: str) -> dict[str, Any]:
    """Build the machine-first endpoint manifest for autonomous clients."""
    return {
        "service": "cognirelay",
        "version": app_version,
        "auth": {"type": "bearer", "header": "Authorization: Bearer <token>"},
        "endpoints": {
            "GET /health": {"scope": None},
            "GET /capabilities": {"scope": None},
            "GET /v1/manifest": {"scope": None},
            "GET /v1/contracts": {"scope": None},
            "GET /v1/governance/policy": {"scope": None},
            "GET /v1/discovery": {"scope": None},
            "GET /v1/discovery/tools": {"scope": None},
            "GET /v1/discovery/workflows": {"scope": None},
            "GET /.well-known/cognirelay.json": {"scope": None},
            "GET /.well-known/mcp.json": {"scope": None},
            "POST /v1/mcp": {"scope": "mixed (depends on tool)"},
            "POST /v1/write": {"scope": "write:* by write_namespace"},
            "POST /v1/append": {"scope": "write:* by write_namespace"},
            "GET /v1/read": {"scope": "read:files"},
            "POST /v1/index/rebuild": {"scope": "read:index"},
            "POST /v1/index/rebuild-incremental": {"scope": "read:index"},
            "GET /v1/index/status": {"scope": "read:index"},
            "GET /v1/peers": {"scope": "read:files"},
            "POST /v1/peers/register": {"scope": "admin:peers"},
            "POST /v1/peers/{peer_id}/trust": {"scope": "admin:peers"},
            "GET /v1/peers/{peer_id}/manifest": {"scope": "read:files"},
            "POST /v1/search": {"scope": "search"},
            "POST /v1/recent": {"scope": "search"},
            "POST /v1/context/retrieve": {"scope": "search"},
            "POST /v1/continuity/upsert": {"scope": "write:projects"},
            "POST /v1/continuity/read": {"scope": "read:files"},
            "POST /v1/continuity/compare": {"scope": "read:files"},
            "POST /v1/continuity/revalidate": {"scope": "write:projects"},
            "POST /v1/continuity/refresh/plan": {"scope": "read:files + write:projects + write_namespaces"},
            "POST /v1/continuity/list": {"scope": "read:files"},
            "POST /v1/continuity/archive": {"scope": "write:projects"},
            "POST /v1/continuity/delete": {"scope": "write:projects"},
            "POST /v1/coordination/handoff/create": {"scope": "write:projects"},
            "GET /v1/coordination/handoff/{handoff_id}": {"scope": "authenticated sender|recipient|admin"},
            "GET /v1/coordination/handoffs/query": {"scope": "read:files"},
            "POST /v1/coordination/handoff/{handoff_id}/consume": {"scope": "authenticated recipient"},
            "POST /v1/context/snapshot": {"scope": "search + write:projects"},
            "GET /v1/context/snapshot/{snapshot_id}": {"scope": "read:files"},
            "POST /v1/tasks": {"scope": "write:projects"},
            "PATCH /v1/tasks/{task_id}": {"scope": "write:projects"},
            "GET /v1/tasks/query": {"scope": "read:files"},
            "POST /v1/docs/patch/propose": {"scope": "write:projects"},
            "POST /v1/docs/patch/apply": {"scope": "write:projects"},
            "POST /v1/code/patch/propose": {"scope": "write:projects"},
            "POST /v1/code/checks/run": {"scope": "write:projects"},
            "POST /v1/code/merge": {"scope": "write:projects"},
            "GET /v1/security/tokens": {"scope": "admin:peers"},
            "POST /v1/security/tokens/issue": {"scope": "admin:peers"},
            "POST /v1/security/tokens/revoke": {"scope": "admin:peers"},
            "POST /v1/security/tokens/rotate": {"scope": "admin:peers"},
            "POST /v1/security/keys/rotate": {"scope": "admin:peers"},
            "POST /v1/messages/verify": {"scope": "write:messages"},
            "GET /v1/metrics": {"scope": "read:index"},
            "POST /v1/replay/messages": {"scope": "write:messages"},
            "POST /v1/replication/pull": {"scope": "admin:peers"},
            "POST /v1/replication/push": {"scope": "admin:peers"},
            "POST /v1/messages/send": {"scope": "write:messages"},
            "POST /v1/messages/ack": {"scope": "write:messages"},
            "GET /v1/messages/pending": {"scope": "read:files"},
            "GET /v1/messages/inbox": {"scope": "read:files"},
            "GET /v1/messages/thread": {"scope": "read:files"},
            "POST /v1/relay/forward": {"scope": "write:messages"},
            "POST /v1/compact/run": {"scope": "compact:trigger"},
            "POST /v1/backup/create": {"scope": "admin:peers"},
            "POST /v1/backup/restore-test": {"scope": "admin:peers"},
            "GET /v1/ops/catalog": {"scope": "admin:peers (local-only)"},
            "GET /v1/ops/status": {"scope": "admin:peers (local-only)"},
            "POST /v1/ops/run": {"scope": "admin:peers (local-only)"},
            "GET /v1/ops/schedule/export": {"scope": "admin:peers (local-only)"},
        },
    }


def contracts_payload(*, contract_version: str, tools: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the frozen contract metadata payload for tool compatibility checks."""
    return {
        "ok": True,
        "contract_version": contract_version,
        "compatibility": {
            "policy": "backward-compatible additive changes within same contract_version",
            "breaking_change_rule": "increment contract_version before breaking fields/methods",
        },
        "tool_catalog_hash": hashlib.sha256(canonical_json(tools).encode("utf-8")).hexdigest(),
    }
