from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class WriteRequest(BaseModel):
    path: str
    content: str
    commit_message: Optional[str] = None


class AppendRequest(BaseModel):
    path: str
    record: Dict[str, Any]
    commit_message: Optional[str] = None


class SearchRequest(BaseModel):
    query: str
    limit: int = Field(default=10, ge=1, le=100)
    include_types: List[str] = Field(default_factory=list)
    sort_by: Literal["relevance", "recent"] = "relevance"
    time_window_hours: Optional[int] = Field(default=None, ge=1, le=87600)


class ContextRetrieveRequest(BaseModel):
    task: str
    max_tokens_estimate: int = Field(default=4000, ge=256, le=100000)
    include_types: List[str] = Field(default_factory=list)
    time_window_days: int = Field(default=30, ge=1, le=3650)
    limit: int = Field(default=10, ge=1, le=100)


class RecentRequest(BaseModel):
    limit: int = Field(default=10, ge=1, le=100)
    include_types: List[str] = Field(default_factory=list)
    time_window_hours: Optional[int] = Field(default=None, ge=1, le=87600)
    time_window_days: Optional[int] = Field(default=None, ge=1, le=3650)


class SnapshotAsOfRequest(BaseModel):
    mode: Literal["working_tree", "commit", "timestamp"] = "working_tree"
    value: Optional[str] = None


class ContextSnapshotRequest(BaseModel):
    task: str
    as_of: SnapshotAsOfRequest = Field(default_factory=SnapshotAsOfRequest)
    include_types: List[str] = Field(default_factory=list)
    limit: int = Field(default=20, ge=1, le=200)
    include_core: bool = True


class DeliveryPolicy(BaseModel):
    requires_ack: bool = False
    ack_timeout_seconds: int = Field(default=300, ge=1, le=86400)
    max_retries: int = Field(default=5, ge=0, le=100)


class SignedEnvelope(BaseModel):
    key_id: str
    nonce: str
    expires_at: Optional[str] = None
    signature: str
    algorithm: Literal["hmac-sha256"] = "hmac-sha256"
    consume_nonce: bool = True


class MessageSendRequest(BaseModel):
    thread_id: str
    sender: str
    recipient: str
    subject: str
    body_md: str
    priority: str = "normal"
    attachments: List[str] = Field(default_factory=list)
    idempotency_key: Optional[str] = None
    delivery: DeliveryPolicy = Field(default_factory=DeliveryPolicy)
    signed_envelope: Optional[SignedEnvelope] = None


class CompactRequest(BaseModel):
    source_path: Optional[str] = None
    note: Optional[str] = None


class RelayForwardRequest(BaseModel):
    relay_id: str = "relay-local"
    target_recipient: str
    thread_id: str
    sender: str
    subject: str
    body_md: str
    priority: str = "normal"
    attachments: List[str] = Field(default_factory=list)
    envelope: Dict[str, Any] = Field(default_factory=dict)
    signed_envelope: Optional[SignedEnvelope] = None


class MessageAckRequest(BaseModel):
    message_id: str
    status: Literal["accepted", "rejected", "deferred"]
    reason: Optional[str] = None
    ack_id: Optional[str] = None


class PeerRegisterRequest(BaseModel):
    peer_id: str
    base_url: str
    public_key: Optional[str] = None
    capabilities_url: str = "/v1/manifest"
    trust_level: Literal["trusted", "restricted", "untrusted"] = "restricted"
    allowed_scopes: List[str] = Field(default_factory=list)
    expected_public_key_fingerprint: Optional[str] = None
    transition_reason: Optional[str] = None


class PeerTrustTransitionRequest(BaseModel):
    trust_level: Literal["trusted", "restricted", "untrusted"]
    reason: str = Field(min_length=3, max_length=500)
    expected_public_key_fingerprint: Optional[str] = None


class TaskCreateRequest(BaseModel):
    task_id: str
    title: str
    description: str = ""
    status: Literal["open", "in_progress", "blocked", "done"] = "open"
    owner_peer: str
    collaborators: List[str] = Field(default_factory=list)
    thread_id: Optional[str] = None
    blocked_by: List[str] = Field(default_factory=list)
    due_at: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class TaskUpdateRequest(BaseModel):
    status: Optional[Literal["open", "in_progress", "blocked", "done"]] = None
    title: Optional[str] = None
    description: Optional[str] = None
    owner_peer: Optional[str] = None
    collaborators: Optional[List[str]] = None
    thread_id: Optional[str] = None
    blocked_by: Optional[List[str]] = None
    due_at: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class PatchProposeRequest(BaseModel):
    patch_id: Optional[str] = None
    target_path: str
    base_ref: str = "HEAD"
    format: Literal["unified_diff"] = "unified_diff"
    diff: str
    reason: Optional[str] = None
    thread_id: Optional[str] = None


class PatchApplyRequest(BaseModel):
    patch_id: str
    commit_message: Optional[str] = None


class CodeCheckRunRequest(BaseModel):
    ref: str = "HEAD"
    profile: Literal["lint", "test", "build"] = "test"


class CodeMergeRequest(BaseModel):
    source_ref: str
    target_ref: str = "HEAD"
    required_checks: List[Literal["lint", "test", "build"]] = Field(default_factory=lambda: ["test"])


class SecurityKeysRotateRequest(BaseModel):
    key_id: Optional[str] = None
    secret: Optional[str] = None
    activate: bool = True
    retire_previous: bool = False
    return_secret: bool = False


class SecurityTokenIssueRequest(BaseModel):
    peer_id: str
    scopes: List[str] = Field(default_factory=list)
    read_namespaces: List[str] = Field(default_factory=list)
    write_namespaces: List[str] = Field(default_factory=list)
    expires_at: Optional[str] = None
    ttl_seconds: Optional[int] = Field(default=None, ge=1, le=31536000)
    description: Optional[str] = None
    token_id: Optional[str] = None


class SecurityTokenRevokeRequest(BaseModel):
    token_id: Optional[str] = None
    token_sha256: Optional[str] = None
    peer_id: Optional[str] = None
    revoke_all_for_peer: bool = False
    reason: Optional[str] = None


class SecurityTokenRotateRequest(BaseModel):
    token_id: Optional[str] = None
    token_sha256: Optional[str] = None
    new_token_id: Optional[str] = None
    scopes: Optional[List[str]] = None
    read_namespaces: Optional[List[str]] = None
    write_namespaces: Optional[List[str]] = None
    expires_at: Optional[str] = None
    ttl_seconds: Optional[int] = Field(default=None, ge=1, le=31536000)
    description: Optional[str] = None
    reason: Optional[str] = None


class MessageVerifyRequest(BaseModel):
    payload: Dict[str, Any]
    key_id: str
    nonce: str
    expires_at: Optional[str] = None
    signature: str
    algorithm: Literal["hmac-sha256"] = "hmac-sha256"
    consume_nonce: bool = True


class MessageReplayRequest(BaseModel):
    message_id: str
    reason: Optional[str] = None
    force: bool = False
    ack_timeout_seconds: int = Field(default=300, ge=1, le=86400)
    requires_ack: bool = True


class ReplicationFilePayload(BaseModel):
    path: str
    content: Optional[str] = None
    sha256: Optional[str] = None
    modified_at: Optional[str] = None
    deleted: bool = False
    tombstone_at: Optional[str] = None


class ReplicationPullRequest(BaseModel):
    source_peer: str
    files: List[ReplicationFilePayload] = Field(default_factory=list)
    mode: Literal["upsert", "overwrite"] = "upsert"
    conflict_policy: Literal["last_write_wins", "source_wins", "target_wins", "error"] = "last_write_wins"
    idempotency_key: Optional[str] = None
    commit_message: Optional[str] = None


class ReplicationPushRequest(BaseModel):
    peer_id: Optional[str] = None
    base_url: Optional[str] = None
    idempotency_key: Optional[str] = None
    target_path: str = "/v1/replication/pull"
    include_prefixes: List[str] = Field(
        default_factory=lambda: ["memory", "messages", "tasks", "patches", "runs", "projects", "essays", "journal", "snapshots"]
    )
    max_files: int = Field(default=2000, ge=1, le=10000)
    dry_run: bool = False
    target_token: Optional[str] = None
    include_deleted: bool = True
    conflict_policy: Literal["last_write_wins", "source_wins", "target_wins", "error"] = "last_write_wins"


class BackupCreateRequest(BaseModel):
    include_prefixes: List[str] = Field(default_factory=lambda: ["memory", "messages", "tasks", "patches", "runs", "projects", "essays", "journal", "snapshots", "peers", "config", "logs"])
    note: Optional[str] = None


class BackupRestoreTestRequest(BaseModel):
    backup_path: str
    verify_index_rebuild: bool = True


class OpsRunRequest(BaseModel):
    job_id: Literal[
        "index.rebuild_incremental",
        "metrics.poll_and_alarm_eval",
        "backup.create",
        "backup.restore_test",
        "replication.pull",
        "replication.push",
        "messages.replay_dead_letter_sweep",
        "security.rotation_check",
        "compact.plan",
    ]
    dry_run: bool = False
    force: bool = False
    arguments: Dict[str, Any] = Field(default_factory=dict)
