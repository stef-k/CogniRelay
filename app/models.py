"""Pydantic request and state models used across the API surface."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class WriteRequest(BaseModel):
    """Request payload for writing a full file into the repository."""
    path: str
    content: str
    commit_message: Optional[str] = None


class AppendRequest(BaseModel):
    """Request payload for appending one JSONL record to a file."""
    path: str
    record: Dict[str, Any]
    commit_message: Optional[str] = None


class SearchRequest(BaseModel):
    """Search query parameters for the local index."""
    query: str
    limit: int = Field(default=10, ge=1, le=100)
    include_types: List[str] = Field(default_factory=list)
    sort_by: Literal["relevance", "recent"] = "relevance"
    time_window_hours: Optional[int] = Field(default=None, ge=1, le=87600)


class ContextRetrieveRequest(BaseModel):
    """Context retrieval parameters for search-backed continuation bundles."""
    task: str
    subject_kind: Optional[Literal["user", "peer", "thread", "task"]] = None
    subject_id: Optional[str] = Field(default=None, max_length=200)
    continuity_mode: Literal["auto", "required", "off"] = "auto"
    max_tokens_estimate: int = Field(default=4000, ge=256, le=100000)
    include_types: List[str] = Field(default_factory=list)
    time_window_days: int = Field(default=30, ge=1, le=3650)
    limit: int = Field(default=10, ge=1, le=100)


class RecentRequest(BaseModel):
    """Parameters for listing recent repository files."""
    limit: int = Field(default=10, ge=1, le=100)
    include_types: List[str] = Field(default_factory=list)
    time_window_hours: Optional[int] = Field(default=None, ge=1, le=87600)
    time_window_days: Optional[int] = Field(default=None, ge=1, le=3650)


class SnapshotAsOfRequest(BaseModel):
    """Selector for creating a snapshot from the working tree or history."""
    mode: Literal["working_tree", "commit", "timestamp"] = "working_tree"
    value: Optional[str] = None


class ContextSnapshotRequest(BaseModel):
    """Request payload for deterministic context snapshots."""
    task: str
    as_of: SnapshotAsOfRequest = Field(default_factory=SnapshotAsOfRequest)
    include_types: List[str] = Field(default_factory=list)
    limit: int = Field(default=20, ge=1, le=200)
    include_core: bool = True


class DeliveryPolicy(BaseModel):
    """Delivery behavior attached to outbound messages."""
    requires_ack: bool = False
    ack_timeout_seconds: int = Field(default=300, ge=1, le=86400)
    max_retries: int = Field(default=5, ge=0, le=100)


class SignedEnvelope(BaseModel):
    """Message signing metadata carried with signed requests."""
    key_id: str
    nonce: str
    expires_at: Optional[str] = None
    signature: str
    algorithm: Literal["hmac-sha256"] = "hmac-sha256"
    consume_nonce: bool = True


class MessageSendRequest(BaseModel):
    """Outbound direct message payload."""
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
    """Request payload for compaction plan generation."""
    source_path: Optional[str] = None
    note: Optional[str] = None


class RelayForwardRequest(BaseModel):
    """Request payload for forwarding a message through a relay."""
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
    """Acknowledgement payload for previously delivered messages."""
    message_id: str
    status: Literal["accepted", "rejected", "deferred"]
    reason: Optional[str] = None
    ack_id: Optional[str] = None


class PeerRegisterRequest(BaseModel):
    """Peer registry entry creation payload."""
    peer_id: str
    base_url: str
    public_key: Optional[str] = None
    capabilities_url: str = "/v1/manifest"
    trust_level: Literal["trusted", "restricted", "untrusted"] = "restricted"
    allowed_scopes: List[str] = Field(default_factory=list)
    expected_public_key_fingerprint: Optional[str] = None
    transition_reason: Optional[str] = None


class PeerTrustTransitionRequest(BaseModel):
    """Peer trust-level transition payload."""
    trust_level: Literal["trusted", "restricted", "untrusted"]
    reason: str = Field(min_length=3, max_length=500)
    expected_public_key_fingerprint: Optional[str] = None


class TaskCreateRequest(BaseModel):
    """Task creation payload for shared task records."""
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
    """Mutable fields for updating an existing task record."""
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
    """Patch proposal payload for docs or code workflows."""
    patch_id: Optional[str] = None
    target_path: str
    base_ref: str = "HEAD"
    format: Literal["unified_diff"] = "unified_diff"
    diff: str
    reason: Optional[str] = None
    thread_id: Optional[str] = None


class PatchApplyRequest(BaseModel):
    """Patch apply request referencing a previously proposed patch."""
    patch_id: str
    commit_message: Optional[str] = None


class CodeCheckRunRequest(BaseModel):
    """Code check execution request."""
    ref: str = "HEAD"
    profile: Literal["lint", "test", "build"] = "test"


class CodeMergeRequest(BaseModel):
    """Merge request gated by prior code check results."""
    source_ref: str
    target_ref: str = "HEAD"
    required_checks: List[Literal["lint", "test", "build"]] = Field(default_factory=lambda: ["test"])


class SecurityKeysRotateRequest(BaseModel):
    """Security key rotation request."""
    key_id: Optional[str] = None
    secret: Optional[str] = None
    activate: bool = True
    retire_previous: bool = False
    return_secret: bool = False


class SecurityTokenIssueRequest(BaseModel):
    """Token issuance request for a peer."""
    peer_id: str
    scopes: List[str] = Field(default_factory=list)
    read_namespaces: List[str] = Field(default_factory=list)
    write_namespaces: List[str] = Field(default_factory=list)
    expires_at: Optional[str] = None
    ttl_seconds: Optional[int] = Field(default=None, ge=1, le=31536000)
    description: Optional[str] = None
    token_id: Optional[str] = None


class SecurityTokenRevokeRequest(BaseModel):
    """Token revocation request by id, hash, or peer."""
    token_id: Optional[str] = None
    token_sha256: Optional[str] = None
    peer_id: Optional[str] = None
    revoke_all_for_peer: bool = False
    reason: Optional[str] = None


class SecurityTokenRotateRequest(BaseModel):
    """Token rotation request with optional metadata overrides."""
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
    """Signed payload verification request."""
    payload: Dict[str, Any]
    key_id: str
    nonce: str
    expires_at: Optional[str] = None
    signature: str
    algorithm: Literal["hmac-sha256"] = "hmac-sha256"
    consume_nonce: bool = True


class MessageReplayRequest(BaseModel):
    """Replay request for dead-letter or deferred messages."""
    message_id: str
    reason: Optional[str] = None
    force: bool = False
    ack_timeout_seconds: int = Field(default=300, ge=1, le=86400)
    requires_ack: bool = True


class ReplicationFilePayload(BaseModel):
    """One file entry included in a replication pull payload."""
    path: str
    content: Optional[str] = None
    sha256: Optional[str] = None
    modified_at: Optional[str] = None
    deleted: bool = False
    tombstone_at: Optional[str] = None


class ReplicationPullRequest(BaseModel):
    """Inbound replication request carrying file state."""
    source_peer: str
    files: List[ReplicationFilePayload] = Field(default_factory=list)
    mode: Literal["upsert", "overwrite"] = "upsert"
    conflict_policy: Literal["last_write_wins", "source_wins", "target_wins", "error"] = "last_write_wins"
    idempotency_key: Optional[str] = None
    commit_message: Optional[str] = None


class ReplicationPushRequest(BaseModel):
    """Outbound replication request describing a push operation."""
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
    """Backup creation request."""
    include_prefixes: List[str] = Field(default_factory=lambda: ["memory", "messages", "tasks", "patches", "runs", "projects", "essays", "journal", "snapshots", "peers", "config", "logs"])
    note: Optional[str] = None


class BackupRestoreTestRequest(BaseModel):
    """Restore-test request for an existing backup archive."""
    backup_path: str
    verify_index_rebuild: bool = True


class OpsRunRequest(BaseModel):
    """Host-local operations runner payload."""
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


class ContinuitySource(BaseModel):
    """Metadata about how a continuity capsule was produced."""
    producer: str = Field(min_length=1, max_length=100)
    update_reason: Literal["startup_refresh", "pre_compaction", "interaction_boundary", "manual", "migration"]
    inputs: List[str] = Field(default_factory=list, max_length=12)


class ContinuityRelationshipModel(BaseModel):
    """Relationship-specific continuity hints for a subject."""
    trust_level: Optional[Literal["low", "guarded", "normal", "high"]] = None
    preferred_style: List[str] = Field(default_factory=list, max_length=5)
    sensitivity_notes: List[str] = Field(default_factory=list, max_length=5)


class ContinuityRetrievalHints(BaseModel):
    """Retrieval preferences embedded in a continuity capsule."""
    must_include: List[str] = Field(default_factory=list, max_length=8)
    avoid: List[str] = Field(default_factory=list, max_length=8)
    load_next: List[str] = Field(default_factory=list, max_length=8)


class ContinuityAttentionPolicy(BaseModel):
    """Attention allocation hints used during continuity loading."""
    early_load: List[str] = Field(default_factory=list, max_length=8)
    presence_bias_overrides: List[str] = Field(default_factory=list, max_length=5)


class ContinuityConfidence(BaseModel):
    """Confidence values attached to continuity inferences."""
    continuity: float = Field(ge=0.0, le=1.0)
    relationship_model: float = Field(ge=0.0, le=1.0)


class ContinuityFreshness(BaseModel):
    """Freshness metadata for continuity decay and expiration rules."""
    freshness_class: Optional[Literal["persistent", "durable", "situational", "ephemeral"]] = None
    expires_at: Optional[str] = None
    stale_after_seconds: Optional[int] = Field(default=None, ge=300, le=31536000)


class ContinuityState(BaseModel):
    """Operational orientation state preserved across resets and compaction."""
    top_priorities: List[str] = Field(max_length=5)
    active_concerns: List[str] = Field(max_length=5)
    active_constraints: List[str] = Field(max_length=5)
    open_loops: List[str] = Field(max_length=5)
    stance_summary: str = Field(max_length=240)
    drift_signals: List[str] = Field(max_length=5)
    working_hypotheses: List[str] = Field(default_factory=list, max_length=5)
    relationship_model: Optional[ContinuityRelationshipModel] = None
    retrieval_hints: Optional[ContinuityRetrievalHints] = None
    long_horizon_commitments: List[str] = Field(default_factory=list, max_length=5)


class ContinuityCapsule(BaseModel):
    """Persisted continuity capsule for one subject."""
    schema_version: Literal["1.0"] = "1.0"
    subject_kind: Literal["user", "peer", "thread", "task"]
    subject_id: str = Field(min_length=1, max_length=200)
    updated_at: str
    verified_at: str
    source: ContinuitySource
    continuity: ContinuityState
    confidence: ContinuityConfidence
    verification_kind: Optional[Literal["self_review", "external_observation", "user_confirmation", "peer_confirmation", "system_check"]] = None
    attention_policy: Optional[ContinuityAttentionPolicy] = None
    freshness: Optional[ContinuityFreshness] = None
    canonical_sources: List[str] = Field(default_factory=list, max_length=8)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ContinuityUpsertRequest(BaseModel):
    """Top-level request for storing or replacing a continuity capsule."""
    subject_kind: Literal["user", "peer", "thread", "task"]
    subject_id: str = Field(min_length=1, max_length=200)
    capsule: ContinuityCapsule
    commit_message: Optional[str] = None
    idempotency_key: Optional[str] = Field(default=None, max_length=200)
