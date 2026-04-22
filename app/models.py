"""Pydantic request and state models used across the API surface."""

from __future__ import annotations

from typing import Annotated, Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


PeerId = Annotated[str, Field(min_length=1, max_length=200)]


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
    continuity_verification_policy: Literal["allow_degraded", "prefer_healthy", "require_healthy"] = "allow_degraded"
    continuity_resilience_policy: Literal["allow_fallback", "prefer_active", "require_active"] = "allow_fallback"
    continuity_selectors: List["ContinuitySelector"] = Field(default_factory=list, max_length=4)
    continuity_max_capsules: int = Field(default=1, ge=1, le=4)
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


class CoordinationHandoffSourceSelector(BaseModel):
    """Explicit continuity selector projected into a coordination handoff."""

    subject_kind: Literal["user", "peer", "thread", "task"]
    subject_id: str = Field(min_length=1, max_length=200)


class CoordinationHandoffSourceSummary(BaseModel):
    """Source-capsule summary copied into a coordination handoff artifact."""

    path: str
    updated_at: str
    verified_at: str
    verification_status: str
    health_status: str


class CoordinationHandoffSharedContinuity(BaseModel):
    """The bounded 5A continuity subset allowed to cross the peer boundary."""

    active_constraints: List[str] = Field(default_factory=list, max_length=5)
    drift_signals: List[str] = Field(default_factory=list, max_length=5)


class CoordinationHandoffArtifact(BaseModel):
    """Stored handoff artifact exchanged between one sender and one recipient."""

    schema_type: Literal["continuity_handoff"] = "continuity_handoff"
    schema_version: Literal["1.0"] = "1.0"
    handoff_id: str = Field(min_length=1, max_length=64)
    created_at: str
    created_by: str = Field(min_length=1, max_length=200)
    sender_peer: str = Field(min_length=1, max_length=200)
    recipient_peer: str = Field(min_length=1, max_length=200)
    source_selector: CoordinationHandoffSourceSelector
    source_summary: CoordinationHandoffSourceSummary
    task_id: Optional[str] = Field(default=None, max_length=200)
    thread_id: Optional[str] = Field(default=None, max_length=200)
    note: Optional[str] = Field(default=None, max_length=240)
    shared_continuity: CoordinationHandoffSharedContinuity
    recipient_status: Literal["pending", "accepted_advisory", "deferred", "rejected"] = "pending"
    recipient_reason: Optional[str] = Field(default=None, max_length=240)
    consumed_at: Optional[str] = None
    consumed_by: Optional[str] = Field(default=None, max_length=200)


class CoordinationHandoffCreateRequest(BaseModel):
    """Create request for one inter-agent continuity handoff artifact."""

    recipient_peer: str = Field(min_length=1, max_length=200)
    subject_kind: Literal["user", "peer", "thread", "task"]
    subject_id: str = Field(min_length=1, max_length=200)
    task_id: Optional[str] = Field(default=None, max_length=200)
    thread_id: Optional[str] = Field(default=None, max_length=200)
    note: Optional[str] = Field(default=None, max_length=240)
    commit_message: Optional[str] = None


class CoordinationHandoffQueryRequest(BaseModel):
    """Filter parameters for discovering visible handoff artifacts."""

    recipient_peer: Optional[str] = Field(default=None, max_length=200)
    sender_peer: Optional[str] = Field(default=None, max_length=200)
    status: Optional[Literal["pending", "accepted_advisory", "deferred", "rejected"]] = None
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=20, ge=1, le=100)


class CoordinationHandoffConsumeRequest(BaseModel):
    """Recipient-only consume request for one handoff artifact."""

    status: Literal["accepted_advisory", "deferred", "rejected"]
    reason: Optional[str] = Field(default=None, max_length=240)


class SharedCoordinationState(BaseModel):
    """The bounded 5B shared coordination payload visible across participants."""

    constraints: List[str] = Field(default_factory=list, max_length=8)
    drift_signals: List[str] = Field(default_factory=list, max_length=8)
    coordination_alerts: List[str] = Field(default_factory=list, max_length=8)


class CoordinationSharedArtifact(BaseModel):
    """Stored shared coordination artifact owned by one peer and visible to participants."""

    schema_type: Literal["coordination_shared_state"] = "coordination_shared_state"
    schema_version: Literal["1.0"] = "1.0"
    shared_id: str = Field(min_length=1, max_length=64)
    created_at: str
    updated_at: str
    created_by: str = Field(min_length=1, max_length=200)
    owner_peer: str = Field(min_length=1, max_length=200)
    participant_peers: List[PeerId] = Field(..., min_length=1, max_length=8)
    task_id: Optional[str] = Field(default=None, max_length=200)
    thread_id: Optional[str] = Field(default=None, max_length=200)
    title: str
    summary: Optional[str] = None
    shared_state: SharedCoordinationState
    version: int = Field(ge=1)
    last_updated_by: str = Field(min_length=1, max_length=200)


class CoordinationSharedCreateRequest(BaseModel):
    """Create request for one owner-authored shared coordination artifact."""

    participant_peers: List[PeerId] = Field(default_factory=list, min_length=1, max_length=8)
    task_id: Optional[str] = Field(default=None, max_length=200)
    thread_id: Optional[str] = Field(default=None, max_length=200)
    title: str
    summary: Optional[str] = None
    constraints: List[str] = Field(default_factory=list, max_length=8)
    drift_signals: List[str] = Field(default_factory=list, max_length=8)
    coordination_alerts: List[str] = Field(default_factory=list, max_length=8)
    commit_message: Optional[str] = None


class CoordinationSharedQueryRequest(BaseModel):
    """Filter parameters for discovering visible shared coordination artifacts."""

    owner_peer: Optional[str] = Field(default=None, max_length=200)
    participant_peer: Optional[str] = Field(default=None, max_length=200)
    task_id: Optional[str] = Field(default=None, max_length=200)
    thread_id: Optional[str] = Field(default=None, max_length=200)
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=20, ge=1, le=100)

    @model_validator(mode="after")
    def _require_one_filter(self) -> "CoordinationSharedQueryRequest":
        """Require at least one query filter for shared coordination discovery."""
        if self.owner_peer is None and self.participant_peer is None and self.task_id is None and self.thread_id is None:
            raise ValueError("owner_peer, participant_peer, task_id, or thread_id is required")
        return self


class CoordinationSharedUpdateRequest(BaseModel):
    """Owner-only replacement request for one shared coordination artifact."""

    model_config = ConfigDict(extra="forbid")

    expected_version: int = Field(ge=1)
    title: str
    summary: Optional[str] = None
    constraints: List[str] = Field(default_factory=list, max_length=8)
    drift_signals: List[str] = Field(default_factory=list, max_length=8)
    coordination_alerts: List[str] = Field(default_factory=list, max_length=8)
    commit_message: Optional[str] = None


class CoordinationReconciliationClaim(BaseModel):
    """One bounded coordination claim carried inside a reconciliation record."""

    model_config = ConfigDict(extra="forbid")

    source_kind: Literal["handoff", "shared"]
    source_id: str = Field(min_length=1, max_length=64)
    claimant_peer: PeerId
    claim_summary: str
    epistemic_status: Literal[
        "frame_present",
        "frame_absent_evidence_confirms",
        "frame_status_unknown",
    ]
    evidence_refs: List[str] = Field(default_factory=list, max_length=4)
    observed_version: Optional[int] = Field(default=None, ge=1)


class CoordinationReconciliationArtifact(BaseModel):
    """Stored reconciliation record for one bounded inter-agent disagreement."""

    schema_type: Literal["coordination_reconciliation_record"] = "coordination_reconciliation_record"
    schema_version: Literal["1.0"] = "1.0"
    reconciliation_id: str = Field(min_length=1, max_length=64)
    created_at: str
    updated_at: str
    opened_by: PeerId
    owner_peer: PeerId
    participant_peers: List[PeerId] = Field(default_factory=list, max_length=8)
    task_id: Optional[str] = Field(default=None, max_length=200)
    thread_id: Optional[str] = Field(default=None, max_length=200)
    title: str
    summary: Optional[str] = None
    classification: Literal["contradictory", "stale_observation", "frame_conflict", "concurrent_race"]
    trigger: Literal["handoff_vs_handoff", "shared_vs_shared", "handoff_vs_shared", "concurrent_mutation_race"]
    claims: List[CoordinationReconciliationClaim] = Field(default_factory=list, min_length=2, max_length=4)
    status: Literal["open", "resolved"] = "open"
    resolution_outcome: Optional[Literal["advisory_only", "conflicted", "rejected"]] = None
    resolution_summary: Optional[str] = None
    resolved_at: Optional[str] = None
    resolved_by: Optional[PeerId] = None
    version: int = Field(default=1, ge=1)
    last_updated_by: PeerId


class CoordinationReconciliationOpenRequest(BaseModel):
    """Open request for one bounded reconciliation record."""

    model_config = ConfigDict(extra="forbid")

    task_id: Optional[str] = Field(default=None, max_length=200)
    thread_id: Optional[str] = Field(default=None, max_length=200)
    title: str
    summary: Optional[str] = None
    classification: Literal["contradictory", "stale_observation", "frame_conflict", "concurrent_race"]
    trigger: Literal["handoff_vs_handoff", "shared_vs_shared", "handoff_vs_shared", "concurrent_mutation_race"]
    claims: List[CoordinationReconciliationClaim] = Field(default_factory=list, max_length=4)
    commit_message: Optional[str] = None


class CoordinationReconciliationQueryRequest(BaseModel):
    """Filter parameters for discovering visible reconciliation records."""

    model_config = ConfigDict(extra="forbid")

    owner_peer: Optional[str] = Field(default=None, max_length=200)
    claimant_peer: Optional[str] = Field(default=None, max_length=200)
    status: Optional[Literal["open", "resolved"]] = None
    classification: Optional[Literal["contradictory", "stale_observation", "frame_conflict", "concurrent_race"]] = None
    task_id: Optional[str] = Field(default=None, max_length=200)
    thread_id: Optional[str] = Field(default=None, max_length=200)
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=20, ge=1, le=100)

    @model_validator(mode="after")
    def _require_one_filter(self) -> "CoordinationReconciliationQueryRequest":
        """Require at least one bounded query filter."""
        if self.owner_peer is None and self.claimant_peer is None and self.status is None and self.classification is None and self.task_id is None and self.thread_id is None:
            raise ValueError("At least one reconciliation query filter is required")
        return self


class CoordinationReconciliationResolveRequest(BaseModel):
    """Resolve request for one open reconciliation record under version checking."""

    model_config = ConfigDict(extra="forbid")

    expected_version: int = Field(ge=1)
    outcome: Literal["advisory_only", "conflicted", "rejected"]
    resolution_summary: Optional[str] = None
    commit_message: Optional[str] = None


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
    include_prefixes: List[str] = Field(default_factory=lambda: ["memory", "messages", "tasks", "patches", "runs", "projects", "essays", "journal", "snapshots"])
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
    verify_continuity: bool = True


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
        "continuity_cold_store",
        "continuity_cold_rehydrate",
        "continuity_retention_apply",
        "artifact_history_cold_store",
        "artifact_history_cold_rehydrate",
        "registry_history_cold_store",
        "registry_history_cold_rehydrate",
        "segment_history_maintenance",
        "segment_history_cold_store",
        "segment_history_cold_rehydrate",
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


class ContinuityVerificationState(BaseModel):
    """Verification status recorded for one continuity capsule."""

    status: Literal[
        "unverified",
        "self_attested",
        "externally_supported",
        "user_confirmed",
        "peer_confirmed",
        "system_confirmed",
        "conflicted",
    ]
    last_revalidated_at: str
    strongest_signal: Literal["self_review", "external_observation", "user_confirmation", "peer_confirmation", "system_check"]
    evidence_refs: List[str] = Field(default_factory=list, max_length=4)
    conflict_summary: Optional[str] = Field(default=None, max_length=240)


class ContinuityFreshness(BaseModel):
    """Freshness metadata for continuity decay and expiration rules."""

    freshness_class: Optional[Literal["persistent", "durable", "situational", "ephemeral"]] = None
    expires_at: Optional[str] = None
    stale_after_seconds: Optional[int] = Field(default=None, ge=300, le=31536000)


class ContinuityCapsuleHealth(BaseModel):
    """Operational health state recorded for one continuity capsule."""

    status: Literal["healthy", "degraded", "conflicted"]
    reasons: List[str] = Field(default_factory=list, max_length=5)
    last_checked_at: str


class ContinuitySelector(BaseModel):
    """Explicit continuity subject selector used by multi-capsule retrieval."""

    subject_kind: Literal["user", "peer", "thread", "task"]
    subject_id: str = Field(min_length=1, max_length=200)


class NegativeDecision(BaseModel):
    """A deliberate non-action plus rationale; bounds are enforced in the continuity service."""

    model_config = ConfigDict(extra="forbid")

    decision: str = Field(description="1-160 chars, validated at the continuity service layer.")
    rationale: str = Field(description="1-240 chars, validated at the continuity service layer.")
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    last_confirmed_at: Optional[str] = None


class RationaleEntry(BaseModel):
    """One bounded, agent-authored decision rationale or unresolved tension.

    Tags must be unique within a capsule's ``rationale_entries`` list.
    Field-length bounds for ``summary``, ``reasoning``,
    ``alternatives_considered`` items, and ``depends_on`` items are
    enforced at the service layer (HTTP 400), consistent with
    ``NegativeDecision``.
    """

    model_config = ConfigDict(extra="forbid")

    tag: str = Field(min_length=1, max_length=80)
    kind: Literal["decision", "assumption", "tension"]
    status: Literal["active", "superseded", "retired"]
    summary: str
    reasoning: str
    alternatives_considered: List[str] = Field(default_factory=list, max_length=3)
    depends_on: List[str] = Field(default_factory=list, max_length=3)
    supersedes: Optional[str] = Field(default=None, max_length=80)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    last_confirmed_at: Optional[str] = None


class ContinuityState(BaseModel):
    """Operational orientation state preserved across resets and compaction."""

    top_priorities: List[str] = Field(max_length=5)
    active_concerns: List[str] = Field(max_length=5)
    active_constraints: List[str] = Field(max_length=5)
    open_loops: List[str] = Field(max_length=5)
    stance_summary: str = Field(max_length=240)
    drift_signals: List[str] = Field(max_length=5)
    working_hypotheses: List[str] = Field(default_factory=list, max_length=5)
    long_horizon_commitments: List[str] = Field(default_factory=list, max_length=5)
    session_trajectory: List[str] = Field(default_factory=list, max_length=5)
    negative_decisions: List[NegativeDecision] = Field(default_factory=list, max_length=4)
    trailing_notes: List[str] = Field(default_factory=list, max_length=3)
    curiosity_queue: List[str] = Field(default_factory=list, max_length=5)
    rationale_entries: List[RationaleEntry] = Field(default_factory=list, max_length=6)
    related_documents: Any | None = None
    relationship_model: Optional[ContinuityRelationshipModel] = None
    retrieval_hints: Optional[ContinuityRetrievalHints] = None


class StablePreference(BaseModel):
    """A durable user/peer preference surfaced across sessions.

    Stable preferences represent explicitly stated, cross-thread standing
    instructions that the agent should honour regardless of thread context.
    Tags must be unique within a capsule's ``stable_preferences`` list.
    """

    model_config = ConfigDict(extra="forbid")

    tag: str = Field(min_length=1, max_length=80)
    content: str = Field(min_length=1, max_length=240)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    last_confirmed_at: Optional[str] = None


class IdentityAnchor(BaseModel):
    """A stable identity pin for deterministic thread discovery."""

    kind: str = Field(min_length=1, max_length=40)
    value: str = Field(min_length=1, max_length=200)


class ThreadDescriptor(BaseModel):
    """Structured identity block for thread and task capsules."""

    label: str = Field(min_length=1, max_length=120)
    keywords: List[str] = Field(default_factory=list, max_length=6)
    scope_anchors: List[str] = Field(default_factory=list, max_length=4)
    identity_anchors: List[IdentityAnchor] = Field(default_factory=list, max_length=4)
    lifecycle: Optional[Literal["active", "suspended", "concluded", "superseded"]] = None
    superseded_by: Optional[str] = Field(default=None, min_length=1, max_length=200)


class ContinuityCapsule(BaseModel):
    """Persisted continuity capsule for one subject."""

    schema_version: Literal["1.0", "1.1"] = "1.1"
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
    verification_state: Optional[ContinuityVerificationState] = None
    capsule_health: Optional[ContinuityCapsuleHealth] = None
    stable_preferences: List[StablePreference] = Field(default_factory=list, max_length=12)
    thread_descriptor: Optional[ThreadDescriptor] = None


class SessionEndSnapshot(BaseModel):
    """Compact session-end helper targeting startup-critical fields.

    P0 fields are required — the helper funnels caller attention to these
    at session end. P1 fields are optional: None means 'preserve whatever
    the base capsule already has'; an explicit list (even empty) overrides.
    """

    # P0 — required, always override capsule.continuity counterparts
    open_loops: List[str] = Field(max_length=5)
    top_priorities: List[str] = Field(max_length=5)
    active_constraints: List[str] = Field(max_length=5)
    stance_summary: str = Field(max_length=240)

    # P1 — optional; None = preserve capsule value, explicit value = override
    negative_decisions: Optional[List[NegativeDecision]] = Field(default=None, max_length=4)
    session_trajectory: Optional[List[str]] = Field(default=None, max_length=5)
    rationale_entries: Optional[List[RationaleEntry]] = Field(default=None, max_length=6)


class ContinuityUpsertRequest(BaseModel):
    """Top-level request for storing or replacing a continuity capsule."""

    subject_kind: Literal["user", "peer", "thread", "task"]
    subject_id: str = Field(min_length=1, max_length=200)
    capsule: ContinuityCapsule
    commit_message: Optional[str] = Field(default=None, max_length=240)
    idempotency_key: Optional[str] = Field(default=None, max_length=200)
    session_end_snapshot: Optional[SessionEndSnapshot] = Field(default=None)
    lifecycle_transition: Optional[Literal["suspend", "resume", "conclude", "supersede"]] = None
    superseded_by: Optional[str] = Field(default=None, min_length=1, max_length=200)
    merge_mode: Literal["replace", "preserve"] = "replace"


class PatchOperation(BaseModel):
    """One append/remove/replace_at operation targeting a capsule list field."""

    target: Literal[
        # ContinuityState string lists
        "continuity.open_loops",
        "continuity.top_priorities",
        "continuity.active_constraints",
        "continuity.active_concerns",
        "continuity.drift_signals",
        "continuity.working_hypotheses",
        "continuity.long_horizon_commitments",
        "continuity.session_trajectory",
        "continuity.trailing_notes",
        "continuity.curiosity_queue",
        # ContinuityState structured lists
        "continuity.negative_decisions",
        "continuity.rationale_entries",
        # Capsule-level structured lists
        "stable_preferences",
        # ThreadDescriptor lists
        "thread_descriptor.keywords",
        "thread_descriptor.scope_anchors",
        "thread_descriptor.identity_anchors",
    ]
    action: Literal["append", "remove", "replace_at"]
    value: Any = None
    match: Optional[str] = None
    index: Optional[int] = None


class ContinuityPatchRequest(BaseModel):
    """Request for partial list-field mutations on an existing continuity capsule."""

    subject_kind: Literal["user", "peer", "thread", "task"]
    subject_id: str = Field(min_length=1, max_length=200)
    updated_at: str
    operations: List[PatchOperation] = Field(min_length=1, max_length=10)
    commit_message: Optional[str] = Field(default=None, max_length=240)


class ContinuityLifecycleRequest(BaseModel):
    """Standalone lifecycle transition for a thread or task capsule."""

    subject_kind: Literal["thread", "task"]
    subject_id: str = Field(min_length=1, max_length=200)
    transition: Literal["suspend", "resume", "conclude", "supersede"]
    superseded_by: Optional[str] = Field(default=None, min_length=1, max_length=200)
    updated_at: str
    commit_message: Optional[str] = Field(default=None, max_length=240)


class ContinuityReadRequest(BaseModel):
    """Exact-selector request for reading one continuity capsule with optional fallback."""

    subject_kind: Literal["user", "peer", "thread", "task"]
    subject_id: str = Field(min_length=1, max_length=200)
    allow_fallback: bool = False
    view: Optional[Literal["startup"]] = None


class ContinuityListRequest(BaseModel):
    """Filter parameters for listing active, fallback, and archived continuity capsules."""

    subject_kind: Optional[Literal["user", "peer", "thread", "task"]] = None
    limit: int = Field(default=50, ge=1, le=200)
    include_fallback: bool = False
    include_archived: bool = False
    include_cold: bool = False
    lifecycle: Optional[Literal["active", "suspended", "concluded", "superseded"]] = None
    scope_anchor: Optional[str] = Field(default=None, max_length=200)
    keyword: Optional[str] = Field(default=None, max_length=40)
    label_exact: Optional[str] = Field(default=None, max_length=120)
    anchor_kind: Optional[str] = Field(default=None, max_length=40)
    anchor_value: Optional[str] = Field(default=None, max_length=200)
    sort: Optional[Literal["default", "salience"]] = "default"


class ContinuityRefreshPlanRequest(BaseModel):
    """Parameters for deterministic continuity refresh planning."""

    subject_kind: Optional[Literal["user", "peer", "thread", "task"]] = None
    limit: int = Field(default=25, ge=1, le=100)
    include_healthy: bool = False


class ContinuityRetentionPlanRequest(BaseModel):
    """Parameters for deterministic continuity retention planning."""

    subject_kind: Optional[Literal["user", "peer", "thread", "task"]] = None
    limit: int = Field(default=25, ge=1, le=100)


class ContinuityArchiveRequest(BaseModel):
    """Exact-selector request for archiving one active continuity capsule."""

    subject_kind: Literal["user", "peer", "thread", "task"]
    subject_id: str = Field(min_length=1, max_length=200)
    reason: str = Field(min_length=3, max_length=240)


class ContinuityColdStoreRequest(BaseModel):
    """Host-local request for cold-storing one archived continuity envelope."""

    source_archive_path: str = Field(min_length=1, max_length=400)


class ArtifactHistoryColdStoreRequest(BaseModel):
    """Host-local request for cold-storing one artifact-history payload."""

    source_payload_path: str = Field(min_length=1, max_length=400)


class ContinuityRetentionApplyRequest(BaseModel):
    """Host-local request for batch-applying continuity retention policy."""

    source_archive_paths: List[str] = Field(min_length=1, max_length=100)


class ContinuityColdRehydrateRequest(BaseModel):
    """Host-local request for rehydrating one cold-stored continuity envelope."""

    source_archive_path: Optional[str] = Field(default=None, max_length=400)
    cold_stub_path: Optional[str] = Field(default=None, max_length=400)

    @model_validator(mode="after")
    def _require_exactly_one_selector(self) -> "ContinuityColdRehydrateRequest":
        """Require exactly one selector field for cold rehydration."""
        if bool(self.source_archive_path) == bool(self.cold_stub_path):
            raise ValueError("exactly one of source_archive_path or cold_stub_path is required")
        return self


class ArtifactHistoryColdRehydrateRequest(BaseModel):
    """Host-local request for rehydrating one artifact-history cold payload."""

    source_payload_path: Optional[str] = Field(default=None, max_length=400)
    cold_stub_path: Optional[str] = Field(default=None, max_length=400)

    @model_validator(mode="after")
    def _require_exactly_one_selector(self) -> "ArtifactHistoryColdRehydrateRequest":
        """Require exactly one selector field for artifact-history rehydration."""
        if bool(self.source_payload_path) == bool(self.cold_stub_path):
            raise ValueError("exactly one of source_payload_path or cold_stub_path is required")
        return self


class RegistryHistoryColdStoreRequest(BaseModel):
    """Host-local request for cold-storing one registry-history shard."""

    source_payload_path: str = Field(min_length=1, max_length=400)


class RegistryHistoryColdRehydrateRequest(BaseModel):
    """Host-local request for rehydrating one cold-stored registry-history shard."""

    source_payload_path: Optional[str] = Field(default=None, max_length=400)
    cold_stub_path: Optional[str] = Field(default=None, max_length=400)

    @model_validator(mode="after")
    def _require_exactly_one_selector(self) -> "RegistryHistoryColdRehydrateRequest":
        """Require exactly one selector field for registry-history rehydration."""
        if bool(self.source_payload_path) == bool(self.cold_stub_path):
            raise ValueError("exactly one of source_payload_path or cold_stub_path is required")
        return self


class SegmentHistoryMaintenanceRequest(BaseModel):
    """Host-local request for rolling active sources into history segments."""

    family: Literal["journal", "api_audit", "ops_runs", "message_stream", "message_thread", "episodic"]
    batch_limit: Optional[int] = Field(default=None, gt=0)


class SegmentHistoryColdStoreRequest(BaseModel):
    """Host-local request for cold-storing rolled segment-history payloads."""

    family: Literal["journal", "api_audit", "ops_runs", "message_stream", "message_thread", "episodic"]
    batch_limit: Optional[int] = Field(default=None, gt=0)
    segment_ids: Optional[List[str]] = Field(default=None, max_length=500)


class SegmentHistoryColdRehydrateRequest(BaseModel):
    """Host-local request for rehydrating one cold-stored segment-history payload."""

    family: Literal["journal", "api_audit", "ops_runs", "message_stream", "message_thread", "episodic"]
    segment_id: str = Field(min_length=1, max_length=200)


class ContinuityDeleteRequest(BaseModel):
    """Exact-selector request for deleting continuity artifacts."""

    subject_kind: Literal["user", "peer", "thread", "task"]
    subject_id: str = Field(min_length=1, max_length=200)
    delete_active: bool = False
    delete_archive: bool = False
    delete_fallback: bool = False
    reason: str = Field(min_length=3, max_length=240)

    @model_validator(mode="after")
    def _require_any_delete_flag(self) -> "ContinuityDeleteRequest":
        """Require at least one explicit delete target."""
        if not (self.delete_active or self.delete_archive or self.delete_fallback):
            raise ValueError("at least one delete flag must be true")
        return self


class ContinuityVerificationSignal(BaseModel):
    """Structured verification signal used by continuity compare and revalidate workflows."""

    kind: Literal["self_review", "external_observation", "user_confirmation", "peer_confirmation", "system_check"]
    source_ref: str = Field(min_length=1, max_length=200)
    observed_at: str
    summary: str = Field(min_length=1, max_length=240)


class ContinuityCompareRequest(BaseModel):
    """Exact-selector request for comparing an active continuity capsule to a candidate."""

    subject_kind: Literal["user", "peer", "thread", "task"]
    subject_id: str = Field(min_length=1, max_length=200)
    candidate_capsule: ContinuityCapsule
    signals: List[ContinuityVerificationSignal] = Field(min_length=1, max_length=8)


class ContinuityRevalidateRequest(BaseModel):
    """Exact-selector request for confirming or correcting one active continuity capsule."""

    subject_kind: Literal["user", "peer", "thread", "task"]
    subject_id: str = Field(min_length=1, max_length=200)
    outcome: Literal["confirm", "correct", "degrade", "conflict"]
    signals: List[ContinuityVerificationSignal] = Field(min_length=1, max_length=8)
    candidate_capsule: Optional[ContinuityCapsule] = None
    reason: Optional[str] = Field(default=None, min_length=1, max_length=120)
