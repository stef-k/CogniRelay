# Payload Reference

This document describes the request and response payloads for key CogniRelay operations. The runtime source of truth for all input schemas is `GET /v1/discovery/tools`, which returns full JSON schemas for every endpoint.

## Continuity Capsule Structure

A continuity capsule is the core unit of orientation state. It is stored as a JSON file under `memory/continuity/`.

### ContinuityCapsule

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `schema_version` | `"1.0"` | no | default `"1.0"` |
| `subject_kind` | `"user"` \| `"peer"` \| `"thread"` \| `"task"` | yes | |
| `subject_id` | string | yes | 1–200 chars |
| `updated_at` | ISO datetime string | yes | |
| `verified_at` | ISO datetime string | yes | |
| `source` | ContinuitySource | yes | |
| `continuity` | ContinuityState | yes | |
| `confidence` | ContinuityConfidence | yes | |
| `verification_kind` | `"self_review"` \| `"external_observation"` \| `"user_confirmation"` \| `"peer_confirmation"` \| `"system_check"` | no | |
| `attention_policy` | ContinuityAttentionPolicy | no | |
| `freshness` | ContinuityFreshness | no | |
| `canonical_sources` | list of strings | no | max 8 items, default `[]` |
| `metadata` | object | no | default `{}` |
| `verification_state` | ContinuityVerificationState | no | |
| `capsule_health` | ContinuityCapsuleHealth | no | |

### ContinuityState

These are the core orientation fields that an agent writes to preserve working state across resets.

| Field | Type | Required | Constraints | Purpose |
|-------|------|----------|-------------|---------|
| `top_priorities` | list of strings | yes | max 5 | What matters most right now |
| `active_concerns` | list of strings | yes | max 5 | Active worries or risks |
| `active_constraints` | list of strings | yes | max 5 | Hard boundaries on action |
| `open_loops` | list of strings | yes | max 5 | Unresolved questions or pending items |
| `stance_summary` | string | yes | max 240 chars | Current direction in one sentence |
| `drift_signals` | list of strings | yes | max 5 | Signs that orientation may be shifting |
| `working_hypotheses` | list of strings | no | max 5, default `[]` | Current best-guess assumptions |
| `long_horizon_commitments` | list of strings | no | max 5, default `[]` | Commitments that outlast this session |
| `session_trajectory` | list of strings | no | max 5, default `[]` | Key direction changes within this session |
| `negative_decisions` | list of NegativeDecision | no | max 4, default `[]` | Decisions not to act |
| `trailing_notes` | list of strings | no | max 3, default `[]` | Low-priority context worth preserving |
| `curiosity_queue` | list of strings | no | max 5, default `[]` | Questions to revisit later |
| `relationship_model` | ContinuityRelationshipModel | no | | Relationship-specific hints |
| `retrieval_hints` | ContinuityRetrievalHints | no | | Preferences for what to load next |

Optional fields have a deterministic trim order under token pressure. When the capsule must fit within a budget, the system trims from the bottom of the table upward — `retrieval_hints` first, then `relationship_model`, then `curiosity_queue`, and so on.

### NegativeDecision

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `decision` | string | yes | 1–160 chars |
| `rationale` | string | yes | 1–240 chars |

### ContinuitySource

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `producer` | string | yes | 1–100 chars |
| `update_reason` | `"startup_refresh"` \| `"pre_compaction"` \| `"interaction_boundary"` \| `"manual"` \| `"migration"` | yes | |
| `inputs` | list of strings | no | max 12, default `[]` |

When `update_reason` is `"interaction_boundary"`, the capsule must also include `metadata.interaction_boundary_kind` as a valid scalar value.

### ContinuityConfidence

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `continuity` | float | yes | 0.0–1.0 |
| `relationship_model` | float | yes | 0.0–1.0 |

### ContinuityFreshness

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `freshness_class` | `"persistent"` \| `"durable"` \| `"situational"` \| `"ephemeral"` | no | |
| `expires_at` | ISO datetime string | no | |
| `stale_after_seconds` | integer | no | 300–31,536,000 |

### ContinuityVerificationState

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `status` | `"unverified"` \| `"self_attested"` \| `"externally_supported"` \| `"user_confirmed"` \| `"peer_confirmed"` \| `"system_confirmed"` \| `"conflicted"` | yes | |
| `last_revalidated_at` | ISO datetime string | yes | |
| `strongest_signal` | `"self_review"` \| `"external_observation"` \| `"user_confirmation"` \| `"peer_confirmation"` \| `"system_check"` | yes | |
| `evidence_refs` | list of strings | no | max 4, default `[]` |
| `conflict_summary` | string | no | max 240 chars |

## Continuity Requests

### Upsert — `POST /v1/continuity/upsert`

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `subject_kind` | `"user"` \| `"peer"` \| `"thread"` \| `"task"` | yes | |
| `subject_id` | string | yes | 1–200 chars |
| `capsule` | ContinuityCapsule | yes | |
| `commit_message` | string | no | max 240 chars |
| `idempotency_key` | string | no | max 200 chars |

Response includes `recovery_warnings` (list of strings) when the fallback snapshot refresh fails after the active write has already committed.

### Read — `POST /v1/continuity/read`

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `subject_kind` | `"user"` \| `"peer"` \| `"thread"` \| `"task"` | yes | |
| `subject_id` | string | yes | 1–200 chars |
| `allow_fallback` | boolean | no | default `false` |

Response:

```json
{
  "ok": true,
  "path": "memory/continuity/user-stef.json",
  "capsule": { },
  "archived": false,
  "source_state": "active",
  "recovery_warnings": []
}
```

`source_state` is `"active"`, `"fallback"`, or `"missing"`. When `allow_fallback` is `false` (default) and the active capsule is missing, the response is an HTTP error. When `true`, the response degrades to fallback or missing state with appropriate `recovery_warnings`.

### Compare — `POST /v1/continuity/compare`

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `subject_kind` | `"user"` \| `"peer"` \| `"thread"` \| `"task"` | yes | |
| `subject_id` | string | yes | 1–200 chars |
| `candidate_capsule` | ContinuityCapsule | yes | |
| `signals` | list of ContinuityVerificationSignal | yes | 1–8 items |

Returns deterministic changed fields, strongest signal, and a recommended verification outcome without mutating the active capsule.

### Revalidate — `POST /v1/continuity/revalidate`

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `subject_kind` | `"user"` \| `"peer"` \| `"thread"` \| `"task"` | yes | |
| `subject_id` | string | yes | 1–200 chars |
| `outcome` | `"confirm"` \| `"correct"` \| `"degrade"` \| `"conflict"` | yes | |
| `signals` | list of ContinuityVerificationSignal | yes | 1–8 items |
| `candidate_capsule` | ContinuityCapsule | no | required when outcome is `"correct"` |
| `reason` | string | no | 1–120 chars |

Response includes `recovery_warnings` when the fallback snapshot refresh fails after the active write.

### List — `POST /v1/continuity/list`

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `subject_kind` | `"user"` \| `"peer"` \| `"thread"` \| `"task"` | no | filter by kind |
| `limit` | integer | no | 1–200, default 50 |
| `include_fallback` | boolean | no | default `false` |
| `include_archived` | boolean | no | default `false` |
| `include_cold` | boolean | no | default `false` |

Response includes `artifact_state` and `retention_class` for each entry. Archive entries include `archive_stale` classification based on `COGNIRELAY_CONTINUITY_RETENTION_ARCHIVE_DAYS`.

### Archive — `POST /v1/continuity/archive`

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `subject_kind` | `"user"` \| `"peer"` \| `"thread"` \| `"task"` | yes | |
| `subject_id` | string | yes | 1–200 chars |
| `reason` | string | yes | 3–240 chars |

### Delete — `POST /v1/continuity/delete`

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `subject_kind` | `"user"` \| `"peer"` \| `"thread"` \| `"task"` | yes | |
| `subject_id` | string | yes | 1–200 chars |
| `delete_active` | boolean | no | default `false` |
| `delete_archive` | boolean | no | default `false` |
| `delete_fallback` | boolean | no | default `false` |
| `reason` | string | yes | 3–240 chars |

At least one delete flag must be `true`.

## Context Retrieval

### Retrieve — `POST /v1/context/retrieve`

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `task` | string | yes | description of active task |
| `subject_kind` | `"user"` \| `"peer"` \| `"thread"` \| `"task"` | no | explicit capsule selector |
| `subject_id` | string | no | max 200 chars |
| `continuity_mode` | `"auto"` \| `"required"` \| `"off"` | no | default `"auto"` |
| `continuity_verification_policy` | `"allow_degraded"` \| `"prefer_healthy"` \| `"require_healthy"` | no | default `"allow_degraded"` |
| `continuity_resilience_policy` | `"allow_fallback"` \| `"prefer_active"` \| `"require_active"` | no | default `"allow_fallback"` |
| `continuity_selectors` | list of ContinuitySelector | no | max 4 items |
| `continuity_max_capsules` | integer | no | 1–4, default 1 |
| `max_tokens_estimate` | integer | no | 256–100,000, default 4000 |
| `include_types` | list of strings | no | default `[]` |
| `time_window_days` | integer | no | 1–3650, default 30 |
| `limit` | integer | no | 1–100, default 10 |

Response:

```json
{
  "ok": true,
  "bundle": {
    "task": "...",
    "generated_at": "2024-01-01T00:00:00Z",
    "core_memory": [{"path": "...", "snippet": "..."}],
    "recent_relevant": [{"path": "...", "type": "...", "snippet": "...", "score": 1.0}],
    "open_questions": ["..."],
    "token_budget_hint": "4000",
    "time_window_days": 30,
    "notes": ["..."],
    "continuity_state": {
      "present": true,
      "capsules": [{"source_state": "active", "...": "..."}],
      "warnings": [],
      "fallback_used": false,
      "recovery_warnings": []
    }
  }
}
```

When derived search indexes are stale, `continuity_state.warnings` includes `"continuity_index_stale"`. When indexes are missing, retrieval falls back to a bounded raw scan and adds `"continuity_index_missing"`.

## Coordination Payloads

### Handoff Create — `POST /v1/coordination/handoff/create`

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `recipient_peer` | string | yes | 1–200 chars |
| `subject_kind` | `"user"` \| `"peer"` \| `"thread"` \| `"task"` | yes | |
| `subject_id` | string | yes | 1–200 chars |
| `task_id` | string | no | max 200 chars |
| `thread_id` | string | no | max 200 chars |
| `note` | string | no | max 240 chars |
| `commit_message` | string | no | |

The handoff artifact projects only `active_constraints` and `drift_signals` from the referenced active continuity capsule. No other capsule fields cross the boundary.

### Handoff Consume — `POST /v1/coordination/handoff/{handoff_id}/consume`

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `status` | `"accepted_advisory"` \| `"deferred"` \| `"rejected"` | yes | |
| `note` | string | no | max 240 chars |

### Shared Coordination Create — `POST /v1/coordination/shared/create`

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `participant_peers` | list of strings | no | max 8 items |
| `task_id` | string | no | max 200 chars |
| `thread_id` | string | no | max 200 chars |
| `title` | string | yes | |
| `summary` | string | no | |
| `constraints` | list of strings | no | max 8 items |
| `drift_signals` | list of strings | no | max 8 items |
| `coordination_alerts` | list of strings | no | max 8 items |
| `commit_message` | string | no | |

### Reconciliation Open — `POST /v1/coordination/reconciliation/open`

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `task_id` | string | no | max 200 chars |
| `thread_id` | string | no | max 200 chars |
| `title` | string | yes | |
| `summary` | string | no | |
| `classification` | `"contradictory"` \| `"stale_observation"` \| `"frame_conflict"` \| `"concurrent_race"` | yes | |
| `trigger` | `"handoff_vs_handoff"` \| `"shared_vs_shared"` \| `"handoff_vs_shared"` \| `"concurrent_mutation_race"` | yes | |
| `claims` | list of ReconciliationClaim | no | max 4 items |
| `commit_message` | string | no | |

### Reconciliation Resolve — `POST /v1/coordination/reconciliation/{reconciliation_id}/resolve`

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `expected_version` | integer | yes | first-write-wins concurrency check |
| `resolution_outcome` | `"advisory_only"` \| `"conflicted"` \| `"rejected"` | yes | |
| `resolution_summary` | string | no | max 240 chars |

## Runtime Schema Discovery

For the complete and always-current input schemas, use:

- `GET /v1/discovery/tools` — returns full JSON schema for every endpoint's request model
- `POST /v1/mcp` with `tools/list` — returns the same schemas in MCP tool format
- `GET /v1/manifest` — returns the endpoint map with method and path metadata

These runtime endpoints are the authoritative source. This document is a static reference for human readers and may lag behind the implementation.
