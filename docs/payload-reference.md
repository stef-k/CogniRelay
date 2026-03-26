# Payload Reference

This document describes the request and response payloads for key CogniRelay operations. The runtime source of truth for all input schemas is `GET /v1/discovery/tools`, which returns full JSON schemas for every endpoint.

## Capsule Size and Token Budget

A fully-populated capsule with realistic content in every field is approximately **~10 KB of JSON** and **~2,400–2,800 tokens**. Read this section first to understand the budget envelope before diving into individual field tables.

### Field count by object

ContinuityCapsule (top-level): 15 fields

Nested objects when all optional fields are populated:

- ContinuitySource: 3 fields
- ContinuityState: 14 fields
  - NegativeDecision (×4): 2 fields each = 8 fields
  - ContinuityRelationshipModel: 3 fields
  - ContinuityRetrievalHints: 3 fields
- ContinuityConfidence: 2 fields
- ContinuityAttentionPolicy: 2 fields
- ContinuityFreshness: 3 fields
- ContinuityVerificationState: 5 fields
- ContinuityCapsuleHealth: 3 fields

Total: **~58 distinct fields** across all nested objects.

### Maximum list items at full population

| Area | Items | Max chars per item |
|------|-------|--------------------|
| ContinuityState lists (6 required) | 5 each = 30 strings | no per-item limit |
| ContinuityState optional lists (5) | 3–5 each = 23 strings | no per-item limit |
| `negative_decisions` | 4 × (160 + 240) = 1,600 chars | structured |
| `stance_summary` | 1 string | 240 chars |
| `relationship_model` lists (2) | 5 each = 10 strings | no per-item limit |
| `retrieval_hints` lists (3) | 8 each = 24 strings | no per-item limit |
| `attention_policy` lists (2) | 5 + 8 = 13 strings | no per-item limit |
| `canonical_sources` | 8 strings | no per-item limit |
| `source.inputs` | 12 strings | no per-item limit |
| `evidence_refs` | 4 strings | no per-item limit |
| `capsule_health.reasons` | 5 strings | no per-item limit |
| `conflict_summary` | 1 string | 240 chars |
| `subject_id` | 1 string | 200 chars |
| `producer` | 1 string | 100 chars |

Total: **~130 strings + 4 negative decision objects + ~10 scalar fields**.

### Estimated JSON size

The string list items don't have per-item length limits in the model (only `stance_summary`, `conflict_summary`, `decision`, `rationale`, `producer`, `subject_id` are bounded). In practice, if each unbounded string averages 80–120 chars:

- **Typical full capsule**: ~4–8 KB JSON
- **Maximum realistic capsule** (all lists full, verbose strings): ~12–18 KB JSON
- **Theoretical extreme** (very long strings in every slot): could approach the `COGNIRELAY_MAX_PAYLOAD_BYTES` limit (default 262 KB), but this would be pathological

The system is designed so that a fully-populated capsule with practical content fits comfortably in a few KB — well within context-window budgets and storage constraints.

### Token estimates by section

| Section | ~Tokens | Fields | Notes |
|---------|---------|--------|-------|
| Core orientation (6 required lists + `stance_summary`) | ~670 | 31 strings + 1 scalar | Always present — the essential orientation |
| Optional lists (`working_hypotheses`, `long_horizon_commitments`, `session_trajectory`, `negative_decisions`, `trailing_notes`, `curiosity_queue`) | ~840 | 27 strings + 4 objects | Trimmed first under token pressure |
| `retrieval_hints` (`must_include`, `avoid`, `load_next`) | ~270 | up to 24 strings | Dropped early in trim order |
| `relationship_model` (`trust_level`, `preferred_style`, `sensitivity_notes`) | ~200 | up to 11 fields | Dropped early in trim order |
| `attention_policy` (`early_load`, `presence_bias_overrides`) | ~175 | up to 13 strings | |
| `source` + `canonical_sources` + `metadata` | ~280 | variable | |
| `verification_state` + `capsule_health` + `confidence` | ~185 | ~10 fields | |
| Top-level metadata (`subject_kind`, `subject_id`, timestamps, etc.) | ~60 | ~6 fields | |
| **Total (fully populated)** | **~2,400–2,800** | **~58 fields** | **~10 KB JSON** |

### Context window impact

| Context window | % used by one full capsule |
|----------------|---------------------------|
| 8K | ~35% |
| 32K | ~9% |
| 128K | ~2% |
| 200K | ~1.4% |
| 1M | ~0.3% |

### Practical guidance

**Minimum viable capsule** — populate only the 6 required `ContinuityState` lists, `stance_summary`, `source`, and `confidence`. This costs approximately **~900–1,000 tokens** and provides enough orientation for basic restart recovery.

**Full capsule** — populate every field including `relationship_model`, `retrieval_hints`, `attention_policy`, `negative_decisions`, `session_trajectory`, `verification_state`, and `capsule_health`. This costs approximately **~2,400–2,800 tokens** and provides the richest possible orientation.

**Under token pressure** — the system trims optional fields in deterministic order starting from the bottom of the `ContinuityState` table: `retrieval_hints` first, then `relationship_model`, `curiosity_queue`, `trailing_notes`, `negative_decisions`, `session_trajectory`, `long_horizon_commitments`, and `working_hypotheses`. The 6 required core lists and `stance_summary` are never trimmed.

**Multi-capsule retrieval** — `POST /v1/context/retrieve` supports loading up to 4 capsules via `continuity_selectors` and `continuity_max_capsules`. At full population, 4 capsules would cost ~10,000–11,000 tokens. The `max_tokens_estimate` parameter (default 4,000) controls the total continuity token budget, and the system trims each capsule to fit.

### Per-item string constraints

Most list item strings in `ContinuityState` do not have a per-item character limit enforced in the model — the system relies on list count limits (max 3–5 items per field) and the deterministic trim mechanism to control total size. The exceptions with explicit character limits are:

| Field | Max length |
|-------|-----------|
| `stance_summary` | 240 chars |
| `negative_decisions[].decision` | 160 chars |
| `negative_decisions[].rationale` | 240 chars |
| `conflict_summary` (in `verification_state`) | 240 chars |
| `subject_id` | 200 chars |
| `source.producer` | 100 chars |

Agents should keep individual list item strings concise (roughly 80–120 chars) to stay within practical token budgets. Very long strings in many fields simultaneously would be legal but would consume disproportionate token budget for diminishing orientation value.

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
| `view` | `"startup"` | no | default `null` — when omitted the response is unchanged from the current contract |

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

#### Startup view (`view="startup"`)

When `view` is set to `"startup"`, the response includes one additional top-level key `startup_summary` alongside the unchanged full `capsule`. The summary mechanically extracts startup-relevant fields from the already-loaded capsule into a fixed-order structure. The `capsule` value is byte-identical to the response without a view parameter.

**`startup_summary` shape (active/fallback capsule):**

```json
{
  "startup_summary": {
    "recovery": {
      "source_state": "active",
      "recovery_warnings": [],
      "capsule_health_status": "healthy",
      "capsule_health_reasons": []
    },
    "orientation": {
      "top_priorities": ["..."],
      "active_constraints": ["..."],
      "open_loops": ["..."],
      "negative_decisions": [{"decision": "...", "rationale": "..."}]
    },
    "context": {
      "session_trajectory": ["..."],
      "stance_summary": "...",
      "active_concerns": ["..."]
    },
    "updated_at": "2026-03-24T18:06:26Z"
  }
}
```

**`startup_summary` shape (missing capsule):**

When `source_state` is `"missing"`: `orientation` is `null`, `context` is `null`, `updated_at` is `null`. The `recovery` block is always present and never null.

**Key order contract:** Top-level keys are always `recovery`, `orientation`, `context`, `updated_at` in that order. Within each block, keys appear in the order shown above. Python 3.7+ dict insertion order is preserved through FastAPI/JSON serialization.

**Field defaults:**

| Condition | Field | Value |
|-----------|-------|-------|
| Capsule is `null` (missing) | `capsule_health_status` | `null` |
| Capsule is `null` (missing) | `capsule_health_reasons` | `[]` |
| Capsule has no `capsule_health` | `capsule_health_status` | `null` |
| Capsule has no `capsule_health` | `capsule_health_reasons` | `[]` |
| Legacy capsule missing `negative_decisions` | `orientation.negative_decisions` | `[]` |
| Legacy capsule missing `session_trajectory` | `context.session_trajectory` | `[]` |

**`negative_decisions` pass-through:** Each element in `orientation.negative_decisions` is the same `{"decision": str, "rationale": str}` object stored in the capsule — no transformation, flattening, or summarization.

**Response overhead:** The `startup_summary` block adds approximately **~1.0–1.5 KB** and **~250–370 tokens** to the response, depending on capsule content density. This is a mechanical extraction — no additional I/O or computation is performed beyond building the summary dict from the already-loaded capsule.

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
