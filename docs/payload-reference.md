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
| `session_end_snapshot` | SessionEndSnapshot | no | See below |

Response includes `recovery_warnings` (list of strings) when the fallback snapshot refresh fails after the active write has already committed.

#### Session-end snapshot helper

When `session_end_snapshot` is provided, the server merges its fields into `capsule.continuity` before validation and persistence. This reduces caller burden at session end by focusing on the six startup-critical fields. The base capsule carries forward all non-snapshot fields unchanged.

**`SessionEndSnapshot` fields:**

| Field | Type | Required | Maps to | Override behavior |
|-------|------|----------|---------|-------------------|
| `open_loops` | list of strings (max 5, each ≤ 160 chars) | yes | `capsule.continuity.open_loops` | Always overrides |
| `top_priorities` | list of strings (max 5, each ≤ 160 chars) | yes | `capsule.continuity.top_priorities` | Always overrides |
| `active_constraints` | list of strings (max 5, each ≤ 160 chars) | yes | `capsule.continuity.active_constraints` | Always overrides |
| `stance_summary` | string (≤ 240 chars) | yes | `capsule.continuity.stance_summary` | Always overrides |
| `negative_decisions` | list of NegativeDecision (max 4) | no | `capsule.continuity.negative_decisions` | `null` = preserve capsule value; explicit value = override |
| `session_trajectory` | list of strings (max 5, each ≤ 80 chars) | no | `capsule.continuity.session_trajectory` | `null` = preserve capsule value; explicit value = override |

**Merge algorithm:** P0 fields (required) always override their `capsule.continuity` counterparts. P1 fields (optional) override only when non-null; null means the capsule's existing value is preserved. All other `ContinuityState` fields remain from the capsule unchanged. The merged capsule is then validated and persisted through the standard path. Note: the snapshot does not update `capsule.updated_at` — the caller must still set `updated_at` to the current time on the base capsule to avoid a 409 conflict rejection.

Per-item string length constraints (e.g. each ≤ 160 chars for list fields, each ≤ 80 chars for `session_trajectory`) are enforced by capsule validation after the merge, consistent with `ContinuityState` validation. See [NegativeDecision](#negativedecision) for per-item constraints on `negative_decisions` items.

**Additional response fields** (only when `session_end_snapshot` is provided):

| Key | Type | Description |
|-----|------|-------------|
| `session_end_snapshot_applied` | boolean | `true` confirms the merge was applied |
| `resume_quality` | `{"adequate": bool}` | `true` iff all P0 fields are non-empty and `stance_summary` ≥ 30 chars |

When `session_end_snapshot` is omitted or null, behavior and response are identical to the baseline — no merge, no additional response keys.

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
  "recovery_warnings": [],
  "trust_signals": {
    "recency": {
      "updated_age_seconds": 3600,
      "verified_age_seconds": 7200,
      "phase": "fresh",
      "freshness_class": "durable",
      "stale_threshold_seconds": 15552000
    },
    "completeness": {
      "orientation_adequate": true,
      "empty_orientation_fields": [],
      "trimmed": false,
      "trimmed_fields": []
    },
    "integrity": {
      "source_state": "active",
      "health_status": "healthy",
      "health_reasons": [],
      "verification_status": "self_attested"
    },
    "scope_match": {
      "exact": true
    }
  }
}
```

`trust_signals` is an objective, mechanical trust assessment of the returned capsule across four dimensions: recency, completeness, integrity, and scope_match. It is `null` when `capsule` is `null` (i.e. `source_state == "missing"`). No dimension produces a score — each exposes enumerated states and counts that consumers interpret. `completeness.trimmed` is always `false` on the read path (no token-budget trimming applies). All fields are deterministically derived from existing capsule state and retrieval metadata — there is no model inference, heuristic scoring, or hidden weighting.

**Derivation sources by dimension:**

- `recency`: `updated_at` and `verified_at` timestamps (age computation), `freshness.freshness_class` and `freshness.stale_after_seconds` (phase thresholds), `freshness.expires_at` (hard expiry)
- `completeness`: `continuity.*` orientation fields — `top_priorities`, `active_constraints`, `open_loops`, `active_concerns`, `stance_summary`, `drift_signals` (adequacy and empty-field tracking); token-budget trimming metadata (trimmed flag and field list)
- `integrity`: `capsule_health.status` and `capsule_health.reasons` (health), `verification_state.status` (verification), active-vs-fallback resolution (source state)
- `scope_match`: selector resolution outcome (exact match flag); on the multi-capsule path, selector request/return/omit counts

**Age field nullability:** `recency.updated_age_seconds` and `recency.verified_age_seconds` are `null` (not `0`) when the corresponding timestamp is missing or malformed. A `null` age means the age could not be computed — consumers must not treat it as zero (maximally fresh). When `verified_at` is malformed, `recency.phase` falls back to `"expired"` rather than crashing or producing misleading freshness.

On the context-retrieval path (`build_continuity_state`), trust_signals are budgeted as part of the delivered continuity token allocation. When the full trust_signals shape would leave insufficient room for capsule content, a **compact** form is emitted instead. The compact shape sets `"compact": true` and includes only the minimum subfields: `recency.phase`, `completeness.orientation_adequate`, `completeness.trimmed`, `integrity.source_state`, `integrity.health_status`, and `scope_match.exact`. If even the compact form cannot fit alongside minimum capsule content, trust_signals is `null` and the capsule is trimmed normally. When compact trust_signals are used, `recovery_warnings` includes `"trust_signals_compact"`.

**Aggregate trust_signals** (`continuity_state.trust_signals`) aggregates per-capsule signals across all returned capsules. It correctly handles a mix of full and compact per-capsule shapes. `oldest_updated_age_seconds` and `oldest_verified_age_seconds` are `null` when no per-capsule signal provides a known age value (e.g. all compact, or all with malformed timestamps). `completeness.total_count` counts capsules with non-null trust_signals; it may be less than `scope_match.selectors_returned` when some capsules have `trust_signals: null`. If aggregate computation fails, `recovery_warnings` includes `"trust_signals_aggregate_failed"` and `continuity_state.trust_signals` is `null`.

**`continuity_state.trust_signals` on early-return paths:** When `continuity_state.present` is `false` (continuity mode is `"off"`, no selectors resolved, no capsules loaded, or all capsules omitted), `continuity_state.trust_signals` is always `null`. The key is present on every response shape — consumers can unconditionally access `continuity_state["trust_signals"]` without checking `present` first.

`source_state` is `"active"`, `"fallback"`, or `"missing"`. When `allow_fallback` is `false` (default) and the active capsule is missing, the response is an HTTP error. When `true`, the response degrades to fallback or missing state with appropriate `recovery_warnings`.

#### Startup view (`view="startup"`)

When `view` is set to `"startup"`, the response includes one additional top-level key `startup_summary` alongside the unchanged full `capsule`. The summary mechanically extracts startup-relevant fields from the already-loaded capsule into a fixed-order structure. The `capsule` value is identical to the response without a view parameter.

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
    "updated_at": "2026-03-24T18:06:26Z",
    "trust_signals": {
      "recency": { "updated_age_seconds": 60, "verified_age_seconds": 120, "phase": "fresh", "freshness_class": "situational", "stale_threshold_seconds": 2592000 },
      "completeness": { "orientation_adequate": true, "empty_orientation_fields": [], "trimmed": false, "trimmed_fields": [] },
      "integrity": { "source_state": "active", "health_status": "healthy", "health_reasons": [], "verification_status": "self_attested" },
      "scope_match": { "exact": true }
    }
  }
}
```

`startup_summary.trust_signals` is the same trust_signals block as the top-level response key. It is `null` when `source_state` is `"missing"` or capsule is `null`.

**`startup_summary` shape (missing capsule):**

When `source_state` is `"missing"`: `orientation` is `null`, `context` is `null`, `updated_at` is `null`, `trust_signals` is `null`. The `recovery` block is always present and never null.

**Key order contract:** Top-level keys are always `recovery`, `orientation`, `context`, `updated_at`, `trust_signals` in that order. Within each block, keys appear in the order shown above. Python 3.7+ dict insertion order is preserved through FastAPI/JSON serialization.

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

**Degradation:** If building the `startup_summary` fails unexpectedly (e.g., a malformed capsule bypassing validation), `startup_summary` is set to `null` in the response and `"startup_summary_build_failed"` is appended to `recovery_warnings`. The full `capsule` is always unaffected. An agent receiving this warning should fall back to reading orientation fields directly from `capsule.continuity`.

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
      "capsules": [{"source_state": "active", "trust_signals": {"recency": {"updated_age_seconds": 60, "verified_age_seconds": 120, "phase": "fresh", "freshness_class": "situational", "stale_threshold_seconds": 2592000}, "completeness": {"orientation_adequate": true, "empty_orientation_fields": [], "trimmed": false, "trimmed_fields": []}, "integrity": {"source_state": "active", "health_status": "healthy", "health_reasons": [], "verification_status": "self_attested"}, "scope_match": {"exact": true}}, "...": "..."}],
      "trust_signals": {"recency": {"worst_phase": "fresh", "oldest_updated_age_seconds": 0, "oldest_verified_age_seconds": 0}, "completeness": {"all_adequate": true, "adequate_count": 1, "total_count": 1, "any_trimmed": false}, "integrity": {"worst_health": "healthy", "any_fallback": false, "any_degraded": false, "any_conflicted": false}, "scope_match": {"selectors_requested": 1, "selectors_returned": 1, "selectors_omitted": 0, "all_returned": true}},
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
