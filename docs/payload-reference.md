# Payload Reference

This document describes the request and response payloads for key CogniRelay operations. The runtime source of truth for all input schemas is `GET /v1/discovery/tools`, which returns full JSON schemas for every endpoint.

## Capsule Size and Token Budget

A fully-populated capsule with realistic content in every field is approximately **~10 KB of JSON** and **~2,400–2,800 tokens**. Read this section first to understand the budget envelope before diving into individual field tables.

### Field count by object

ContinuityCapsule (top-level): 16 fields

Nested objects when all optional fields are populated:

- ContinuitySource: 3 fields
- ContinuityState: 15 fields
  - NegativeDecision (×4): 2 fields each = 8 fields
  - RationaleEntry (×6): 9 fields each = 54 fields
  - ContinuityRelationshipModel: 3 fields
  - ContinuityRetrievalHints: 3 fields
- ContinuityConfidence: 2 fields
- ContinuityAttentionPolicy: 2 fields
- ContinuityFreshness: 3 fields
- ContinuityVerificationState: 5 fields
- ContinuityCapsuleHealth: 3 fields
- StablePreference (×12): 3 fields each = 36 fields

Total: **~94 distinct fields** across all nested objects.

### Maximum list items at full population

| Area | Items | Max chars per item |
|------|-------|--------------------|
| ContinuityState lists (6 required) | 5 each = 30 strings | no per-item limit |
| ContinuityState optional lists (5) | 3–5 each = 23 strings | no per-item limit |
| `negative_decisions` | 4 × (160 + 240) = 1,600 chars | structured |
| `rationale_entries` | 6 × (80 + 240 + 400 + 3×160 + 3×120 + 80) = ~7,680 chars | structured |
| `stable_preferences` | 12 × (80 + 240) = 3,840 chars | structured |
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
| Optional lists (`working_hypotheses`, `long_horizon_commitments`, `session_trajectory`, `negative_decisions`, `trailing_notes`, `curiosity_queue`, `rationale_entries`) | ~1,400–2,800 | 27 strings + 4+6 objects | Trimmed first under token pressure; upper bound assumes all `rationale_entries` slots fully populated |
| `retrieval_hints` (`must_include`, `avoid`, `load_next`) | ~270 | up to 24 strings | Dropped early in trim order |
| `relationship_model` (`trust_level`, `preferred_style`, `sensitivity_notes`) | ~200 | up to 11 fields | Dropped early in trim order |
| `attention_policy` (`early_load`, `presence_bias_overrides`) | ~175 | up to 13 strings | |
| `source` + `canonical_sources` + `metadata` | ~280 | variable | |
| `stable_preferences` (max 12 entries) | ~960 | up to 36 fields | Trimmed as whole unit under token pressure |
| `verification_state` + `capsule_health` + `confidence` | ~185 | ~10 fields | |
| Top-level metadata (`subject_kind`, `subject_id`, timestamps, etc.) | ~60 | ~6 fields | |
| **Total (fully populated)** | **~3,400–3,800** | **~94 fields** | **~14 KB JSON (theoretical; see note below)** |

> **Note on write-time size limit:** The server enforces a 12 KB serialized-UTF-8 cap on each capsule at write time. The ~14 KB figure above is the theoretical maximum when *every* optional field is populated at its maximum length simultaneously — a shape that exceeds the write limit and would be rejected. In practice a fully-featured capsule with realistic content fits within 10–11 KB. When populating both `stable_preferences` (up to ~4.6 KB at max lengths) and the full set of continuity fields, agents should expect to trade off less-critical optional content to stay within the 12 KB cap.

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

**Full capsule** — populate every field including `relationship_model`, `retrieval_hints`, `attention_policy`, `negative_decisions`, `rationale_entries`, `session_trajectory`, `verification_state`, and `capsule_health`. This costs approximately **~2,800–3,600 tokens** and provides the richest possible orientation.

**Under token pressure** — the system trims optional fields in two phases. Phase 1 drops whole optional sections in deterministic order: `metadata`, `canonical_sources`, `freshness`, `attention_policy.presence_bias_overrides`, `relationship_model` sub-fields (`sensitivity_notes`, `preferred_style`), `retrieval_hints` sub-fields (`avoid`, `load_next`), `trailing_notes`, `curiosity_queue`, `rationale_entries`, `negative_decisions`, `working_hypotheses`, then `stable_preferences` (dropped as a whole unit — all-or-nothing). Phase 2, if still over budget, progressively trims `retrieval_hints.must_include`, the remaining `relationship_model`, `long_horizon_commitments`, `stance_summary`, `drift_signals`, and finally the core lists. The 6 required core lists and `stance_summary` are trimmed only as a last resort. When `stable_preferences` is trimmed, `"stable_preferences"` appears in `trimmed_fields`; when `rationale_entries` is trimmed, `"continuity.rationale_entries"` appears in `trimmed_fields`.

**Multi-capsule retrieval** — `POST /v1/context/retrieve` supports loading up to 4 capsules via `continuity_selectors` and `continuity_max_capsules`. At full population, 4 capsules would cost ~10,000–11,000 tokens. The `max_tokens_estimate` parameter (default 4,000) controls the total continuity token budget, and the system trims each capsule to fit.

### Per-item string constraints

Most list item strings in `ContinuityState` do not have a per-item character limit enforced in the model — the system relies on list count limits (max 3–5 items per field) and the deterministic trim mechanism to control total size. The exceptions with explicit character limits are:

| Field | Max length |
|-------|-----------|
| `stance_summary` | 240 chars |
| `negative_decisions[].decision` | 160 chars |
| `negative_decisions[].rationale` | 240 chars |
| `rationale_entries[].tag` | 80 chars |
| `rationale_entries[].summary` | 240 chars |
| `rationale_entries[].reasoning` | 400 chars |
| `rationale_entries[].alternatives_considered[]` | 160 chars |
| `rationale_entries[].depends_on[]` | 120 chars |
| `conflict_summary` (in `verification_state`) | 240 chars |
| `subject_id` | 200 chars |
| `source.producer` | 100 chars |
| `stable_preferences[].tag` | 80 chars |
| `stable_preferences[].content` | 240 chars |

Agents should keep individual list item strings concise (roughly 80–120 chars) to stay within practical token budgets. Very long strings in many fields simultaneously would be legal but would consume disproportionate token budget for diminishing orientation value.

## Continuity Capsule Structure

A continuity capsule is the core unit of orientation state. It is stored as a JSON file under `memory/continuity/`.

Persisted legacy compatibility is intentionally bounded. On-disk capsules from older releases can be upgraded automatically when they still have the stabilized capsule shape and only need timestamp repair or structured-entry field upgrade. This supported legacy bucket includes older active capsules, fallback snapshots, archive envelopes, and cold artifacts whose capsule payload already contains the modern required fields (`updated_at`, `verified_at`, `source`, `confidence`, and the required core `continuity` fields). Sammy's oldest real continuity capsule sample falls into this supported bucket. Pre-stabilization payloads that are missing required top-level fields such as `updated_at`, `verified_at`, `source`, or `confidence`, or that omit required core `continuity` fields such as `active_concerns`, `active_constraints`, `open_loops`, `stance_summary`, or `drift_signals`, are not auto-migrated by the runtime loader and are treated as invalid legacy state.

### ContinuityCapsule

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `schema_version` | `"1.1"` | no | default `"1.1"`. Stabilized legacy `"1.0"` capsules are still accepted and upgraded on load where supported. |
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
| `stable_preferences` | list of StablePreference | no | max 12, default `[]`. Only valid on user/peer capsules (non-empty list on thread/task → HTTP 400). |
| `thread_descriptor` | ThreadDescriptor | no | Structured identity block for thread and task capsules. See below. |

### ThreadDescriptor

A structured identity block that gives thread and task capsules deterministic labels, keyword tags, scope anchors, and lifecycle state. Agents use thread descriptors to distinguish concurrent threads, filter by scope, and manage lifecycle transitions.

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `label` | string | yes | 1–120 chars. Human-readable thread label. |
| `keywords` | list of strings | no | max 6 items, default `[]`. Short tags for keyword-based discovery. |
| `scope_anchors` | list of strings | no | max 4 items, default `[]`. Stable scope identifiers (e.g. repo name, project key). |
| `identity_anchors` | list of IdentityAnchor | no | max 4 items, default `[]`. Typed key-value pins for deterministic thread discovery. |
| `lifecycle` | `"active"` \| `"suspended"` \| `"concluded"` \| `"superseded"` | no | Current lifecycle state of the thread. |
| `superseded_by` | string | no | max 200 chars. References the `subject_id` of the successor when `lifecycle` is `"superseded"`. |

### IdentityAnchor

A stable typed pin for deterministic thread discovery. Used inside `ThreadDescriptor.identity_anchors`.

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `kind` | string | yes | 1–40 chars. Anchor type (e.g. `"repo"`, `"project"`, `"issue"`). |
| `value` | string | yes | 1–200 chars. Anchor value (e.g. `"stef-k/CogniRelay"`, `"INGEST-42"`). |

### StablePreference

A durable user/peer preference surfaced across sessions. Tags must be unique within a capsule's list.

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `tag` | string | yes | 1–80 chars, unique within the list |
| `content` | string | yes | 1–240 chars |
| `created_at` | ISO datetime string | no | System-managed on persist. UTC (Z suffix). |
| `updated_at` | ISO datetime string | no | System-managed on persist. UTC (Z suffix). |
| `last_confirmed_at` | ISO datetime string | no | Agent-managed. Accepted only when explicitly provided. UTC (Z suffix). |

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
| `rationale_entries` | list of RationaleEntry | no | max 6, default `[]` | Decision rationale, assumptions, and unresolved tensions |
| `relationship_model` | ContinuityRelationshipModel | no | | Relationship-specific hints |
| `retrieval_hints` | ContinuityRetrievalHints | no | | Preferences for what to load next |

Optional fields have a deterministic trim order under token pressure. When the capsule must fit within a budget, the system trims from the bottom of the table upward — `retrieval_hints` first, then `relationship_model`, then `curiosity_queue`, then `rationale_entries`, and so on.

### RationaleEntry

One bounded, agent-authored decision rationale or unresolved tension. Tags must be unique within a capsule's `rationale_entries` list.

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `tag` | string | yes | 1–80 chars, unique within the list |
| `kind` | `"decision"` \| `"assumption"` \| `"tension"` | yes | |
| `status` | `"active"` \| `"superseded"` \| `"retired"` | yes | |
| `summary` | string | yes | 1–240 chars |
| `reasoning` | string | yes | 1–400 chars |
| `alternatives_considered` | list of strings | no | max 3 items, each 1–160 chars |
| `depends_on` | list of strings | no | max 3 items, each 1–120 chars |
| `supersedes` | string | no | max 80 chars. Must reference a tag in the same list with `status: "superseded"` |
| `created_at` | ISO datetime string | no | System-managed on persist. UTC (Z suffix). |
| `updated_at` | ISO datetime string | no | System-managed on persist. UTC (Z suffix). |
| `last_confirmed_at` | ISO datetime string | no | Agent-managed. Accepted only when explicitly provided. UTC (Z suffix). |

**Supersession:** to supersede an entry, set the old entry's `status` to `"superseded"` and add a new entry with `supersedes` pointing to the old tag. The old entry remains in the list for auditability; full history is preserved in git commits.

**Startup summary filtering:** only `status: "active"` entries appear in the startup summary orientation tier. Superseded and retired entries are filtered out at summary-build time but remain in the capsule for direct-read access.

### NegativeDecision

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `decision` | string | yes | 1–160 chars |
| `rationale` | string | yes | 1–240 chars |
| `created_at` | ISO datetime string | no | System-managed on persist. UTC (Z suffix). |
| `updated_at` | ISO datetime string | no | System-managed on persist. UTC (Z suffix). |
| `last_confirmed_at` | ISO datetime string | no | Agent-managed. Accepted only when explicitly provided. UTC (Z suffix). |

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
| `lifecycle_transition` | `"suspend"` \| `"resume"` \| `"conclude"` \| `"supersede"` | no | When provided, atomically transitions the capsule's `thread_descriptor.lifecycle` as part of this upsert. |
| `superseded_by` | string | no | max 200 chars. Required when `lifecycle_transition` is `"supersede"`. Sets `thread_descriptor.superseded_by`. |
| `merge_mode` | `"replace"` \| `"preserve"` | no | Default `"replace"`. When `"preserve"`, fields absent from the incoming capsule JSON are preserved from the stored capsule rather than being overwritten. See [Preserve-by-default merge](#preserve-by-default-merge) below. |

Response includes `recovery_warnings` (list of strings) when the fallback snapshot refresh fails after the active write has already committed.

Response includes `normalizations_applied` (list of strings) describing write-path normalizations that fired (e.g. `"strip:continuity.open_loops"`, `"dedup:stable_preferences"`). Empty list when no normalizations fired.

#### Session-end snapshot helper

When `session_end_snapshot` is provided, the server merges its fields into `capsule.continuity` before validation and persistence. This reduces caller burden at session end by focusing on the fixed startup-critical snapshot field set: required P0 fields `open_loops`, `top_priorities`, `active_constraints`, `stance_summary`, plus optional P1 fields `negative_decisions`, `session_trajectory`, and `rationale_entries`. The base capsule carries forward all non-snapshot fields unchanged.

**`SessionEndSnapshot` fields:**

| Field | Type | Required | Maps to | Override behavior |
|-------|------|----------|---------|-------------------|
| `open_loops` | list of strings (max 5, each ≤ 160 chars) | yes | `capsule.continuity.open_loops` | Always overrides |
| `top_priorities` | list of strings (max 5, each ≤ 160 chars) | yes | `capsule.continuity.top_priorities` | Always overrides |
| `active_constraints` | list of strings (max 5, each ≤ 160 chars) | yes | `capsule.continuity.active_constraints` | Always overrides |
| `stance_summary` | string (≤ 240 chars) | yes | `capsule.continuity.stance_summary` | Always overrides |
| `negative_decisions` | list of NegativeDecision (max 4) | no | `capsule.continuity.negative_decisions` | `null` = preserve capsule value; explicit value = override |
| `session_trajectory` | list of strings (max 5, each ≤ 80 chars) | no | `capsule.continuity.session_trajectory` | `null` = preserve capsule value; explicit value = override |
| `rationale_entries` | list of RationaleEntry (max 6) | no | `capsule.continuity.rationale_entries` | `null` = preserve capsule value; explicit value = override |

**Merge algorithm:** P0 fields (required) always override their `capsule.continuity` counterparts. P1 fields (optional) override only when non-null; null means the capsule's existing value is preserved. All other `ContinuityState` fields remain from the capsule unchanged. The merged capsule is then validated and persisted through the standard path. Note: the snapshot does not update `capsule.updated_at` — the caller must still set `updated_at` to the current time on the base capsule to avoid a 409 conflict rejection.

**Canonical hook-contract usage rule:** Under the canonical `pre_compaction_or_handoff` contract, use `session_end_snapshot` only when no write-eligible field outside the snapshot field set changed relative to the last persisted capsule after applying the snapshot overlay to the candidate state. If `active_concerns`, `drift_signals`, `stable_preferences`, or any other write-eligible non-snapshot field changed, omit `session_end_snapshot` and send a full `capsule`-only upsert instead. Direct `thread_descriptor.lifecycle` and `thread_descriptor.superseded_by` deltas are not hook-persistable through this slice-2 surface; use `lifecycle_transition`/`superseded_by` on the upsert request or the standalone lifecycle endpoint when those fields must change.

Per-item string length constraints (e.g. each ≤ 160 chars for list fields, each ≤ 80 chars for `session_trajectory`) are enforced by capsule validation after the merge, consistent with `ContinuityState` validation. See [NegativeDecision](#negativedecision) for per-item constraints on `negative_decisions` items.

**Additional response fields** (only when `session_end_snapshot` is provided):

| Key | Type | Description |
|-----|------|-------------|
| `session_end_snapshot_applied` | boolean | `true` confirms the merge was applied |
| `resume_quality` | `{"adequate": bool}` | `true` iff all P0 fields are non-empty and `stance_summary` ≥ 30 chars |

When `session_end_snapshot` is omitted or null, behavior and response are identical to the baseline — no merge, no additional response keys.

#### Preserve-by-default merge

When `merge_mode` is `"preserve"`, the service inspects the raw JSON body to determine per-field intent:

- **Required list fields** (`top_priorities`, `active_concerns`, `active_constraints`, `open_loops`, `drift_signals`): `[]` in JSON → preserve stored value; non-empty → override.
- **Optional list fields** (`working_hypotheses`, `long_horizon_commitments`, `session_trajectory`, `trailing_notes`, `curiosity_queue`, `negative_decisions`, `rationale_entries`): absent → preserve; `[]` → override to empty; `null` → clear; non-empty → override.
- **Optional object fields** (`relationship_model`, `retrieval_hints`): absent → preserve; `null` → clear; present → override.
- **Capsule-level fields** (`attention_policy`, `freshness`, `canonical_sources`, `metadata`, `stable_preferences`): absent → preserve; `null` → clear to type-appropriate empty; present → override.
- **`thread_descriptor`**: absent → preserve entire stored descriptor; `null` → clear; present → merge sub-fields (`keywords`, `scope_anchors`, `identity_anchors` are individually merge-eligible).

Session-end snapshot fields are treated as explicitly provided and are not merged from stored. Lifecycle transitions run after the merge.

### Patch — `POST /v1/continuity/patch`

Applies partial list-field mutations to an existing capsule. All operations execute atomically within the subject lock — if any operation fails, none apply.

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `subject_kind` | `"user"` \| `"peer"` \| `"thread"` \| `"task"` | yes | |
| `subject_id` | string | yes | 1–200 chars |
| `updated_at` | string (ISO UTC) | yes | Must be strictly newer than stored `updated_at` |
| `operations` | list of PatchOperation | yes | 1–10 operations |
| `commit_message` | string | no | max 240 chars |

**PatchOperation:**

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `target` | string | yes | Dotted path to list field (e.g. `continuity.open_loops`, `stable_preferences`, `thread_descriptor.keywords`) |
| `action` | `"append"` \| `"remove"` \| `"replace_at"` | yes | |
| `value` | any | append/replace_at | String for string-list targets; full object for structured-list targets |
| `match` | string | remove, structured replace_at | Exact string for string lists; key match for structured lists (`tag`, `decision`, `kind:value`) |
| `index` | integer | string-list replace_at | 0-based index |

Response includes `operations_applied` (count) and `normalizations_applied` (list).

### Lifecycle — `POST /v1/continuity/lifecycle`

Standalone lifecycle transition for thread/task capsules without a full upsert. Only mutates `lifecycle`, `superseded_by`, and `updated_at`.

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `subject_kind` | `"thread"` \| `"task"` | yes | |
| `subject_id` | string | yes | 1–200 chars |
| `transition` | `"suspend"` \| `"resume"` \| `"conclude"` \| `"supersede"` | yes | |
| `superseded_by` | string | supersede only | max 200 chars. Required when `transition` is `"supersede"` |
| `updated_at` | string (ISO UTC) | yes | Must be strictly newer than stored `updated_at` |
| `commit_message` | string | no | max 240 chars |

Response includes `lifecycle` (new state) and `previous_lifecycle`.

### Reduced-Authoring Patterns

The three endpoints above — preserve-mode upsert, patch, and lifecycle — reduce agent authoring burden through deterministic mechanical assistance. Each avoids full-capsule rewrites for common operations:

**Preserve-mode upsert** (`merge_mode="preserve"`): Send only the fields you want to change. Omitted fields are carried forward from the stored capsule. Required list fields sent as `[]` are treated as "not provided" and preserved. Example: update `stance_summary` and `top_priorities` without re-sending `open_loops`, `relationship_model`, `stable_preferences`, etc.

**Patch** (`POST /v1/continuity/patch`): Append, remove, or replace individual items in list fields without rewriting the full list. Example: append one `rationale_entry` or remove a specific `open_loop` by exact match, with atomic all-or-nothing semantics.

**Lifecycle** (`POST /v1/continuity/lifecycle`): Transition a thread or task capsule's lifecycle state (`suspend`, `resume`, `conclude`, `supersede`) without submitting a full capsule upsert. Only `lifecycle`, `superseded_by`, and `updated_at` change.

#### Mechanical vs Agent-Authored Responsibilities

These three endpoints provide *mechanical assistance* — deterministic structural operations that reduce the agent's authoring burden. CogniRelay does not generate, infer, or synthesize semantic content through any of these surfaces.

**What the system handles mechanically:** field retention in preserve mode, atomic list-item mutations via patch, standalone lifecycle transitions, write-path normalization/deduplication (reported via `normalizations_applied`), and fallback snapshot refresh after successful writes.

**What agents must still author explicitly:** all meaning-bearing content — `stance_summary`, `source`, `confidence`, priorities, constraints, concerns, loops, drift signals, rationale entries, stable preferences, negative decisions, hypotheses, commitments, trajectory, labels, keywords, scope anchors, and identity anchors. The system stores, merges, and retrieves this content but never originates it.

For the full responsibility matrix, conceptual rationale, and worked examples, see [System Overview: Mechanical Assistance and Agent Authorship](system-overview.md#mechanical-assistance-and-agent-authorship).

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
      "negative_decisions": [{"decision": "...", "rationale": "...", "created_at": "...", "updated_at": "..."}],
      "rationale_entries": [{"tag": "...", "kind": "decision", "status": "active", "summary": "...", "reasoning": "...", "created_at": "...", "updated_at": "...", "last_confirmed_at": "..."}]
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
    },
    "stable_preferences": [
      {"tag": "timezone", "content": "UTC+2 (Athens)", "created_at": "2026-03-20T10:00:00Z", "updated_at": "2026-03-20T10:00:00Z", "last_confirmed_at": "2026-03-20T10:00:00Z"},
      {"tag": "no_emojis", "content": "Do not use emojis in responses", "created_at": "2026-03-15T14:30:00Z", "updated_at": "2026-03-15T14:30:00Z", "last_confirmed_at": "2026-03-15T14:30:00Z"}
    ]
  }
}
```

`startup_summary.trust_signals` is the same trust_signals block as the top-level response key. It is `null` when `source_state` is `"missing"` or capsule is `null`.

`startup_summary.stable_preferences` is the raw list of stable preferences from the capsule, including the structured-entry timestamps (`created_at`, `updated_at`, and optional `last_confirmed_at`). It is `[]` when the capsule has no preferences and `null` when `source_state` is `"missing"`. The list is capped at 12 entries (~4 KB worst case), comparable to the orientation tier.

**`startup_summary` shape (missing capsule):**

When `source_state` is `"missing"`: `orientation` is `null`, `context` is `null`, `updated_at` is `null`, `trust_signals` is `null`, `stable_preferences` is `null`. The `recovery` block is always present and never null.

**Key order contract:** Top-level keys are always `recovery`, `orientation`, `context`, `updated_at`, `trust_signals`, `stable_preferences` in that order. Within each block, keys appear in the order shown above. Python 3.7+ dict insertion order is preserved through FastAPI/JSON serialization.

**Field defaults:**

| Condition | Field | Value |
|-----------|-------|-------|
| Capsule is `null` (missing) | `capsule_health_status` | `null` |
| Capsule is `null` (missing) | `capsule_health_reasons` | `[]` |
| Capsule has no `capsule_health` | `capsule_health_status` | `null` |
| Capsule has no `capsule_health` | `capsule_health_reasons` | `[]` |
| Legacy capsule missing `negative_decisions` | `orientation.negative_decisions` | `[]` |
| Legacy capsule missing `rationale_entries` | `orientation.rationale_entries` | `[]` |
| Legacy capsule missing `session_trajectory` | `context.session_trajectory` | `[]` |

**`negative_decisions` pass-through:** Each element in `orientation.negative_decisions` is the same structured object stored in the capsule. In this slice that means `decision`, `rationale`, and any structured-entry timestamps present on the capsule item (`created_at`, `updated_at`, optional `last_confirmed_at`) pass through unchanged. No transformation, flattening, or summarization is applied.

**`rationale_entries` filtering:** The startup summary `orientation.rationale_entries` contains only entries with `status: "active"`. Superseded and retired entries are filtered out at summary-build time but remain available in the full capsule read.

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
| `lifecycle` | `"active"` \| `"suspended"` \| `"concluded"` \| `"superseded"` | no | Filter by `thread_descriptor.lifecycle`. Only meaningful for capsules with a thread descriptor. |
| `scope_anchor` | string | no | max 200 chars. Filter to capsules whose `thread_descriptor.scope_anchors` contains this value. |
| `keyword` | string | no | max 40 chars. Filter to capsules whose `thread_descriptor.keywords` contains this value. |
| `label_exact` | string | no | max 120 chars. Filter to capsules whose `thread_descriptor.label` matches exactly. |
| `anchor_kind` | string | no | max 40 chars. Filter to capsules with an `identity_anchors` entry matching this kind. Combine with `anchor_value` for exact anchor matching. |
| `anchor_value` | string | no | max 200 chars. Filter to capsules with an `identity_anchors` entry matching this value. Requires `anchor_kind`. |
| `sort` | `"default"` \| `"salience"` | no | default `"default"`. When `"salience"`, results are sorted by the deterministic salience ranking described below. |

Response includes `artifact_state` and `retention_class` for each entry. Archive entries include `archive_stale` classification based on `COGNIRELAY_CONTINUITY_RETENTION_ARCHIVE_DAYS`. Each summary entry includes `stable_preference_count` (integer, 0 when empty, `null` for cold stubs where the count cannot be determined without decompression) and `rationale_entry_count` (total count of all entries regardless of status — active, superseded, and retired are all counted; integer, 0 when empty or pre-feature, `null` for cold stubs).

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

## Salience Ranking

When `sort="salience"` is set on `POST /v1/continuity/list`, or when capsules are returned through `POST /v1/context/retrieve`, the system applies a deterministic multi-signal sort that surfaces the most decision-relevant capsules first. Nothing is stored — salience is computed at retrieval time from in-memory capsule state.

### Sort key

Each capsule is ranked by a lexicographic key composed of six signals plus two deterministic tiebreakers, evaluated from highest to lowest priority:

| Priority | Signal | Direction | Description |
|----------|--------|-----------|-------------|
| 1 | `lifecycle_rank` | lower = better | Derived from `thread_descriptor.lifecycle`: `active` (0), `suspended` (1), `concluded` (2), `superseded` (3). Capsules without a thread descriptor sort after all lifecycle-bearing capsules. |
| 2 | `health_rank` | lower = better | Derived from `capsule_health.status`: `healthy` (0), `degraded` (1), `conflicted` (2). |
| 3 | `freshness_rank` | lower = better | Derived from the capsule's freshness phase: `fresh` (0) through `expired` (4). |
| 4 | `resume_adequate` | adequate first | `true` when the capsule has non-empty `open_loops`, `top_priorities`, `active_constraints`, and a `stance_summary` ≥ 30 chars. |
| 5 | `verification_rank` | higher = better | Derived from `verification_state.kind`: stronger verification sorts first. |
| 6 | `updated_age_seconds` | lower = better | Seconds since `updated_at`. More recent capsules sort first. `null` timestamps are treated as maximally stale. |
| 7 | `subject_kind` | alphabetical | Deterministic tiebreaker. |
| 8 | `subject_id` | alphabetical | Deterministic tiebreaker. |

The sort is total — no two capsules produce the same key.

### Per-capsule `salience` block

When salience sorting is applied, each returned capsule includes a `salience` block:

```json
{
  "salience": {
    "rank": 0,
    "sort_key": {
      "lifecycle_rank": 0,
      "health_rank": 0,
      "freshness_rank": 0,
      "resume_adequate": true,
      "verification_rank": 3,
      "updated_age_seconds": 120
    }
  }
}
```

All values are in human-readable, natural-direction form — negation and inversion are internal to the sort, not exposed here. `rank` is 0-based (0 = most salient).

### Aggregate `salience_metadata`

When salience sorting is applied, the response includes a top-level `salience_metadata` block summarising the result set:

| Field | Type | Description |
|-------|------|-------------|
| `sort_applied` | boolean | Always `true` when salience sort was used. |
| `capsule_count` | integer | Number of capsules in the result. |
| `best_lifecycle_rank` | integer | Lowest (best) lifecycle rank across all capsules. |
| `worst_health_rank` | integer | Highest (worst) health rank across all capsules. |
| `worst_freshness_rank` | integer | Highest (worst) freshness rank across all capsules. |
| `all_resume_adequate` | boolean | `true` when every capsule has adequate resume quality. |

`salience_metadata` is `null` when the result set is empty.

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
