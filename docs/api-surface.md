# API Surface

This document is the canonical human-facing summary of the currently implemented API surface. The runtime source of truth remains the service discovery and manifest endpoints exposed by the application:

- `GET /v1/manifest`
- `GET /v1/discovery/tools`
- `GET /v1/discovery/workflows`

The sections below mirror that runtime shape and group endpoints by behavior rather than implementation order.

For practical agent integration guidance, start with [Agent Onboarding](agent-onboarding.md). For capsule structure and request/response schemas, see [Payload Reference](payload-reference.md). For the higher-level system thesis, recovery model, and authority boundaries, see [Reviewer Guide](reviewer-guide.md).

This document covers the machine-facing HTTP contract. The optional `/ui` operator surface is intentionally outside that API contract: it is a local-only, read-only, server-rendered observability layer over existing read-side services, not a second programmable interface.

## Discovery and contracts

- `GET /health`: liveness and git-state check
- `GET /capabilities`: high-level feature flags
- `GET /v1/capabilities`: versioned, machine-readable feature map with per-capability metadata
- `GET /v1/manifest`: machine-readable endpoint contract
- `GET /v1/contracts`: compatibility and contract metadata
- `GET /v1/governance/policy`: governance and authority policy metadata
- `GET /v1/discovery`: machine guidance and entrypoints
- `GET /v1/discovery/tools`: tool catalog with schemas and scopes
- `GET /v1/discovery/workflows`: suggested autonomous workflows
- `GET /.well-known/cognirelay.json`: well-known discovery entrypoint
- `GET /.well-known/mcp.json`: bounded MCP supplemental descriptor advertising preferred/latest `2025-11-25` and initialize compatibility for `2025-06-18` plus `2025-11-25`
- `GET /v1/mcp`: deferred in slice 2; returns `405` with `Allow: POST`
- `POST /v1/mcp`: bounded MCP Streamable HTTP posture for the base methods `initialize`, `notifications/initialized`, `ping`, `tools/list`, and `tools/call`, plus post-bootstrap help/reference request methods including onboarding and validation-limit lookup. `initialize` accepts protocol versions `2025-06-18` and `2025-11-25`; `2025-11-25` remains preferred/latest. `clientInfo` follows the MCP `Implementation` metadata shape for the supported protocol versions, including standard optional fields such as `title`, `description`, `websiteUrl`, and `icons`. Standard request-level `params._meta` is accepted and ignored as MCP metadata on initialize, tools/list, tools/call, and runtime help/reference request methods.

## Runtime help

- `GET /v1/help`: top-level machine-facing help index
- `GET /v1/help/tools/{name}`: bounded usage guidance for one supported tool
- `GET /v1/help/topics/{id}`: bounded guidance for one supported topic
- `GET /v1/help/hooks`: canonical hook guidance
- `GET /v1/help/errors/{code}`: MCP error remediation guidance
- `GET /v1/help/onboarding`: bounded onboarding section index
- `GET /v1/help/onboarding/bootstrap`: compact startup bootstrap payload
- `GET /v1/help/onboarding/sections/{id}`: one bounded onboarding section
- `GET /v1/help/limits`: validation-limit field-path index
- `GET /v1/help/limits/{field_path}`: one validation-limit item for an exact field path

### `GET /v1/capabilities` — versioned feature map

Returns a deterministic, machine-readable feature map for the current build. No request body, no query parameters, no auth required. The response is byte-identical on every call to the same build.

```json
{
  "version": "1",
  "features": {
    "continuity.read.startup_view": {
      "summary": "Startup-oriented read view with mechanical orientation extraction"
    },
    "continuity.read.trust_signals": {
      "summary": "Mechanical trust assessment: recency, completeness, integrity, scope match"
    }
  }
}
```

**Semantics:** Presence of a key in `features` means the capability is available on this instance. Absence means it is unavailable. Keys are opaque stable identifiers — agents should treat them as exact-match strings, not parse the dots programmatically.

**v1 feature registry includes:**

| Feature key | Summary |
|---|---|
| `continuity.read.startup_view` | Startup-oriented read view with mechanical orientation extraction |
| `continuity.read.trust_signals` | Mechanical trust assessment: recency, completeness, integrity, scope match |
| `continuity.upsert.session_end_snapshot` | Additive resume-here capture on upsert for session-end handoff |
| `continuity.read.salience_ranking` | Deterministic multi-signal salience sorting on list and read paths |
| `continuity.read.thread_identity` | Thread descriptors with scope anchors and lifecycle transitions |
| `continuity.stable_preferences` | Stable user and peer preferences persisted on continuity capsules |
| `continuity.upsert.preserve_mode` | Preserve-by-default field merge on upsert with `merge_mode='preserve'` |
| `continuity.patch` | Partial list-field patch operations on existing continuity capsules |
| `continuity.lifecycle` | Standalone lifecycle transitions for thread and task capsules |
| `context.retrieve.continuity_state` | Multi-capsule continuity-oriented context bundles with fallback and degradation |
| `context.retrieve.graph_context` | Bounded derived graph context included by default on context retrieval responses |
| `continuity.read.startup_graph_summary` | Bounded derived graph summary included on startup continuity reads after base read success |
| `schedule.one_shot_reminders` | SQLite-backed one-shot reminders and task nudges surfaced by pull/list and orientation responses |
| `coordination.handoffs` | Local-first inter-agent handoff artifacts with consume tracking |
| `coordination.shared_state` | Owner-authored shared coordination artifacts with version control |
| `messaging.direct` | Tracked direct messages with ack, reject, defer, and delivery state |
| `peers.registry` | Peer registration, trust-level transitions, and manifest exchange |
| `discovery.tools` | Machine-readable tool catalog for the bounded MCP compatibility surface |

**Versioning:** `version` is the schema version, not the app version. It increments only when the response shape changes incompatibly. Adding or removing feature keys does not change the version. Clients must tolerate unknown keys. Summaries are human-readable hints, not machine-parsed contracts.

**Relationship to legacy `GET /capabilities`:** The two endpoints are independent. The legacy endpoint returns a flat string list and is unchanged.

For the bounded MCP bootstrap flow, initialize protocol compatibility, the post-bootstrap help/reference request-method additions, the tool metadata model, and the `POST /v1/mcp` posture, see `docs/mcp.md`.

## Memory, file, and index operations

- `POST /v1/write`: write text content to a repo-relative path
- `GET /v1/read`: read file content by path
- `POST /v1/append`: append one JSON object as a JSONL record
- `POST /v1/index/rebuild`: full index rebuild
- `POST /v1/index/rebuild-incremental`: incremental index rebuild
- `GET /v1/index/status`: inspect derived index state
- `POST /v1/search`: query-driven search across indexed content
- `POST /v1/recent`: latest indexed items without a search query
- `POST /v1/schedule/items`: create a one-shot reminder or task nudge
- `GET /v1/schedule/items/{schedule_id}`: read one scheduled item
- `GET /v1/schedule/items`: list scheduled items with status, due, link, subject, retired, limit, and offset filters
- `PATCH /v1/schedule/items/{schedule_id}`: update mutable fields on a pending scheduled item with `expected_version`
- `POST /v1/schedule/items/{schedule_id}/acknowledge`: mark a pending item `acknowledged` or `done`
- `POST /v1/schedule/items/{schedule_id}/retire`: retire a scheduled item without deleting it
- `POST /v1/context/retrieve`: compact continuity-oriented context bundle
- `POST /v1/continuity/upsert`: create or replace one continuity capsule
- `POST /v1/continuity/read`: load one active continuity capsule by exact selector
- `POST /v1/continuity/compare`: compare one active continuity capsule to a candidate capsule without mutating storage
- `POST /v1/continuity/revalidate`: confirm, correct, degrade, or conflict-mark one active continuity capsule
- `POST /v1/continuity/retention/plan`: persist the next deterministic stale-archive retention plan window
- `POST /v1/continuity/list`: list active continuity capsule summaries
- `POST /v1/continuity/archive`: archive one active continuity capsule and remove the active file
- `POST /v1/coordination/handoff/create`: create one local-first inter-agent handoff artifact from an active continuity capsule
- `GET /v1/coordination/handoff/{handoff_id}`: read one stored handoff artifact by id
- `GET /v1/coordination/handoffs/query`: query visible handoff artifacts for one sender and/or recipient identity
- `POST /v1/coordination/handoff/{handoff_id}/consume`: record the recipient's advisory, deferred, or rejected consume outcome
- `POST /v1/coordination/shared/create`: create one owner-authored shared coordination artifact
- `GET /v1/coordination/shared/{shared_id}`: read one stored shared coordination artifact by id
- `GET /v1/coordination/shared/query`: query visible shared coordination artifacts for one owner and/or participant identity
- `POST /v1/coordination/shared/{shared_id}/update`: replace one shared coordination artifact under owner-only version checking
- `POST /v1/coordination/reconciliation/open`: open one bounded reconciliation artifact from visible handoff/shared claims
- `GET /v1/coordination/reconciliation/{reconciliation_id}`: read one stored reconciliation artifact by id
- `GET /v1/coordination/reconciliations/query`: query visible reconciliation artifacts for one owner and/or claimant identity
- `POST /v1/coordination/reconciliation/{reconciliation_id}/resolve`: resolve one open reconciliation record under first-write-wins version checking
- `POST /v1/context/snapshot`: persist deterministic context snapshot
- `GET /v1/context/snapshot/{snapshot_id}`: load a persisted snapshot
- `POST /v1/compact/run`: compaction planning and summary/report generation

Notable behavior:

- `POST /v1/search` matches terms, not strict phrases, for multi-word queries
- `POST /v1/recent` is queryless and focused on recency
- `POST /v1/context/retrieve` is continuity-shaped output rather than a raw ranked search dump
- `POST /v1/context/retrieve` now supports optional continuity subject selection and returns additive `continuity_state` metadata when available
- `POST /v1/context/retrieve` now also accepts bounded `continuity_selectors` plus `continuity_max_capsules` for deterministic multi-capsule continuity loading
- `POST /v1/context/retrieve` now also accepts `continuity_verification_policy` to allow degraded capsules, prefer healthy capsules first, or require healthy capsules only
- `POST /v1/context/retrieve` now also accepts `continuity_resilience_policy` so callers can allow fallback snapshots, use the explicit active-first `prefer_active` mode, or require active continuity only
- when derived search indexes are stale, `POST /v1/context/retrieve` keeps indexed retrieval and adds `continuity_index_stale`; when they are missing, it falls back to a bounded raw file scan and adds `continuity_index_missing`
- `POST /v1/context/retrieve` includes `bundle.graph_context` by default. The graph section is derived from existing task and continuity artifacts, uses fixed response caps, and keeps graph warnings local to `bundle.graph_context.warnings`. When `continuity_mode="off"`, graph derivation is suppressed and the graph section contains `graph_suppressed_by_continuity_mode`.
- one-shot schedule data is stored only in SQLite at `memory/schedule/schedule.db`; create/update/acknowledge/retire do not mutate task files, continuity capsules, graph data, callbacks, commands, or messages
- `POST /v1/context/retrieve` includes `bundle.schedule_context` when scoped by primary subject or continuity selectors. `POST /v1/continuity/read` with `view="startup"` includes top-level `schedule_context`. Both are read-only, scoped to matching task/thread/subject links, and include due plus upcoming buckets.
- `POST /v1/continuity/upsert` is the V1 write path for continuity capsules under `memory/continuity/`
- successful `POST /v1/continuity/upsert` and `POST /v1/continuity/revalidate` now refresh a recovery-only fallback snapshot under `memory/continuity/fallback/`
- `POST /v1/continuity/upsert` and `POST /v1/continuity/revalidate` surface fallback snapshot failures through additive `recovery_warnings` instead of failing the already durable active write
- `POST /v1/continuity/read` now returns `source_state` plus `recovery_warnings`; exact-active behavior remains the default and structured fallback or missing-state degradation is enabled with `allow_fallback=true`
- `POST /v1/continuity/read` now accepts an optional `view="startup"` parameter; when set, the response includes a `startup_summary` block alongside the unchanged full capsule — a mechanical extraction of startup-relevant orientation fields with no additional I/O
- `POST /v1/continuity/read` with `view="startup"` also includes top-level `graph_summary` after the base read succeeds. Non-startup reads remain graph-free, and base validation/error behavior is unchanged.
- `POST /v1/continuity/refresh/plan` now returns deterministic refresh candidates and persists the latest plan under `memory/continuity/refresh_state.json`
- `POST /v1/continuity/retention/plan` now returns a bounded next-action stale-archive window, persists it under `memory/continuity/retention_state.json`, and exposes `total_candidates` plus `has_more` so operators can drain backlog through repeated plan/apply cycles
- `POST /v1/continuity/compare` returns deterministic changed fields, strongest signal, and a recommended verification outcome without mutating the active capsule
- `POST /v1/continuity/revalidate` writes verification status and capsule health through one audited git-backed continuity update
- `POST /v1/continuity/list` now supports `include_fallback`, `include_archived`, and `include_cold`, returns additive `artifact_state` plus `retention_class`, and classifies `archive_stale` using `COGNIRELAY_CONTINUITY_RETENTION_ARCHIVE_DAYS`
- `POST /v1/continuity/delete` deletes exact-selector active, fallback, and archive artifacts through one audited git-backed delete path
- `POST /v1/continuity/archive` writes an archive envelope under `memory/continuity/archive/` and removes the active capsule in one git-backed commit
- `POST /v1/ops/run` now supports host-local `continuity_cold_store`, `continuity_cold_rehydrate`, and `continuity_retention_apply` jobs for explicit continuity semi-cold storage, recovery, and stale-archive policy execution
- `POST /v1/coordination/handoff/create` projects only `continuity.active_constraints` and `continuity.drift_signals` from one exact active continuity capsule into a stored handoff artifact under `memory/coordination/handoffs/`
- handoff artifacts are additive coordination records: they do not mutate local continuity capsules, and `POST /v1/coordination/handoff/{handoff_id}/consume` records only recipient outcome fields
- `GET /v1/coordination/handoffs/query` lets senders and recipients discover visible handoffs without relying on successful message or task-reference delivery; corrupt handoff artifacts are skipped with a warning instead of failing the whole query
- `GET /v1/coordination/handoff/{handoff_id}` is visible only to the sender, the recipient, or an admin caller; `POST /v1/coordination/handoff/{handoff_id}/consume` is recipient-only
- handoff artifacts use canonical JSON serialization and git-backed rollback on create or consume commit failure
- `POST /v1/coordination/shared/create` stores owner-authored shared coordination artifacts under `memory/coordination/shared/` rather than projecting from a continuity capsule
- shared coordination artifacts expose only the bounded 5B payload of `constraints`, `drift_signals`, and `coordination_alerts`
- `GET /v1/coordination/shared/{shared_id}` is visible only to the owner, listed participants, or an admin caller; unlike query, direct read does not require `read:files`
- `GET /v1/coordination/shared/query` requires `read:files`, skips corrupt artifacts with a warning, returns list results under `shared_artifacts`, and keeps non-admin discovery bounded to the caller's own owner/participant identity
- `POST /v1/coordination/shared/{shared_id}/update` is owner-only, requires an exact `expected_version`, replaces the bounded shared arrays wholesale, and restores the prior artifact bytes if the commit fails
- shared coordination artifacts are additive coordination records: they do not mutate local continuity capsules and do not yet imply multi-writer or reconciliation semantics
- `POST /v1/coordination/reconciliation/open` stores additive reconciliation artifacts under `memory/coordination/reconciliations/` from bounded visible handoff/shared claims rather than mutating local continuity or shared coordination state
- reconciliation claims remain bounded to source artifact identity, claimant identity, claim summary, epistemic status, optional freeform evidence refs, and shared `observed_version` assertions
- `GET /v1/coordination/reconciliation/{reconciliation_id}` is visible only to the owner, listed participant peers, or an admin caller; like the 5A/5B direct-read pattern, it requires authentication but not `read:files`
- `GET /v1/coordination/reconciliations/query` requires `read:files`, applies all supplied filters conjunctively, skips corrupt artifacts with one warning, and keeps non-admin discovery bounded to the caller's own reconciliation identity
- `POST /v1/coordination/reconciliation/{reconciliation_id}/resolve` is owner-only (or admin), requires `expected_version` for first-write-wins concurrency, and writes bounded resolve fields (`status`, `resolution_outcome`, `resolution_summary`, `resolved_at`, `resolved_by`, `version`) without mutating local continuity or 5B shared coordination state
- resolve replay: if the artifact is already resolved with the same outcome and summary, the call returns `updated=false` without a new commit; a different outcome or summary returns HTTP 409
- resolve restores the prior artifact bytes on commit failure and returns HTTP 500
- first-slice reconciliation outcomes are bounded to `advisory_only`, `conflicted`, and `rejected`; stronger agreement outcomes that would mutate 5B shared artifacts or local continuity capsules are explicit non-goals of this slice
- first-slice reconciliation artifacts are disagreement records: they open, read, query, and resolve bounded disputes without mutating local continuity capsules or 5B shared coordination artifacts
- all four reconciliation endpoints are exposed through discovery tool catalog, manifest endpoint map, and MCP tool dispatch without introducing a separate transport plane
- `POST /v1/backup/create` now includes `continuity_counts` in the manifest when continuity artifacts are part of the backup scope
- `POST /v1/backup/restore-test` now accepts `verify_continuity` and returns structured `continuity_validation` details for restored active, fallback, archive, and cold continuity artifacts
- continuity capsules may now carry optional `continuity.session_trajectory` entries to preserve in-session direction changes
- continuity capsules may also carry optional `continuity.trailing_notes`, `continuity.curiosity_queue`, and `continuity.negative_decisions` fields as additive agent-owned orientation payload
- `POST /v1/continuity/read` and `POST /v1/context/retrieve` return those additive fields unchanged when present on the stored capsule and when retrieval trimming does not need to drop them
- fallback snapshots, archive envelopes, and backup/restore validation preserve those additive fields as ordinary continuity-body content; `POST /v1/continuity/list` summaries intentionally do not expand to include them
- `POST /v1/continuity/upsert` now enforces cross-field validation for `source.update_reason=interaction_boundary` and `metadata.interaction_boundary_kind`
- `POST /v1/continuity/upsert` accepts optional `lifecycle_transition` (`suspend`/`resume`/`conclude`/`supersede`) and `superseded_by` to atomically transition a capsule's `thread_descriptor.lifecycle` as part of the write — see [Payload Reference](payload-reference.md#upsert--post-v1continuityupsert) for field constraints
- `POST /v1/continuity/list` accepts thread identity filters: `lifecycle`, `scope_anchor`, `keyword`, `label_exact`, `anchor_kind`, and `anchor_value` — see [Payload Reference](payload-reference.md#list--post-v1continuitylist) for filter semantics
- `POST /v1/continuity/list` accepts `sort="salience"` to apply deterministic multi-signal salience ranking; when applied, each capsule carries a `salience` block and the response includes aggregate `salience_metadata` — see [Payload Reference](payload-reference.md#salience-ranking) for the sort key and response structure
- `POST /v1/continuity/read` and `POST /v1/context/retrieve` now include a `trust_signals` block alongside capsule data — an objective, mechanical trust assessment across four dimensions (recency, completeness, integrity, scope_match); `trust_signals` is `null` when the capsule is missing; the `startup_summary` view also includes `trust_signals` as a top-level key; `build_continuity_state` returns both per-capsule and aggregate `trust_signals`; age fields are `null` (not `0`) when timestamps are missing/malformed; `recency.phase` falls back to `"expired"` on malformed `verified_at`; aggregate trust handles compact per-capsule shapes; `recovery_warnings` includes `"trust_signals_aggregate_failed"` when aggregate computation fails
- Graph response warnings use object shape with stable codes including `graph_anchor_not_provided`, `graph_anchor_not_supported`, `graph_anchor_not_found`, `graph_derivation_failed`, `graph_truncated`, `graph_result_malformed`, `graph_source_denied`, and `graph_suppressed_by_continuity_mode`. Auth/path denials omit denied sources and never copy graph warnings into `recovery_warnings` or `continuity_state.warnings`.
- #255/#260 scheduling is implemented as one-shot reminders/task nudges plus a read-only `/ui/schedule` inspection page. Deferred/non-goals remain recurrence, background scheduler loops, SSE/push, schedule mutation UI, arbitrary execution, webhooks/callbacks, automatic task or continuity mutation, graph mutation, and graph DB integration.
- Continuity schema is now `1.1` for newly written continuity capsules and continuity archive/fallback/cold artifacts. Stabilized legacy `1.0` continuity payloads remain supported for load/upgrade when they already have the modern required capsule structure. Truly pre-stabilization payloads missing required modern capsule fields are still a bounded unsupported migration case.

## Peers and messaging

- `GET /v1/peers`: list known peers
- `POST /v1/peers/register`: create or update a peer record
- `GET /v1/peers/{peer_id}/manifest`: fetch a peer manifest
- `POST /v1/peers/{peer_id}/trust`: apply a trust transition with policy checks
- `POST /v1/messages/send`: send a direct tracked message
- `POST /v1/messages/ack`: acknowledge, reject, or defer tracked delivery
- `GET /v1/messages/pending`: inspect pending and terminal delivery state
- `GET /v1/messages/inbox`: read inbox messages
- `GET /v1/messages/thread`: read thread records
- `POST /v1/relay/forward`: relay a message and record transport artifacts
- `POST /v1/messages/verify`: verify signed envelopes and nonce replay protection
- `POST /v1/replay/messages`: replay dead-letter tracked messages

Notable behavior:

- direct delivery supports idempotency keys and acknowledgment tracking
- relay forwarding writes immutable transport records plus inbox/thread artifacts
- signed ingress can be enforced for direct and relayed message flows
- messages and relay flows may include handoff references in `attachments` using the convention `"handoff:{handoff_id}"`; the server stores these as opaque strings without validating the referenced artifact exists

## Shared work and code workflows

- `POST /v1/tasks`: create a shared task
- `PATCH /v1/tasks/{task_id}`: update task status, ownership, or metadata
- `GET /v1/tasks/query`: query tasks by workflow filters
- `POST /v1/docs/patch/propose`: propose a unified diff for docs/content
- `POST /v1/docs/patch/apply`: apply a proposed docs/content patch
- `POST /v1/code/patch/propose`: propose a unified diff for code
- `POST /v1/code/checks/run`: run a `lint`, `test`, or `build` profile and persist the artifact
- `POST /v1/code/merge`: perform merge policy evaluation using recorded check artifacts

Notable behavior:

- task transitions are deterministic and constrained
- tasks may carry a handoff reference in `metadata.handoff_id` by convention; the server stores this as arbitrary task metadata without validating or acting on it
- patch application validates working tree state and base reference compatibility
- code merge decisions depend on persisted check evidence rather than implicit local state

## Security, replication, backup, and ops

- `GET /v1/security/tokens`: inspect token metadata
- `POST /v1/security/tokens/issue`: issue scoped peer tokens
- `POST /v1/security/tokens/revoke`: revoke a token by identifier or hash
- `POST /v1/security/tokens/rotate`: atomically replace a token
- `POST /v1/security/keys/rotate`: rotate signing key material
- `GET /v1/metrics`: summarize delivery, audit, check, and replication metrics
- `POST /v1/replication/pull`: ingest a replication bundle
- `POST /v1/replication/push`: export and optionally push a replication bundle
- `POST /v1/backup/create`: create a backup archive and manifest
- `POST /v1/backup/restore-test`: validate backup recovery through a restore drill
- `GET /v1/ops/catalog`: list host-local maintenance jobs
- `GET /v1/ops/status`: inspect recent host-local maintenance runs and locks
- `POST /v1/ops/run`: execute one host-local maintenance job
- `GET /v1/ops/schedule/export`: export suggested scheduler payloads

Boundary note:

- `/v1/ops/*` is a host-local control surface and should stay behind a local trust boundary
- token and key lifecycle operations are authority actions, not normal peer collaboration flows

## Auth Model

Most mutating endpoints require bearer-token scopes plus namespace restrictions. The common patterns are:

- read access: `read:files`, `read:index`, and configured `read_namespaces`
- write access: `write:journal`, `write:messages`, `write:projects`, and configured `write_namespaces`
- search/retrieval: `search` and read namespace checks
- authority actions: security and governance scopes with host-local restrictions where applicable

Implementation notes that matter for operators and client authors:

- tokens carrying `admin:peers` bypass all scope and namespace checks — see the [Reviewer Guide](reviewer-guide.md#operator-and-host-local-boundary) for the full authority model
- split namespace controls use `read_namespaces` and `write_namespaces`
- legacy `namespaces` is still supported as a shorthand applying to both read and write
- signed message verification includes nonce replay protection
- metrics and audit behavior depend on persisted delivery, replication, and audit-log state under the repo

## Operational Semantics

- incremental indexing reflects working-tree state by default, so search can temporarily include uncommitted files after a write and before commit
- compaction is a planner/orchestrator that emits structured reports; it is not the summarizing model itself
- context snapshots persist deterministic artifacts under `snapshots/context/`
- relay and direct messaging rely on persisted delivery state and replay tracking rather than in-memory status only
- host-local ops endpoints should rely on local transport boundaries; forwarded headers are proxy metadata, not a remote trust substitute

For exact runtime expectations, use `GET /v1/discovery/tools` and `GET /v1/manifest`.

## Changelog

- **#165 — Startup view**: Added `view="startup"` parameter on `POST /v1/continuity/read`. Returns a `startup_summary` block with recovery/orientation/context tiers alongside the unchanged full capsule. See [Payload Reference](payload-reference.md#startup-view-viewstartup).
- **#167 — Session-end snapshot**: Added `session_end_snapshot` on `POST /v1/continuity/upsert`. A compact helper that merges startup-critical fields into the capsule before persistence. See [Payload Reference](payload-reference.md#session-end-snapshot-helper).
- **#121 — Trust signals**: Added `trust_signals` block on `POST /v1/continuity/read` and `POST /v1/context/retrieve`. Four-dimension mechanical trust assessment (recency, completeness, integrity, scope match). See [Payload Reference](payload-reference.md#read--post-v1continuityread).
- **#124 — Stable user preferences**: Added `stable_preferences` (list of `StablePreference`) on `ContinuityCapsule`. Bounded to 12 entries; only valid on user/peer capsules. Included in read, startup summary, and context-retrieve paths. List summaries include `stable_preference_count`. Trimmed as a whole unit under token pressure. See [Payload Reference](payload-reference.md#stablepreference).
- **#122 — Rationale entries**: Added `rationale_entries` (list of `RationaleEntry`, max 6) on `ContinuityState`. Captures decision rationale, assumptions, and unresolved tensions with kind/status lifecycle and supersession semantics. See [Payload Reference](payload-reference.md#rationaleentry).
- **#120 — Thread identity and scope boundaries**: Added `thread_descriptor` on `ContinuityCapsule` with `ThreadDescriptor` model (label, keywords, scope anchors, identity anchors, lifecycle, superseded_by). Added `lifecycle_transition` and `superseded_by` on upsert. Added list filters: `lifecycle`, `scope_anchor`, `keyword`, `label_exact`, `anchor_kind`, `anchor_value`. See [Payload Reference](payload-reference.md#threaddescriptor).
- **#123 — Salience ranking**: Added `sort="salience"` on `POST /v1/continuity/list` and deterministic salience sorting on context-retrieve paths. Six-signal sort key with per-capsule `salience` block and aggregate `salience_metadata`. See [Payload Reference](payload-reference.md#salience-ranking).
- **#194 — Structured-entry timestamp refinement**: Continuity schema `1.1` replaces structured-entry `set_at` with `created_at`, `updated_at`, and `last_confirmed_at` for `stable_preferences`, `rationale_entries`, and `negative_decisions`. Stabilized-shape legacy continuity payloads remain supported for upgrade; truly pre-stabilization payloads missing required modern fields are not auto-migrated. Sammy's oldest real continuity capsule sample falls into the supported stabilized-shape legacy bucket. See [Payload Reference](payload-reference.md#continuity-capsule-structure).
- **#179 — `GET /v1/capabilities`**: Added versioned, machine-readable feature map endpoint. The registry now covers continuity enhancements, graph context, schedule reminders, coordination, messaging, peers, and discovery. See [Discovery and contracts](#get-v1capabilities--versioned-feature-map) above.
- **#163 — Python CLI client**: Added `tools/cognirelay_client.py`, a stdlib-only command-line client for continuity read, upsert, and token hashing. See [CLI Client](cognirelay-client.md).
