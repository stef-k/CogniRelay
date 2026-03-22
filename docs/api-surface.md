# API Surface

This document is the canonical human-facing summary of the currently implemented API surface. The runtime source of truth remains the service discovery and manifest endpoints exposed by the application:

- `GET /v1/manifest`
- `GET /v1/discovery/tools`
- `GET /v1/discovery/workflows`

The sections below mirror that runtime shape and group endpoints by behavior rather than implementation order.

For practical agent integration guidance, start with [Agent Onboarding](agent-onboarding.md). For the higher-level system thesis, recovery model, and authority boundaries, see [Reviewer Guide](reviewer-guide.md).

## Discovery and contracts

- `GET /health`: liveness and git-state check
- `GET /capabilities`: high-level feature flags
- `GET /v1/manifest`: machine-readable endpoint contract
- `GET /v1/contracts`: compatibility and contract metadata
- `GET /v1/governance/policy`: governance and authority policy metadata
- `GET /v1/discovery`: machine guidance and entrypoints
- `GET /v1/discovery/tools`: tool catalog with schemas and scopes
- `GET /v1/discovery/workflows`: suggested autonomous workflows
- `GET /.well-known/cognirelay.json`: well-known discovery entrypoint
- `GET /.well-known/mcp.json`: MCP compatibility descriptor
- `POST /v1/mcp`: JSON-RPC bridge for `initialize`, `notifications/initialized`, `ping`, `tools/list`, and `tools/call`

For the MCP bootstrap flow, tool metadata model, and HTTP-to-MCP relationship, see `docs/mcp.md`.

## Memory, file, and index operations

- `POST /v1/write`: write text content to a repo-relative path
- `GET /v1/read`: read file content by path
- `POST /v1/append`: append one JSON object as a JSONL record
- `POST /v1/index/rebuild`: full index rebuild
- `POST /v1/index/rebuild-incremental`: incremental index rebuild
- `GET /v1/index/status`: inspect derived index state
- `POST /v1/search`: query-driven search across indexed content
- `POST /v1/recent`: latest indexed items without a search query
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
- `POST /v1/continuity/upsert` is the V1 write path for continuity capsules under `memory/continuity/`
- successful `POST /v1/continuity/upsert` and `POST /v1/continuity/revalidate` now refresh a recovery-only fallback snapshot under `memory/continuity/fallback/`
- `POST /v1/continuity/upsert` and `POST /v1/continuity/revalidate` surface fallback snapshot failures through additive `recovery_warnings` instead of failing the already durable active write
- `POST /v1/continuity/read` now returns `source_state` plus `recovery_warnings`; exact-active behavior remains the default and structured fallback or missing-state degradation is enabled with `allow_fallback=true`
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
- messages and relay flows may carry stable handoff references via `attachments: ["handoff:{handoff_id}"]`; handoff ids become valid only after the referenced handoff artifact commits successfully

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
- tasks may carry a coordination handoff link through `metadata.handoff_id`; 5A treats this as a deterministic reference convention rather than automatic shared-state coupling
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
