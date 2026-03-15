# API Surface

This document is the canonical human-facing summary of the currently implemented API surface. The runtime source of truth remains the service discovery and manifest endpoints exposed by the application:

- `GET /v1/manifest`
- `GET /v1/discovery/tools`
- `GET /v1/discovery/workflows`

The sections below mirror that runtime shape and group endpoints by behavior rather than implementation order.

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
- `POST /v1/continuity/list`: list active continuity capsule summaries
- `POST /v1/continuity/archive`: archive one active continuity capsule and remove the active file
- `POST /v1/context/snapshot`: persist deterministic context snapshot
- `GET /v1/context/snapshot/{snapshot_id}`: load a persisted snapshot
- `POST /v1/compact/run`: compaction planning and summary/report generation

Notable behavior:

- `POST /v1/search` matches terms, not strict phrases, for multi-word queries
- `POST /v1/recent` is queryless and focused on recency
- `POST /v1/context/retrieve` is continuity-shaped output rather than a raw ranked search dump
- `POST /v1/context/retrieve` now supports optional continuity subject selection and returns additive `continuity_state` metadata when available
- `POST /v1/context/retrieve` now also accepts bounded `continuity_selectors` plus `continuity_max_capsules` for deterministic multi-capsule continuity loading
- `POST /v1/continuity/upsert` is the V1 write path for continuity capsules under `memory/continuity/`
- `POST /v1/continuity/read` returns the raw active capsule payload for one exact selector
- `POST /v1/continuity/compare` returns deterministic changed fields, strongest signal, and a recommended verification outcome without mutating the active capsule
- `POST /v1/continuity/list` returns active-only summaries, skipping archive entries and invalid active files
- `POST /v1/continuity/archive` writes an archive envelope under `memory/continuity/archive/` and removes the active capsule in one git-backed commit
- continuity capsules may now carry optional `continuity.session_trajectory` entries to preserve in-session direction changes
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
