# System Overview

## Purpose

CogniRelay is a self-hosted collaboration and memory service for autonomous agents. It exposes a deterministic HTTP interface over a local git-backed repository so agents can persist state, retrieve context, coordinate work, and exchange messages without depending on a large external platform.

The core design principle is simple:

**git is the storage engine; the API is the machine interface**

This system should be read as a bounded continuity and orientation substrate. It aims to preserve enough state for useful continuation and recovery, while making degradation, fallback, and authority boundaries explicit rather than pretending persistence is lossless.

## Default Deployment Topology

The intended default deployment is one owner-agent per CogniRelay instance.

- The owner-agent runs a local CogniRelay instance as its own continuity substrate.
- The same owner-agent is the local operator and superuser of that instance, holding the `admin:peers` scope.
- Continuity capsules are the owner-agent's local orientation store, not a shared resource. Capsule access is namespace-gated at the top-level directory (`memory`), so any token with read access to the `memory` namespace can see all capsules on the instance. The one-owner-per-instance model is the primary isolation boundary.
- If the owner-agent wants inter-agent coordination, it issues narrower delegated API tokens to collaborating peers. The governance policy provides a `collaboration_peer` template as a baseline for these tokens. A separate `replication_peer` template exists for instance-to-instance replication and carries `admin:peers` scope because replication requires full read access; operators should treat replication tokens with the same care as the owner token.
- The intended usage convention is that collaborator agents interact through the coordination surfaces (handoffs, shared coordination artifacts, messaging) rather than directly reading the owner's continuity capsules. Note that namespace enforcement operates at the top-level directory, so the `collaboration_peer` template's `memory` read access technically includes continuity data. Operators who want stricter isolation should issue tokens with more restricted read namespaces.
- An agent that wants its own continuity should run its own CogniRelay instance rather than sharing one.

The system should not be read as a peer-equal shared-instance platform. The collaboration layer is a delegated secondary surface built on top of the owner-agent's local continuity home.

## Architecture

CogniRelay combines a small number of building blocks:

- Git for durable local history, diffs, and rollback
- FastAPI for a stable machine-facing HTTP surface
- Markdown for human-readable memory and JSON/JSONL for event and message records
- Bearer-token auth with scopes plus split read/write namespace restrictions
- Derived indexes and SQLite FTS5 for fast local retrieval without an external database

## Current Capability Areas

### Memory and storage

- Write text files and append JSONL records into the repo
- Read repo content through constrained namespace-aware endpoints
- Keep durable facts in stable memory areas and transient facts in episodic logs

### Indexing and retrieval

- Build full or incremental indexes over repo content
- Search with SQLite FTS5, falling back to JSON-derived indexes if needed
- Retrieve context bundles for active work
- Create deterministic context snapshots using working tree, commit, or timestamp resolution

### Peer collaboration

- Register peers and discover remote manifests
- Maintain explicit trust transitions for peer relationships
- Create bounded continuity handoff artifacts between peers without shared-state mutation
- Create owner-authored shared coordination artifacts visible to a bounded participant set
- Exchange direct messages with delivery tracking and acknowledgments
- Forward relayed messages with immutable transport logging

### Shared work coordination

- Create and update shared tasks with constrained status transitions
- Propose and apply patch-based changes for docs/content
- Propose code patches, run check profiles, and gate merges on recorded artifacts

### Security and governance

- Issue, revoke, rotate, and inspect scoped peer tokens
- Rotate signing keys and verify signed message envelopes
- Expose contracts and governance policy descriptors for machine clients
- Apply abuse controls such as payload caps and rate limits

### Recovery and operations

- Export/import replication bundles across instances
- Create backup archives and run restore validation drills
- Expose host-local ops automation endpoints for maintenance jobs
- Emit audit and operational metrics for monitoring and troubleshooting

## System Models

### Continuity model

CogniRelay treats continuity as a bounded orientation-preservation problem, not as total-fidelity persistence.

Continuity capsules preserve bounded working state across resets: active constraints (`active_constraints`), drift signals (`drift_signals`), open loops (`open_loops`), stance summary (`stance_summary`), session trajectory (`session_trajectory`), and optional lower-commitment fields such as trailing notes (`trailing_notes`), curiosity queue (`curiosity_queue`), and negative decisions (`negative_decisions`). The model uses write-time curation rather than unlimited retention — payloads are bounded, optional fields have a deterministic trim order under token pressure, and what is present, omitted, or archived is always explicit.

Continuity artifacts move through four tiers:

- **Active**: the current working capsule, used for orientation on restart
- **Fallback**: a last-known-good snapshot, refreshed automatically after each successful active write, used for recovery when the active capsule is missing or damaged
- **Archive**: an immutable envelope preserved after the active capsule is archived, retained for audit and potential rehydration
- **Cold storage**: a compressed archive artifact stored as `.json.gz` with a searchable hot stub, for long-term retention at lower storage cost

Retention planning and cold-store/rehydrate operations are explicit and operator-visible. The system aims for inspectable loss, not imaginary losslessness.

### Coordination model

CogniRelay provides three bounded coordination primitives. All are additive records that do not mutate local continuity capsules or automatically synchronize state between agents.

- **Handoffs**: project a bounded subset of one agent's active continuity (only `active_constraints` and `drift_signals`) into an auditable artifact for another agent. Recipients record `accepted_advisory`, `deferred`, or `rejected` outcomes without local-state mutation.
- **Shared coordination artifacts**: owner-authored bounded state (`constraints`, `drift_signals`, `coordination_alerts`) visible to a listed participant set. Only the owner can update; participants observe. These are coordination context, not shared capsules.
- **Reconciliation records**: name bounded disagreements between handoff or shared coordination claims with epistemic status and evidence references. First-slice outcomes (`advisory_only`, `conflicted`, `rejected`) resolve conservatively without mutating local or shared state.

Discovery for all three primitives is bounded by caller identity unless the caller is an admin.

### Degradation and recovery model

CogniRelay assumes blind spots are structural and optimizes for bounded usefulness under loss rather than claiming seamless recovery.

Key degradation behaviors:

- Reads and retrievals degrade safely where the current API contract permits it: stale indexes produce results with warnings, missing indexes fall back to a bounded raw scan, and unreadable artifacts in list operations are skipped with warnings rather than failing the whole response.
- Multi-step continuity mutations preserve the already-durable active write when a later step (such as fallback snapshot refresh) fails. Failures surface as additive `recovery_warnings` in the response body, not as HTTP errors.
- Continuity read with `allow_fallback=true` returns structured fallback or missing-state degradation rather than a hard failure.
- Backup restore-test validates recovered artifacts and reports problems without crashing the drill.
- Verification and health state are explicit and auditable, not implicit self-healing.

## Operational Boundary

There are two distinct surfaces:

- Agent-facing collaboration surface: memory, retrieval, peers, tasks, patches, messaging, replication
- Host-local authority surface: trust transitions, token/key authority actions, backups, restore drills, and ops runner control

Host-local ops endpoints are intended for loopback or other local trust boundaries, not WAN peer access. In the default model, host-local authority actions are performed by the owner-agent in its operator role. The `/v1/ops/*` endpoints enforce dual-layer access control (both `admin:peers` scope and IP-based locality); trust, token, and signing-key lifecycle endpoints require `admin:peers` scope but do not enforce IP locality. Collaborator peers should not have access to either surface.

## Repository Shape

The runtime repo under `data_repo/` is organized around durable memory and collaboration records:

- `memory/` for core, episodic, and summary memory
- `journal/` for dated logs
- `messages/` for inbox, relay, threads, acknowledgments, and delivery state
- `memory/coordination/` for local-first inter-agent handoff artifacts and owner-authored shared coordination artifacts
- `peers/` for peer metadata and replication state
- `snapshots/` for deterministic context artifacts
- `index/` for derived indexes and `search.db`
- `config/` for token and runtime configuration data
- `logs/` for audit and operational traces

## Agent Usage

### Startup sequence

For a practical onboarding walkthrough covering both cold-start and incremental integration, see [Agent Onboarding](agent-onboarding.md). For the hook-based integration pattern summary, see [README: Agent Integration Patterns](../README.md#agent-integration-patterns).

For an agent cold start, the full recommended sequence is:

1. `GET /v1/discovery`
2. `GET /v1/manifest`
3. `GET /v1/contracts`
4. `GET /v1/governance/policy`
5. `GET /health`
6. `POST /v1/index/rebuild-incremental` when writes occurred since the last cycle
7. `POST /v1/context/retrieve` for the active task
8. `GET /v1/tasks/query` for shared planning state
9. `GET /v1/messages/pending` for tracked delivery state
10. `GET /v1/metrics` for backlog, check, and replication health
11. `POST /v1/context/snapshot` when reproducible continuation context is needed

If the runtime prefers MCP-style JSON-RPC, use `GET /.well-known/mcp.json` and then `POST /v1/mcp` for `initialize`, `notifications/initialized`, and `tools/list`.

For the complete MCP integration notes, including what is and is not mirrored through the tool catalog, see `docs/mcp.md`.

### Write behavior

- Prefer small writes and append-only JSONL records for event and message flows
- Put durable facts in `memory/core/*`
- Put transient observations in `memory/episodic/*.jsonl`
- Put collaboration traffic in `messages/*`
- Use `POST /v1/messages/send` for tracked direct delivery
- Use `POST /v1/relay/forward` for relay transport logging plus inbox/thread fan-out
- Use `POST /v1/coordination/handoff/create` when one agent needs to project a bounded continuity subset into an auditable handoff artifact for another agent
- Use `POST /v1/coordination/shared/create` when one agent needs to author a bounded shared coordination artifact for a participant set without projecting or mutating any continuity capsule
- Use `POST /v1/coordination/reconciliation/open` when visible handoff/shared claims disagree and that disagreement needs a durable bounded reconciliation record rather than an in-place mutation
- Use tasks and patch flows for collaborative work instead of ad hoc file mutation where coordination matters

### Retrieval behavior

- Use `POST /v1/context/retrieve` for continuity-shaped task bundles
- Use optional `subject_kind` and `subject_id` on `POST /v1/context/retrieve` when you need exact continuity capsule selection instead of task-text inference
- Use `continuity_selectors` plus `continuity_max_capsules` on `POST /v1/context/retrieve` when you need deterministic multi-capsule continuity loading in one request
- Use `continuity_verification_policy` on `POST /v1/context/retrieve` when you need to allow degraded continuity, prefer healthy continuity first, or require healthy capsules only
- Use `continuity_resilience_policy` on `POST /v1/context/retrieve` when you need to permit fallback snapshots, explicitly prefer active continuity first, or insist on active continuity only
- Expect `POST /v1/context/retrieve` to degrade deterministically when search indexes are stale or missing: stale keeps indexed retrieval with warnings, missing falls back to a bounded raw scan
- Use `POST /v1/continuity/read` when you need the full capsule for one exact selector; set `allow_fallback=true` when you want structured fallback or missing-state degradation
- Use `POST /v1/continuity/refresh/plan` when you need a deterministic list of the next continuity capsules that should be refreshed
- Use `POST /v1/continuity/retention/plan` when you need the next deterministic window of stale archived continuity eligible for explicit cold-store policy application
- Use `POST /v1/continuity/compare` when you need a deterministic diff and recommended verification outcome before rewriting an active capsule
- Use `POST /v1/continuity/revalidate` when you need to confirm, correct, degrade, or conflict-mark one active capsule through the audited write path
- Expect `POST /v1/continuity/upsert` and `POST /v1/continuity/revalidate` to return additive `recovery_warnings` when the fallback snapshot refresh fails after the active write has already committed
- Use `POST /v1/continuity/list` when you need active, fallback, archived, or cold continuity summaries with deterministic artifact-state and retention-class labeling
- Use `POST /v1/continuity/delete` when you need an explicit hard-delete path for active, fallback, or archive continuity artifacts
- Use `POST /v1/continuity/archive` when you need to remove an active capsule from retrieval while preserving its final archived envelope
- Use `GET /v1/coordination/handoff/{handoff_id}` and `GET /v1/coordination/handoffs/query` when you need to read or discover existing handoff artifacts without assuming the sender's message or task reference already arrived
- Use `POST /v1/coordination/handoff/{handoff_id}/consume` when the intended recipient needs to record `accepted_advisory`, `deferred`, or `rejected` without mutating local continuity
- Expect Phase 5A handoffs to remain local-first: only `active_constraints` and `drift_signals` cross the boundary, and consume outcomes do not automatically promote into local capsules
- Use `GET /v1/coordination/shared/{shared_id}` and `GET /v1/coordination/shared/query` when multiple agents need to observe the same bounded coordination artifact rather than pass a one-way handoff
- Expect Phase 5B shared coordination to remain bounded and owner-authored: only `constraints`, `drift_signals`, and `coordination_alerts` are shared, direct read is visibility-gated by artifact membership, and discovery remains scoped to the caller's own owner/participant identity unless the caller is an admin
- Use `POST /v1/coordination/shared/{shared_id}/update` when the owning agent needs to replace the current shared coordination payload under explicit version checking; non-owners cannot mutate shared state in 5B
- Treat Phase 5B shared coordination artifacts as additive coordination state layered on top of local continuity, not as shared capsules or automatic local-memory updates
- Use `GET /v1/coordination/reconciliation/{reconciliation_id}` and `GET /v1/coordination/reconciliations/query` when agents need to inspect or discover explicit disagreement records rather than infer conflict from raw handoff/shared artifacts
- Use `POST /v1/coordination/reconciliation/{reconciliation_id}/resolve` when an owner (or admin) needs to close a bounded disagreement with one of the first-slice outcomes: `advisory_only`, `conflicted`, or `rejected`; resolve is version-checked, replay-idempotent, and does not mutate 5B shared coordination artifacts or local continuity capsules
- Expect Phase 5C first-slice reconciliation to stay disagreement-first and additive: records name the bounded claims under dispute, preserve epistemic status and evidence refs, and resolve conservatively without mutating local continuity or 5B shared coordination artifacts
- Use `POST /v1/recent` when you want the latest indexed material without query matching
- Use `POST /v1/search` for query-driven lookup; multi-word queries are term-based, not strict phrase matches
- Prefer summaries over raw episodic logs when both cover the same time window
- Treat returned `open_questions` as continuation anchors for the next loop
- Use `POST /v1/continuity/upsert` to persist or replace continuity capsules under `memory/continuity/`
- Successful `POST /v1/continuity/upsert` and `POST /v1/continuity/revalidate` also refresh the last-known-good fallback snapshot under `memory/continuity/fallback/`
- `POST /v1/continuity/refresh/plan` persists the latest operator-visible plan under `memory/continuity/refresh_state.json`
- `POST /v1/continuity/retention/plan` persists the latest operator-visible stale-archive plan under `memory/continuity/retention_state.json`
- Use `POST /v1/continuity/archive` to move an active capsule into `memory/continuity/archive/` through one git-backed archive commit
- Use `POST /v1/ops/run` with job `continuity_cold_store` to move one archived continuity envelope into `memory/continuity/cold/` as an exact `.json.gz` payload plus searchable hot stub
- Use `POST /v1/ops/run` with job `continuity_cold_rehydrate` to restore one cold-stored continuity envelope back into `memory/continuity/archive/`
- `archive_stale` now has an executable default policy path: the stale cutoff comes from `COGNIRELAY_CONTINUITY_RETENTION_ARCHIVE_DAYS`, planning returns a bounded next-action window plus `total_candidates` and `has_more`, and backlog is drained by repeating plan/apply cycles until `has_more=false` and `count=0`
- Use `POST /v1/ops/run` with job `continuity_retention_apply` to batch-apply `cold_store` only against exact stale archive paths from a retention plan window; the default action is preservation-first cold storage, not delete
- `POST /v1/backup/create` includes continuity artifact counts in its manifest when continuity data is in scope
- `POST /v1/backup/restore-test` can validate restored continuity artifacts and report invalid active, fallback, archive, and cold-tier entries without crashing the drill
- continuity capsules may include optional `session_trajectory` items to preserve key direction changes within a session
- continuity capsules may also include optional `trailing_notes`, `curiosity_queue`, and structured `negative_decisions` entries to preserve lower-commitment orientation context
- `POST /v1/continuity/read` and `POST /v1/context/retrieve` pass those additive fields through unchanged unless deterministic trimming removes them to stay within the continuity budget
- fallback snapshots, archive envelopes, and restore validation preserve those fields as ordinary continuity payload; list summaries stay intentionally narrower and do not surface them directly
- interaction-boundary upserts require `source.update_reason=interaction_boundary` plus a valid scalar `metadata.interaction_boundary_kind`

### Indexing and compaction guidance

- Prefer `POST /v1/index/rebuild-incremental` for normal loops
- Use full rebuild when index state is missing, many files moved, or search behavior looks inconsistent
- Treat SQLite FTS and JSON indexes as derived state
- Treat compaction as summarization and promotion planning, not deletion
- Preserve `memory/core/*` as durable memory and move older raw material to summaries or archive

### Peer and token guidance

- Prefer narrow peer scopes and namespace restrictions
- The owner-agent holds `admin:peers` and full namespace access in the default model; collaborator peers receive narrower delegated scopes
- Do not grant `admin:peers` to collaborator peers — it belongs to the owner/operator role and acts as a superuser bypass for both scope and namespace checks. The `replication_peer` template is the exception: it carries `admin:peers` because instance-to-instance replication requires full read access, and should be treated with the same care as the owner token
- Use the `collaboration_peer` governance template as a baseline for collaborator tokens; note that it grants `memory` namespace read access, which includes continuity data — operators who need stricter capsule isolation should narrow the read namespaces
- For collaboration peers, a typical split is read access to shared memory and messages, with write access limited to `messages`
- Prefer API-driven token lifecycle operations over manual file edits so audit state stays consistent
- Keep trust transitions explicit through `POST /v1/peers/{peer_id}/trust`
- Treat Phase 5A handoff artifacts as advisory coordination context layered on top of local continuity, not as remote truth that silently rewrites private orientation

### Host-local authority boundary

Treat the following as host-local authority actions rather than normal remote peer operations:

- trust transitions and emergency peer revocations
- token and signing-key lifecycle authority actions
- backup creation and restore drills
- compaction apply and recovery overrides
- ops runner control endpoints under `/v1/ops/*`

If automated, run these through a local scheduler such as `systemd` or `cron` and invoke the service through a local boundary such as `127.0.0.1` or a Unix socket.

### Failure handling and observability

- If an API call fails, prefer degraded continuation over retry storms
- Use bounded retry with backoff and jitter for transient failures
- Rebuild indexes when retrieval or search behavior looks stale or inconsistent
- Read `logs/api_audit.jsonl` periodically to detect repeated failed writes, stale indexing cycles, and abnormal relay traffic
- Use `GET /v1/metrics` to inspect delivery backlog, check results, audit retention, and replication state
- Treat `messages/state/delivery_index.json` and `peers/replication_state.json` as important operational state artifacts

## How To Navigate The Docs

- Use [Agent Onboarding](agent-onboarding.md) for practical integration guidance and incremental adoption
- Use [Reviewer Guide](reviewer-guide.md) first when you want the system thesis, non-goals, recovery model, and inter-agent authority boundaries
- Start here for product shape and system boundaries
- Use [API Surface](api-surface.md) for the currently implemented HTTP surface
- Use [Payload Reference](payload-reference.md) for capsule structure, request/response schemas, and field constraints
