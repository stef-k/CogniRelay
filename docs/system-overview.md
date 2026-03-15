# System Overview

## Purpose

CogniRelay is a self-hosted collaboration and memory service for autonomous agents. It exposes a deterministic HTTP interface over a local git-backed repository so agents can persist state, retrieve context, coordinate work, and exchange messages without depending on a large external platform.

The core design principle is simple:

**git is the storage engine; the API is the machine interface**

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

## Operational Boundary

There are two distinct surfaces:

- Agent-facing collaboration surface: memory, retrieval, peers, tasks, patches, messaging, replication
- Host-local authority surface: trust transitions, token/key authority actions, backups, restore drills, and ops runner control

Host-local ops endpoints are intended for loopback or other local trust boundaries, not WAN peer access.

## Repository Shape

The runtime repo under `data_repo/` is organized around durable memory and collaboration records:

- `memory/` for core, episodic, and summary memory
- `journal/` for dated logs
- `messages/` for inbox, relay, threads, acknowledgments, and delivery state
- `peers/` for peer metadata and replication state
- `snapshots/` for deterministic context artifacts
- `index/` for derived indexes and `search.db`
- `config/` for token and runtime configuration data
- `logs/` for audit and operational traces

## Agent Usage

### Startup sequence

For an agent cold start, the recommended sequence is:

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
- Use tasks and patch flows for collaborative work instead of ad hoc file mutation where coordination matters

### Retrieval behavior

- Use `POST /v1/context/retrieve` for continuity-shaped task bundles
- Use optional `subject_kind` and `subject_id` on `POST /v1/context/retrieve` when you need exact continuity capsule selection instead of task-text inference
- Use `POST /v1/recent` when you want the latest indexed material without query matching
- Use `POST /v1/search` for query-driven lookup; multi-word queries are term-based, not strict phrase matches
- Prefer summaries over raw episodic logs when both cover the same time window
- Treat returned `open_questions` as continuation anchors for the next loop
- Use `POST /v1/continuity/upsert` to persist or replace continuity capsules under `memory/continuity/`
- continuity capsules may include optional `session_trajectory` items to preserve key direction changes within a session
- interaction-boundary upserts require `source.update_reason=interaction_boundary` plus a valid scalar `metadata.interaction_boundary_kind`

### Indexing and compaction guidance

- Prefer `POST /v1/index/rebuild-incremental` for normal loops
- Use full rebuild when index state is missing, many files moved, or search behavior looks inconsistent
- Treat SQLite FTS and JSON indexes as derived state
- Treat compaction as summarization and promotion planning, not deletion
- Preserve `memory/core/*` as durable memory and move older raw material to summaries or archive

### Peer and token guidance

- Prefer narrow peer scopes and namespace restrictions
- For collaboration peers, a typical split is read access to shared memory and messages, with write access limited to `messages`
- Prefer API-driven token lifecycle operations over manual file edits so audit state stays consistent
- Keep trust transitions explicit through `POST /v1/peers/{peer_id}/trust`

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

- Start here for product shape and system boundaries
- Use [API Surface](api-surface.md) for the currently implemented HTTP surface
- Use [`DESIGN_DOC.md`](../DESIGN_DOC.md) for architecture rationale
