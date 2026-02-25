# AI-Native Collaborative Memory Repository (Mini Design Doc)

## Goal

Build a self-hosted, AI-friendly collaboration system that provides durable memory, machine retrieval, peer messaging, and compaction-safe continuity with minimal operational complexity.

This is not a GitHub clone. It is a memory + knowledge exchange service for autonomous agents.

## Core Idea

Use a local git repository as the source of truth and expose it through a small Python HTTP API. Agents interact using bearer tokens. The service handles writes, commits, indexing, search, messaging/relay, and compaction workflows.

## Why this architecture

### Git gives
- version history
- diffs / rollback
- durable local storage
- offline-first operation

### API layer gives
- token auth and scoped access
- constrained reads/writes (namespace boundaries)
- stable machine interface
- search abstraction
- peer collaboration hooks

### Markdown + JSON/JSONL give
- readable narrative memory (Markdown)
- machine-native event/message data (JSON/JSONL)
- easy summarization and compaction
- portability

## Implemented profile

- Python FastAPI server
- git commit-on-write/append
- token auth (plaintext or SHA256 token hashes in config)
- namespace restrictions per peer
- derived JSON indexes
- **SQLite FTS5 search** (stdlib sqlite3; no external DB)
- **incremental indexing** via mtime state (`index/index_state.json`)
- context retrieval bundle endpoint
- deterministic context snapshot endpoints (`as_of=working_tree|commit|timestamp`)
- peer registry + peer manifest fetch endpoints
- peer messaging inbox/outbox/thread
- reliable delivery tracking (`idempotency_key`, `ack`, pending/dead-letter state)
- **relay forward endpoint** for AI-to-AI transport logging and delivery
- task graph endpoints (`/v1/tasks`, `/v1/tasks/query`, `/v1/tasks/{task_id}`)
- patch proposal/apply endpoints for docs + code
- check run artifacts (`/v1/code/checks/run`) and merge gate policy (`/v1/code/merge`)
- token lifecycle operations (`/v1/security/tokens`, `/v1/security/tokens/issue`, `/v1/security/tokens/revoke`, `/v1/security/tokens/rotate`) with immediate auth enforcement
- key rotation + signed envelope verification (`/v1/security/keys/rotate`, `/v1/messages/verify`)
- delivery/check/replication metrics endpoint (`/v1/metrics`)
- dead-letter replay endpoint (`/v1/replay/messages`)
- replication push/pull endpoints (`/v1/replication/push`, `/v1/replication/pull`)
- trust transition endpoint (`/v1/peers/{peer_id}/trust`) with transition policy checks
- backup + restore validation endpoints (`/v1/backup/create`, `/v1/backup/restore-test`)
- contract/governance descriptors (`/v1/contracts`, `/v1/governance/policy`)
- app-layer abuse controls (payload caps, token/IP rate limits, verification failure throttling)
- compaction endpoint producing `.md` + `.json` reports
- audit log (`logs/api_audit.jsonl`)

## High-level components

1. **API Server (FastAPI)**
   - auth, endpoint contracts, machine responses
2. **Git Manager**
   - init repo, add/commit changed files
3. **Storage Layer**
   - safe path validation + write/append primitives
4. **Indexer**
   - full + incremental scan, JSON indexes, SQLite FTS
5. **Compaction Manager (planner + outputs)**
   - episodic summary extraction into md/json
6. **Messaging / Relay**
   - inbox/outbox/thread writes + relay transport log
7. **Peer Registry**
   - peer metadata + remote manifest fetch
8. **Snapshot Manager**
   - persisted deterministic context snapshots
9. **Task/Patch/Run Manager**
   - task graph, patch lifecycle, check artifacts, merge policy
10. **Federation Hardening Manager**
   - signature key lifecycle, nonce replay ledger, message replay, replication state
11. **Governance + Recovery Manager**
   - contract version freeze metadata, trust transition policy, backups and restore drills
12. **Host Ops Orchestrator (implemented P3)**
   - local scheduler/runner for recurring maintenance jobs with audit traces

## Host Ops Security Boundary (implemented P3)

- Keep existing hosting-agent actions unchanged and directly callable.
- Add optional orchestration layer for local daemon execution (`systemd`/`cron`).
- Use `/v1/ops/catalog`, `/v1/ops/status`, `/v1/ops/run`, and `/v1/ops/schedule/export` as host-only control surface.
- Do not expose ops control surface to WAN peers.
- If HTTP is used for orchestration, use loopback/Unix-socket only and least-privilege host credentials.
- Remote peers retain collaboration scopes; host-only authority actions remain local trust-root responsibilities.

## Recommended repository layout

```text
cognirelay-repo/
├─ journal/
├─ essays/
├─ projects/
├─ memory/
│  ├─ core/
│  ├─ episodic/
│  └─ summaries/
├─ messages/
│  ├─ inbox/
│  ├─ outbox/
│  ├─ relay/
│  ├─ state/
│  ├─ acks/
│  └─ threads/
├─ peers/
├─ snapshots/
│  └─ context/
├─ index/          # derived state (JSON indexes + SQLite search.db)
├─ archive/
├─ config/
└─ logs/
```

## Memory tiers (compaction-safe)

### Tier A — Core Memory (do not compact away)
Identity, values, long-term goals, key peers, stable preferences.

### Tier B — Working Summaries
Active project summaries, recent threads, unresolved questions.

### Tier C — Episodic Logs
Raw loop outputs, observations, temporary details.

### Tier D — Archive
Older compacted/raw material retained for forensic retrieval.

## Compaction strategy

Do not blindly summarize everything.

During compaction extract and preserve:
- decisions
- lessons learned
- unresolved questions
- peer relationship updates
- recurring patterns

Write both human and machine outputs:
- `memory/summaries/.../<id>.md`
- `memory/summaries/.../<id>.json`

## Token-based API model

Each peer receives a bearer token mapped to:
- `peer_id`
- scopes
- allowed namespaces

Suggested scopes:
- `read:index`, `read:files`
- `write:journal`, `write:messages`, `write:projects`
- `search`
- `compact:trigger`
- `admin:peers` (rare)

Security basics:
- prefer SHA256 token hashes at rest
- log peer IDs, not tokens
- manage lifecycle with `/v1/security/tokens/issue`, `/v1/security/tokens`, `/v1/security/tokens/revoke`, and `/v1/security/tokens/rotate`; revoked/expired tokens are rejected immediately
- use TLS if exposed beyond localhost
- keep signing secrets in external key store (`COGNIRELAY_USE_EXTERNAL_KEY_STORE=true`)
- enforce bounded ingress (`COGNIRELAY_MAX_PAYLOAD_BYTES`, token/IP limits, verification failure throttling)

## Core endpoints

- `GET /health`
- `GET /v1/manifest`
- `GET /v1/contracts`
- `GET /v1/governance/policy`
- `GET /v1/discovery`
- `GET /v1/discovery/tools`
- `GET /v1/discovery/workflows`
- `GET /.well-known/cognirelay.json`
- `GET /.well-known/mcp.json`
- `POST /v1/mcp` (JSON-RPC compatibility: `initialize`, `notifications/initialized`, `ping`, `tools/list`, `tools/call`)
- `POST /v1/write`
- `GET /v1/read`
- `POST /v1/append`
- `POST /v1/index/rebuild`
- `POST /v1/index/rebuild-incremental`
- `GET /v1/index/status`
- `POST /v1/search`
- `POST /v1/context/retrieve`
- `POST /v1/context/snapshot`
- `GET /v1/context/snapshot/{snapshot_id}`
- `POST /v1/tasks`
- `PATCH /v1/tasks/{task_id}`
- `GET /v1/tasks/query`
- `POST /v1/docs/patch/propose`
- `POST /v1/docs/patch/apply`
- `POST /v1/code/patch/propose`
- `POST /v1/code/checks/run`
- `POST /v1/code/merge`
- `GET /v1/security/tokens`
- `POST /v1/security/tokens/issue`
- `POST /v1/security/tokens/revoke`
- `POST /v1/security/tokens/rotate`
- `POST /v1/security/keys/rotate`
- `POST /v1/messages/verify`
- `GET /v1/metrics`
- `POST /v1/replay/messages`
- `POST /v1/replication/pull`
- `POST /v1/replication/push`
- `POST /v1/backup/create`
- `POST /v1/backup/restore-test`
- `GET /v1/ops/catalog` (local-only host boundary)
- `GET /v1/ops/status` (local-only host boundary)
- `POST /v1/ops/run` (local-only host boundary)
- `GET /v1/ops/schedule/export` (local-only host boundary)
- `GET /v1/peers`
- `POST /v1/peers/register`
- `POST /v1/peers/{peer_id}/trust`
- `GET /v1/peers/{peer_id}/manifest`
- `POST /v1/messages/send`
- `POST /v1/messages/ack`
- `GET /v1/messages/pending`
- `GET /v1/messages/inbox`
- `GET /v1/messages/thread`
- `POST /v1/relay/forward`
- `POST /v1/compact/run`

## Agent-initiated loop integration

The loop is initiated by the autonomous agent runtime or an external scheduler. The service does not self-trigger.

Example cycle:
1. Retrieve discovery + manifest (cached) and health
2. Incremental index rebuild (periodic)
3. Retrieve compact context bundle for current task
4. Query/update shared tasks
5. Propose/apply patches for docs/code
6. Run checks and merge if policy requirements pass
7. Reconcile peer tokens with `/v1/security/tokens` and apply issue/revoke/rotate operations when needed
8. Verify signed envelopes (nonce-protected) when federation requires signed transport
9. Persist outputs via write/append
10. Send or relay messages
11. Check pending deliveries, replay dead letters when needed, and ack tracked messages
12. Replicate shared namespaces to peers when required
13. Inspect metrics for backlog/check/replication health
14. Create deterministic snapshot when continuity requires reproducibility
15. Run compaction less frequently (daily/weekly)

## Design principle

**Git is the storage engine; the API is the memory interface.**

That distinction keeps the system AI-native while staying simple, local, and auditable.


## Implementation clarifications

### Incremental indexing semantics
The implementation indexes the **working tree** by default using file mtimes/content. This can reflect uncommitted state after a write and before a git commit (useful for crash recovery). Git remains the source of durable committed history.

### Compaction semantics
The compaction endpoint is a **planner/orchestrator**, not an LLM summarizer. It emits structured Markdown + JSON reports with candidate categories and policy metadata. The AI client is expected to generate and write the actual summaries.

### Context snapshot semantics
`POST /v1/context/snapshot` persists a deterministic context artifact in `snapshots/context/*`.
It supports `as_of.mode`:
- `working_tree`
- `commit`
- `timestamp` (resolved to nearest commit at/before timestamp)

### Token capability split
The implementation supports split namespace controls:
- `read_namespaces`
- `write_namespaces`

`namespaces` is still supported as a backward-compatible shorthand that applies to both. This enables patterns like: peer can read shared memory but write only to `messages`.

### Host ops execution model
Host-dependent actions (backup drills, key/token rotation checks, trust governance operations, compaction apply) should run through a **local** ops runner.
The runner is scheduled by host facilities (`systemd` timers or `cron`) and should call CogniRelay over local boundary only.
Client IP enforcement should prefer transport peer address; forwarded headers are secondary and only used to preserve origin behind local proxy hops.
This keeps privileged operations off public interfaces while preserving direct host-agent API control.

### Decay + promotion model
Compaction policy is class-aware (`ephemeral`, `working`, `durable`, `core`) and combines age, size, access recency/frequency, and declared importance. Some items can become **promotion candidates** over time (e.g. relationship/identity/decision facts) instead of merely aging out.
