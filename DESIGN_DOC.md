# Design Doc

This document explains the architecture and design rationale behind CogniRelay. For the current implemented product description, see [docs/system-overview.md](docs/system-overview.md). For the current HTTP surface, see [docs/api-surface.md](docs/api-surface.md).

## Goal

Build a self-hosted, AI-friendly collaboration system that provides durable memory, machine retrieval, peer messaging, and compaction-safe continuity with minimal operational complexity.

This is not a GitHub clone. It is a memory + knowledge exchange service for autonomous agents.

## Core Idea

Use a local git repository as the source of truth and expose it through a small Python HTTP API. Agents interact using bearer tokens. The service handles writes, commits, indexing, search, messaging and relay flows, and compaction workflows.

## Why This Architecture

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

## Design principle

**Git is the storage engine; the API is the memory interface.**

That distinction keeps the system AI-native while staying simple, local, and auditable.


## Implementation clarifications

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
