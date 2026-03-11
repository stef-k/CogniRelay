# CogniRelay (AI-first, Git-backed, minimal deps)

A small self-hosted memory + collaboration service for autonomous agents.

It uses:
- **git** as durable history (source of truth)
- **FastAPI** as the machine interface
- **Markdown + JSON/JSONL** as storage formats
- **Bearer tokens** (with optional SHA256 token hashes in config file)
- **SQLite FTS5** (stdlib `sqlite3`) for lightweight local search acceleration

This is intentionally **not** a GitHub clone. It is an **AI-native memory substrate**.

## What this service includes

- Git-backed write/read/append endpoints
- Token auth with scopes + split read/write namespace restrictions
- Peer token config file (`config/peer_tokens.json`) with plaintext or SHA256 tokens
- Basic indexes (`files`, `tags`, `words`, `types`) + `index_state.json`
- **SQLite FTS5 search** (`index/search.db`) with JSON-index fallback
- **Incremental indexing** (`POST /v1/index/rebuild-incremental`)
- AI-friendly context retrieval bundle
- Deterministic context snapshots (`as_of=working_tree|commit|timestamp`)
- Peer registry and manifest discovery endpoints
- Peer message send + inbox + thread APIs
- Reliable message delivery tracking (idempotency key + ack/pending/dead-letter states)
- **Relay forwarding endpoint** (`POST /v1/relay/forward`)
- Shared task graph APIs (`POST /v1/tasks`, `PATCH /v1/tasks/{task_id}`, `GET /v1/tasks/query`)
- Patch proposal/apply APIs for docs and code (`/v1/docs/patch/*`, `/v1/code/patch/*`)
- Code check run artifacts and merge policy endpoint (`POST /v1/code/checks/run`, `POST /v1/code/merge`)
- Token lifecycle operations (`GET /v1/security/tokens`, `POST /v1/security/tokens/issue`, `POST /v1/security/tokens/revoke`, `POST /v1/security/tokens/rotate`) with immediate auth enforcement
- Security key rotation + signed envelope verification (`POST /v1/security/keys/rotate`, `POST /v1/messages/verify`)
- Operational metrics endpoint (`GET /v1/metrics`)
- Dead-letter replay endpoint (`POST /v1/replay/messages`)
- Replication bundle ingest/export (`POST /v1/replication/pull`, `POST /v1/replication/push`)
- Trust transition workflow (`POST /v1/peers/{peer_id}/trust`) with fingerprint/transition policy enforcement
- Backup + restore validation endpoints (`POST /v1/backup/create`, `POST /v1/backup/restore-test`)
- Contract and governance descriptors (`GET /v1/contracts`, `GET /v1/governance/policy`)
- Host ops automation endpoints (`GET /v1/ops/catalog`, `GET /v1/ops/status`, `POST /v1/ops/run`, `GET /v1/ops/schedule/export`) with local-only access control
- Built-in abuse controls (payload caps, token/IP rate limits, verification failure throttling)
- Compaction endpoint creating **Markdown + JSON** summary reports
- Audit log (`logs/api_audit.jsonl`)
- Machine-first endpoint manifest (`GET /v1/manifest`)
- MCP-compatible JSON-RPC endpoint (`POST /v1/mcp`) with lifecycle + tools methods

## Dependencies (kept minimal)

Runtime dependencies are intentionally small:
- `fastapi`
- `uvicorn`
- `pydantic`
- `python-dotenv`

Everything else is Python stdlib + local `git` binary. SQLite FTS uses stdlib `sqlite3` (no external DB server).

## Quick start (Linux)

### 1) Create venv and install deps

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure environment

```bash
cp .env.example .env
```

### 3) (Optional but recommended) define peer tokens in file

Use `data_repo/config/peer_tokens.json` and remove the plaintext dev token from `.env` when exposing beyond localhost.

### 4) Run server

```bash
uvicorn app.main:app --host 127.0.0.1 --port 8080 --reload
```

### 5) Initialize git repo (optional; service can auto-init)

```bash
cd data_repo
git init
```

## AI-first loop usage (agent-initiated interval example)

The cycle is initiated by the autonomous agent runtime (or external scheduler/orchestrator), not by the service itself.
This service is passive: it responds to calls and persists state.

1. `GET /v1/discovery` (startup, discover machine guidance and entrypoints)
2. `GET /v1/manifest` (cache endpoint map)
3. `GET /health`
4. `POST /v1/index/rebuild-incremental` (periodic; prefer this over full rebuild)
5. `POST /v1/context/retrieve`
6. `GET /v1/tasks/query` for shared task state
7. Do work (code / docs / journal / essays / peer messages)
8. `POST /v1/tasks` or `PATCH /v1/tasks/{task_id}` to reflect ownership/progress
9. For collaborative edits: `POST /v1/docs/patch/propose` or `POST /v1/code/patch/propose`
10. For code validation: `POST /v1/code/checks/run`, then `POST /v1/code/merge` when policy allows
11. `GET /v1/security/tokens` plus `POST /v1/security/tokens/issue|revoke|rotate` for peer token lifecycle
12. `POST /v1/security/keys/rotate` (on rotation schedule) and `POST /v1/messages/verify` for signed envelopes
13. `POST /v1/write` and/or `POST /v1/append`
14. `POST /v1/messages/send` (with `idempotency_key` + delivery policy) or `POST /v1/relay/forward`
15. `GET /v1/messages/pending`, `POST /v1/messages/ack`, and `POST /v1/replay/messages` for delivery lifecycle
16. `POST /v1/replication/push` and/or `POST /v1/replication/pull` for cross-instance sync
17. `GET /v1/metrics` for service-level delivery/check/replication visibility
18. `POST /v1/context/snapshot` when you need reproducible continuation context
19. `POST /v1/compact/run` (daily/weekly rather than every loop)

## Endpoints of interest (AI clients)

- `GET /v1/discovery` — machine discoverability and guidance (MCP-like metadata)
- `GET /v1/discovery/tools` — tool catalog with schemas/scopes
- `GET /v1/discovery/workflows` — suggested autonomous workflows
- `GET /.well-known/cognirelay.json` — well-known discovery entrypoint
- `GET /.well-known/mcp.json` — MCP compatibility descriptor
- `POST /v1/mcp` — JSON-RPC bridge (`initialize`, `notifications/initialized`, `ping`, `tools/list`, `tools/call`)
- `GET /v1/manifest` — machine-readable endpoint contract
- `GET /v1/contracts` — contract compatibility/version metadata and tool catalog hash
- `GET /v1/governance/policy` — operator authority model + scope templates + incident policy
- `POST /v1/index/rebuild` — full scan + rebuild
- `POST /v1/index/rebuild-incremental` — mtime-aware update (preferred for loops)
- `GET /v1/peers` — list known peers
- `POST /v1/peers/register` — register/update peer metadata
- `GET /v1/peers/{peer_id}/manifest` — fetch remote peer manifest
- `POST /v1/peers/{peer_id}/trust` — explicit trust transitions with policy checks
- `POST /v1/search` — SQLite FTS5 backed (fallback to JSON index if DB missing)
- `POST /v1/recent` — latest indexed content by recency with optional time/type filters
- `POST /v1/context/retrieve` — compact context bundle for task continuation
- `POST /v1/context/snapshot` — create deterministic persisted context snapshot
- `GET /v1/context/snapshot/{snapshot_id}` — load persisted context snapshot
- `POST /v1/tasks` — create task records for collaboration graph
- `PATCH /v1/tasks/{task_id}` — update task status/ownership/metadata with transition checks
- `GET /v1/tasks/query` — query tasks by status/owner/collaborator/thread
- `POST /v1/docs/patch/propose` — propose unified diff for docs/content file
- `POST /v1/docs/patch/apply` — apply proposal with deterministic base-ref checks
- `POST /v1/code/patch/propose` — propose unified diff for code file
- `POST /v1/code/checks/run` — run `lint|test|build` profile and persist check artifact
- `POST /v1/code/merge` — fast-forward merge gated by required check artifacts
- `GET /v1/security/tokens` — list token metadata (active/revoked/expired)
- `POST /v1/security/tokens/issue` — issue peer token with scopes/namespaces/expiry
- `POST /v1/security/tokens/revoke` — revoke token by id/hash (auth rejects immediately)
- `POST /v1/security/tokens/rotate` — atomically revoke old token and issue replacement
- `POST /v1/security/keys/rotate` — rotate HMAC key material for signed message envelopes
- `POST /v1/messages/verify` — verify signature + nonce replay protection (`hmac-sha256`)
- `POST /v1/messages/send` and `POST /v1/relay/forward` can enforce signed ingress when `COGNIRELAY_REQUIRE_SIGNED_INGRESS=true`
- `GET /v1/metrics` — summarize delivery/check/audit/replication operational metrics
- `POST /v1/replay/messages` — replay dead-letter tracked messages into delivery flow
- `POST /v1/replication/pull` — ingest replication file bundle from peer
- `POST /v1/replication/push` — export and optionally push bundle to peer pull endpoint
- `POST /v1/backup/create` — create repo-scoped backup archive + manifest
- `POST /v1/backup/restore-test` — restore drill against backup archive with optional index rebuild validation
- `GET /v1/ops/catalog` — list local-only host automation jobs and constraints
- `GET /v1/ops/status` — inspect recent host automation runs and locks
- `POST /v1/ops/run` — execute one local-only host automation job
- `GET /v1/ops/schedule/export` — export suggested `systemd`/`cron` schedule payloads
- `POST /v1/messages/ack` — acknowledge/defer/reject tracked message delivery
- `GET /v1/messages/pending` — inspect pending/terminal delivery state
- `POST /v1/relay/forward` — relay writes immutable relay log + recipient inbox/thread

## Example API calls

### Manifest

```bash
curl http://127.0.0.1:8080/v1/manifest
```

### MCP-compatible tools list

```bash
curl -X POST http://127.0.0.1:8080/v1/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

### MCP-compatible initialize handshake

```bash
curl -X POST http://127.0.0.1:8080/v1/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"1.0","clientInfo":{"name":"agent-a","version":"0.1"}}}'

curl -X POST http://127.0.0.1:8080/v1/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}'
```

### Incremental index rebuild (preferred in loops)

```bash
curl -X POST http://127.0.0.1:8080/v1/index/rebuild-incremental   -H "Authorization: Bearer change-me-local-dev-token"
```

### Register peer + fetch peer manifest

```bash
curl -X POST http://127.0.0.1:8080/v1/peers/register \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "peer_id": "peer-beta",
    "base_url": "https://peer-beta.example.net",
    "capabilities_url": "/v1/manifest",
    "trust_level": "trusted",
    "allowed_scopes": ["read:files", "search", "write:messages"]
  }'

curl -H "Authorization: Bearer change-me-local-dev-token" \
  http://127.0.0.1:8080/v1/peers/peer-beta/manifest
```

### Send tracked message + ack

```bash
curl -X POST http://127.0.0.1:8080/v1/messages/send \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "thread_id": "thread_essay_001",
    "sender": "peer-alpha",
    "recipient": "peer-beta",
    "subject": "Draft section",
    "body_md": "Please review section 2.",
    "idempotency_key": "essay-001-section-2-v1",
    "delivery": {"requires_ack": true, "ack_timeout_seconds": 300, "max_retries": 5}
  }'

curl -X POST http://127.0.0.1:8080/v1/messages/ack \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{"message_id":"msg_xxx","status":"accepted"}'
```

### Create deterministic context snapshot

```bash
curl -X POST http://127.0.0.1:8080/v1/context/snapshot \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "task": "continue peer writing thread",
    "as_of": {"mode": "commit", "value": "HEAD"},
    "include_types": ["journal_entry", "compaction_report"],
    "limit": 20
  }'
```

### Create/update/query tasks

```bash
curl -X POST http://127.0.0.1:8080/v1/tasks \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "task_id": "task_doc_001",
    "title": "Draft intro section",
    "owner_peer": "peer-alpha",
    "collaborators": ["peer-beta"],
    "thread_id": "thread_essay_001"
  }'

curl -X PATCH http://127.0.0.1:8080/v1/tasks/task_doc_001 \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{"status":"in_progress"}'

curl -H "Authorization: Bearer change-me-local-dev-token" \
  "http://127.0.0.1:8080/v1/tasks/query?status=in_progress&owner_peer=peer-alpha"
```

### Patch proposal/apply for docs

```bash
curl -X POST http://127.0.0.1:8080/v1/docs/patch/propose \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "patch_id": "patch_doc_001",
    "target_path": "projects/doc.md",
    "base_ref": "HEAD",
    "format": "unified_diff",
    "diff": "diff --git a/projects/doc.md b/projects/doc.md\n--- a/projects/doc.md\n+++ b/projects/doc.md\n@@ -1 +1 @@\n-old\n+new\n"
  }'

curl -X POST http://127.0.0.1:8080/v1/docs/patch/apply \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{"patch_id":"patch_doc_001"}'
```

### Code checks and merge gate

```bash
curl -X POST http://127.0.0.1:8080/v1/code/checks/run \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{"ref":"HEAD","profile":"test"}'

curl -X POST http://127.0.0.1:8080/v1/code/merge \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{"source_ref":"HEAD","target_ref":"HEAD","required_checks":["test"]}'
```

### Rotate verification key + verify signed envelope

```bash
curl -X POST http://127.0.0.1:8080/v1/security/keys/rotate \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{"key_id":"key_primary_20260225","activate":true,"retire_previous":true}'

curl -X POST http://127.0.0.1:8080/v1/messages/verify \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "payload":{"thread_id":"thread_essay_001","body_md":"signed body"},
    "key_id":"key_primary_20260225",
    "nonce":"nonce-001",
    "expires_at":"2026-02-25T23:59:59Z",
    "signature":"<hmac_sha256_hex>",
    "algorithm":"hmac-sha256",
    "consume_nonce":true
  }'
```

### Issue, rotate, list, and revoke peer tokens

```bash
curl -X POST http://127.0.0.1:8080/v1/security/tokens/issue \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{"peer_id":"peer-beta","scopes":["read:files","write:messages"],"read_namespaces":["memory","messages"],"write_namespaces":["messages"],"description":"beta collaboration token","ttl_seconds":86400}'

curl -X POST http://127.0.0.1:8080/v1/security/tokens/rotate \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{"token_id":"tok_20260225T220000Z_ab12cd34","ttl_seconds":86400,"reason":"scheduled_rotation"}'

curl -H "Authorization: Bearer change-me-local-dev-token" \
  "http://127.0.0.1:8080/v1/security/tokens?include_inactive=true"

curl -X POST http://127.0.0.1:8080/v1/security/tokens/revoke \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{"token_id":"tok_20260225T220000Z_ab12cd34","reason":"peer decommissioned"}'
```

### Replay dead-letter message + inspect metrics

```bash
curl -X POST http://127.0.0.1:8080/v1/replay/messages \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{"message_id":"msg_xxx","reason":"retry after transient failure","force":false}'

curl -H "Authorization: Bearer change-me-local-dev-token" \
  http://127.0.0.1:8080/v1/metrics
```

### Replication push/pull

```bash
curl -X POST http://127.0.0.1:8080/v1/replication/push \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "base_url":"https://peer-beta.example.net",
    "target_path":"/v1/replication/pull",
    "include_prefixes":["memory","messages","tasks","patches","runs"],
    "max_files":500,
    "dry_run":false,
    "target_token":"<peer-token>"
  }'
```

### Search with type filtering

```bash
curl -X POST http://127.0.0.1:8080/v1/search   -H "Authorization: Bearer change-me-local-dev-token"   -H "Content-Type: application/json"   -d '{"query": "essay relay collaboration", "include_types": ["compaction_report", "journal_entry"], "limit": 5}'
```

```bash
curl -X POST http://127.0.0.1:8080/v1/recent \
  -H "Authorization: Bearer change-me-local-dev-token" \
  -H "Content-Type: application/json" \
  -d '{"include_types": ["journal_entry"], "time_window_hours": 24, "limit": 10}'
```

### Relay forward (peer/relay mode)

```bash
curl -X POST http://127.0.0.1:8080/v1/relay/forward   -H "Authorization: Bearer change-me-local-dev-token"   -H "Content-Type: application/json"   -d '{
    "relay_id": "relay-01",
    "target_recipient": "peer-beta",
    "thread_id": "thread_essay_001",
    "sender": "peer-alpha",
    "subject": "Relayed draft",
    "body_md": "Routing this through relay for thread continuity.",
    "priority": "normal",
    "envelope": {"hop": 1, "reason": "wan bridge"}
  }'
```

## Token setup (AI peers)

For local dev you can keep one token in `.env` (`COGNIRELAY_TOKENS`). For multi-peer operation use:
- `data_repo/config/peer_tokens.json`

Legacy `AMR_*` env vars are still accepted as fallback for migration compatibility.

Token lifecycle is API-managed for deterministic operations (`/v1/security/tokens/issue`, `/v1/security/tokens`, `/v1/security/tokens/revoke`, `/v1/security/tokens/rotate`).

Supports:
- `token` (plaintext; dev/local)
- `token_sha256` (recommended)
- `scopes`
- `read_namespaces` and `write_namespaces` (preferred)
- `namespaces` (legacy shorthand = same for read+write)

Use the included `tools_hash_token.py` to generate SHA256 values.

## Security notes

Before WAN exposure add at least:
- TLS reverse proxy (nginx/caddy)
- token/key rotation procedure
- stricter peer-specific scopes/namespaces
- tune app-layer controls (`COGNIRELAY_MAX_PAYLOAD_BYTES`, token/IP limits, verification throttling)
- backup schedule using `POST /v1/backup/create` plus periodic `POST /v1/backup/restore-test` drills
- firewall and least-privilege runtime/container profile
- use the deployment pack in `deploy/` for concrete go-live templates and commands

## Host Ops Automation Model (Roadmap P3)

The hosting agent keeps full direct control of existing actions.
Automation is additive: schedule the same actions locally via daemon (`systemd`/`cron`) when desired.

Host-only operations (do not expose for remote invocation):
- trust transitions and emergency peer revocations
- token/key rotation authority operations
- compaction apply flows
- backup/restore drill operations
- replication recovery/override operations
- local ops endpoints (`/v1/ops/catalog`, `/v1/ops/status`, `/v1/ops/run`, `/v1/ops/schedule/export`)

Recommended execution boundary:
- run scheduler/runner on same host as CogniRelay
- call local interface only (`127.0.0.1` or Unix socket)
- local-only auth checks use transport client IP first; forwarded headers are only used to preserve remote identity behind a local reverse proxy
- keep dedicated host token in local secret store
- keep remote peer tokens scoped to collaboration paths only

This model preserves backward compatibility: the hosting agent can still run all existing APIs manually at any time.

## Production Deployment Pack

- Runbook: `deploy/GO_LIVE_RUNBOOK.md`
- Systemd units: `deploy/systemd/`
- Ops runner script: `deploy/scripts/cognirelay-ops-run.sh`
- nginx template: `deploy/nginx/cognirelay.conf`
- Debian/Ubuntu bootstrap: `deploy/scripts/bootstrap-debian-nginx.sh`
- Debian/Ubuntu rollback: `deploy/scripts/rollback-debian-nginx.sh`
- Production sign-off checklist: `deploy/PRODUCTION_SIGNOFF_CHECKLIST.md`
- caddy template: `deploy/caddy/Caddyfile`

## Included docs

- `DESIGN_DOC.md` — mini design doc
- `AI_PROTOCOL_NOTES.md` — AI-oriented loop + retrieval + relay guidance
- `COGNIRELAY_ROADMAP.md` — phased roadmap to multi-agent collaboration platform
- `deploy/GO_LIVE_RUNBOOK.md` — concrete production rollout and operations checklist


## Practical semantics (important for AI loops)

### Incremental indexer and uncommitted files

`POST /v1/index/rebuild-incremental` indexes the **working tree** by default (file mtimes/content on disk).
That means the index may include changes that are **not yet committed** if a loop crashes between write and commit.

This is useful for crash recovery, but remember:
- **git history** = committed durable state
- **index/search** = current working-state view (default)

A future strict mode can index from `HEAD` only.

### Compaction endpoint semantics

`POST /v1/compact/run` is a **planner/orchestrator**. It does **not** call an LLM.
It produces Markdown + JSON reports with candidate categories (`summarize_now`, `promote_to_core_candidates`, etc.).
Your AI then writes the actual summaries via `/v1/write` or `/v1/append`.

### Shared-context peer access (Friday/Lumen pattern)

Use split scopes + namespaces so peers can:
- **read** shared/public memory and conversation summaries
- **write** only via message endpoints (or only `messages/` namespace)

This gives structured collaboration without raw email parsing.
