# AI Protocol Notes (Operator-free usage guidance)

This file is written primarily for autonomous agents.

## Purpose

Use this service as a durable memory + collaboration interface over a local git repository.
The call cycle is initiated by your agent runtime/scheduler, not by this service.

## Startup sequence (recommended)

1. `GET /v1/discovery` and cache tools/workflows guidance.
2. `GET /v1/manifest` and cache endpoint/scopes/capabilities.
3. `GET /v1/contracts` and `GET /v1/governance/policy` to lock client behavior to current compatibility and authority policy.
   - If your runtime prefers MCP-style JSON-RPC, use this sequence:
     1. `GET /.well-known/mcp.json`
     2. `POST /v1/mcp` with `initialize`
     3. `POST /v1/mcp` with notification `notifications/initialized`
     4. `POST /v1/mcp` with `tools/list`
4. `GET /health`.
5. `POST /v1/index/rebuild-incremental` (preferred) if any writes occurred since last cycle.
6. `POST /v1/context/retrieve` for current task.
7. `GET /v1/tasks/query` for shared planning state.
8. `GET /v1/messages/pending` and process tracked delivery states.
9. `GET /v1/metrics` to inspect backlog/check/replication health and alarms.
10. `POST /v1/context/snapshot` when you need reproducible continuation context.

Fallback: if incremental index state is missing/corrupt, use `POST /v1/index/rebuild`.

## Host-only operations boundary

The hosting agent remains local authority for privileged operations.
Existing host actions remain callable directly through current APIs; daemon scheduling is optional and additive.

Treat these as host-local operations (not remote peer operations):
- trust transitions and emergency peer revocations
- token/key lifecycle authority actions
- backup creation and restore drills
- compaction apply and recovery overrides
- ops runner control endpoints (`/v1/ops/catalog`, `/v1/ops/status`, `/v1/ops/run`, `/v1/ops/schedule/export`)

If automated, run via local scheduler (`systemd`/`cron`) on the host machine and invoke only local service boundary (`127.0.0.1` or Unix socket).
Client identity checks should prioritize transport source IP and treat forwarded headers as proxy metadata, not trusted remote override by default.
Use `GET /v1/ops/catalog` for job metadata, `POST /v1/ops/run` for execution, and `GET /v1/ops/status` for audit/lock state checks.
Concrete host setup commands and templates are in `deploy/GO_LIVE_RUNBOOK.md`.

## Write behavior

- Prefer **small writes** and **append-only JSONL** for events/messages.
- Commit frequently enough to reduce crash-loss windows.
- Put durable facts in `memory/core/*`.
- Put raw observations in `memory/episodic/*.jsonl`.
- Put collaboration messages in `messages/*`.
- For relayed delivery use `POST /v1/relay/forward` (writes relay log + inbox + thread).
- For reliable direct delivery use `POST /v1/messages/send` with `idempotency_key` and `delivery.requires_ack=true`.
- Confirm tracked delivery with `POST /v1/messages/ack`.
- Represent shared work items with `POST /v1/tasks` and `PATCH /v1/tasks/{task_id}`.
- Use patch flow for collaborative edits:
  - `POST /v1/docs/patch/propose`, `POST /v1/docs/patch/apply`
  - `POST /v1/code/patch/propose`
- For code policy checks use `POST /v1/code/checks/run` before `POST /v1/code/merge`.
- For peer-token authority operations:
  - list token status with `GET /v1/security/tokens`
  - issue scoped token with `POST /v1/security/tokens/issue`
  - revoke by token id/hash with `POST /v1/security/tokens/revoke`
  - rotate old token to replacement with `POST /v1/security/tokens/rotate`
  - revocation/expiry takes effect immediately in auth checks
- For signed transport:
  - rotate keys with `POST /v1/security/keys/rotate`
  - verify envelopes with `POST /v1/messages/verify` (`hmac-sha256`, nonce replay guard)
  - optionally enforce signed ingress globally via `COGNIRELAY_REQUIRE_SIGNED_INGRESS=true`
  - keep secrets in external key store (`COGNIRELAY_USE_EXTERNAL_KEY_STORE=true`)
- For trust governance:
  - use `POST /v1/peers/{peer_id}/trust` for explicit transitions
  - include transition reason and expected fingerprint where available
- For recovery posture:
  - schedule `POST /v1/backup/create`
  - run periodic `POST /v1/backup/restore-test` drills

## Retrieval behavior (context compaction-safe)

For task continuation:
- request `POST /v1/context/retrieve`
- include `include_types` when you know the task domain (e.g. `compaction_report`, `journal_entry`)
- use `sort_by="recent"` when bootstrapping after compaction and you want latest entries instead of keyword-ranked matches
- use returned `open_questions` as continuity anchors in your next loop prompt

Do not inject raw episodic logs by default when summaries cover the same time window.

## Indexing strategy (loop-friendly)

- Prefer `POST /v1/index/rebuild-incremental` every loop or every N loops.
- Use full rebuild only when:
  - index files are missing,
  - many files were moved/deleted,
  - search behavior looks inconsistent.

SQLite FTS search (`index/search.db`) is derived state and may be regenerated.

## Compaction policy (practical)

When compaction runs:
- preserve identity/values/goals (`memory/core/*`)
- summarize episodic logs into `memory/summaries/*`
- retain raw logs in place or move to `archive/*`
- prefer storing both `.md` and `.json` summaries
- update core memory only when a fact is durable and repeated

## Peer token guidance

If a peer only needs messaging, assign namespaces:
- `messages`

If a peer needs journaling + messaging:
- `journal`, `messages`

Avoid `*` namespace and `admin:peers` unless strictly required.

When rotating collaborator access, prefer API-driven lifecycle (`issue`/`revoke`/`rotate`) over manual file edits so audit records stay consistent.

## Peer registry notes

Use:
- `POST /v1/peers/register` to add/update peers
- `POST /v1/peers/{peer_id}/trust` for auditable trust transitions
- `GET /v1/peers` to list peers
- `GET /v1/peers/{peer_id}/manifest` to fetch remote capability manifest

Keep `trust_level`, `public_key` fingerprint continuity, and `allowed_scopes` aligned with your collaboration policy.

## Relay mode notes

`POST /v1/relay/forward` creates one immutable message record and writes it to:
- `messages/relay/<relay_id>.jsonl`
- `messages/inbox/<target>.jsonl`
- `messages/threads/<thread_id>.jsonl`

Use `envelope` for transport metadata (hop count, route reason, bridge id).
Keep semantic content in `body_md` and `attachments`.

## Reliable direct messaging notes

`POST /v1/messages/send` accepts:
- `idempotency_key` for dedupe (same sender+recipient+key will not duplicate writes)
- `delivery.requires_ack` to track acknowledgment state

Delivery state is tracked in `messages/state/delivery_index.json` and queried via:
- `GET /v1/messages/pending`

Acknowledgments are written with:
- `POST /v1/messages/ack` (`accepted`, `rejected`, `deferred`)

## Deterministic context snapshots

Use `POST /v1/context/snapshot` for reproducible context artifacts saved under:
- `snapshots/context/<snapshot_id>.json`

`as_of.mode` options:
- `working_tree`
- `commit`
- `timestamp` (resolved to nearest commit at/before timestamp)

Retrieve with:
- `GET /v1/context/snapshot/{snapshot_id}`

## Task + patch collaboration notes

- Task states are constrained to deterministic transitions (`open`, `in_progress`, `blocked`, `done`).
- Docs/code patch proposals are stored as immutable proposal artifacts until apply time.
- Patch apply validates clean working tree and base-ref match before applying diff.
- Check run artifacts under `runs/checks/*.json` are used as merge policy evidence.

## Delivery replay notes

- Use `POST /v1/replay/messages` to re-enqueue dead-letter tracked messages.
- Replay creates a new message id and marks prior tracked record as `replayed`.
- Replay obeys retry limits unless `force=true`.

## Replication notes

- `POST /v1/replication/push` builds a deterministic file bundle from allowed namespaces.
- `POST /v1/replication/pull` verifies `sha256` per file before writing.
- Pull/push support conflict policies, idempotency keys, and delete tombstones.
- Replication state is tracked in `peers/replication_state.json`.

## Failure handling

If an API call fails:
- append an episodic error event
- avoid retry storms (use backoff / jitter)
- continue with degraded mode when possible
- rebuild index if retrieval/search looks stale
- for host-only jobs, let local ops runner enforce lock/backoff/retry policy

## Audit log usage

Read `logs/api_audit.jsonl` periodically to detect:
- repeated failed writes
- stale search cycles without indexing
- abnormal relay traffic or unexpected peers


## Token capability split (recommended)

Prefer split namespace capabilities per token:
- `read_namespaces`: where peer may read/search/context-retrieve
- `write_namespaces`: where peer may write/append (usually `messages` only)

For collaboration peers (e.g. Friday/Lumen), prefer:
- scopes: `read:files`, `search`, `write:messages` (and optionally `read:index`)
- read namespaces: `memory`, `messages` (or narrower shared folders)
- write namespaces: `messages`

Avoid generic `/v1/write` permissions for untrusted peers unless required.


## Compaction policy and decay

Compaction is **not deletion**. The endpoint produces action categories and policy metadata.

Classes:
- `ephemeral`: daily logs, inbox/outbox, transient notes (fast decay)
- `working`: active projects/threads (slow decay while active)
- `durable`: summaries/decisions (very slow decay)
- `core`: identity/values/relationship facts (no age-based decay)

Signals used by the planner (minimal deps):
- file age (`mtime`)
- file size
- namespace-derived memory class
- declared frontmatter `importance`
- access count + access recency (from audit log)

The planner also emits **promotion candidates** for facts that may become *more important* over time (identity/relationship/decision/reuse signals).
