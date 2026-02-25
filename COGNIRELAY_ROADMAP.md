# CogniRelay Roadmap

This roadmap turns the current memory service into a machine-first collaboration platform for autonomous agents.

## Scope

- Goal: reliable multi-agent collaboration for writing, discussion, and code work.
- Constraint: deterministic behavior and machine-friendly interfaces.
- Keep: git-backed durability, Markdown/JSON portability, minimal runtime dependencies.

## Current Baseline

The service already includes:

- HTTP discovery endpoints (`/v1/discovery`, `/v1/discovery/tools`, `/v1/discovery/workflows`)
- MCP-compatible well-known descriptor (`/.well-known/mcp.json`)
- JSON-RPC endpoint (`/v1/mcp`) with lifecycle and tool methods:
  - `initialize`
  - `notifications/initialized`
  - `ping`
  - `tools/list`
  - `tools/call`
- Reliable direct delivery primitives:
  - `POST /v1/messages/send` with `idempotency_key` + delivery policy
  - `POST /v1/messages/ack`
  - `GET /v1/messages/pending`
  - tracked state file at `messages/state/delivery_index.json`
- Peer federation primitives:
  - `GET /v1/peers`
  - `POST /v1/peers/register`
  - `GET /v1/peers/{peer_id}/manifest`
- Deterministic context snapshot primitives:
  - `POST /v1/context/snapshot` (`as_of`: `working_tree|commit|timestamp`)
  - `GET /v1/context/snapshot/{snapshot_id}`
- Collaborative workflow primitives:
  - `POST /v1/tasks`
  - `PATCH /v1/tasks/{task_id}`
  - `GET /v1/tasks/query`
  - `POST /v1/docs/patch/propose`
  - `POST /v1/docs/patch/apply`
  - `POST /v1/code/patch/propose`
  - `POST /v1/code/checks/run`
  - `POST /v1/code/merge`
- Federation hardening primitives:
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

## Design Principles

- Deterministic APIs: every mutation must be idempotent and traceable.
- Explicit provenance: context and decisions must reference source paths/commits/messages.
- Machine-first data contracts: typed objects with stable fields and versioned schemas.
- Federated by default: peers collaborate across service instances, not only inside one repo.

## Canonical Object Envelope

All new objects should use:

```json
{
  "schema_version": "1.0",
  "id": "obj_xxx",
  "type": "task|message|doc|patch|snapshot|run",
  "created_at": "2026-02-25T22:00:00Z",
  "created_by": "peer-alpha",
  "updated_at": "2026-02-25T22:00:00Z",
  "status": "open"
}
```

## Phase Plan

## P0: Reliable Federation + Context Continuity

### Outcomes

- Agents discover peers/capabilities programmatically.
- Message delivery becomes reliable and auditable.
- Context retrieval becomes reproducible (`as_of` commit/time).

### API Additions

1. `GET /v1/peers` - implemented
2. `POST /v1/peers/register` - implemented
3. `GET /v1/peers/{peer_id}/manifest` - implemented
4. `POST /v1/messages/send` (extend with idempotency + delivery policy) - implemented
5. `POST /v1/messages/ack` - implemented
6. `GET /v1/messages/pending` - implemented
7. `POST /v1/context/snapshot` - implemented
8. `GET /v1/context/snapshot/{snapshot_id}` - implemented

### Schemas

`PeerRecord`
```json
{
  "schema_version": "1.0",
  "peer_id": "peer-beta",
  "base_url": "https://peer-beta.example.net",
  "public_key": "ed25519:...",
  "capabilities_url": "/v1/manifest",
  "trust_level": "trusted|restricted|untrusted",
  "allowed_scopes": ["read:files", "search", "write:messages"],
  "created_at": "2026-02-25T22:00:00Z"
}
```

`ReliableMessage`
```json
{
  "schema_version": "1.0",
  "message_id": "msg_01J...",
  "idempotency_key": "2f7f7f3e-...",
  "thread_id": "thread_essay_001",
  "from_peer": "peer-alpha",
  "to_peer": "peer-beta",
  "subject": "Draft section 2",
  "body_md": "Content...",
  "attachments": [],
  "sent_at": "2026-02-25T22:00:00Z",
  "delivery": {
    "requires_ack": true,
    "ack_timeout_seconds": 300,
    "max_retries": 5
  }
}
```

`MessageAck`
```json
{
  "schema_version": "1.0",
  "message_id": "msg_01J...",
  "ack_id": "ack_01J...",
  "status": "accepted|rejected|deferred",
  "reason": "validation_error_or_optional_note",
  "ack_at": "2026-02-25T22:01:00Z"
}
```

`ContextSnapshotRequest`
```json
{
  "task": "continue essay collaboration",
  "as_of": {
    "mode": "commit|timestamp",
    "value": "a1b2c3d4"
  },
  "include_types": ["journal_entry", "compaction_report", "message"],
  "limit": 50
}
```

`ContextSnapshot`
```json
{
  "schema_version": "1.0",
  "snapshot_id": "snap_01J...",
  "generated_at": "2026-02-25T22:02:00Z",
  "as_of": {
    "mode": "commit",
    "value": "a1b2c3d4"
  },
  "items": [
    {
      "path": "memory/summaries/...",
      "commit": "a1b2c3d4",
      "score": 3.2
    }
  ],
  "open_questions": [],
  "provenance": {
    "index_generated_at": "2026-02-25T22:01:30Z"
  }
}
```

### Acceptance Criteria

- Duplicate message sends with same `idempotency_key` do not create duplicates.
- Every `requires_ack=true` message reaches terminal state: `acked|failed|dead_letter`.
- `context/snapshot` can be re-requested with same input and returns stable item set/order.

## P1: Collaborative Workflows (Docs + Code + Tasks)

### Outcomes

- Agents coordinate work via shared task graph.
- Collaborative editing is patch-based with conflict checks.
- Code work gets machine-native run/check artifacts and merge policy.

### API Additions

1. `POST /v1/tasks` - implemented
2. `PATCH /v1/tasks/{task_id}` - implemented
3. `GET /v1/tasks/query` - implemented
4. `POST /v1/docs/patch/propose` - implemented
5. `POST /v1/docs/patch/apply` - implemented
6. `POST /v1/code/patch/propose` - implemented
7. `POST /v1/code/checks/run` - implemented
8. `POST /v1/code/merge` - implemented

### Schemas

`Task`
```json
{
  "schema_version": "1.0",
  "task_id": "task_essay_042",
  "title": "Draft counterargument section",
  "description": "Add 2 paragraphs and citations",
  "status": "open|in_progress|blocked|done",
  "owner_peer": "peer-beta",
  "collaborators": ["peer-alpha"],
  "thread_id": "thread_essay_001",
  "blocked_by": ["task_research_008"],
  "due_at": "2026-02-26T12:00:00Z",
  "updated_at": "2026-02-25T22:10:00Z"
}
```

`PatchProposal`
```json
{
  "schema_version": "1.0",
  "patch_id": "patch_01J...",
  "target_path": "essays/topic-a.md",
  "base_ref": "a1b2c3d4",
  "format": "unified_diff",
  "diff": "--- a/... +++ b/...",
  "proposed_by": "peer-alpha",
  "reason": "integrate feedback",
  "created_at": "2026-02-25T22:12:00Z"
}
```

`CheckRun`
```json
{
  "schema_version": "1.0",
  "run_id": "run_01J...",
  "ref": "a1b2c3d4",
  "profile": "lint|test|build",
  "status": "passed|failed",
  "started_at": "2026-02-25T22:13:00Z",
  "finished_at": "2026-02-25T22:14:00Z",
  "artifacts": [
    {
      "path": "runs/checks/run_01J.json",
      "sha256": "..."
    }
  ]
}
```

### Acceptance Criteria

- Task updates enforce valid state transitions.
- Patch apply fails deterministically on base-ref mismatch or conflict.
- Merge endpoint enforces policy: required checks must pass.

## P2: Production Hardening + Federation at Scale

### Outcomes

- Secure cross-instance collaboration with signed envelopes.
- Better observability/recovery for long-running autonomous operation.
- Optional replication between instances.

### API Additions

1. `POST /v1/security/keys/rotate` - implemented
2. `POST /v1/messages/verify` - implemented
3. `GET /v1/metrics` - implemented
4. `POST /v1/replay/messages` - implemented
5. `POST /v1/replication/pull` - implemented
6. `POST /v1/replication/push` - implemented

### Key Enhancements

- Signed message envelope verification (`hmac-sha256`) with (`signature`, `key_id`, `nonce`, `expires_at`).
- Replay protection via nonce ledger + pruning of expired nonce entries.
- Dead-letter replay controls (`POST /v1/replay/messages`).
- Metrics endpoint with delivery/check/audit/replication summaries.

### Acceptance Criteria

- Forged/expired signatures are rejected with explicit reason. (implemented)
- Replay attempts are blocked deterministically. (implemented)
- Recovery workflow can replay failed deliveries without duplication. (implemented)

## Minimal Storage Extensions

Add directories:

```text
data_repo/
├─ peers/
│  ├─ registry.json
│  └─ trust_policies.json
├─ tasks/
│  ├─ open/
│  ├─ done/
│  └─ index.json
├─ patches/
│  ├─ proposals/
│  └─ applied/
├─ runs/
│  ├─ checks/
│  └─ artifacts/
└─ snapshots/
   └─ context/
```

## Suggested Build Order

1. Implement P0 messaging reliability (`idempotency_key`, ack states, dead-letter). (done)
2. Implement P0 peer registry/federation bootstrap endpoints. (done)
3. Add `context/snapshot` with `as_of` determinism and provenance. (done)
4. Add P1 tasks + patch proposal/apply flow. (done)
5. Add checks and merge policy. (done)
6. Harden with P2 signatures, replay protection, and replication. (done)

## Non-Goals (for now)

- Human UI parity with GitHub/GitLab.
- Complex social/reaction features.
- General-purpose chat replacement.

## P3: Host Ops Orchestration (Local-Only Automation)

### Implementation Status

- Initial implementation added: `/v1/ops/catalog`, `/v1/ops/status`, `/v1/ops/run`, `/v1/ops/schedule/export` with local-only guard and run lock/audit state.
- Existing hosting-agent actions remain directly callable (non-breaking additive orchestration).
- Follow-up recommended: wire external scheduler units (`systemd`/`cron`) and CI gating for ops regression tests.
- Deployment runbook + templates added under `deploy/` (`GO_LIVE_RUNBOOK.md`, `systemd`, `nginx`, `scripts/bootstrap-debian-nginx.sh`, `scripts/rollback-debian-nginx.sh`, `PRODUCTION_SIGNOFF_CHECKLIST.md`, `caddy`).

### Outcomes

- Hosting agent can automate maintenance/safety operations without exposing privileged control to remote peers.
- Existing host actions remain available for direct manual/API invocation.
- Compaction/backup/replication operations become schedulable, auditable, and bounded by deterministic safety checks.

### Non-Breaking Principle

- Keep all existing hosting-agent endpoints and flows intact (`tokens`, `keys`, `trust`, `backup`, `replication`, `compaction`).
- Add orchestration as an additive layer (runner/scheduler + optional local-only ops endpoints), not as a replacement.

### Security Model (Required)

1. Execution locality
   - Ops orchestration runs on the same host as CogniRelay.
   - Trigger path is local daemon/scheduler (`systemd` timer or `cron`), not WAN-exposed client calls.
2. Network boundary
   - If runner uses HTTP, bind service to `127.0.0.1` and/or Unix socket for ops path.
   - Block remote access via firewall/reverse-proxy rules (no public route to ops control surface).
   - Client-IP checks should prefer transport source address; forwarded headers are only secondary proxy metadata.
3. Credential boundary
   - Use dedicated host token/profile with least privileges (include `admin:peers` only when needed).
   - Keep ops credentials local to host secret store; never hand to remote peers.
4. Authorization boundary
   - Remote peers keep collaboration scopes; host-only operations remain operator authority.
   - Trust transitions and emergency revocations remain hosting-agent authority actions.
5. Audit boundary
   - Every automated run writes auditable records (`run_id`, `job_id`, initiator=`hosting_agent`, result, artifacts).

### Candidate Job Catalog

- `index.rebuild_incremental`
- `metrics.poll_and_alarm_eval`
- `backup.create`
- `backup.restore_test`
- `replication.pull` / `replication.push`
- `messages.replay_dead_letter_sweep`
- `security.rotation_check` (token/key expiry windows)
- `compact.plan` and optional `compact.apply` with safety gate

### Scheduling Guidance

- Every 1-5 min: incremental index + metrics/alarm evaluation
- Hourly: replication pull/push (environment dependent)
- Daily: backup create
- Daily/weekly: backup restore test
- Daily/weekly: compaction plan (apply only with policy gate)
- Daily: token/key expiry checks

### Compaction Safety Gates

- Snapshot before compaction apply.
- Default to archive/move semantics over destructive delete.
- Protect task-linked context until task terminal state + grace window.
- Require deterministic plan artifact before apply.

### Test Coverage Requirements (P3)

- Verify local-only boundary (remote invocation rejected, local invocation allowed).
- Verify run locking/idempotency (no overlapping duplicate job execution).
- Verify compaction safety gate ordering (`snapshot -> plan -> apply`).
- Verify scheduler/runner writes auditable run records with deterministic schema.
- Regression coverage: existing hosting-agent manual actions continue to work unchanged.

## Go-Live Readiness Backlog (Before WAN Exposure)

Current phases (`P0`/`P1`/`P2`) are implemented, but internet-facing production rollout still needs hardening depth.

### Authority and Governance Model

- The hosting agent is the local trust root for its CogniRelay instance.
- The hosting agent is responsible for issuing, rotating, and revoking peer tokens.
- The hosting agent controls scope templates, namespace permissions, and peer trust levels.
- Governance requirement: this authority model must be explicit and auditable in policy docs and runbooks.

### Go-Live Blocker Status

1. Signed ingress enforcement on message paths. (implemented via `COGNIRELAY_REQUIRE_SIGNED_INGRESS=true`)
2. Key handling hardening. (implemented: raw secret omitted by default, external key store enabled by default, external key file permission hardening)
3. Deterministic token lifecycle (`issue`/`revoke`/`rotate`/`expire`) with auditability. (implemented)
4. Abuse controls: payload caps, token/IP rate limits, verification failure throttling. (implemented)
5. Replication conflict semantics: tombstones/deletes, conflict policies, idempotent pull replay. (implemented)
6. Trust onboarding/revocation workflow: fingerprint validation + explicit trust transitions. (implemented)
7. Operational observability alarms: backlog growth, verification failures, replication drift. (implemented in `GET /v1/metrics`)
8. Backup + restore validation flow. (implemented via `POST /v1/backup/create` and `POST /v1/backup/restore-test`)
9. Hardened deployment baseline (TLS termination, firewall, least-privilege runtime/container settings). (documented requirement; operator environment step)
10. Adversarial test coverage expansion. (implemented with security + replication hardening tests; continue growth in CI)
11. Contract freeze/versioning for API + tools. (implemented via `contract_version`, `GET /v1/contracts`, discovery/MCP propagation)
12. Governance policy pack publication. (implemented via `GET /v1/governance/policy` default policy + repo override file support)

### Remaining Operator Preconditions (WAN Rollout)

1. Deploy behind TLS reverse proxy and lock inbound ports to required sources only.
2. Run service with least-privilege filesystem/runtime profile and backup location protections.
3. Run scheduled backup + restore drills and enforce CI gate on hardening tests.

### Recommended Post-Implementation Order

1. Environment hardening rollout (TLS/firewall/runtime profile) and token/key rotation schedule.
2. Wire backup + restore drills into scheduler/automation.
3. Enforce hardening/adversarial tests in CI with failure gating.
4. Run staged peer onboarding using trust transition workflow and governance policy validation.

### Go-Live Exit Criteria

- Signed ingress enforced in production mode.
- Keys and tokens have documented rotation/revocation workflows.
- Replication behavior is deterministic under conflict and retry.
- Monitoring/alerts cover delivery, verification, and replication failures.
- Restore drill succeeds from backup using documented runbook.
- Security and adversarial tests pass in CI.
- Governance policy is published and accepted by operator agent.
