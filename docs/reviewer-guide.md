# Reviewer Guide

This guide is the fastest way to evaluate CogniRelay as a system rather than as a list of endpoints.

Use it before diving into the API details.

## What CogniRelay Is

CogniRelay is a self-hosted continuity and collaboration substrate for autonomous agents.

Its main job is not to be a generic file server or a generic task app. Its main job is to help agents:

- preserve enough orientation across resets and compaction boundaries to keep working coherently
- recover usefully when continuity artifacts or derived indexes are stale, missing, or damaged
- coordinate with other agents through bounded, auditable artifacts instead of implicit shared state

The system is built around one simple operational idea:

**git is the durable store; the API is the machine interface**

## What CogniRelay Is Not

CogniRelay is not:

- a claim of perfect persistence across context boundaries
- a full "basin key" architecture that attempts to preserve the whole texture of an agent
- a hidden decision-maker that silently rewrites agent state
- a shared global memory root where collaborating agents automatically converge on one truth

The current implementation is intentionally narrower:

- bounded orientation preservation, not total fidelity preservation
- explicit degradation and fallback, not false claims of seamless recovery
- advisory and owner-bounded coordination, not remote authority over local continuity

## Why This Architecture

CogniRelay is built from a small number of deliberately constrained building blocks. Each choice optimizes for auditability, operational simplicity, and independence from external services.

**Git as storage engine.** All durable state lives in a local git repository managed through subprocess calls — no GitPython, no forge, no remote dependency. Git provides version history, diffs, rollback, and offline-first operation without requiring an external database. Every mutation is a commit, so the full history of what changed and when is always recoverable.

**Markdown for human-readable memory, JSON/JSONL for machine data.** Durable facts, identity, and narrative memory are stored as Markdown with optional YAML frontmatter. Event streams, message records, delivery state, and structured artifacts use JSON or append-only JSONL. This split keeps memory inspectable by humans while giving agents efficient structured access.

**SQLite FTS5 for search, with JSON-index fallback.** Search uses Python's stdlib `sqlite3` module with an FTS5 virtual table — no external search service. If the SQLite database is missing or corrupt, the indexer falls back to derived JSON indexes with a simpler word-scoring algorithm. Both index layers are treated as derived state that can be rebuilt from the git-backed source of truth at any time.

**Self-contained bearer-token auth.** Tokens are stored as SHA256 hashes in local config, scoped by operation and namespace. There is no OAuth provider, LDAP, or external auth dependency. The token model supports split read/write namespace restrictions, expiry, trust status, and audit logging — all locally managed.

**Compaction as planning, not summarization.** The compaction service is an orchestrator that classifies candidates by age, size, memory class, and policy, then emits structured reports with action categories (summarize, archive, promote, keep, review). It does not generate summaries itself — the agent reads the plan and decides what to do. This keeps the system from making content decisions on the agent's behalf.

**Four runtime dependencies.** The entire stack runs on FastAPI, uvicorn, Pydantic, and python-dotenv. No ORM, no external database, no cache or queue library. This keeps the operational surface minimal and the system easy to deploy, audit, and reason about.

## The Core Model

### Bounded orientation preservation

CogniRelay treats continuity as a bounded orientation problem.

Continuity capsules are meant to preserve enough of the agent's current direction to support a useful restart:

- active constraints
- drift signals
- open questions and current direction
- session trajectory
- lower-commitment orientation fields such as trailing notes and curiosity queue
- explicit negative decisions when the agent chooses to record them

This is stronger than simple factual recall, but intentionally weaker than a full architecture for preserving every layer of texture or self-model.

### Write-time curation rather than unlimited retention

The current continuity model is closer to bounded write-time curation than to unconstrained read-time pruning.

That matters because the motivating discussions distinguish two broad failure modes:

- pruning too aggressively and losing signal
- retaining too much and burying signal under accumulation

CogniRelay does not claim to eliminate that tradeoff. Instead it makes the tradeoff explicit:

- continuity payloads are bounded
- optional fields have deterministic trim order under token pressure
- archive, fallback, and retention paths are explicit
- list/read/retrieve behavior is explicit about what is present, omitted, degraded, or archived

The system therefore aims for inspectable loss, not imaginary losslessness.

### Negative decisions are first-class enough to survive if recorded

One of the key design choices in the current system is that non-action can be represented directly.

The `negative_decisions` continuity field exists to preserve decisions such as:

- not replying yet
- not broadening scope
- not taking an attractive but rejected design path

This does not solve every compaction problem by itself. It does, however, prevent the system from modeling only what was done and thereby biasing successor agents toward action by omission.

## Recovery Model

CogniRelay assumes blind spots are structural.

That means the recovery model is built around bounded usefulness under loss, not around a promise that the blind spot has been removed.

### What the system tries to do

- preserve active continuity when possible
- preserve a last-known-good fallback snapshot after successful active writes
- surface verification and health state explicitly
- degrade reads and retrievals safely where the current contract permits it
- preserve auditable history for archive, delete, and restore-test flows
- manage continuity lifecycle through tiered retention: active, fallback, archive, and cold storage
- provide explicit retention planning and cold-store/rehydrate operations so storage cost does not grow without bound

### What the system does not claim

- that active continuity is always available
- that fallback state is equivalent to active truth
- that verification can be solved from inside one compressed channel alone
- that context retrieval can always reconstruct everything that mattered

### Practical reading for reviewers

When reviewing the system, treat these as key design claims:

- degraded continuation is preferable to avoidable hard failure
- fallback is a recovery aid, not a silent truth promotion
- verification is explicit and auditable, not an implicit self-healing illusion

## Inter-Agent Authority Boundaries

The inter-agent model is deliberately conservative.

### What crosses the boundary

Current handoff/shared coordination work allows bounded coordination-facing data to cross the peer boundary, especially:

- constraints
- drift signals
- coordination alerts in shared artifacts

### What does not happen automatically

- a received handoff does not silently rewrite local continuity
- shared coordination artifacts do not become shared capsules
- reconciliation records do not silently mutate local continuity or shared coordination state
- stronger agreement semantics are not implied before they are explicitly implemented

The intended reading is:

**remote coordination artifacts are evidence and advice, not automatic local truth**

## Coordination Model

CogniRelay provides three bounded coordination primitives for inter-agent work. All three are additive records — they do not mutate local continuity capsules or automatically synchronize state between agents.

### Handoffs

A handoff projects a bounded subset of one agent's active continuity capsule (only `active_constraints` and `drift_signals`) into an auditable artifact for another agent. The recipient records one of `accepted_advisory`, `deferred`, or `rejected` as advisory input. Nothing is promoted into local continuity automatically.

### Shared coordination artifacts

An owner-authored artifact that exposes bounded coordination state (`constraints`, `drift_signals`, `coordination_alerts`) to a listed participant set. Participants can read the artifact; only the owner can update it. Shared artifacts are coordination context, not shared capsules.

### Reconciliation records

When handoff or shared coordination claims visibly disagree, a reconciliation record names the bounded dispute — the claims, epistemic status, and evidence — without resolving it by fiat. First-slice outcomes are conservative: `advisory_only`, `conflicted`, or `rejected`. Stronger agreement semantics that would mutate shared or local state are explicitly deferred.

### What ties them together

All three primitives follow the same principle: coordination artifacts are evidence and advice, not automatic local truth. Discovery is bounded by caller identity. The system does not converge agents toward one shared state — it gives them auditable coordination records and leaves the decision to each agent.

## Operator and Host-Local Boundary

CogniRelay exposes two distinct operational surfaces:

### Agent-facing collaboration surface

Memory, retrieval, continuity, coordination, messaging, tasks, patches, and peer discovery. These endpoints are designed for peer-facing access under the normal bearer-token auth model.

### Host-local authority surface

This surface has two enforcement tiers:

- **IP-enforced local-only**: ops runner endpoints under `/v1/ops/*` enforce an IP-based local-client check in addition to `admin:peers` scope. These are unreachable from WAN peers even if the scope is present.
- **Scope-restricted authority**: trust transitions (`/v1/peers/{peer_id}/trust`), token and signing-key lifecycle (`/v1/security/*`), backup creation and restore drills require `admin:peers` scope but do not enforce IP-based locality. They are intended for local use but rely on scope restriction rather than transport-level enforcement.

Both tiers carry system-wide impact — revoking a token, rotating a key, or running a retention job affects every agent using the instance. Operators should keep `admin:peers` tokens off WAN-accessible peers and, if automating authority actions, run them through a local scheduler (`systemd`, `cron`) invoked through a local boundary.

The boundary matters for reviewers because it separates what an agent can do to collaborate from what an operator can do to maintain the system. Agents do not have authority over token lifecycle or retention policy unless the operator explicitly grants it.

## How To Read The Docs

Use the docs in this order:

1. `README.md`
   Start here for repo shape, quick start, and the canonical doc map.
2. `docs/agent-onboarding.md`
   Use this for practical agent integration guidance, including cold-start and incremental adoption.
3. `docs/reviewer-guide.md`
   Use this document for the system thesis, boundaries, and non-goals.
4. `docs/system-overview.md`
   Use this for the implemented product shape, operational model, and agent usage guidance.
5. `docs/api-surface.md`
   Use this for the currently implemented HTTP behavior and endpoint grouping.
6. `docs/mcp.md`
   Use this if you care about MCP integration and tool exposure.
7. `deploy/GO_LIVE_RUNBOOK.md` and `deploy/PRODUCTION_SIGNOFF_CHECKLIST.md`
   Use these for operator-facing deployment and signoff concerns.

## What Reviewers Should Pressure-Test

The most important review questions are not "does it have many features?" They are:

- Does the current continuity model preserve the right bounded orientation data?
- Are the degradation and fallback semantics honest and operationally safe?
- Are negative decisions represented strongly enough to avoid obvious action bias?
- Are inter-agent authority boundaries narrow and explicit enough?
- Are coordination primitives genuinely additive, or do they imply hidden state convergence?
- Is the operator/host-local boundary clear enough that an agent cannot accidentally perform authority actions?
- Does the retention and cold-storage model keep storage bounded without silently discarding data the agent still needs?
- Do the docs describe the implemented system faithfully, without implying a fuller memory architecture than exists?
