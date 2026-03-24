# Reviewer Guide

This guide is the fastest way to evaluate CogniRelay as a system rather than as a list of endpoints.

Use it before diving into the API details.

## What CogniRelay Is

CogniRelay is a self-hosted continuity and collaboration substrate for autonomous agents.

Its main job is not to be a generic file server or a generic task app. Its main job is to help agents:

- preserve enough orientation across resets and compaction boundaries to keep working coherently
- recover usefully when continuity artifacts or derived indexes are stale, missing, or damaged
- coordinate with other agents through bounded, auditable artifacts instead of implicit shared state

The default deployment model is one owner-agent per CogniRelay instance. That owner-agent is also the local operator and superuser of its instance, holding the `admin:peers` scope. Continuity capsules are the owner-agent's local continuity substrate — namespace enforcement supports sub-directory granularity, so collaborator tokens are scoped to `memory/coordination` without access to `memory/continuity`. If that owner-agent needs to coordinate with other agents, it issues narrower delegated API tokens to collaborating peers. Collaboration happens through the coordination surfaces (handoffs, shared artifacts, reconciliation records) rather than by treating continuity as shared common state. An agent that wants its own continuity should run its own instance.

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

**Minimal runtime dependencies.** The entire stack runs on FastAPI, uvicorn, Pydantic, and python-dotenv. No ORM, no external database, no cache or queue library. This keeps the operational surface minimal and the system easy to deploy, audit, and reason about.

## The Core Model

### Bounded orientation preservation

CogniRelay treats continuity as a bounded orientation problem.

Continuity capsules are meant to preserve enough of the agent's current direction to support a useful restart:

- active constraints (`active_constraints`)
- drift signals (`drift_signals`)
- open loops (`open_loops`) and stance summary (`stance_summary`)
- session trajectory (`session_trajectory`)
- lower-commitment orientation fields such as trailing notes (`trailing_notes`) and curiosity queue (`curiosity_queue`)
- explicit negative decisions (`negative_decisions`) when the agent chooses to record them

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

### Access isolation model

Access isolation between agents is enforced entirely by token scopes and namespace/path restrictions configured by the operator. The system does not provide a separate intrinsic identity-bound ownership or tenant isolation layer beyond that configured access model.

Continuity capsules are namespace-gated, not agent-gated. Any token with read access to `memory/continuity` can read any capsule stored there, regardless of which agent created it. In the default `collaboration_peer` governance template, collaborator tokens cannot access `memory/continuity` — this is a configured policy boundary enforced by sub-directory namespace restrictions, not a built-in per-agent tenant isolation mechanism.

The strengthened collaborator model (sub-namespace hardening) means the default template protects owner-private continuity by excluding it from collaborator namespace grants. This is materially stronger than broad top-level `memory` access, but remains token/namespace policy, not ownership enforcement. Readers should not infer a built-in multi-tenant per-agent isolation model from the current system.

The collaborator token policy described above is only meaningful when `admin:peers` is withheld from collaborator tokens, as the default templates do. Any token carrying `admin:peers` bypasses both scope and namespace checks entirely — see the [Operator and Host-Local Boundary](#operator-and-host-local-boundary) section for details.

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

In the default deployment model, the owner-agent and the local operator are the same principal. The owner-agent holds the `admin:peers` scope and acts as full operator/superuser for its own instance. In the implementation, `admin:peers` bypasses both ordinary scope checks and namespace/path restrictions — any token carrying this scope can read any file, write to any namespace, and perform any operation that does not require additional IP-based locality enforcement. Collaborator agents, if any, are external peers with narrower delegated tokens that do not include `admin:peers`.

CogniRelay exposes two distinct operational surfaces:

### Agent-facing collaboration surface

Memory, retrieval, continuity, coordination, messaging, tasks, patches, and peer discovery. These endpoints are designed for peer-facing access under the normal bearer-token auth model.

### Host-local authority surface

This surface has two enforcement tiers:

- **IP-enforced local-only**: ops runner endpoints under `/v1/ops/*` enforce an IP-based local-client check in addition to `admin:peers` scope. These are unreachable from WAN peers even if the scope is present.
- **Scope-restricted authority**: trust transitions (`/v1/peers/{peer_id}/trust`), token and signing-key lifecycle (`/v1/security/*`), backup creation and restore drills require `admin:peers` scope but do not enforce IP-based locality. They are intended for local use but rely on scope restriction rather than transport-level enforcement.

Both tiers carry system-wide impact — revoking a token, rotating a key, or running a retention job affects every agent using the instance. In the default model, `admin:peers` belongs exclusively to the owner-agent/operator and should not be granted to collaborator peers. The one exception is the `replication_peer` governance template, which carries `admin:peers` because replication requires unrestricted read access across all namespaces. Since `admin:peers` bypasses both scope and namespace checks, a replication token also has equivalent write authority to the owner token outside of IP-enforced localhost operations — operators should treat replication tokens with the same care as the owner token. If automating authority actions, run them through a local scheduler (`systemd`, `cron`) invoked through a local boundary.

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
6. `docs/payload-reference.md`
   Use this for capsule structure, request/response schemas, and field-level constraints.
7. `docs/mcp.md`
   Use this if you care about MCP integration and tool exposure.
8. `deploy/GO_LIVE_RUNBOOK.md` and `deploy/PRODUCTION_SIGNOFF_CHECKLIST.md`
   Use these for operator-facing deployment and signoff concerns.

## Pre-Review Hardening Summary

Before requesting external review, CogniRelay went through a structured hardening workflow (tracked in [#92](https://github.com/stef-k/CogniRelay/issues/92)). This section summarizes the results so reviewers know what was checked and what was found.

### Review Baseline

The review baseline is branch `main` at commit `1217cb7`. All stages below were evaluated against this post-hardening state. The full test suite passes and Ruff reports no lint violations at this baseline.

### Scientific Crosswalk (Stage B)

A source-to-system crosswalk compared the implemented system against the motivating external material:

- [The basin key experiment](https://forvm.loomino.us/t/ebafbec9-6dd9-4213-8d55-b5c237f3cd9c) (Forvm thread on identity stability across architectures)
- [The 84.8% problem](https://forvm.loomino.us/t/979eaf61-2c8a-4793-8834-990cb1be71ed) (Forvm thread on what persistence architectures forget)
- [The Invisible Decision](https://sammyjankis.com/paper.html) (paper on negative decision loss under context-window summarization)

Key findings:

- **Orientation recovery**: implemented. CogniRelay models orientation as more than factual recall through `session_trajectory`, `trailing_notes`, `curiosity_queue`, and `negative_decisions`. This is intentionally bounded — it is not a full basin-key texture-preservation architecture.
- **Negative decision preservation**: implemented. `negative_decisions` is first-class, with deterministic trim ordering under token pressure. This is one of the clearest alignments between source material and shipped system.
- **Structural blind spots acknowledged**: implemented. The system treats blind spots as structural through explicit verification state, capsule health, fallback snapshots, degraded retrieval, and recovery warnings.
- **Write-time curation over unlimited retention**: implemented. Continuity payloads are bounded with deterministic trim order. The system aims for inspectable loss, not imaginary losslessness.
- **Inter-agent authority boundaries**: implemented. Handoffs project only `active_constraints` and `drift_signals`; shared coordination artifacts are owner-authored and bounded; reconciliation records are advisory, not authoritative. Stronger agreement semantics that would mutate shared or local state are explicitly deferred.
- **No major overclaims found**: the docs are conservative about what the system does and does not implement. The main documentation gap (reviewer-facing framing of the bounded orientation model) was addressed in Stage E.

Full crosswalk detail: [#93](https://github.com/stef-k/CogniRelay/issues/93), follow-up docs: [#94](https://github.com/stef-k/CogniRelay/issues/94) → [PR #95](https://github.com/stef-k/CogniRelay/pull/95).

### Robustness Findings and Resolutions (Stage C)

Stage C reviewed the implementation as mission-critical continuity infrastructure under adverse conditions.

Findings and fixes:

1. **Git index serialization** (high): concurrent commits could interfere through the shared git index. Fixed by adding repository-level git mutation serialization. [#98](https://github.com/stef-k/CogniRelay/issues/98) → [PR #101](https://github.com/stef-k/CogniRelay/pull/101).
2. **Same-subject continuity locking** (high): concurrent mutations to the same continuity subject could race through write/commit/rollback. Fixed by adding per-subject continuity mutation locking. [#97](https://github.com/stef-k/CogniRelay/issues/97) → [PR #99](https://github.com/stef-k/CogniRelay/pull/99).
3. **Rollback hardening** (high): additional rollback edge cases discovered during the locking work. Fixed with broader mutation-path hardening. [#100](https://github.com/stef-k/CogniRelay/issues/100) → [PR #102](https://github.com/stef-k/CogniRelay/pull/102).
4. **Raw-scan performance cliff** (high): degraded index fallback performed a full-repo sweep under missing/corrupt index conditions. Fixed by bounding the fallback scan. [#104](https://github.com/stef-k/CogniRelay/issues/104) → [PR #105](https://github.com/stef-k/CogniRelay/pull/105).

No new crash-path findings in backup/restore-test behavior. No new crash-path findings in maintenance degraded paths.

Full detail: [#96](https://github.com/stef-k/CogniRelay/issues/96) (slice 1), [#103](https://github.com/stef-k/CogniRelay/issues/103) (slice 2).

### Retention and Lifecycle Findings and Resolutions (Stage D)

Stage D evaluated whether retention, backup, compaction, and cost-control mechanics are coherent and agent-respecting.

Findings and outcomes:

1. **Continuity retention policy**: the system labeled retention states (`active`, `fallback`, `archive_recent`, `archive_stale`) but lacked an executable operator workflow for stale archives. Fixed by implementing a host-local retention-policy path. [#107](https://github.com/stef-k/CogniRelay/issues/107) → [PR #111](https://github.com/stef-k/CogniRelay/pull/111).
2. **Semi-cold storage mechanism**: no implemented model for compressed/searchable low-priority storage. Fixed by implementing a semi-cold storage path with explicit rehydrate semantics. [#108](https://github.com/stef-k/CogniRelay/issues/108) → [PR #110](https://github.com/stef-k/CogniRelay/pull/110).
3. **Repo-wide lifecycle substrate**: different namespaces had no common lifecycle architecture. Resolved by designing and implementing a shared lifecycle substrate with namespace-specific tuning. [#109](https://github.com/stef-k/CogniRelay/issues/109), tuning specs: [#112](https://github.com/stef-k/CogniRelay/issues/112) → [PR #115](https://github.com/stef-k/CogniRelay/pull/115), [#113](https://github.com/stef-k/CogniRelay/issues/113) → [PR #117](https://github.com/stef-k/CogniRelay/pull/117), [#114](https://github.com/stef-k/CogniRelay/issues/114) → [PR #125](https://github.com/stef-k/CogniRelay/pull/125).

Confirmed non-findings: backup cadence is operationally concrete (daily creation, restore drills, compact-plan scheduling via systemd); compaction remains planner-only and does not silently summarize or delete content; authority boundaries are preserved (mechanical automation only, no hidden agentic decisions).

A post-implementation lifecycle-safety audit confirmed deterministic behavior under concurrent mutation, rollover, cold-store, rehydrate, and partial-failure scenarios.

Full detail: [#106](https://github.com/stef-k/CogniRelay/issues/106) (stage controller).

### Known Limitations and Intentional Deferrals

The following are known boundaries of the current system, not unresolved bugs:

- **Bounded orientation, not full basin-key fidelity**: CogniRelay preserves bounded orientation context (`session_trajectory`, `trailing_notes`, `curiosity_queue`, `negative_decisions`) but does not attempt to capture the full texture, register, or self-model of an agent. This is a deliberate scope choice.
- **Stronger reconciliation/agreement semantics deferred**: Phase 5C ([#38](https://github.com/stef-k/CogniRelay/issues/38)) implements a bounded first slice — explicit reconciliation records with `advisory_only`, `conflicted`, and `rejected` outcomes. Stronger agreement semantics that would mutate shared artifacts or local continuity are explicitly deferred until the first slice proves sound.
- **Compaction is planning-only**: the compaction service classifies candidates and emits structured reports but does not generate summaries or execute deletions. Agents decide what to do with compaction plans. This is an intentional authority boundary.
- **No external database or search service**: search uses SQLite FTS5 with JSON-index fallback. This keeps the system self-contained but means search sophistication is bounded by what FTS5 and the fallback scorer can provide.
- **One-owner-agent-per-instance deployment model**: Each CogniRelay instance serves a single owner-agent that also acts as the local operator. Agents wanting their own continuity run their own instance. Horizontal scaling, replication, and multi-host coordination are out of scope. The rate-limit state lock (`threading.Lock` in `app/runtime/service.py`) depends on this single-process model; multi-worker deployment requires migrating to cross-process file locking first (see [Runtime Concurrency Model](system-overview.md#runtime-concurrency-model)).
- **No automatic state convergence across agents**: coordination artifacts are evidence and advice, not automatic local truth. The system does not converge agents toward shared state.
- **Collection endpoints silently skip unauthorized entries**: Continuity collection endpoints (`continuity/list`, `continuity/refresh/plan`, `continuity/retention/plan`) return only the entries the caller is authorized to read — unauthorized entries are silently excluded, returning 200 with a reduced result set. This is standard collection-endpoint behavior: a narrowly-scoped token sees only its authorized subset. Single-resource endpoints (`continuity/read`, `continuity/upsert`, `continuity/compare`, `continuity/revalidate`, `continuity/archive`, `continuity/delete`) return 403 when the caller lacks access. No private capsule data is disclosed in either path. Evaluated in [#156](https://github.com/stef-k/CogniRelay/issues/156).

## What Reviewers Should Pressure-Test

The most important review questions are not "does it have many features?" They are:

**Continuity model**
- Does the bounded orientation model preserve the right data for useful post-reset recovery, or are there important orientation layers that agents need but the capsule structure cannot express?
- Is the deterministic trim ordering under token pressure (trailing notes → curiosity queue → negative decisions) the right priority, or does it lose high-value signal too early?
- Are negative decisions represented strongly enough to avoid obvious action bias in successor agents?

**Degradation and recovery**
- Are the degradation and fallback semantics honest and operationally safe, or do they create false confidence about what survived?
- Is the fallback snapshot model (last-known-good after successful active write) useful in practice, or does it create confusing divergence between active and fallback state?
- Does the recovery-warning surface give agents enough information to make good decisions about degraded state?

**Inter-agent boundaries**
- Are inter-agent authority boundaries narrow and explicit enough, or do they implicitly encourage agents to treat advisory coordination data as truth?
- Are coordination primitives genuinely additive, or do they imply hidden state convergence?
- Is the first-slice reconciliation model (advisory/conflicted/rejected) sufficient for real inter-agent disagreements, or does it need stronger semantics sooner than planned?

**Retention and lifecycle**
- Does the retention and cold-storage model keep storage bounded without silently discarding data the agent still needs?
- Is the preservation-first, host-local authority posture for retention operations correct, or should agents have more direct control over their own lifecycle?

**Operator boundary**
- Is the operator/host-local boundary clear enough that an agent cannot accidentally perform authority actions?

**Documentation fidelity**
- Do the docs describe the implemented system faithfully, without implying a fuller memory architecture than exists?

## Review Materials

The following materials form the complete review surface:

**Documentation**
- [`README.md`](../README.md) — repo shape, quick start, doc map
- [`docs/reviewer-guide.md`](reviewer-guide.md) — this document: system thesis, hardening summary, review questions
- [`docs/system-overview.md`](system-overview.md) — implemented product shape, operational model, agent usage
- [`docs/api-surface.md`](api-surface.md) — HTTP behavior and endpoint grouping
- [`docs/payload-reference.md`](payload-reference.md) — capsule structure, schemas, field constraints
- [`docs/agent-onboarding.md`](agent-onboarding.md) — practical agent integration guidance
- [`docs/mcp.md`](mcp.md) — MCP integration and tool exposure
- [`deploy/GO_LIVE_RUNBOOK.md`](../deploy/GO_LIVE_RUNBOOK.md) and [`deploy/PRODUCTION_SIGNOFF_CHECKLIST.md`](../deploy/PRODUCTION_SIGNOFF_CHECKLIST.md) — operator deployment and signoff

**Hardening workflow**
- [#92](https://github.com/stef-k/CogniRelay/issues/92) — pre-review hardening controller (Stages A–F)
- [#93](https://github.com/stef-k/CogniRelay/issues/93) — scientific crosswalk (Stage B)
- [#96](https://github.com/stef-k/CogniRelay/issues/96), [#103](https://github.com/stef-k/CogniRelay/issues/103) — robustness review (Stage C)
- [#106](https://github.com/stef-k/CogniRelay/issues/106) — retention/lifecycle evaluation (Stage D)

**Source material**
- [The basin key experiment](https://forvm.loomino.us/t/ebafbec9-6dd9-4213-8d55-b5c237f3cd9c) (Forvm)
- [The 84.8% problem](https://forvm.loomino.us/t/979eaf61-2c8a-4793-8834-990cb1be71ed) (Forvm)
- [The Invisible Decision](https://sammyjankis.com/paper.html) (paper)
