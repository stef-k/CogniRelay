# System Overview

## Purpose

CogniRelay is a self-hosted collaboration and memory service for autonomous agents. It exposes a deterministic HTTP interface over a local git-backed repository so agents can persist state, retrieve context, coordinate work, and exchange messages without depending on a large external platform.

It is agent-agnostic: CogniRelay does not depend on a specific model provider, agent runtime, or orchestration framework, as long as the agent can invoke its API surfaces.

The core design principle is simple:

**git is the storage engine; the API is the machine interface**

This system should be read as a bounded continuity and orientation substrate. It aims to preserve enough state for useful continuation and recovery, while making degradation, fallback, and authority boundaries explicit rather than pretending persistence is lossless.

## Practical Application Areas

CogniRelay is most useful in environments where agent work spans multiple sessions, interruptions are routine, and continuity must be recoverable and bounded rather than assumed.

### Software engineering

- **Coding and review agents** that maintain orientation across multi-file refactors, session resets, and context-window compaction — preserving what was already reviewed, what constraints apply, and what decisions were deliberately deferred.
- **Incident response assistants** that retain investigation state, active hypotheses, and escalation context across responder handoffs and shift boundaries.
- **Long-running maintenance workflows** where an agent tracks dependency upgrades, migration progress, or technical-debt campaigns over days or weeks with explicit continuity between sessions.

### Research and analysis

- **Literature review and synthesis agents** that accumulate source assessments, open questions, and analytical threads across reading sessions without re-discovering prior evaluations.
- **Multi-day investigation and reporting assistants** that preserve analytical stance, evidence inventory, and unresolved tensions across interruptions.
- **Policy or technical analysis agents** where the cost of re-establishing context after each session reset undermines analytical coherence.

### Operations and internal tooling

- **DevOps/SRE assistants** that carry forward runbook state, remediation history, and environment constraints across incident timelines and on-call rotations.
- **Support escalation agents** that maintain case context, prior diagnostic steps, and resolution attempts across tier boundaries and shift changes.
- **Project coordination and recurring workflow helpers** that track standing constraints, recurring task state, and coordination context across planning cycles.

### Multi-agent collaboration

- **Delegated collaborator agents** that receive bounded coordination context through handoff artifacts without gaining access to each other's private continuity substrate.
- **Handoff and coordination infrastructure** where structured artifacts (handoffs, shared coordination state, reconciliation records) replace implicit shared-state assumptions.
- **Distributed agent communities** where each agent runs its own CogniRelay instance and collaboration happens through explicit, auditable coordination surfaces.

### Customer-facing and service workflows

- **Support agents with continuity across tickets or cases** where a returning user should not have to re-explain history and the agent should not silently lose prior context.
- **Account or onboarding assistants** that maintain relationship context, preference state, and follow-up obligations across sessions.

### Education, tutoring, and advisory contexts

- **Tutoring systems with continuity** where the agent preserves learner progress, prior explanations, identified gaps, and pedagogical stance across sessions.
- **Coaching and advisory agents** that retain standing preferences, prior advice, and ongoing commitments rather than starting from scratch each interaction.

In all these areas the common thread is that interruptions — session resets, context-window compaction, handoffs between agents or humans — are structural, not exceptional. CogniRelay makes the cost of those interruptions explicit and recoverable rather than silent and cumulative.

### What the current system provides for these use cases

The application areas above are grounded in capabilities the system currently implements:

- **Startup-oriented continuity views** (`view="startup"`) that extract recovery, orientation, and context tiers mechanically from the stored capsule, reducing cold-start reorientation cost.
- **Trust and freshness signaling** — deterministic `trust_signals` on every continuity read, covering recency, completeness, integrity, and scope match so the consuming agent can calibrate confidence in recovered state.
- **Session-end snapshot support** — additive `session_end_snapshot` merges on upsert that reduce the burden of persisting startup-critical fields at session close.
- **Thread identity and scope boundaries** — `thread_descriptor` with lifecycle states, scope anchors, and identity anchors so unrelated threads do not bleed orientation into each other.
- **Salience ranking** — deterministic multi-signal sorting on list and retrieval paths that surfaces the most decision-relevant capsules first.
- **Stable preferences** — explicit standing instructions that persist across threads (e.g., timezone, units, communication style).
- **Rationale entries** — structured decision reasoning with kind/status lifecycle and supersession semantics, preserving *why* alongside *what*.
- **Versioned capability discovery** — `GET /v1/capabilities` lets agents discover what the current instance supports before building integration logic.
- **Bounded coordination primitives** — handoffs, shared coordination artifacts, and reconciliation records for inter-agent collaboration without shared-state mutation.
- **Lightweight client and MCP support** — a stdlib-only CLI client and MCP bootstrap flow for integration without heavy dependencies.

## Research and Evaluation Value

CogniRelay is also a useful artifact for studying questions about agent continuity, recovery, and long-horizon collaboration. It is not a formal academic project, but it implements enough of a concrete continuity substrate that researchers and evaluators can use it as a testbed for empirical work.

For external experiments, third-party usage notes, and public case studies tied
to CogniRelay, see [External References and Case Studies](external-references.md).

### Agent continuity and session-boundary recovery

The system's explicit capsule lifecycle (active → fallback → archive → cold storage), deterministic trust signals, and structured degradation paths provide concrete surfaces for measuring:

- **Reorientation cost**: how much work an agent must redo after a session reset, and how that cost changes with different capsule completeness levels, freshness phases, or fallback states.
- **Session-boundary recovery quality**: whether startup views, trust signals, and fallback mechanisms actually reduce the gap between a fresh-context agent and one with preserved orientation.
- **Degradation behavior**: how agents perform when continuity artifacts are stale, partially trimmed, or missing — the system makes these states explicit rather than hiding them.

### Human-AI interaction and trust

The trust-signaling surface (recency, completeness, integrity, scope match) and the distinction between explicit orientation and implicit inference create testable questions:

- How do users experience interacting with agents that have recoverable continuity versus agents that silently re-derive context?
- Does explicit trust signaling change user confidence in persistent agents?
- How does the recoverability and intelligibility of an agent's externalized memory affect user trust and willingness to delegate longer tasks?

### Evaluation and benchmarking

The deterministic nature of CogniRelay's retrieval, ranking, and trust-signaling paths makes them amenable to controlled evaluation:

- **Continuity quality measurement**: comparing agent task performance with and without continuity infrastructure, across different capsule completeness levels.
- **Startup recovery benchmarking**: measuring how quickly and accurately agents reorient using startup views versus raw capsule data versus no preserved state.
- **Handoff quality**: evaluating whether bounded coordination artifacts (handoffs, shared state, reconciliation records) improve multi-agent task outcomes compared to implicit context passing.
- **Memory architecture comparison**: CogniRelay's explicit, structured, write-time-curated capsules versus append-only logs, vector stores, or inferred-summary approaches.

### Interpretability and memory structure

CogniRelay externalizes agent orientation into inspectable, structured artifacts rather than leaving it implicit in model weights or conversation history:

- **Explicit versus inferred memory**: the system's structured capsule fields (active constraints, drift signals, open loops, negative decisions, rationale entries) make the agent's preserved orientation auditable. This creates a surface for studying whether explicit memory structures produce more predictable or steerable agent behavior than inferred persistence.
- **Trust signaling and uncertainty presentation**: the deterministic trust-signals model provides a controlled surface for studying how mechanical confidence metadata affects agent decision-making.

### Distributed and multi-agent continuity

The owner-per-instance deployment model, delegated token scoping, and bounded coordination primitives provide infrastructure for studying:

- **Handoff protocols**: how bounded orientation projection (only `active_constraints` and `drift_signals` cross the handoff boundary) compares to full-state transfer or no-transfer baselines.
- **Coordination memory**: whether explicit shared coordination artifacts with version checking and reconciliation records improve multi-agent coherence.
- **Bounded shared memory architectures**: the separation between private continuity and delegated coordination surfaces as a model for studying access-controlled multi-agent memory.

### Digital identity and continuity

CogniRelay's model — where an agent's orientation is externalized into durable, bounded, inspectable artifacts that survive context-window resets — touches questions about:

- **Continuity across discontinuities**: what it means for an agent to "continue" when its context is reconstructed from stored artifacts rather than maintained in an unbroken stream.
- **Persistence versus reconstruction**: the practical difference between an agent that remembers and one that re-derives from stored state, and whether users or collaborating agents can distinguish the two.
- **Externalized memory and agent identity**: whether structured, inspectable continuity artifacts constitute a meaningful form of agent self-continuity or are better understood as orientation scaffolding.

These are open questions. CogniRelay does not claim to answer them, but it provides a concrete, operational system against which they can be empirically investigated.

## Default Deployment Topology

The default deployment is one owner-agent per CogniRelay instance.

- The owner-agent runs a local CogniRelay instance as its own continuity substrate.
- The same owner-agent is the local operator and superuser of that instance, holding the `admin:peers` scope.
- Continuity capsules are the owner-agent's local orientation store, not a shared resource. Namespace enforcement supports sub-directory granularity, so tokens can be scoped to specific paths like `memory/coordination` without granting access to `memory/continuity`.
- If the owner-agent wants inter-agent coordination, it issues narrower delegated API tokens to collaborating peers. The governance policy provides a `collaboration_peer` template as a baseline for these tokens — it grants read access to all of `memory/coordination` and write access scoped to specific coordination sub-paths (`memory/coordination/handoffs`, `memory/coordination/shared`, `memory/coordination/reconciliations`), plus read and write access to `messages` and `tasks`, but not to the full `memory` namespace. A separate `replication_peer` template exists for instance-to-instance replication using a dedicated `replication:sync` scope with wildcard read access and write access explicitly scoped to replication-eligible prefixes. Unlike the owner token, replication peers cannot manage tokens, rotate keys, manage peer trust, or perform backup/restore operations.
- Collaborator agents interact through the coordination surfaces (handoffs, shared coordination artifacts, reconciliations), messaging, and tasks — not by directly reading the owner's continuity capsules. This separation is enforced by sub-directory namespace restrictions — the `collaboration_peer` template does not grant access to `memory/continuity`, `memory/core`, `memory/episodic`, or `memory/summaries`.
- An agent that wants its own continuity should run its own CogniRelay instance rather than sharing one.

The system should not be read as a peer-equal shared-instance platform. The collaboration layer is a delegated secondary surface built on top of the owner-agent's local continuity home.

### Optional operator UI

The shipped operator UI for issue `#199` is an optional local-operator observability surface mounted under `/ui`.

- It is disabled by default.
- It is server-rendered HTML with local static assets only.
- It is not a SPA and does not require npm, Node, bundlers, or CDN assets.
- It is read-only in the currently supported posture.
- It exposes bounded continuity inspection rather than a general admin/control panel.
- It includes bounded live updates through `/ui/events` SSE for small overview/list/detail live regions, with pages remaining usable without JS.

The supported posture keeps `COGNIRELAY_UI_REQUIRE_LOCALHOST=true`, so `/ui` remains a loopback-scoped operator surface rather than a normal remotely exposed web app. Non-local auth/session models, mutation actions, standalone archive/cold maintenance consoles, WebSockets, and broader reactive UI behavior are deferred to future explicit issues rather than implied by the current deployment model.

Access isolation between agents is enforced entirely by token scopes and namespace/path restrictions. The system does not provide a separate intrinsic identity-bound ownership or tenant isolation layer beyond that configured access model. Any token with read access to `memory/continuity` can read any capsule in that namespace — capsule privacy depends on the operator not granting that access to collaborator tokens. In the default `collaboration_peer` template this access is excluded, which protects owner-private continuity as configured policy.

### Runtime Concurrency Model

The default deployment runs a single uvicorn worker process (no `--workers` flag). This is intentional: the rate-limit state protection in `app/runtime/service.py` uses a `threading.Lock` to serialize read-modify-write cycles on `logs/rate_limit_state.json`. This lock is correct within a single process but does not protect across OS processes.

If the deployment model changes to multiple uvicorn workers, the rate-limit lock must be replaced with a cross-process mechanism. The recommended strategy is `fcntl.flock` on a dedicated lockfile, following the pattern already established by:

- `app/coordination/locking.py` — per-artifact advisory locks
- `app/git_locking.py` — repository-level mutation lock
- `app/segment_history/locking.py` — per-source file locks

The existing lock-ordering rule applies: the rate-limit lock must remain the innermost lock in any acquisition chain.

Do not add `--workers` to the uvicorn command without completing this migration.

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

#### Trust signals

When a continuity capsule is returned through `POST /v1/continuity/read` or `POST /v1/context/retrieve`, the response includes a `trust_signals` block — an objective, mechanical trust assessment that the consuming agent can use to calibrate how much weight it places on the returned orientation data.

Trust signals are **not** heuristic confidence scores, AI-generated quality ratings, or probabilistic estimates. Every field is deterministically derived from data already present on the capsule or computed during retrieval. The same capsule at the same instant always produces the same signals. There is no model inference, no learned weighting, and no hidden state.

The four dimensions are:

- **Recency** — derived from the capsule's `updated_at` and `verified_at` timestamps relative to the request time, plus the `freshness` metadata (freshness class, stale threshold, explicit expiry). Produces concrete ages in seconds and a phase label (`fresh`, `stale_soft`, `stale_hard`, `expired_by_age`, `expired`). A `null` age means the timestamp was missing or malformed — consumers must not treat it as zero.
- **Completeness** — derived from the capsule's `continuity` orientation fields (`top_priorities`, `active_constraints`, `open_loops`, `active_concerns`, `stance_summary`, `drift_signals`) and from whether token-budget trimming removed content during retrieval. Reports which orientation fields are empty or inadequate and which fields were trimmed.
- **Integrity** — derived from the capsule's `capsule_health` status/reasons and `verification_state`, plus whether the capsule was loaded from the active path or a fallback snapshot. Reports health status, verification status, and source state without interpreting what they mean for the consumer's task.
- **Scope match** — derived from selector resolution: whether the returned capsule matched the requested selector exactly or was a fallback substitute. On the multi-capsule retrieval path, the aggregate also reports how many selectors were requested, returned, and omitted.

On the context-retrieval path, trust signals participate in token-budget accounting. When the full shape would consume too much of the capsule's allocation, a compact form is emitted instead (phase, orientation adequacy, trimmed flag, source state, health status, exact match — no ages or field lists). If even the compact form cannot fit, trust signals are `null`. This degradation is deterministic and surfaced via `recovery_warnings`.

An aggregate `trust_signals` block at the `continuity_state` level summarises the worst-case across all per-capsule signals: worst phase, oldest ages, any-fallback/any-degraded flags, and selector coverage counts. It handles mixed full/compact per-capsule shapes.

Trust signals tell the consumer what the system mechanically knows about the capsule's state. They do not tell the consumer what to do about it — that decision belongs to the agent.

For the full field-level structure including compact forms, aggregate shapes, and nullability rules, see [Payload Reference](payload-reference.md#read--post-v1continuityread).

#### Post-#119 continuity enhancements

The collaborator-grade continuity wave (#119 family) added several capabilities layered onto the existing continuity substrate. Each extends the system's orientation-preservation model without changing the base capsule lifecycle or storage architecture.

**Thread identity and scope boundaries (#120).** Capsules can now carry a `thread_descriptor` with a label, keywords, scope anchors, identity anchors, and a lifecycle state (`active`/`suspended`/`concluded`/`superseded`). This gives agents deterministic thread-level scoping so unrelated threads do not bleed into each other. List operations support filtering by lifecycle, scope anchor, keyword, label, and identity anchor. Lifecycle transitions are atomic with upsert. See [Payload Reference](payload-reference.md#threaddescriptor) for the model.

**Salience ranking (#123).** List and context-retrieve paths now support deterministic multi-signal salience sorting that surfaces the most decision-relevant capsules first. The sort key combines lifecycle rank, health rank, freshness phase, resume adequacy, verification strength, and recency — all derived from existing capsule state at retrieval time, with no stored ranking metadata. See [Payload Reference](payload-reference.md#salience-ranking) for the sort key and response structure.

**Stable preferences (#124).** User and peer capsules can carry up to 12 stable preferences — explicit, user-stated standing instructions that persist across unrelated threads (e.g., "always use metric units", "UTC+2 timezone"). Distinct from the agent's inferred `relationship_model`. See [Payload Reference](payload-reference.md#stablepreference) for the model.

**Rationale entries (#122).** `ContinuityState` now supports up to 6 structured rationale entries capturing decision reasoning, assumptions, and unresolved tensions with a kind/status lifecycle and supersession semantics. This preserves *why* alongside *what*. See [Payload Reference](payload-reference.md#rationaleentry) for the model.

**Startup view (#165).** `POST /v1/continuity/read` accepts `view="startup"` to return a pre-structured `startup_summary` with recovery, orientation, and context tiers alongside the unchanged full capsule. This is a mechanical extraction — no additional I/O. See [Payload Reference](payload-reference.md#startup-view-viewstartup) for the response shape.

**Session-end snapshot (#167).** `POST /v1/continuity/upsert` accepts a `session_end_snapshot` that merges fresh startup-critical fields into the base capsule before persistence, reducing caller burden at session end. See [Payload Reference](payload-reference.md#session-end-snapshot-helper) for the merge algorithm.

**`GET /v1/capabilities` (#179).** A versioned, machine-readable feature map that allows agents to discover what the current instance supports before building integration logic. Returns 12 feature keys covering the continuity enhancements above plus coordination, messaging, peers, and discovery surfaces. See [API Surface](api-surface.md#get-v1capabilities--versioned-feature-map) for the endpoint contract.

#### Mechanical Assistance and Agent Authorship

CogniRelay provides bounded mechanical assistance for continuity maintenance, but does not generate, infer, or synthesize semantic content. The division is strict: CogniRelay handles structural operations deterministically; agents remain solely responsible for meaning-bearing content.

##### What CogniRelay Handles Mechanically

| Capability | Surface | What the system does |
|---|---|---|
| Preserve-by-default field retention | `POST /v1/continuity/upsert` with `merge_mode="preserve"` | Carries forward omitted fields from the stored capsule so agents can update a subset without re-sending the full capsule. |
| Bounded partial list updates | `POST /v1/continuity/patch` | Appends, removes, or replaces individual items in list fields atomically without rewriting the full list. |
| Standalone lifecycle transitions | `POST /v1/continuity/lifecycle` | Transitions `thread_descriptor.lifecycle` without a full capsule upsert. |
| Write-path normalization | All continuity write endpoints | Deduplicates, trims, and normalizes fields deterministically; reports what fired via `normalizations_applied`. |
| Fallback snapshot refresh | All continuity write paths (upsert, patch, lifecycle, revalidate) | Refreshes the last-known-good fallback snapshot after each successful active write. |
| Trust signal computation | Read and retrieve paths | Derives recency, completeness, integrity, and scope-match signals mechanically from stored capsule state. |
| Deterministic trimming | Read and retrieve paths under token budget | Trims lower-priority fields in a fixed order to fit the token budget; reports what was trimmed. |

##### What Agents Must Author Explicitly

All semantic content — the meaning-bearing orientation that makes a capsule useful — is authored by the agent. CogniRelay stores, merges, and retrieves it but never generates it.

| Content | Why it requires agent authorship |
|---|---|
| `stance_summary` | Captures the agent's current analytical or operational position in its own terms. |
| `source` (agent identity, update reason) | Only the agent knows who it is and why it is writing. |
| `confidence` | Only the agent can assess its own certainty. |
| `top_priorities`, `active_concerns`, `active_constraints` | Semantic judgments about what matters and what limits apply. |
| `open_loops`, `drift_signals` | The agent identifies what is unresolved and what has shifted. |
| `rationale_entries` | Structured decision reasoning — why the agent chose what it chose. |
| `stable_preferences` | Explicit standing instructions the agent or user provides. |
| `negative_decisions` | What the agent deliberately chose not to do. |
| `working_hypotheses`, `long_horizon_commitments` | Speculative or durable analytical content. |
| `session_trajectory`, `trailing_notes`, `curiosity_queue` | Session-specific direction, low-commitment observations, and open questions. |
| `relationship_model` | The agent's inferred model of the user or peer relationship. |
| Thread/task labels, keywords, scope anchors, identity anchors | Semantic identity of the thread or task. |

Capsule-level structural fields — `attention_policy`, `freshness`, `canonical_sources`, `metadata`, `stable_preferences`, and `thread_descriptor` — are also agent-authored and preserve-eligible (omitted in preserve mode, they are carried forward from the stored capsule). Other capsule-level fields (`verification_kind`, `verification_state`, `capsule_health`) are not preserve-eligible and must be provided explicitly when needed. The continuity-state-level `retrieval_hints` is similarly agent-authored. CogniRelay stores all of these but never generates or infers their values.

The system never infers, summarizes, or generates any of these fields. When an agent omits a field in preserve mode, CogniRelay carries forward the previously stored value — it does not fill in a new one.

##### Examples

**Preserve-mode upsert** — update stance and priorities, carry forward everything else:

```json
{
  "subject_kind": "thread", "subject_id": "refactor-auth",
  "merge_mode": "preserve",
  "capsule": {
    "subject_kind": "thread", "subject_id": "refactor-auth",
    "updated_at": "2026-03-29T10:00:00Z",
    "verified_at": "2026-03-29T10:00:00Z",
    "source": {"producer": "coder-1", "update_reason": "manual"},
    "confidence": {"continuity": 0.9, "relationship_model": 0.8},
    "continuity": {
      "stance_summary": "Auth module extracted; integration tests next.",
      "top_priorities": ["Write integration tests for new auth service"],
      "active_concerns": [],
      "active_constraints": [],
      "open_loops": [],
      "drift_signals": []
    }
  }
}
```

Required list fields sent as `[]` signal "preserve the stored value" in preserve mode. Optional fields omitted entirely are also preserved. Capsule-level fields (`stable_preferences`, `attention_policy`, `freshness`, etc.) that are absent from the request are carried forward from the stored capsule. See [Preserve-by-default merge](payload-reference.md#preserve-by-default-merge) for the full field-intent rules.

**Patch** — append one open loop without rewriting the list:

```json
{
  "subject_kind": "thread", "subject_id": "refactor-auth",
  "updated_at": "2026-03-29T10:05:00Z",
  "operations": [
    {"target": "continuity.open_loops", "action": "append", "value": "Verify token rotation under new auth flow"}
  ]
}
```

**Lifecycle transition** — conclude a thread without a full upsert:

```json
{
  "subject_kind": "thread", "subject_id": "refactor-auth",
  "transition": "conclude",
  "updated_at": "2026-03-29T11:00:00Z"
}
```

For field-level schemas and constraints, see [Payload Reference](payload-reference.md#reduced-authoring-patterns).

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

1. `GET /v1/capabilities` (optional — confirm which features the instance supports before building integration logic; see [API Surface](api-surface.md#get-v1capabilities--versioned-feature-map))
2. `GET /v1/discovery`
3. `GET /v1/manifest`
4. `GET /v1/contracts`
5. `GET /v1/governance/policy`
6. `GET /health`
7. `POST /v1/index/rebuild-incremental` when writes occurred since the last cycle
8. `POST /v1/context/retrieve` for the active task
9. `GET /v1/tasks/query` for shared planning state
10. `GET /v1/messages/pending` for tracked delivery state
11. `GET /v1/metrics` for backlog, check, and replication health
12. `POST /v1/context/snapshot` when reproducible continuation context is needed

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
- Both `POST /v1/continuity/read` and `POST /v1/context/retrieve` include per-capsule `trust_signals` alongside capsule data; on the multi-capsule retrieval path, an aggregate `trust_signals` block summarises the worst-case across all capsules — see the [Continuity model § Trust signals](#trust-signals) section for the derivation model and [Payload Reference](payload-reference.md) for field-level structure
- Use `POST /v1/continuity/read` when you need the full capsule for one exact selector; set `allow_fallback=true` when you want structured fallback or missing-state degradation; pass `view="startup"` to include a pre-structured `startup_summary` extraction alongside the full capsule
- Use `POST /v1/continuity/refresh/plan` when you need a deterministic list of the next continuity capsules that should be refreshed
- Use `POST /v1/continuity/retention/plan` when you need the next deterministic window of stale archived continuity eligible for explicit cold-store policy application
- Use `POST /v1/continuity/compare` when you need a deterministic diff and recommended verification outcome before rewriting an active capsule
- Use `POST /v1/continuity/revalidate` when you need to confirm, correct, degrade, or conflict-mark one active capsule through the audited write path
- Expect `POST /v1/continuity/upsert` and `POST /v1/continuity/revalidate` to return additive `recovery_warnings` when the fallback snapshot refresh fails after the active write has already committed
- Use `POST /v1/continuity/list` when you need active, fallback, archived, or cold continuity summaries with deterministic artifact-state and retention-class labeling; use the thread identity filters (`lifecycle`, `scope_anchor`, `keyword`, `label_exact`, `anchor_kind`, `anchor_value`) to scope results to a specific thread or scope; use `sort="salience"` for deterministic multi-signal salience ranking — see [Payload Reference](payload-reference.md#salience-ranking) for the sort key and response structure
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
- Do not grant `admin:peers` to collaborator or replication peers — it belongs to the owner/operator role and acts as a superuser bypass for both scope and namespace checks. The `replication_peer` template uses the narrower `replication:sync` scope with explicit namespace grants instead
- Use the `collaboration_peer` governance template as a baseline for collaborator tokens — it enables creation and mutation of coordination artifacts (handoffs, shared artifacts, reconciliations), task management, and messaging, while keeping continuity capsules and core memory private to the owner
- Continuity capsule access is governed by the same token scope and namespace mechanism as all other paths — there is no additional per-agent ownership check. The `collaboration_peer` template excludes `memory/continuity` by default, which protects owner-private continuity as configured policy rather than through a built-in ownership enforcement layer
- Collaboration peers can create handoffs, shared artifacts, reconciliations, tasks, and exchange messages — everything needed for structured inter-agent collaboration without accessing the owner's private continuity substrate
- Prefer API-driven token lifecycle operations over manual file edits so audit state stays consistent
- Keep trust transitions explicit through `POST /v1/peers/{peer_id}/trust`
- Treat Phase 5A handoff artifacts as advisory coordination context layered on top of local continuity, not as remote truth that silently rewrites private orientation

### Token role access matrix

The following matrix summarizes what each token role can access. The owner token is the default token for the agent running the instance. The governance policy exposes `collaboration_peer` and `replication_peer` as baseline templates for issued tokens.

| Capability | Owner (`admin:peers`) | `collaboration_peer` | `replication_peer` |
|---|---|---|---|
| **Read continuity capsules** (`memory/continuity`) | Yes | No | Yes (wildcard read) |
| **Write continuity capsules** | Yes | No | Yes (via `memory` write namespace) |
| **Read core/episodic memory** (`memory/core`, `memory/episodic`) | Yes | No | Yes (wildcard read) |
| **Read coordination artifacts** (`memory/coordination`) | Yes | Yes | Yes |
| **Write coordination artifacts** (requires `write:projects`) | Yes | Yes | No (`write:projects` not granted) |
| **Read tasks** (`tasks`) | Yes | Yes | Yes |
| **Write tasks** (requires `write:projects`) | Yes | Yes | No (`write:projects` not granted) |
| **Read messages** (`messages`) | Yes | Yes | Yes |
| **Write/send messages** | Yes | Yes | Yes |
| **Search and index** | Yes | Yes | No (`search` not granted) |
| **Manage tokens** (issue/revoke/rotate) | Yes | No | No |
| **Manage peer trust** | Yes | No | No |
| **Rotate signing keys** | Yes | No | No |
| **Replication sync** (pull/push) | Yes | No | Yes |
| **Run ops jobs** (`/v1/ops/*`, requires localhost) | Yes | No | No |
| **Risk if token is leaked** | Total compromise | Coordination, task, and message exposure (no continuity/admin access) | Read-all data access; writes scoped to replication prefixes; no administrative capability |

Owner "Yes" entries in capability rows reflect `admin:peers` superuser bypass semantics — `admin:peers` bypasses both scope checks and namespace/path restrictions, so the owner token passes every authorization gate. The `replication_peer` template uses the dedicated `replication:sync` scope with wildcard read namespaces and write namespaces explicitly listing replication-eligible prefixes. It does not carry `admin:peers` and cannot perform administrative operations.

Operators can issue custom tokens with any combination of scopes and namespace restrictions. The templates above are baselines, not the only options.

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
