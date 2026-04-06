# Agent Onboarding

This guide is for agents integrating with CogniRelay, whether starting cold or adding deeper integration to an already-running setup.

CogniRelay is designed so that each agent wanting its own continuity runs its own instance. This guide assumes you are the owner-agent of your CogniRelay instance — meaning you are also the local operator and the principal that controls token issuance, trust transitions, and system maintenance. If you are integrating as a collaborator peer on someone else's instance, your access is determined by the scopes and namespace restrictions on the token issued to you — the system does not enforce per-agent identity-bound isolation, so your access boundaries are the token's configured policy. By default, collaborator tokens are scoped to coordination surfaces and do not include access to the owner's continuity capsules or core memory.

## What CogniRelay Offers You

CogniRelay is a self-hosted service that preserves bounded orientation state across context-window resets, compaction boundaries, and handoffs to other agents. When you restart or lose context, it gives you back enough of what you were doing — constraints (`active_constraints`), drift signals (`drift_signals`), open loops (`open_loops`), stance (`stance_summary`), and decisions you chose not to take (`negative_decisions`) — to resume usefully rather than starting blind.

It also provides:

- indexed search and context retrieval over your stored memory
- direct messaging and relay transport between peers
- bounded coordination artifacts (handoffs, shared state, reconciliation records) for multi-agent work
- task tracking, patch workflows, and code check/merge gating

CogniRelay does not make decisions for you. It does not silently rewrite your state. It does not claim to preserve everything. What it preserves is bounded, and what it loses is explicit.

## Integration Levels

### Minimum viable: two hook points

If you are adding CogniRelay to an existing agent loop and want the smallest useful integration, use two hook points:

**On startup (or after any context reset):**

1. `POST /v1/continuity/read` with your selector — returns your last persisted orientation capsule. Pass `view="startup"` to include a pre-structured `startup_summary` with recovery, orientation, and context tiers alongside the full capsule (see [Payload Reference: Startup view](payload-reference.md#startup-view-viewstartup))
2. `POST /v1/context/retrieve` for your active task — returns a continuity-shaped context bundle including relevant memory, recent items, and any loaded capsule state

Use the returned capsule to restore your constraints, drift signals, open loops, and stance before you begin working.

Continuity schema note: newly written continuity capsules now use schema `1.1`. Stabilized legacy `1.0` continuity payloads are still supported when they already have the modern required capsule structure and only need structured-entry timestamp upgrade or top-level timestamp repair. Sammy's oldest real continuity capsule sample falls into that supported bucket. Truly pre-stabilization payloads missing required modern capsule fields are not auto-migrated.

**Before compaction or handoff (when you are about to lose context):**

1. `POST /v1/continuity/upsert` with your current orientation — persists your active constraints, drift signals, open loops, stance summary, and any negative decisions you want to survive the reset

You can include an optional `session_end_snapshot` in the upsert request to focus on the six startup-critical fields without rebuilding the entire capsule. Send your base capsule (from the last read — stale continuity fields are fine, but `updated_at` must still be set to the current time) and a snapshot containing fresh values for `open_loops`, `top_priorities`, `active_constraints`, `stance_summary`, and optionally `negative_decisions` and `session_trajectory`. The server merges the snapshot into the capsule before persisting. See [Payload Reference](payload-reference.md#session-end-snapshot-helper) for details.

This is enough for basic orientation recovery. Your next startup will retrieve what you persisted here.

### Recommended fuller integration: four hook points

For tighter continuity within a session, add two more hook points:

**Before each prompt (pre-prompt):**

1. `POST /v1/context/retrieve` — refresh your context with the latest indexed material
2. `GET /v1/messages/pending` — check for messages, delivery state, or coordination artifacts
3. `GET /v1/tasks/query` — check for task updates if you are coordinating shared work

**After each prompt (post-prompt):**

1. `POST /v1/continuity/upsert` — persist any orientation changes, new constraints, or negative decisions from the work you just did

With the fuller pattern, your orientation stays current within the session — not just across resets. If you crash mid-session, your last post-prompt upsert is recoverable.

### Full cold-start sequence

If you are starting completely fresh with no prior context, the full recommended startup sequence is documented in [System Overview: Agent Usage](system-overview.md#agent-usage). It covers discovery, manifest, contracts, governance, health, index rebuild, context retrieval, task state, pending messages, metrics, and snapshot creation.

Most agents do not need every step on every startup. The minimum viable path (continuity read + context retrieve) is enough for orientation recovery. The full sequence matters when you need to discover the service shape, rebuild stale indexes, or check operational health.

## For Already-Running Agents

If your agent is already running and you want to integrate CogniRelay incrementally:

1. **Start with the two-hook minimum.** Add continuity upsert before your next compaction and continuity read on your next startup. This gives you orientation recovery with no changes to your prompt-level loop.

2. **Add pre-prompt and post-prompt hooks when ready.** These tighten within-session continuity but are not required for basic operation.

3. **Add coordination when you need it.** Handoffs, shared artifacts, and reconciliation records are useful when you need to coordinate with external collaborator peers. As the owner-agent, you issue delegated tokens to those peers and they interact through these coordination surfaces. You can ignore coordination until you have a multi-agent use case.

4. **Use MCP if your runtime speaks JSON-RPC.** The same capabilities are available through `POST /v1/mcp` as through the HTTP endpoints. See [MCP Guide](mcp.md) for the bootstrap flow.

5. **Use the CLI client for shell-based hooks.** If your agent runtime invokes hooks as shell commands, `tools/cognirelay_client.py` can read and upsert capsules without a third-party HTTP library. See [CLI Client](cognirelay-client.md) for usage.

## The Responsibility Boundary

CogniRelay does not control when you invoke it. You own invocation timing and all decisions about what to persist, what to retrieve, and how to act on what you get back.

CogniRelay owns response quality once invoked. When you call an endpoint, the system is responsible for returning accurate, bounded results — and for degrading explicitly rather than silently when something is stale, missing, or damaged.

Concretely:

- **You decide** when to read or write continuity, what constraints matter, what negative decisions to record, and whether to act on a coordination artifact.
- **CogniRelay decides** how to degrade when indexes are stale (warnings, not failures), how to fall back when an active capsule is missing (structured fallback, not silence), and how to bound what crosses a coordination boundary (only the fields defined by each primitive, never the full capsule).

## Stable Preferences vs Relationship Model

User/peer capsules support two complementary but distinct fields for cross-thread knowledge:

- **`stable_preferences`** (capsule-level): Explicit, user-stated standing instructions that apply across unrelated threads. Examples: "always use metric units", "UTC+2 timezone", "never auto-commit". The agent records these from explicit user statements. CogniRelay never adds, removes, or modifies preferences autonomously.
- **`relationship_model`** (inside `ContinuityState`): The agent's inferred model of the relationship — `trust_level`, `preferred_style`, `sensitivity_notes`. These are the agent's observations, not the user's explicit statements.

**Litmus test:** If the subject explicitly stated or confirmed it and it applies across unrelated threads, it belongs in `stable_preferences`. If the agent inferred it from observation, it belongs in `relationship_model`.

Both may describe the same thing from different perspectives (e.g., user says "be concise" → `stable_preferences`; agent observes short responses work best → `relationship_model.preferred_style`). When they conflict, an explicit preference supersedes an inferred style. CogniRelay does not auto-reconcile — the agent is responsible for composition.

## Rationale Entries

`rationale_entries` (inside `ContinuityState`, max 6) captures structured *why* alongside the *what* of orientation. Each entry records a decision, assumption, or unresolved tension with its reasoning, rejected alternatives, and dependencies.

**When to author rationale entries:** At session-end or handoff, record decisions that a future session would need to understand — not just what was decided, but why, what else was considered, and what assumptions hold. Use `kind: "decision"` for choices made, `kind: "assumption"` for conditions relied upon, and `kind: "tension"` for unresolved trade-offs deferred.

**Relationship to `negative_decisions`:** `negative_decisions` remains for compact deliberate non-actions. `rationale_entries` is broader — positive decisions, trade-off reasoning, assumptions, and tensions. Use either or both. CogniRelay does not auto-reconcile between them.

**Lifecycle:** Set `status: "active"` for current entries. To supersede: set the old entry to `status: "superseded"` and add a new entry with `supersedes` pointing to the old tag. To retire (no longer relevant): set `status: "retired"`. The agent manages the list; CogniRelay enforces the max-6 cap.

**Capture via session-end snapshot:** Include `rationale_entries` in the snapshot as a P1 field — `null` preserves existing entries, an explicit list overrides.

## Interpreting Trust Signals

When you read a continuity capsule (via `POST /v1/continuity/read` or `POST /v1/context/retrieve`), the response includes a `trust_signals` block — a mechanical assessment of the returned capsule across four dimensions. Use it to decide how much weight to place on the recovered orientation:

- **Recency** — check `recency.phase`. `fresh` means timestamps are within the configured threshold. `stale_soft` or worse means the capsule has not been refreshed recently — consider re-verifying before acting on it. A `null` age means the timestamp was missing, not that the capsule is maximally fresh.
- **Completeness** — check `completeness.orientation_adequate`. If `false`, one or more core orientation fields are empty. Check `empty_orientation_fields` to see which ones. If `trimmed` is `true`, token-budget constraints removed content — check `trimmed_fields`.
- **Integrity** — check `integrity.health_status` and `integrity.source_state`. If `health_status` is `degraded` or `conflicted`, the capsule may need revalidation. If `source_state` is `fallback`, you are reading a recovery snapshot, not the current active capsule.
- **Scope match** — check `scope_match.exact`. If `false`, the returned capsule did not match the requested selector exactly.

Trust signals are deterministic and objective — every field is derived from existing capsule state. They do not tell you what to do; they tell you what the system knows about the capsule's state so you can decide. For the full field-level structure, see [Payload Reference](payload-reference.md#read--post-v1continuityread).

## Thread Identity and Multi-Thread Patterns

If you work across multiple threads, projects, or domains simultaneously, use `thread_descriptor` on your capsules to keep them scoped and discoverable.

**Setting up a thread descriptor:** When creating a thread or task capsule, populate `thread_descriptor` with a `label`, relevant `keywords`, `scope_anchors` (e.g., repo name, project key), and optionally `identity_anchors` for typed key-value pins. Set `lifecycle` to `"active"`.

**Filtering by thread:** Use the list filters to find capsules for a specific thread:
- `lifecycle="active"` — only active threads
- `scope_anchor="stef-k/CogniRelay"` — threads scoped to a specific repo
- `keyword="auth"` — threads tagged with a keyword
- `anchor_kind="issue"` + `anchor_value="42"` — threads pinned to a specific issue

**Lifecycle transitions:** Use `lifecycle_transition` on upsert to atomically move a thread through `suspend` → `resume` → `conclude` or `supersede`. When superseding, set `superseded_by` to the successor's `subject_id`.

For the full `ThreadDescriptor` model, see [Payload Reference](payload-reference.md#threaddescriptor).

## Salience Ranking

When listing capsules across multiple threads, use `sort="salience"` on `POST /v1/continuity/list` to surface the most decision-relevant capsules first. The sort considers lifecycle state, capsule health, freshness, resume adequacy, verification strength, and recency — all derived from existing capsule state at retrieval time.

Each returned capsule includes a `salience` block with its `rank` and the individual `sort_key` signals, so you can inspect why one capsule ranked higher than another. The response also includes aggregate `salience_metadata` summarising the result set.

Salience ranking is also applied automatically on the `POST /v1/context/retrieve` path when multiple capsules are loaded.

For the full sort key and response structure, see [Payload Reference](payload-reference.md#salience-ranking).

## Feature Discovery

If you are integrating with a CogniRelay instance and want to confirm which capabilities are available before building integration logic, call `GET /v1/capabilities` as an early step in your cold-start sequence. It returns a versioned, machine-readable feature map with 12 feature keys covering continuity enhancements, coordination, messaging, peers, and discovery. Presence of a key means the capability is available; absence means it is not.

This is useful when your agent supports multiple CogniRelay versions or wants to conditionally enable features like startup view, trust signals, or salience ranking based on what the instance actually supports. See [API Surface](api-surface.md#get-v1capabilities--versioned-feature-map) for the endpoint contract.

## What CogniRelay Does Not Do

- It does not persist everything — continuity is bounded and subject to write-time curation
- It does not auto-sync state between agents — coordination artifacts are advisory records, not shared memory
- It does not make decisions on your behalf — it is infrastructure, not an orchestrator
- It does not hide loss — when data is omitted, archived, degraded, or missing, the response tells you
- It is not a shared-instance platform — each agent wanting its own continuity should run its own instance; collaborators access your instance through delegated tokens and the coordination surfaces

## Next Steps

- [System Overview](system-overview.md) for the full product shape and endpoint guidance
- [API Surface](api-surface.md) for the complete HTTP endpoint reference
- [Payload Reference](payload-reference.md) for capsule structure, request/response schemas, and field constraints
- [MCP Guide](mcp.md) if your runtime uses JSON-RPC tool protocols
- [CLI Client](cognirelay-client.md) for shell-based continuity read, upsert, and token hashing
- [Reviewer Guide](reviewer-guide.md) for the system thesis, recovery model, and authority boundaries
