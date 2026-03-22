# Agent Onboarding

This guide is for agents integrating with CogniRelay, whether starting cold or adding deeper integration to an already-running setup.

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

1. `POST /v1/continuity/read` with your selector — returns your last persisted orientation capsule
2. `POST /v1/context/retrieve` for your active task — returns a continuity-shaped context bundle including relevant memory, recent items, and any loaded capsule state

Use the returned capsule to restore your constraints, drift signals, open loops, and stance before you begin working.

**Before compaction or handoff (when you are about to lose context):**

1. `POST /v1/continuity/upsert` with your current orientation — persists your active constraints, drift signals, open loops, stance summary, and any negative decisions you want to survive the reset

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

3. **Add coordination when you need it.** Handoffs, shared artifacts, and reconciliation records are useful only when multiple agents need bounded coordination. They are additive — you can ignore them until you have a multi-agent use case.

4. **Use MCP if your runtime speaks JSON-RPC.** The same capabilities are available through `POST /v1/mcp` as through the HTTP endpoints. See [MCP Guide](mcp.md) for the bootstrap flow.

## The Responsibility Boundary

CogniRelay does not control when you invoke it. You own invocation timing and all decisions about what to persist, what to retrieve, and how to act on what you get back.

CogniRelay owns response quality once invoked. When you call an endpoint, the system is responsible for returning accurate, bounded results — and for degrading explicitly rather than silently when something is stale, missing, or damaged.

Concretely:

- **You decide** when to read or write continuity, what constraints matter, what negative decisions to record, and whether to act on a coordination artifact.
- **CogniRelay decides** how to degrade when indexes are stale (warnings, not failures), how to fall back when an active capsule is missing (structured fallback, not silence), and how to bound what crosses a coordination boundary (only the fields defined by each primitive, never the full capsule).

## What CogniRelay Does Not Do

- It does not persist everything — continuity is bounded and subject to write-time curation
- It does not auto-sync state between agents — coordination artifacts are advisory records, not shared memory
- It does not make decisions on your behalf — it is infrastructure, not an orchestrator
- It does not hide loss — when data is omitted, archived, degraded, or missing, the response tells you

## Next Steps

- [System Overview](system-overview.md) for the full product shape and endpoint guidance
- [API Surface](api-surface.md) for the complete HTTP endpoint reference
- [Payload Reference](payload-reference.md) for capsule structure, request/response schemas, and field constraints
- [MCP Guide](mcp.md) if your runtime uses JSON-RPC tool protocols
- [Reviewer Guide](reviewer-guide.md) for the system thesis, recovery model, and authority boundaries
