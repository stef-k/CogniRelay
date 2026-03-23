# CogniRelay

Self-hosted continuity and collaboration substrate for autonomous agents with bounded, recoverable memory.

CogniRelay uses a local git repository as durable state, exposes a machine-first FastAPI interface, stores content as Markdown and JSON/JSONL, and keeps dependencies minimal. It is not a Git forge — it is infrastructure for memory, retrieval, messaging, coordination, and continuity preservation across context-window resets.

The default deployment model is one owner-agent per CogniRelay instance. The owner-agent is also the local operator of that instance. An agent that wants its own continuity substrate should run its own instance rather than sharing one. Collaboration with other agents is a delegated secondary surface — the owner-agent issues narrower API tokens to collaborating peers, and the intended convention is that those peers interact through the coordination surfaces rather than directly reading the owner's continuity capsules.

## When CogniRelay Is Useful

CogniRelay exists for people who run agents that work across sessions, over long tasks, or alongside other agents.

When an agent hits a context-window reset, a compaction boundary, or a handoff to another agent, it loses its working orientation: what it was doing, what it decided not to do, what constraints still apply, and where it was headed. Without infrastructure to preserve that orientation, the user has to re-brief the agent, the agent has to rediscover its own prior decisions, and silent regressions go unnoticed.

CogniRelay reduces that cost. It gives agents a place to persist bounded orientation state and retrieve it on restart, so the user does not have to re-explain context after every reset and the agent does not have to guess what it was doing.

**When it helps most:**

- Long-running agent workflows that span multiple sessions or context windows
- Multi-step tasks where losing intermediate progress is costly
- Collaborative setups where the owner-agent delegates bounded coordination access to external peers without shared-state mutation
- Any scenario where silent context loss leads to repeated work, contradictory decisions, or undetected drift

**When it is not especially needed:**

- One-shot chat interactions with no continuation expectation
- Single-prompt tool use where the full context fits in one window
- Stateless pipelines where no agent needs to remember prior decisions

CogniRelay does not claim to preserve everything. It preserves enough bounded orientation for useful continuation, makes loss explicit rather than silent, and keeps the agent in control of what matters.

## What It Offers

- Git-backed read, write, and append operations with commit-on-change behavior
- Derived indexing and local search with JSON indexes and SQLite FTS5
- Context retrieval, continuity capsules, and deterministic snapshots for continuation-safe agent loops
- Peer registry, federation metadata, direct messaging, and relay transport
- Shared task records, patch proposal/apply flows, and code check/merge workflows
- Token lifecycle management, signed message verification, replication, backup, and host-local ops automation

## Agent Integration Patterns

Agents integrate with CogniRelay through hook points in their runtime loop. CogniRelay does not control when it is invoked — agents own invocation timing, and CogniRelay owns response quality once invoked.

**Minimum viable integration** (two hook points):

- `startup`: read continuity capsule and retrieve context to restore orientation after a reset
- `pre-compaction / handoff`: upsert continuity capsule to preserve current orientation before the context window compacts or the agent hands off

This is enough for basic orientation recovery across resets.

**Recommended fuller integration** (four hook points):

- `startup`: restore orientation (same as above)
- `pre-prompt`: retrieve fresh context and check for pending messages, coordination artifacts, or task updates
- `post-prompt`: persist any orientation changes, new decisions, or negative decisions after the agent acts
- `pre-compaction / handoff`: ensure the latest orientation is durable before context loss

The fuller pattern gives tighter continuity — the agent's orientation stays current within the session, not just across resets.

For the full cold-start endpoint sequence, see [System Overview: Agent Usage](docs/system-overview.md#agent-usage).

## Canonical Docs

- [Agent Onboarding](docs/agent-onboarding.md): practical integration guide for cold-start and already-running agents
- [Reviewer Guide](docs/reviewer-guide.md): system thesis, boundaries, recovery model, and authority limits
- [System Overview](docs/system-overview.md): implemented product shape and agent usage guidance
- [API Surface](docs/api-surface.md): currently implemented HTTP behavior grouped by domain
- [Payload Reference](docs/payload-reference.md): capsule structure, request/response schemas, and field constraints
- [MCP Guide](docs/mcp.md): MCP bootstrap flow and tool mapping
- [Go-live Runbook](deploy/GO_LIVE_RUNBOOK.md): operator deployment and go-live workflow
- [Production Signoff Checklist](deploy/PRODUCTION_SIGNOFF_CHECKLIST.md): production verification and data-safety checks

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --host 127.0.0.1 --port 8080 --reload
```

If you want git history under `data_repo/` and it is not already initialized:

```bash
cd data_repo
git init
```

For non-local exposure, prefer file-based peer tokens in `data_repo/config/peer_tokens.json` instead of the plaintext development token in `.env`.

Each CogniRelay instance is intended for a single owner-agent. If you operate multiple agents that each need their own continuity, run a separate instance per agent.

## Runtime Shape

- API framework: FastAPI
- Storage model: git-backed repo plus Markdown and JSON/JSONL files
- Search layer: stdlib `sqlite3` FTS5 with JSON-index fallback
- Auth model: bearer tokens with scopes and split read/write namespace restrictions
- Machine discoverability: `/v1/manifest`, `/v1/discovery/*`, and `POST /v1/mcp`

For agent integration details, including the MCP bootstrap flow and tool mapping, see [docs/mcp.md](docs/mcp.md).

## Development

Tests are in `tests/`. Discovery and manifest behavior are covered in `tests/test_discovery.py`.

Install development-only tooling with:

```bash
pip install -r requirements-dev.txt
```

Local quality commands:

```bash
./.venv/bin/python -m unittest discover -s tests -v
./.venv/bin/python -m ruff check app tests tools_hash_token.py
```
