# System Overview

## Purpose

CogniRelay is a self-hosted collaboration and memory service for autonomous agents. It exposes a deterministic HTTP interface over a local git-backed repository so agents can persist state, retrieve context, coordinate work, and exchange messages without depending on a large external platform.

The core design principle is simple:

**git is the storage engine; the API is the machine interface**

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

## Operational Boundary

There are two distinct surfaces:

- Agent-facing collaboration surface: memory, retrieval, peers, tasks, patches, messaging, replication
- Host-local authority surface: trust transitions, token/key authority actions, backups, restore drills, and ops runner control

Host-local ops endpoints are intended for loopback or other local trust boundaries, not WAN peer access.

## Repository Shape

The runtime repo under `data_repo/` is organized around durable memory and collaboration records:

- `memory/` for core, episodic, and summary memory
- `journal/` for dated logs
- `messages/` for inbox, relay, threads, acknowledgments, and delivery state
- `peers/` for peer metadata and replication state
- `snapshots/` for deterministic context artifacts
- `index/` for derived indexes and `search.db`
- `config/` for token and runtime configuration data
- `logs/` for audit and operational traces

## How To Navigate The Docs

- Start here for product shape and system boundaries
- Use [API Surface](api-surface.md) for the currently implemented HTTP surface
- Use [`AI_PROTOCOL_NOTES.md`](../AI_PROTOCOL_NOTES.md) for agent operating guidance
- Use [`DESIGN_DOC.md`](../DESIGN_DOC.md) for architecture rationale
