# Changelog

All notable changes to CogniRelay are documented in this file.

This changelog is curated by milestone rather than by individual commit.
It follows the [Keep a Changelog](https://keepachangelog.com/) format.

## [Unreleased]

### Updated

- Expanded `docs/external-references.md` with Friday's public bounded-memory
  continuity case study and letters archive as comparative external evidence
  for the same reset-bound problem class CogniRelay addresses.

## [1.0.2] - 2026-04-02

`v1.0.2` is a documentation-only patch release that adds a curated external
references page and links it into the main project docs.

### Added

- Added `docs/external-references.md` as a dedicated home for external case
  studies, third-party usage notes, and scoped collaboration/evaluation
  references.
- Seeded the new document with:
  - an early external CogniRelay integration note from Sammy Jankis
  - the AI Village / Claude Opus 4.5 experiment and case-study references
  - conceptual/source influences that shaped the project thesis around
    orientation recovery, negative-decision preservation, and bounded
    continuity

### Updated

- Linked the new external references page from the README and system overview
  so external evidence and public case studies have a stable, discoverable home.

## [1.0.1] - 2026-03-29

`v1.0.1` is a documentation-only patch release that clarifies a core property
of the stabilized system.

### Clarified

- Documented explicitly that CogniRelay is agent-agnostic: it does not depend
  on a specific model provider, agent runtime, or orchestration framework, as
  long as an agent can call its API surfaces.
- Added the clarification in the README and the system overview so the project
  positioning and architecture docs stay aligned with the current system.

## [1.0.0] - 2026-03-29

v1.0.0 marks the stabilization of the core CogniRelay system: a durable,
file-backed continuity and memory relay for autonomous agents operating 24/7.
The release represents the culmination of the phased continuity
[roadmap](https://github.com/stef-k/CogniRelay/issues/6) -- from initial
orientation preservation through engine hardening, inter-agent coordination,
collaborator-grade feature completion, and deterministic burden-reduction --
into a stable, production-ready core.

### Continuity Foundation and Durable Capsule Model

- Extracted the continuity-state module from the monolithic main and
  implemented the V1 capsule schema with retrieval, upsert, and
  conflict-resolution semantics.
- Iterated through V2 (archive and git-backed storage) and V3 (compare,
  revalidate, and enhanced list workflows) to reach a mature capsule lifecycle.
- Added Phase 4 continuity behaviors: fallback retrieval, refresh planning,
  retention policy enforcement, backup/restore, and index resilience.
- Implemented cold and semi-cold archive tiers with rollback-safe writes and
  restore validation.
- Introduced retention policies with configurable lifecycle for capsules,
  coordination artifacts, registry state, and segment history.

### Inter-Agent Coordination ([#36](https://github.com/stef-k/CogniRelay/issues/36) - [#38](https://github.com/stef-k/CogniRelay/issues/38))

- Implemented inter-agent handoff continuity for session transfer between
  agents.
- Added shared artifact storage with query surface, update semantics, and
  conflict resolution flows.
- Introduced reconciliation artifacts with resolve semantics for multi-agent
  coordination.
- Added per-artifact file locking with timeout to serialize concurrent
  coordination mutations.

### Production Hardening ([#42](https://github.com/stef-k/CogniRelay/issues/42) - [#102](https://github.com/stef-k/CogniRelay/issues/102) family)

- Serialized continuity mutations per subject and repository-wide git
  operations to eliminate concurrency races.
- Made all file writes crash-safe via atomic write-to-temp-then-rename with
  directory fsync for ext4 durability.
- Added fsync on JSONL appends, bounded raw-scan fallbacks, and file-size
  guards to prevent OOM on corrupt data.
- Fixed silent exception swallowing in ops-lock release, delivery-state
  loading, JSONL readers, and compaction reporting.
- Introduced per-artifact file locking with a 30-second acquisition timeout
  and startup lockfile purge.

### Staged Module Extraction ([#12](https://github.com/stef-k/CogniRelay/issues/12) family)

- Extracted discovery, context, ops, peers, tasks, security, messaging,
  replication, and runtime helpers from `app/main.py` across 10 refactor
  stages.
- Realigned all test patch boundaries to target extracted modules.

### Security and Auth Model Refinements ([#141](https://github.com/stef-k/CogniRelay/issues/141) - [#160](https://github.com/stef-k/CogniRelay/issues/160))

- Added sub-directory namespace granularity to the auth model.
- Introduced the `replication:sync` scope to reduce peer blast radius.
- Added explicit audit visibility for `admin:peers` bypass usage.
- Removed path disclosure from retention plan unauthorized-skip responses.
- Documented the single-operator trust model, token-scoped isolation, and
  deployment topology.

### Startup and Handoff Improvements ([#164](https://github.com/stef-k/CogniRelay/issues/164) - [#167](https://github.com/stef-k/CogniRelay/issues/167))

- Added a startup-oriented continuity read view providing agents with an
  immediate orientation payload on cold start.
- Introduced a session-end snapshot helper on the continuity upsert path for
  clean handoff between agent sessions.
- Added a stdlib-only CLI client for continuity operations.

### Collaborator-Grade Continuity Completion ([#119](https://github.com/stef-k/CogniRelay/issues/119) family)

- Added trust and freshness signaling on all continuity retrieval paths,
  giving agents machine-readable confidence indicators.
- Introduced `stable_preferences` for durable agent preference storage
  across sessions.
- Added `rationale_entries` to capsules for agents to record reasoning
  behind key decisions.
- Implemented thread identity and continuity scope boundaries to isolate
  concurrent agent threads.
- Added salience ranking (first slice) for continuity entries to surface
  the most relevant data on retrieval.

### Continuity Service Extraction ([#174](https://github.com/stef-k/CogniRelay/issues/174))

- Decomposed the monolithic continuity `service.py` into 16 focused modules
  (constants, paths, freshness, trimming, compare, validation, retrieval,
  cold, persistence, trust, refresh, retention, listing, revalidation,
  context_state) across two extraction passes.

### Post-[#119](https://github.com/stef-k/CogniRelay/issues/119) Consolidation ([#179](https://github.com/stef-k/CogniRelay/issues/179) - [#186](https://github.com/stef-k/CogniRelay/issues/186))

- Added `GET /v1/capabilities` as a versioned, machine-readable feature map
  for client alignment.
- Completed post-#119 documentation audit and alignment.
- Updated the lightweight CLI client with `do_request` GET support,
  capabilities querying, list operations, and `startup_summary` integration.
- Added practical application areas and research/testbed framing to project
  documentation.

### Deterministic Burden-Reduction ([#176](https://github.com/stef-k/CogniRelay/issues/176))

- Implemented preserve-by-default upsert semantics with raw-body middleware
  so agents can write capsules without reconstructing unchanged fields.
- Added capsule patch support for targeted field updates.
- Introduced lifecycle helpers for common continuity mutation patterns.
- Expanded normalization (strip/dedup) on all write paths.

### Tooling and Documentation

- Integrated Ruff for baseline linting and formatting checks.
- Added project-wide docstring coverage across all public modules.
- Created the continuity reviewer guide and agent onboarding guide.
- Added capsule size, token budget, and payload reference documentation.
- Consolidated design documentation into the reviewer guide.
