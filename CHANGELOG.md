# Changelog

All notable changes to CogniRelay are documented in this file.

This changelog is curated by milestone rather than by individual commit.
It follows the [Keep a Changelog](https://keepachangelog.com/) format.

## [Unreleased]

## [1.2.1] - 2026-04-16

### Updated

- Polished the shipped operator UI presentation with a locally vendored µCSS
  Slate theme, dark mode as the default, a user theme selector, smaller shared
  header/navigation sizing, a shared back-to-top control, and more responsive
  detail-page layout behavior for dense continuity content.
- Improved continuity detail rendering so dense summary/trust sections use a
  flatter full-width layout, stable-preference tables can use the full detail
  row width, and dedicated sections no longer duplicate the same
  `trust_signals` or `stable_preferences` data already rendered elsewhere on
  the page.
- Fixed `/ui/continuity` filter handling so empty server-rendered select values
  degrade to “all” instead of producing 422 responses, and repaired the filter
  form layout so each filter field remains aligned as a single grid item.
- Updated shared UI table rendering so operator tables use hover states and
  scroll safely inside bounded panels instead of clipping wide structured
  content.

## [1.2.0] - 2026-04-15

### Added

- Shipped issue `#199`: an optional local-only read-only operator UI under
  `/ui`, implemented as a server-rendered observability surface with local
  assets only and no SPA/npm toolchain.
- Added lifecycle visibility across active, fallback, archived, and cold
  continuity artifacts on the overview, continuity list, and continuity detail
  UI surfaces.
- Added bounded server-rendered filtering and search on `/ui/continuity` by
  query, subject kind, lifecycle artifact state, and health status.
- Added bounded `/ui/events` SSE live updates with reconnect backoff for small
  progressive live regions on the overview, continuity list, and continuity
  detail pages.

### Updated

- Aligned issue `#199` documentation, env guidance, changelog text, and
  deployment examples with the now-shipped operator UI scope: optional
  local-only read-only `/ui`, server-rendered pages with local assets only,
  bounded continuity lifecycle visibility across active/fallback/archived/cold
  artifacts, bounded continuity filtering/search on `/ui/continuity`, and
  bounded `/ui/events` SSE live updates with reconnect backoff for small
  progressive overview/list/detail live regions.
- Clarified deferred operator UI items as explicit non-goals of the shipped
  scope: non-local auth/session model, mutable UI behavior, WebSockets,
  standalone archive/cold maintenance consoles, and broader reactive UI
  behavior.
- Tightened reverse-proxy deployment examples so they do not accidentally
  publish `/ui` remotely under the current local-only support boundary.

## [1.1.0] - 2026-04-06

### Updated

- Refined the structured continuity entry timestamp model for
  `stable_preferences`, `rationale_entries`, and `negative_decisions`:
  public payloads now use `created_at`, `updated_at`, and optional
  `last_confirmed_at` instead of the older ambiguous `set_at` field, with
  continuity schema advanced to `1.1`. Stabilized-shape legacy continuity
  payloads remain supported for upgrade on load and restore-test validation,
  including Sammy's oldest real continuity capsule sample. Truly
  pre-stabilization continuity payloads missing required modern capsule
  fields remain a bounded unsupported migration case.
- Expanded `docs/external-references.md` with Friday's public bounded-memory
  continuity case study and letters archive as comparative external evidence
  for the same reset-bound problem class CogniRelay addresses.
- Added Ael's public practitioner note on micro-compaction, architecture vs
  governance, and coherence across reindex as comparative external evidence in
  the same continuity problem space.
- Added Lumen's public architecture note on capsule scoping, fact freshness,
  and unknown-unknown stale-state drift as comparative external evidence in
  the same continuity problem space.

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
