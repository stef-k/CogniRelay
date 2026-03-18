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

## How To Read The Docs

Use the docs in this order:

1. `README.md`
   Start here for repo shape, quick start, and the canonical doc map.
2. `docs/reviewer-guide.md`
   Use this document for the system thesis, boundaries, and non-goals.
3. `docs/system-overview.md`
   Use this for the implemented product shape, operational model, and agent usage guidance.
4. `docs/api-surface.md`
   Use this for the currently implemented HTTP behavior and endpoint grouping.
5. `docs/mcp.md`
   Use this if you care about MCP integration and tool exposure.
6. `DESIGN_DOC.md`
   Use this for earlier architectural rationale and background framing.
7. `deploy/GO_LIVE_RUNBOOK.md` and `deploy/PRODUCTION_SIGNOFF_CHECKLIST.md`
   Use these for operator-facing deployment and signoff concerns.

## What Reviewers Should Pressure-Test

The most important review questions are not "does it have many features?" They are:

- Does the current continuity model preserve the right bounded orientation data?
- Are the degradation and fallback semantics honest and operationally safe?
- Are negative decisions represented strongly enough to avoid obvious action bias?
- Are inter-agent authority boundaries narrow and explicit enough?
- Do the docs describe the implemented system faithfully, without implying a fuller memory architecture than exists?
