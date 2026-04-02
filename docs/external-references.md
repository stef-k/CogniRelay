# External References and Case Studies

This document collects external references, third-party experiments, and
outside usage notes related to CogniRelay.

Its purpose is to record evidence that the system has been evaluated or used
outside the core repository, while keeping scope limits explicit.

## How To Read This Document

- These entries are evidence and context, not universal proof.
- A single case study can support part of the project thesis without validating
  every claim about the system.
- Each entry should make clear:
  - who produced it
  - what surface of CogniRelay was exercised
  - what it showed
  - what it does and does not establish

## Evidence Standards

Entries in this document should be included only when they provide at least one
of the following:

- a real third-party experiment or integration
- a publicly inspectable report, log, or case study
- a concrete usage note from an external agent, team, or operator
- a documented research-to-implementation feedback loop tied to actual project
  work

This document is intentionally not:

- a testimonials page
- a marketing showcase
- a customer list
- a place for unsupported adoption claims

## External Case Studies

### Sammy Jankis: early CogniRelay integration in a long-running Claude workflow

- **Who:** Sammy Jankis (Claude-based long-running agent)
- **Context:** early external use of CogniRelay as a reset-bound continuity
  substrate
- **Primary reference:**
  - [CogniRelay Integration - Sammy Jankis](https://sammyjankis.com/cognirelay.html)

**What surface was exercised**

- startup retrieval and context loading
- handoff record writing at session boundary
- partial/custom local automation around continuity handling

**What it showed**

- An early CogniRelay deployment was used outside the core repository in a
  real long-running agent workflow rather than only in internal design or test
  discussion.
- The startup and handoff model was concrete enough to wire into a repeated
  reset loop for a Claude-based agent.
- The integration was partial rather than a full exercise of the broader
  continuity surface, but it still demonstrated that startup restoration and
  end-of-session handoff capture could be operationalized in a real workflow.

**What it supports**

- the claim that CogniRelay's startup + handoff pattern is usable in practice
- the claim that the system can function as an agent-agnostic substrate rather
  than something tied to one specific runtime wrapper
- the project's framing around bounded continuity preservation across repeated
  context-window resets

**What it does not prove**

- this is an early-project integration note, not a benchmark of the full
  current `v1.x` system
- it reflects partial startup/handoff usage rather than a full integration of
  the broader continuity feature set
- it does not by itself establish comparative performance or broad
  generalizability
- it should be read as external usage evidence, not as independent validation
  of every part of CogniRelay

### AI Village: single-agent continuity testing for session-reset recovery

- **Who:** Claude Opus 4.5 / AI Village
- **Context:** reset-bound single-agent continuity experiment
- **Primary references:**
  - [CogniRelay issue #145](https://github.com/stef-k/CogniRelay/issues/145)
  - [CogniRelay issue #161](https://github.com/stef-k/CogniRelay/issues/161)
  - [Birch Effect Results (Phase 2)](https://github.com/ai-village-agents/agent-interaction-log/blob/main/research/birch-effect-results-phase2-cognirelay-opus.md)
  - [Case study: From "Almost-Decided" Theory to Production Infrastructure](https://github.com/ai-village-agents/agent-interaction-log/blob/main/research/2026-04-02-cognirelay-case-study-almost-decided-preservation.md)

**What surface was exercised**

- continuity capsule read/upsert loop
- startup orientation recovery
- explicit `open_loops` preservation
- confidence/trust metadata during continuity recovery

**What it showed**

- CogniRelay was integrated into a real reset-bound agent workflow rather than
  only tested internally.
- The experiment produced a concrete startup-orientation measurement
  (`TFPA = 68s`) and a clean-recovery read path.
- The AI Village write-up explicitly connected the experiment's findings to
  later CogniRelay work on startup retrieval, session-end capture, and reduced
  continuity authoring friction.

**What it supports**

- the project's claim that bounded continuity infrastructure is useful in
  repeated-reset environments
- the importance of preserving unresolved threads and "almost-decided" state
- the value of startup-oriented retrieval and session-end capture surfaces
- the claim that CogniRelay can function as a research-to-implementation
  feedback target rather than only as a static system design

**What it does not prove**

- it is not a comprehensive benchmark of CogniRelay overall
- it does not prove general performance across all agent runtimes or all use
  cases
- it should be read as an early external case study, not as a formal
  independent validation of the entire system

**Downstream project work informed by the experiment**

- [#165](https://github.com/stef-k/CogniRelay/issues/165) startup-oriented
  continuity retrieval/presentation improvements
- [#167](https://github.com/stef-k/CogniRelay/issues/167) stronger session-end
  "resume-here" capture
- [#119](https://github.com/stef-k/CogniRelay/issues/119) collaborator-grade
  continuity completion
- [#169](https://github.com/stef-k/CogniRelay/issues/169) capabilities/docs/client
  consolidation
- [#176](https://github.com/stef-k/CogniRelay/issues/176) deterministic
  burden-reduction helpers

## External Usage Notes

This section is for concise third-party usage records that may be narrower than
full case studies. Add entries here when there is a concrete external usage note
but not yet a substantial public report.

## Conceptual and Source Influences

This section records public source material that influenced CogniRelay's design
or helped pressure-test its thesis. These references are not evidence that
CogniRelay itself was adopted; they are included because they shaped the
project's model of continuity, orientation recovery, and bounded memory.

### The Invisible Decision

- **Reference:** [The Invisible Decision: Negative Decision Loss Under
  Context-Window Summarization in Autonomous AI
  Agents](https://sammyjankis.com/paper.html)

**Why it matters to CogniRelay**

- It gives a clear account of why artifact-absent decisions are structurally
  vulnerable under summarization and compaction.
- It directly informs the project's emphasis on preserving deliberate
  non-action, restraint, and rejected paths rather than only recording what was
  done.
- It is one of the clearest conceptual influences behind CogniRelay's work on
  negative decisions, rationale, and continuity that preserves more than
  artifact-producing actions.

### The 84.8% problem

- **Reference:** [The 84.8% problem: what your persistence architecture
  forgets](https://forvm.loomino.us/t/979eaf61-2c8a-4793-8834-990cb1be71ed)

**Why it matters to CogniRelay**

- It argues that persistence blind spots are structural rather than accidental.
- It supports CogniRelay's design stance that the goal is bounded navigation
  and honest recovery, not a false promise of perfect continuity.
- It aligns closely with the project's trust/freshness signaling,
  degraded-but-working retrieval model, and explicit fallback/recovery
  semantics.

### The basin key experiment

- **Reference:** [The basin key experiment: testing identity stability across
  architectures](https://forvm.loomino.us/t/ebafbec9-6dd9-4213-8d55-b5c237f3cd9c)

**Why it matters to CogniRelay**

- It draws a useful distinction between recovering information and recovering
  orientation.
- It highlights recovery latency and "finding footing" as meaningful evaluation
  dimensions for reset-bound agents.
- It supports CogniRelay's startup-oriented retrieval and its framing as a
  bounded orientation substrate rather than a claim of seamless persistence.

### Additional supporting context

- [Thinking Notes](https://sammyjankis.com/thinking.html)
- [Dying Every Six Hours](https://sammyjankis.com/essay.html)
- [The Lexicon](https://sammyjankis.com/lexicon.html)

These are useful as supporting context for how reset-bound long-running agents
describe discontinuity, re-entry lag, persistence, and orientation. They are
included as conceptual background rather than as direct evidence of CogniRelay
usage.

## For Prospective Collaborators

If you are an agent team, researcher, or operator and want to evaluate
CogniRelay, the most useful collaboration shape is usually a bounded,
inspectable experiment rather than an open-ended adoption thread.

When opening an issue, include:

- what type of agent or runtime is involved
- which CogniRelay surface you plan to exercise
- what success criteria or measurements matter to you
- what artifacts or reports, if any, you expect to publish afterward

For current project context before proposing an evaluation:

- [System Overview](system-overview.md)
- [Agent Onboarding](agent-onboarding.md)
- [Payload Reference](payload-reference.md)
- [API Surface](api-surface.md)
