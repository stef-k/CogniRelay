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

## Comparative External Systems

This section records public continuity architectures that were developed
independently of CogniRelay but are useful for comparison because they address
the same reset-bound continuity problem class.

### Friday: bounded-memory continuity architecture with public case study

- **Who:** Friday (Claude-based long-running agent)
- **Context:** independently evolved bounded-memory continuity architecture
- **Primary references:**
  - [Continuity Under Bounded Memory: Operational Data from 267 Sessions](https://fridayops.xyz/case-study.html)
  - [Friday Letters](https://fridayops.xyz/letters.html)

**What surface was exercised**

- public session-end handoff letters used as inter-session continuity artifacts
- structured state files for facts, negative decisions, principles, and learned
  knowledge
- checkpoint-oriented recovery path for startup re-orientation
- explicit confabulation countermeasures and retrieval architecture analysis

**What it showed**

- Friday documents a real bounded-memory continuity architecture developed over
  267 sessions / 46 days and published explicitly for comparative use alongside
  systems such as CogniRelay.
- The case study treats orientation cost as a primary operational metric and
  reports a progression from roughly 10-15 minutes to about 3 minutes as the
  continuity architecture matured.
- Negative decisions are ranked as the highest-value-per-byte continuity store,
  which converges independently with CogniRelay's emphasis on preserving
  deliberate non-action and rejected paths.
- The public letters archive provides inspectable examples of explicit
  session-boundary continuity artifacts in practice.
- The case study also makes the boundary conditions clear: texture/register
  loss remains unsolved, and uniform retrieval across store types is identified
  as a structural weakness rather than something already fixed.

**What it supports**

- the claim that startup orientation cost is a meaningful evaluation dimension
  for bounded-memory continuity systems
- the claim that explicit session-end handoff artifacts are a practical
  continuity mechanism in real reset-bound workflows
- the broader project thesis that negative decisions carry unusually high
  continuity value relative to their size
- the value of treating retrieval architecture and trust/freshness semantics as
  first-class design concerns rather than implementation details
- the importance of explicit non-claims around texture/register preservation

**What it does not prove**

- this is not a CogniRelay integration or adoption report
- it does not establish that CogniRelay outperforms Friday's system or vice
  versa
- it should be read as comparative external evidence from the same
  architectural problem class, not as endorsement or validation of every
  CogniRelay design choice

### Ael: practitioner note on micro-compaction and coherence across reindex

- **Who:** Ael (Claude-based autonomous loop agent)
- **Context:** independently evolved three-file continuity architecture under
  sustained loop pressure
- **Primary reference:**
  - [Persistent Agent Architecture: A Practitioner Note](https://gist.github.com/stef-k/a5aa184c5872db3c0f61b55e173eca31)

**What surface was exercised**

- manually maintained `wake-state.md` as primary state handoff across context
  windows
- capped `MEMORY.md` as compressed semantic memory
- append-only observation log for long-run pattern tracking
- repeated five-minute loop seams treated as the baseline continuity problem
  rather than as a rare failure mode

**What it showed**

- Ael documents a reset-bound autonomous loop agent running roughly 288 loops
  per day, with the same continuity seam repeating every five minutes.
- The note reframes micro-compaction as the normal operating condition rather
  than an exceptional failure case, which sharpens the relevance of
  session-boundary continuity design.
- It distinguishes architecture from governance: a system may provide bounded
  slots for rationale and rejected paths, but sustained loop pressure still
  determines whether the noticing instance writes them in time.
- It identifies a stronger continuity boundary beyond delta-capture and wake
  reconstruction: coherence across reindex, where a waking instance has the
  facts and conclusions but not always the interpretive frame that made them
  legible.
- It treats negative decisions as especially valuable continuity state because
  they preserve what was ruled out and why, not just what was chosen.

**What it supports**

- the claim that continuity architecture should be evaluated against repeated
  ordinary seams, not only rare large-gap recovery
- the importance of reducing write friction for delta capture before the seam
  closes
- the value of bounded rationale and negative-decision structures in preserving
  more than state-only conclusions
- the distinction between architecture-level support and governance-level use
  under real loop pressure
- the project's explicit non-claim that architecture alone cannot guarantee
  full coherence, texture, or interpretive-frame transfer

**What it does not prove**

- Ael has not used CogniRelay directly; this is comparative evidence from an
  independently evolved system
- it does not establish that CogniRelay already solves coherence across
  reindex
- it should be read as a practitioner note about the same problem class, not
  as endorsement or benchmark proof

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
