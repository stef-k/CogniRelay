---
name: cognirelay-continuity-authoring
description: Use when maintaining CogniRelay continuity responsibly from an agent runtime. The agent authors semantic capsule meaning; hooks and adapters only read, gather facts, template, validate, diff,
  write, and read back.
---

# CogniRelay Continuity Authoring

Use this skill when an agent runtime needs to maintain CogniRelay continuity responsibly.

## Responsibility Split

CogniRelay is the substrate for continuity storage, startup reads, bounded retrieval, graph orientation, schedule orientation, trust signals, and validation limits. CogniRelay is not the semantic author of a capsule.

The running agent authors semantic fields through explicit judgment before any continuity save. Semantic fields include stance, priorities, open loops, constraints, negative decisions, rationale, durable preferences, retrieval hints, and next-step meaning.

Hooks and adapters must not infer semantic continuity from prompts, transcripts, tool output, stale plans, git history, logs, or schedule items. They may gather mechanical facts, provide templates, validate payloads, show diffs, submit an agent-authored payload, and read back stored state.

## Startup And Retrieval

At startup or pre-prompt time, use `agent-assets/hooks/cognirelay_retrieval_hook.py` for read-only orientation. It reads `POST /v1/continuity/read` with `view="startup"` and may call `POST /v1/context/retrieve` only when explicitly enabled and a task is supplied.

Do not use retrieval output as a continuity write. Do not persist prompt text, transcript text, tool chatter, shell output, or copied retrieval snippets.

Graph and schedule sections are read-only orientation adjuncts. They can help the agent decide what to do next, but they are not capsule fields to copy mechanically.

## Save Flow

Use `agent-assets/hooks/cognirelay_continuity_save_hook.py` after the agent has enough context to author a durable update.

1. Run `facts` for mechanical subject/config/runtime facts and help links.
2. Run `template` for a generic full `continuity.upsert` skeleton.
3. Author semantic fields explicitly in the payload.
4. Run `dry-run` to reject placeholders and inspect a candidate-only semantic diff.
5. Run `write` only after the explicit agent-authored payload exists.
6. Run `readback` or `doctor` to verify warnings, trust signals, and stored state.

## Scheduling

Agents may create one-shot reminders or task nudges through `schedule.create` or `POST /v1/schedule/items` only when the user or an explicit work plan needs future follow-up.

- Use UTC timestamps only.
- Use `kind="reminder"` for general follow-up.
- Use `kind="task_nudge"` only when linked to a task, thread, or subject.
- Scheduling does not execute work; it surfaces future orientation through `schedule_context`, `schedule.list`, and `/ui/schedule`.
- Do not auto-create reminders from every open loop.
- Do not infer due dates.
- Do not acknowledge or retire schedule items unless the work was actually handled or made irrelevant.

## Write Discipline

Before saving, verify that the payload is bounded, durable, and agent-authored. Reject prompt dumping, transcript dumping, copied retrieval text, and automatic semantic inference. Treat warnings and degraded trust signals as operational input for the agent, not as hook-authored meaning.
