---
name: cognirelay-continuity-authoring
description: Use when maintaining CogniRelay continuity responsibly from an agent runtime. The agent authors semantic capsule meaning; hooks and adapters only read, gather facts, query runtime help, template, validate, diff,
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

## Runtime Help Gate

Before every continuity mutation, consult the live runtime help contract for the write tool and the field limits that apply to the fields being authored. This is mandatory for `continuity.upsert`, `POST /v1/continuity/upsert`, `continuity.patch`, lifecycle updates, schedule mutations, and any hook mode that will mutate CogniRelay state.

Use MCP when operating through the runtime tool protocol:

```json
{"jsonrpc":"2.0","id":1,"method":"system.tool_usage","params":{"name":"continuity.upsert"}}
```

```json
{"jsonrpc":"2.0","id":2,"method":"system.validation_limits","params":{}}
```

```json
{"jsonrpc":"2.0","id":3,"method":"system.validation_limit","params":{"field_path":"continuity.session_trajectory"}}
```

Use HTTP when operating through REST:

```http
GET /v1/help/tools/continuity.upsert
GET /v1/help/limits
GET /v1/help/limits/continuity.session_trajectory
```

Query targeted limits for every bounded field you plan to change. Common continuity authoring fields include:

- `continuity.stance_summary`
- `continuity.top_priorities`
- `continuity.active_concerns`
- `continuity.active_constraints`
- `continuity.open_loops`
- `continuity.drift_signals`
- `continuity.session_trajectory`
- `continuity.retrieval_hints.must_include`
- `continuity.relationship_model.preferred_style`
- `continuity.relationship_model.sensitivity_notes`
- `continuity.attention_policy.presence_bias_overrides`
- `continuity.capsule_serialized_utf8`

If the runtime help lookup is unavailable, do not guess. Use the shipped hook `facts` output and local `dry-run` as a degraded fallback, then treat `continuity.upsert` as authoritative and fix any runtime error by querying `system.error_guide` or `GET /v1/help/errors/{code}` before retrying.

## Save Flow

Use `agent-assets/hooks/cognirelay_continuity_save_hook.py` after the agent has enough context to author a durable update.

1. Run `facts` for mechanical subject/config/runtime facts and help links.
2. Query runtime help for `continuity.upsert` and runtime limits for every field you will author or mutate.
3. Run `template` for a generic full `continuity.upsert` skeleton.
4. Author semantic fields explicitly in the payload while staying inside the live field limits.
5. Run `dry-run` to reject placeholders and inspect a candidate-only semantic diff.
6. Run `write` only after the explicit agent-authored payload exists and the runtime help gate has been satisfied.
7. Run `readback` or `doctor` to verify warnings, trust signals, and stored state.

Example savepoint flow:

```text
system.tool_usage(name="continuity.upsert")
system.validation_limit(field_path="continuity.stance_summary")
system.validation_limit(field_path="continuity.active_concerns")
system.validation_limit(field_path="continuity.session_trajectory")
agent-assets/hooks/cognirelay_continuity_save_hook.py template
agent authors payload
agent-assets/hooks/cognirelay_continuity_save_hook.py dry-run --input payload.json
agent-assets/hooks/cognirelay_continuity_save_hook.py write --input payload.json
agent-assets/hooks/cognirelay_continuity_save_hook.py readback
```

Example pre-compaction flow:

```text
GET /v1/help/tools/continuity.upsert
GET /v1/help/limits/continuity.open_loops
GET /v1/help/limits/continuity.top_priorities
GET /v1/help/limits/continuity.active_constraints
GET /v1/help/limits/continuity.stance_summary
GET /v1/help/limits/continuity.capsule_serialized_utf8
author compact payload from agent judgment
dry-run
write
doctor
```

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

Before saving, verify through runtime help that the payload is bounded, durable, and agent-authored. Reject prompt dumping, transcript dumping, copied retrieval text, and automatic semantic inference. Treat warnings and degraded trust signals as operational input for the agent, not as hook-authored meaning. If a write fails, consult runtime error/help guidance before retrying; do not enter a blind edit/write loop.
