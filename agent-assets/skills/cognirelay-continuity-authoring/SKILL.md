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

Before every CogniRelay mutation, consult the live runtime help contract for the exact write tool or route about to be called. This is mandatory for `continuity.upsert`, `continuity.patch`, `continuity.lifecycle`, `continuity.revalidate`, `continuity.archive`, `continuity.delete`, `schedule.create`, `schedule.update`, `schedule.acknowledge`, `schedule.retire`, `coordination.handoff_create`, and any hook mode that will mutate CogniRelay state.

For any mutation, first query exact tool usage:

```json
{"jsonrpc":"2.0","id":1,"method":"system.tool_usage","params":{"name":"<exact.mutation_tool>"}}
```

```http
GET /v1/help/tools/<exact.mutation_tool>
```

For continuity payloads, also query the runtime limits index and targeted limits for every bounded or schema-sensitive field being authored or changed:

```json
{"jsonrpc":"2.0","id":2,"method":"system.validation_limits","params":{}}
```

```json
{"jsonrpc":"2.0","id":3,"method":"system.validation_limit","params":{"field_path":"continuity.session_trajectory"}}
```

```http
GET /v1/help/limits
GET /v1/help/limits/continuity.session_trajectory
```

Use MCP when operating through the runtime tool protocol:

```json
{"jsonrpc":"2.0","id":10,"method":"system.tool_usage","params":{"name":"continuity.upsert"}}
```

```json
{"jsonrpc":"2.0","id":11,"method":"system.tool_usage","params":{"name":"continuity.patch"}}
```

```json
{"jsonrpc":"2.0","id":12,"method":"system.tool_usage","params":{"name":"schedule.create"}}
```

```json
{"jsonrpc":"2.0","id":13,"method":"system.tool_usage","params":{"name":"coordination.handoff_create"}}
```

Use HTTP when operating through REST:

```http
GET /v1/help/tools/continuity.upsert
GET /v1/help/tools/continuity.patch
GET /v1/help/tools/schedule.create
GET /v1/help/tools/coordination.handoff_create
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

If the runtime help lookup is unavailable, do not guess and do not mutate. Stop and report that runtime help is unavailable. A degraded write is allowed only with explicit user approval for that specific mutation; when approved, name the skipped help calls, run the shipped hook `facts` output and local `dry-run` when applicable, then treat the mutation result as authoritative. If a mutation fails, query `system.error_guide` or `GET /v1/help/errors/{code}` before retrying.

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
system.validation_limits()
system.validation_limit(field_path="continuity.stance_summary")
system.validation_limit(field_path="continuity.active_concerns")
system.validation_limit(field_path="continuity.session_trajectory")
system.validation_limit(field_path="continuity.capsule_serialized_utf8")
repeat system.validation_limit for every bounded or schema-sensitive field in the candidate payload
agent-assets/hooks/cognirelay_continuity_save_hook.py template
agent authors payload
agent-assets/hooks/cognirelay_continuity_save_hook.py dry-run --input payload.json
agent-assets/hooks/cognirelay_continuity_save_hook.py write --input payload.json
agent-assets/hooks/cognirelay_continuity_save_hook.py readback
```

Example pre-compaction flow:

```text
GET /v1/help/tools/continuity.upsert
GET /v1/help/limits
GET /v1/help/limits/continuity.open_loops
GET /v1/help/limits/continuity.top_priorities
GET /v1/help/limits/continuity.active_constraints
GET /v1/help/limits/continuity.stance_summary
GET /v1/help/limits/continuity.capsule_serialized_utf8
repeat GET /v1/help/limits/{field_path} for every bounded or schema-sensitive field in the candidate payload
author compact payload from agent judgment
dry-run
write
doctor
```

Example patch flow:

```text
system.tool_usage(name="continuity.patch")
system.validation_limits()
system.validation_limit(field_path="patch.operations")
system.validation_limit(field_path="patch.target.continuity.open_loops")
system.validation_limit(field_path="continuity.patch.updated_at")
author patch operations from agent judgment
call continuity.patch
read back the patched subject
```

Example lifecycle flow:

```text
system.tool_usage(name="continuity.lifecycle")
author lifecycle transition only when the existing thread or task identity should move state
call continuity.lifecycle
read back the subject lifecycle
```

Example handoff flow:

```text
system.tool_usage(name="continuity.upsert")
system.validation_limits()
targeted continuity limits for the local savepoint payload
write/readback local continuity first
system.tool_usage(name="coordination.handoff_create")
call coordination.handoff_create only after the local continuity step succeeds
```

## Scheduling

Agents may create one-shot reminders or task nudges through `schedule.create` or `POST /v1/schedule/items` only when the user or an explicit work plan needs future follow-up.

- Before any schedule mutation, call `system.tool_usage` for the exact schedule tool or `GET /v1/help/tools/{name}` for the HTTP equivalent.
- For schedule creation, query `schedule.create`; for changes, query `schedule.update`; for completion or acknowledgement, query `schedule.acknowledge`; for no-longer-relevant items, query `schedule.retire`.
- Use UTC timestamps only.
- Use `kind="reminder"` for general follow-up.
- Use `kind="task_nudge"` only when linked to a task, thread, or subject.
- Scheduling does not execute work; it surfaces future orientation through `schedule_context`, `schedule.list`, and `/ui/schedule`.
- Do not auto-create reminders from every open loop.
- Do not infer due dates.
- Do not acknowledge or retire schedule items unless the work was actually handled or made irrelevant.

## Write Discipline

Before any mutation, verify through runtime help that the operation is allowed, bounded, durable, and agent-authored. Reject prompt dumping, transcript dumping, copied retrieval text, and automatic semantic inference. Treat warnings and degraded trust signals as operational input for the agent, not as hook-authored meaning. If a write fails, consult runtime error/help guidance before retrying; do not enter a blind edit/write loop.
