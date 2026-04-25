# Agent Onboarding

This is the single concentrated bootstrap and operating manual for agents using CogniRelay. Read this first to operate the shipped system correctly without preloading the full docs corpus; when runtime help is available, use `GET /v1/help/onboarding`, `GET /v1/help/onboarding/bootstrap`, `GET /v1/help/onboarding/sections/{id}`, `system.onboarding_index`, `system.onboarding_bootstrap`, and `system.onboarding_section` as the repo-doc-free bounded onboarding path.

## What CogniRelay Is For

CogniRelay is a bounded continuity and orientation system for agents, not a transcript archive, general notes database, or memory dump. Use it to recover orientation after startup, reset, or compaction; retrieve bounded context for current work; track tasks when explicit work-item state matters; preserve durable user or peer preferences separately from thread/task state; and act on explicit trust and degradation signals.

- Preserve orientation state that helps the next work step: priorities, loops, constraints, stance, decisions, rationale, task state, and durable preferences.
- Retrieve enough bounded context to resume useful work, not enough to reconstruct a full conversation history.
- Keep continuity scoped by subject: use threads for bounded streams of work, tasks for tracked deliverables, and `stable_preferences` for durable cross-session instructions.
- Treat warnings, fallback use, stale results, and degraded trust as operational signals that change how confidently you act.
- Do not persist operational exhaust: prompts, transcripts, shell output, tool chatter, arbitrary search results, or copied snippets stored for convenience.

## Minimum Startup Path

The minimum shipped bootstrap path is a read first, then bounded retrieval only when the first work step needs more context. Do not turn startup into a discovery tour or write merely because an agent resumed.

1. Call `continuity.read` / `POST /v1/continuity/read` with `view="startup"` and `allow_fallback=true`.
2. Consume the returned startup-oriented continuity result as the first orientation input, including top-level `graph_summary` when present.
3. Check `schedule_context.due.items` in startup output when present. Due reminders are read-only orientation data; they are not executed or auto-acknowledged.
4. Optionally call `context.retrieve` / `POST /v1/context/retrieve` when the first work step needs bounded fresh context beyond startup orientation; it includes `bundle.graph_context` by default unless `continuity_mode="off"` suppresses graph derivation and includes scoped `schedule_context` when the request has a primary subject or continuity selectors.

If the startup result has warnings, fallback state, stale continuity, trimming, or degraded trust, use the best returned result already received and verify critical assumptions only against the shipped help/reference lookup surfaces named in this manual and the current task inputs already in hand.

## Canonical Hooks

Map runtime-specific hook names to these four canonical hooks. `startup` and `pre_prompt` are read-oriented; `post_prompt` and `pre_compaction_or_handoff` are write-eligible only when write-eligible continuity fields changed.

| Hook | Operating route | Write discipline |
| --- | --- | --- |
| `startup` | Read with `POST /v1/continuity/read` / `continuity.read`, using `view="startup"` and `allow_fallback=true`. | Read-only. Do not write to mark resume. |
| `pre_prompt` | Retrieve with `POST /v1/context/retrieve` / `context.retrieve`. | Read-only. Do not persist prompt text, retrieved snippets, graph context, or transcript material. |
| `post_prompt` | Write/update only through `POST /v1/continuity/upsert` / `continuity.upsert`. | Write only when write-eligible fields changed. Skip otherwise. |
| `pre_compaction_or_handoff` | Write/update only through `POST /v1/continuity/upsert` / `continuity.upsert`. | Write only when write-eligible fields changed before context loss. Skip otherwise. |

- At onboarding level, `post_prompt` and `pre_compaction_or_handoff` use only the continuity write/update route: HTTP `POST /v1/continuity/upsert` and MCP `continuity.upsert`.
- Shipped narrow update variants such as `POST /v1/continuity/patch` / `continuity.patch` and lifecycle-specialized surfaces are deeper specialized follow-ons, not canonical hook routes in onboarding.
- Prompt text, response text, transcripts, raw tool chatter, shell output, and copied retrieval snippets must not be written into continuity at any hook.
- Graph orientation is derived response data only. Read `graph_summary.warnings` or `bundle.graph_context.warnings` for graph-local degradation such as `graph_source_denied` or `graph_truncated`; non-startup `continuity.read` remains graph-free.
- Reminder orientation is also read-only. `schedule_context` appears in startup/context orientation for matching thread, task, or subject scopes; use `schedule.list` for manual inspection, `schedule.acknowledge` with `status="done"` for completion, or `schedule.retire` when a reminder is no longer relevant.
- Use deeper references only for exact hook matrix details after this mapping is clear.

## How To Ask CogniRelay For Help

Use built-in help/reference lookup for exact route, tool, topic, hook, and error guidance instead of guessing. Use HTTP help surfaces as the runtime lookup path, MCP help/reference methods as the JSON-RPC lookup path, and `GET /v1/capabilities` only when instance support is uncertain.

- `GET /v1/help`: use this as the top-level help index to discover the shipped machine-facing help surfaces.
- `GET /v1/help/tools/{name}`: use this to get exact usage guidance for one shipped tool or route-level operation.
- `GET /v1/help/topics/{id}`: use this to get exact bounded guidance for one named onboarding or continuity topic.
- `GET /v1/help/hooks`: use this to review the canonical startup, prompt, persistence, and handoff hook guidance.
- `GET /v1/help/errors/{code}`: use this to look up exact remediation guidance for one shipped MCP error code.
- `GET /v1/help/onboarding`, `GET /v1/help/onboarding/bootstrap`, `GET /v1/help/onboarding/sections/{id}`: use these for bounded runtime onboarding lookup instead of loading this full manual when the runtime surface is available.
- `GET /v1/help/limits`, `GET /v1/help/limits/{field_path}`: use these after ordinary continuity write, patch, session-snapshot, or retrieval validation-limit failures instead of loading `docs/payload-reference.md` for routine limit recovery.
- `system.help`: use this as the MCP/JSON-RPC help index equivalent of `GET /v1/help`.
- `system.tool_usage`: use this as the MCP/JSON-RPC exact-usage lookup for one shipped tool.
- `system.topic_help`: use this as the MCP/JSON-RPC exact-topic lookup for one named onboarding or continuity topic.
- `system.hook_guide`: use this as the MCP/JSON-RPC hook-guidance lookup for startup, prompt, persistence, and handoff rules.
- `system.error_guide`: use this as the MCP/JSON-RPC exact remediation lookup for one shipped MCP error code.
- `system.onboarding_index`, `system.onboarding_bootstrap`, `system.onboarding_section`, `system.validation_limits`, `system.validation_limit`: use these as the MCP/JSON-RPC equivalents of the bounded onboarding and validation-limit help routes.

## Bootstrap-Critical Limits and Routing Rules

Use this closed routing list for ordinary operation. Prefer HTTP identifiers at onboarding level unless your runtime uses MCP; the MCP identifiers below are transport-equivalent alternates except where a route is explicitly described as a specialized follow-on.

| Common goal | Exact HTTP identifier | Exact MCP identifier | Preferred onboarding-level route | Status of the other route |
| --- | --- | --- | --- | --- |
| startup/orientation recovery | `POST /v1/continuity/read` | `continuity.read` | `POST /v1/continuity/read` with `view="startup"` and `allow_fallback=true` | `continuity.read` is the transport-equivalent alternate for runtimes using MCP |
| bounded retrieval | `POST /v1/context/retrieve` | `context.retrieve` | `POST /v1/context/retrieve` | `context.retrieve` is the transport-equivalent alternate for runtimes using MCP |
| continuity write/update | `POST /v1/continuity/upsert` | `continuity.upsert` | `POST /v1/continuity/upsert` | `continuity.upsert` is the transport-equivalent alternate for runtimes using MCP; shipped narrow update variants `POST /v1/continuity/patch` / `continuity.patch` and `POST /v1/continuity/lifecycle` / `continuity.lifecycle` are specialized follow-ons, not the preferred onboarding-level route |
| reminder inspection | `GET /v1/schedule/items` | `schedule.list` | `GET /v1/schedule/items?due=true` for explicit due inspection | Due reminders also arrive through startup/context `schedule_context`; no SSE, recurrence, UI schedule page, callback, or background scheduler exists |
| task lookup | `GET /v1/tasks/query` | `tasks.query` | `GET /v1/tasks/query` | `tasks.query` is the transport-equivalent alternate for runtimes using MCP |
| help lookup | `GET /v1/help` | `system.help` | `GET /v1/help` | `system.help` is the transport-equivalent alternate for runtimes using MCP; exact sub-lookups stay `GET /v1/help/tools/{name}`, `GET /v1/help/topics/{id}`, `GET /v1/help/hooks`, `GET /v1/help/errors/{code}` and `system.tool_usage`, `system.topic_help`, `system.hook_guide`, `system.error_guide` |

Task creation and task updates are described at onboarding level only through the continuity write/update path: `POST /v1/continuity/upsert` on HTTP and `continuity.upsert` on MCP. Any specialized task-write behavior belongs in deeper reference/help lookup, not in bootstrap routing.

For exact limits beyond the compact bootstrap-critical names below, query `GET /v1/help/limits/{field_path}` or `system.validation_limit` with one of the field paths from the runtime limits index.

Field-selection rules:

- Use `stable_preferences` only for explicit, durable standing instructions or preferences worth carrying across sessions and work threads.
- Use thread continuity for one bounded stream of ongoing context; use a task artifact when a deliverable needs explicit status, completion state, ownership, or blocking relationships.
- Use `related_documents` for a bounded set of repo-relative documents that should be pulled into retrieval context deterministically.
- Use `negative_decisions` for concise deliberate non-actions or rejected paths; use `rationale_entries` for structured why/assumptions/tensions/decision reasoning.

Current shipped bootstrap-critical limits/caps agents routinely author against:

- `continuity.top_priorities = 8` entries; each item is bounded to `160` chars.
- `continuity.open_loops = 8` entries; each item is bounded to `160` chars.
- `continuity.active_constraints = 8` entries; each item is bounded to `160` chars.
- `continuity.related_documents = 8` entries.
- `continuity.negative_decisions = 4` entries; decision and rationale fields are bounded.
- `continuity.rationale_entries = 6` entries; tag, summary, reasoning, alternatives, and dependency fields are bounded.
- `continuity.stance_summary = 240` chars.
- Native retrieval default budget (`max_tokens_estimate`) = `12000`.
- Multi-capsule continuity retrieval cap (`continuity_max_capsules`) = `4`.
- Continuity capsule serialized write cap = `20 KB`.
- Schedule orientation caps: due items default to `10`, upcoming items default to `5`, and upcoming window defaults to `72` hours. These are response caps for reminders, not continuity capsule storage.

## Operational Workflow Rules

These rules define what to persist and how to select the right continuity anchor. Keep every write bounded, explicit, and useful for future orientation.

- `stable_preferences`: use only for durable standing instructions or preferences that should survive across sessions and across work threads. Treat them as user/peer-level continuity, not thread/task-level working state. Do not store `stable_preferences` on thread or task capsules. Do not persist inferred behavior, one-off choices, temporary tactics, or unconfirmed guesses as `stable_preferences`. Persist only when the preference is explicit, durable, and worth carrying forward.
- Threads: use a thread when the work is one bounded stream of ongoing context that needs continuity over time. A thread is the default continuity anchor for one topic, issue, case, incident, feature, or conversation stream. Do not collapse unrelated work into one giant thread. Retrieve by thread when resuming or continuing the same bounded stream of work.
- Tasks: create a task when there is a bounded deliverable, tracked work item, or dependency-managed unit of work that needs explicit status over time. Use tasks for multi-session work when progress, completion state, ownership, or blocking relationships matter. A task may exist under a thread when it is one actionable unit inside a broader stream. Do not rely on thread continuity alone for large multi-session work that needs explicit status tracking.
- `related_documents`: attach these when a bounded set of repo-relative documents is repeatedly relevant to the current thread or task and should be pulled into retrieval context deterministically. Use them for durable document pointers, not transient snippets or arbitrary search output. Keep them bounded and relevant.
- `blocked_by[]`: use this on tasks when the task cannot proceed until one or more other concrete tasks complete. Use it for explicit task dependencies, not vague risks, concerns, informational prerequisites, soft uncertainty, or general caution.
- Supersede vs mutate: mutate existing thread/task continuity when the same subject identity still represents the same ongoing work. Supersede when the old thread/task/rationale entry should remain historically true but a new successor now carries the active meaning. Do not repurpose an old thread or task `subject_id` to mean a new stream of work. For lifecycle replacement, use supersession. For structured rationale history, retain the old entry and mark it superseded rather than silently rewriting history away.
- Thread vs task retrieval: retrieve by thread when broad continuity for the current line of work should dominate. Retrieve by task when one tracked work item is the immediate execution anchor. When both matter, use the thread + task pattern: thread carries broad continuity, task carries the current actionable slice.
- `negative_decisions` vs `rationale_entries`: use `negative_decisions` for compact records of "we are not doing X"; use `rationale_entries` for structured reasoning, assumptions, tensions, and decision context. Do not store long-form reasoning, transcript-like analysis, or raw notes in `negative_decisions`. Do not use `rationale_entries` as a transcript dump, prompt/response log, notes bucket, or shell/tool output archive. Misuse degrades retrieval quality and weakens continuity signal.
- Distinguishing example: `negative_decisions`: "Do not add a new task-write route for onboarding." `rationale_entries`: "Onboarding keeps task creation/update routed through continuity upsert because bootstrap guidance must stay transport-simple and avoid specialized write mechanics."
- Never persist into continuity: raw prompts, response transcripts, tool chatter, shell output dumps, retrieved snippets copied verbatim for storage convenience, broad conversation logs, arbitrary search results, temporary noise that does not belong in bounded orientation state, secrets or credentials, or anything the current contract treats as operational exhaust rather than orientation.

## Retrieval Mental Model

Continuity is bounded, not lossless. Retrieval may combine thread/task continuity with bounded search or context results, and selectors shape which continuity state is returned.

- Use retrieval to resume useful work, not to recreate full transcript history.
- Use thread selectors when the broader stream of continuity matters.
- Use task selectors when the immediate next step is task-specific and task continuity should dominate.
- Use a thread + task pattern when the task is one actionable unit inside a broader stream: retrieve enough thread continuity to preserve direction, and task continuity to drive execution.
- Use `related_documents` as deterministic document proximity for key repo-relative docs that repeatedly matter to a thread or task.
- Keep retrieval bounded. If you need exact payload, endpoint, MCP, hook, or error details, use the built-in help/reference surfaces named here or the linked reference docs.

## Trust and Degradation Rules

Warnings and degraded retrieval are operational signals, not noise. Degrade safely: use the best available returned continuity/context, reduce certainty, and avoid treating missing or degraded context as known.

- If retrieval reports warnings, fallback use, stale continuity, trimming, missing capsules, or degraded trust, proceed cautiously.
- Do not invent facts to fill continuity gaps.
- Verify critical assumptions only against the shipped help/reference lookup surfaces named in this manual and the current task inputs already in hand, including the current user request and any attached task documents supplied with that task input.
- Do not introduce another onboarding-level fallback route for degraded startup or retrieval handling.
- If startup summary construction degrades, consume the best returned startup result already received, then perform bounded exact lookup only against the closed source set above.
- If a write/update route returns warnings, preserve existing orientation semantics and avoid broad rewrite attempts; write only the bounded correction needed for the same subject identity.

## Minimal Examples

These examples are transport-neutral by default and name the preferred onboarding-level route only where the route matters operationally.

### Thread-Only Workflow

A user asks for several short follow-ups about one incident investigation. The work is one bounded stream and does not need explicit status tracking.

- Anchor continuity to thread `incident-cache-timeouts`.
- At startup, call `POST /v1/continuity/read` with `view="startup"` and `allow_fallback=true`.
- Before a major next step, retrieve by thread with `POST /v1/context/retrieve`.
- Persist only changed thread orientation such as current hypothesis, active constraints, open loops, or a concise negative decision.
- Do not create a task merely because the thread spans more than one prompt.

### Thread + Task Workflow

A broader feature thread has one concrete deliverable that needs progress and blocking state.

- Thread `feature-agent-onboarding` carries broad continuity: goal, constraints, standing decisions, and open concerns.
- Task `rewrite-onboarding-doc` carries the actionable work item: status, owner, next step, `blocked_by[]`, and task-specific rationale.
- Retrieve by thread when reorienting to the feature; retrieve by task when the next step is executing or reviewing the rewrite.
- Create or update the task through `POST /v1/continuity/upsert` / `continuity.upsert` at onboarding level.
- Do not hide task progress only in thread prose when explicit completion state matters.

### Task + `related_documents` Workflow

A task depends repeatedly on a small set of repo docs.

- Task `update-continuity-guidance` uses `related_documents` such as `docs/payload-reference.md`, `docs/api-surface.md`, and `docs/mcp.md`.
- Retrieval for that task can pull those bounded repo-relative documents close without broad search or arbitrary snippet storage.
- Keep the list to durable pointers that repeatedly matter; remove documents that stop shaping the task.
- Do not copy sections of those docs into continuity for convenience.

### Resume After Reset

An agent restarts after context loss and must resume the next work step without guessing.

- First call `POST /v1/continuity/read` with `view="startup"` and `allow_fallback=true`.
- Consume the returned startup result as the first orientation input, including warnings and trust signals.
- If the next step needs bounded current context, call `POST /v1/context/retrieve` with the relevant thread or task selector.
- If retrieval is degraded or warning-bearing, use the best returned context already received and verify only against the named help/reference surfaces plus the current task inputs already in hand.
- Do not fabricate missing prior decisions, hidden task state, or unstated preferences.

## Anti-Patterns

These are operational errors. Apply the correction directly instead of broadening the continuity record.

- One giant thread for everything: split unrelated topics into separate thread subjects so retrieval remains bounded and meaningful.
- Persisting prompts, transcripts, or tool chatter into continuity: store only compact orientation state; leave raw interaction logs and shell output out.
- Guessing through degraded retrieval: acknowledge warnings, lower certainty, and verify only against the closed help/reference and current-input source set.
- Treating inferred behavior as `stable_preferences`: persist a preference only when it is explicit, durable, and useful across future work.
- Ignoring tasks on large multi-session work: create a task when status, ownership, completion state, or dependencies must survive across sessions.
- Using `negative_decisions` for long reasoning: keep the rejection compact and put structured why/assumptions/tensions in `rationale_entries`.
- Using `rationale_entries` as a notes bucket: store structured reasoning only, not transcripts, raw notes, or tool output.
- Dumping broad `related_documents`: keep only bounded repo-relative documents that repeatedly shape the current thread or task.
- Encoding vague caution in `blocked_by[]`: use explicit task dependencies only.
- Repurposing an old `subject_id`: supersede when the old subject remains historically true but a successor now carries the active meaning.
- Do not request the full onboarding document or full payload schema by default; use section and field-specific runtime lookup.

## References / Where To Look Next

Use these references for exact lookup after this manual determines the right operating path. Do not preload them all for normal startup.

- `docs/payload-reference.md`: exact payload fields, bounds beyond the bootstrap-critical limits above, and schema details.
- `docs/api-surface.md`: exact HTTP route contracts.
- `docs/mcp.md`: exact MCP/JSON-RPC method contracts.
- `docs/system-overview.md`: system-level orientation after the bootstrap path is understood.
- `GET /v1/help`, `GET /v1/help/tools/{name}`, `GET /v1/help/topics/{id}`, `GET /v1/help/hooks`, `GET /v1/help/errors/{code}`: built-in runtime help lookup aids.
- `GET /v1/help/onboarding`, `GET /v1/help/onboarding/bootstrap`, `GET /v1/help/onboarding/sections/{id}`, `GET /v1/help/limits`, `GET /v1/help/limits/{field_path}`: bounded runtime onboarding and validation-limit lookup aids.
- `system.help`, `system.tool_usage`, `system.topic_help`, `system.hook_guide`, `system.error_guide`: built-in MCP/request help lookup aids.
- `system.onboarding_index`, `system.onboarding_bootstrap`, `system.onboarding_section`, `system.validation_limits`, `system.validation_limit`: built-in MCP/request onboarding and validation-limit lookup aids.
