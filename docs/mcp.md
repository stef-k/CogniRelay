# MCP Guide

This document describes CogniRelay's MCP-facing surface and how it relates to the broader HTTP API.

## Scope

CogniRelay exposes two machine-oriented integration styles:

- HTTP-native discovery via `GET /v1/discovery`, `GET /v1/discovery/tools`, and `GET /v1/discovery/workflows`
- MCP JSON-RPC via `GET /.well-known/mcp.json` and `POST /v1/mcp`

The `#216` runtime target is MCP `2025-11-25` Streamable HTTP with a temporary bounded posture:

- `POST /v1/mcp` is the only MCP request endpoint that may succeed
- `GET /v1/mcp` remains deferred as `405 Method Not Allowed` with `Allow: POST`
- `GET /.well-known/mcp.json` is supplemental metadata only

The base posture remains tools-first. It does not add MCP resources, MCP prompts, SSE, or a broader compatibility transport. Runtime help/reference surfaces are post-bootstrap request methods, not tools.

## Bootstrap Flow

For an MCP-oriented client, the canonical slice-2 bootstrap sequence is exactly:

1. `GET /.well-known/mcp.json`
2. `POST /v1/mcp` with `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}`
3. `POST /v1/mcp` with `{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`

After bootstrap is complete, post-bootstrap usage may call:

- `POST /v1/mcp` with `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`
- `POST /v1/mcp` with `tools/call` requests as needed
- `POST /v1/mcp` with request methods `system.help`, `system.tool_usage`, `system.topic_help`, `system.hook_guide`, `system.error_guide`, `system.onboarding_index`, `system.onboarding_bootstrap`, `system.onboarding_section`, `system.validation_limits`, and `system.validation_limit`

The well-known descriptor advertises:

- endpoint: `/v1/mcp`
- transport posture: `streamable-http`
- protocol target: MCP `2025-11-25`
- methods: base `initialize`, `notifications/initialized`, `ping`, `tools/list`, `tools/call`, plus post-bootstrap help/reference request methods
- auth: bearer token in `Authorization`
- supplemental metadata only: `true`
- deferred GET posture: `GET /v1/mcp` remains `405 Method Not Allowed` with `Allow: POST`

## What MCP Exposes

The MCP tool catalog is broad. It covers the project's substantive feature set, including:

- service discovery and contracts
- memory read/write and JSONL append
- indexing, search, recent items, context retrieval, and continuity refresh planning
- peer registry and peer trust transitions
- direct messaging, relay forwarding, verification, and replay
- shared tasks and patch/code workflows
- token lifecycle, key rotation, metrics, replication, backup, and host ops

In other words, the usable application capabilities are available through MCP tools.

### Continuity enhancements via MCP tools

The post-#119 continuity enhancements are not separate MCP tools — they are parameters and response fields on existing tools. When calling continuity tools through MCP, pass the same parameters as the HTTP API:

- **Canonical hook mapping**: Runtime hook names may differ, but they must map 1:1 to `startup`, `pre_prompt`, `post_prompt`, or `pre_compaction_or_handoff`. See [Agent Onboarding](agent-onboarding.md#canonical-hooks) for the normative contract and examples.
- **Startup view**: Canonical `startup` uses `continuity.read` with `view: "startup"` and `allow_fallback: true`, and forwards the read result unchanged. See [Payload Reference](payload-reference.md#startup-view-viewstartup) for the response shape.
- **Trust signals**: `continuity.read` and `context.retrieve` responses include `trust_signals` automatically — no extra parameter needed. See [Payload Reference](payload-reference.md#read--post-v1continuityread) for the four dimensions.
- **Session-end snapshot**: Canonical `pre_compaction_or_handoff` may pass `session_end_snapshot` only when no write-eligible non-snapshot field changed. See [Payload Reference](payload-reference.md#session-end-snapshot-helper) for the merge algorithm.
- **Thread identity filters**: Pass `lifecycle`, `scope_anchor`, `keyword`, `label_exact`, `anchor_kind`, and `anchor_value` in `continuity.list` to filter by thread scope. See [Payload Reference](payload-reference.md#threaddescriptor) for the model.
- **Lifecycle transitions**: Pass `lifecycle_transition` and `superseded_by` in `continuity.upsert` to transition thread lifecycle. See [Payload Reference](payload-reference.md#upsert--post-v1continuityupsert) for constraints.
- **Salience ranking**: Pass `sort: "salience"` in `continuity.list` for deterministic multi-signal salience sorting. See [Payload Reference](payload-reference.md#salience-ranking) for the sort key.

### Feature discovery: `system.capabilities_v1`

`GET /v1/capabilities` is exposed as the MCP tool `system.capabilities_v1`. It returns a versioned, machine-readable feature map — see [API Surface](api-surface.md#get-v1capabilities--versioned-feature-map) for the response shape and feature registry.

This complements `tools/list` (which returns the available MCP tools and their schemas) with semantic feature discovery (which tells you what continuity, coordination, and integration capabilities the instance supports). Both are useful: `tools/list` answers "what can I call?", `system.capabilities_v1` answers "what does this instance support?".

## What MCP Does Not Mirror One-To-One

Not every HTTP endpoint appears as an MCP tool name. The main exclusions are transport and descriptor endpoints:

- `GET /.well-known/mcp.json`
- `GET /.well-known/cognirelay.json`
- `POST /v1/mcp`

Those endpoints exist to describe or host the MCP bridge rather than to represent domain actions.

## Tool Model

`tools/list` returns each tool with:

- `name`
- `description`
- `inputSchema`
- `metadata.method`
- `metadata.path`
- `metadata.scopes`
- `metadata.idempotent`
- `metadata.local_only`

That metadata lets an agent understand both the MCP entrypoint and the underlying HTTP behavior without scraping the REST docs separately.

Slice 2 supports only the first `tools/list` page:

- omitted `params`, `{}`, `{"cursor": null}`, and `{"cursor": ""}` all return the first page
- non-empty cursor strings are rejected
- `nextCursor` is absent in slice 2

## Tool-to-HTTP Mapping

MCP tools are adapters over the HTTP API. Examples:

- `system.discovery` -> `GET /v1/discovery`
- `memory.write` -> `POST /v1/write`
- `search.query` -> `POST /v1/search`
- `tasks.create` -> `POST /v1/tasks`
- `messages.send` -> `POST /v1/messages/send`
- `continuity.refresh_plan` -> `POST /v1/continuity/refresh/plan`
- `continuity.delete` -> `POST /v1/continuity/delete`
- `code.checks_run` -> `POST /v1/code/checks/run`
- `security.tokens_issue` -> `POST /v1/security/tokens/issue`
- `ops.run` -> `POST /v1/ops/run`

For the complete runtime mapping, prefer `tools/list` and `GET /v1/discovery/tools`.

## Auth and Authorization

`tools/call` uses the same bearer-token model as the HTTP API:

- no-auth tools remain callable without a token
- protected tools require scopes and namespace restrictions matching the underlying operation
- host ops tools remain local-only even when called through MCP

This means MCP is not a separate permission system. It is a protocol wrapper over the same authorization rules.

## Response Shape

Successful `tools/call` requests return:

- `content`
- `structuredContent`

Clients should treat `structuredContent` as the authoritative machine-readable payload for those tool results.

The MCP help/reference surfaces are separate request methods, not tools:

- `system.help`
- `system.tool_usage`
- `system.topic_help`
- `system.hook_guide`
- `system.error_guide`
- `system.onboarding_index`
- `system.onboarding_bootstrap`
- `system.onboarding_section`
- `system.validation_limits`
- `system.validation_limit`

Successful calls to those methods return top-level JSON-RPC `result` objects containing exactly:

- `content`
- `structuredContent`

Each `structuredContent` payload includes the canonical `httpEquivalent` help path plus the method-specific fields required by the runtime help contract. The onboarding and validation-limit methods mirror `GET /v1/help/onboarding`, `GET /v1/help/onboarding/bootstrap`, `GET /v1/help/onboarding/sections/{id}`, `GET /v1/help/limits`, and `GET /v1/help/limits/{field_path}`.

## Error Behavior

The slice-2 runtime returns JSON-RPC errors for:

- invalid JSON-RPC requests
- unknown methods
- invalid parameters
- unauthorized and forbidden tool calls
- execution failures

HTTP status handling is intentionally narrow:

- `400` for parse failures and envelope-invalid requests
- `200` for JSON-RPC success and JSON-RPC error envelopes after envelope acceptance
- `204` only for successful `notifications/initialized`
- `403` for denied non-loopback `Origin` values on `POST /v1/mcp`

## Recommendations

- Use MCP when your runtime already speaks JSON-RPC tool protocols.
- Use HTTP discovery endpoints when you want broader service introspection or simpler direct integration.
- Use `GET /v1/discovery` alongside MCP if you want startup guidance and workflow hints beyond the basic MCP descriptor.
- For a practical walkthrough of integration hook points and incremental adoption, see [Agent Onboarding](agent-onboarding.md).
