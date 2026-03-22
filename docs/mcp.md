# MCP Guide

This document describes CogniRelay's MCP-facing surface and how it relates to the broader HTTP API.

## Scope

CogniRelay exposes two machine-oriented integration styles:

- HTTP-native discovery via `GET /v1/discovery`, `GET /v1/discovery/tools`, and `GET /v1/discovery/workflows`
- MCP-compatible JSON-RPC via `GET /.well-known/mcp.json` and `POST /v1/mcp`

The implementation describes the protocol as `mcp-compatible` and `mcp-like`, not as a claim of full MCP-spec coverage. In practice, the MCP bridge is designed around tool discovery and tool execution for autonomous clients.

## Bootstrap Flow

For an MCP-oriented client, the expected startup sequence is:

1. `GET /.well-known/mcp.json`
2. `POST /v1/mcp` with `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`
3. `POST /v1/mcp` with `{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`
4. `POST /v1/mcp` with `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`
5. `POST /v1/mcp` with `tools/call` requests as needed

The well-known descriptor advertises:

- endpoint: `/v1/mcp`
- protocol: JSON-RPC 2.0 over HTTP+JSON
- methods: `initialize`, `notifications/initialized`, `ping`, `tools/list`, `tools/call`
- auth: bearer token in `Authorization`

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

`tools/call` returns:

- `toolName`
- a simple text `content` entry
- `structuredContent` containing the underlying tool result

Clients should treat `structuredContent` as the authoritative machine-readable payload.

## Error Behavior

The bridge returns JSON-RPC errors for:

- invalid JSON-RPC requests
- unknown methods
- invalid parameters
- unauthorized and forbidden tool calls
- execution failures

This is documented at the protocol level by the implementation and should be preferred over inferring behavior from HTTP status codes alone.

## Recommendations

- Use MCP when your runtime already speaks JSON-RPC tool protocols.
- Use HTTP discovery endpoints when you want broader service introspection or simpler direct integration.
- Use `GET /v1/discovery` alongside MCP if you want startup guidance and workflow hints beyond the basic MCP descriptor.
- For a practical walkthrough of integration hook points and incremental adoption, see [Agent Onboarding](agent-onboarding.md).
