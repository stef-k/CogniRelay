# CogniRelay Documentation

CogniRelay is a self-hosted continuity and collaboration substrate for
autonomous agents with bounded, recoverable memory. It uses a local git
repository as durable state, exposes a machine-first FastAPI interface, stores
content as Markdown and JSON/JSONL, and keeps dependencies minimal.

CogniRelay is not a Git forge or a full orchestration framework. It is
infrastructure for memory, retrieval, messaging, coordination, and continuity
preservation across context-window resets. It is agent-agnostic: any runtime
that can call the HTTP or MCP surfaces can use it.

The default deployment model is one owner-agent per CogniRelay instance.
Collaborating peers can receive narrower API tokens and interact through
coordination surfaces without access to the owner's continuity capsules.

For the full product introduction, installation notes, and feature summary, see
the [README](../README.md).

## Documentation

- [System Overview](system-overview.md): implemented product shape, runtime
  model, and agent usage guidance.
- [Agent Onboarding](agent-onboarding.md): practical integration guide for
  cold-start and already-running agents.
- [API Surface](api-surface.md): currently implemented HTTP behavior grouped by
  domain.
- [MCP Guide](mcp.md): MCP bootstrap flow, request methods, and tool mapping.
- [Payload Reference](payload-reference.md): continuity capsule structure,
  request/response schemas, and field constraints.
- [CogniRelay Client](cognirelay-client.md): stdlib-only command-line tool for
  continuity read, upsert, and token hashing.
- [Reviewer Guide](reviewer-guide.md): system thesis, boundaries, recovery
  model, and authority limits.
- [External References and Case Studies](external-references.md): external
  experiments, usage notes, and scoped collaboration/evaluation references.

## Releases

- [Latest release notes: v1.4.1](releases/v1.4.1.md)
- [v1.4.0 release notes](releases/v1.4.0.md)
- [Changelog](../CHANGELOG.md)
- [GitHub releases](https://github.com/stef-k/CogniRelay/releases)
