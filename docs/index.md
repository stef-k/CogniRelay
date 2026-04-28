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
the [README](https://github.com/stef-k/CogniRelay/blob/main/README.md).

## Installation Paths

Source and GitHub release installs use a virtual environment, `pip install -r
requirements.txt`, and `python -m uvicorn app.main:app --host 127.0.0.1 --port
8080`.

PyPI installs use `pip install cognirelay` and `cognirelay serve --host
127.0.0.1 --port 8080`. Set `COGNIRELAY_REPO_ROOT` to a durable writable
runtime-state directory outside `site-packages`; the default `./data_repo` is
only for local/manual development.

PyPI installs also include the last-mile agent assets under the installed
`cognirelay/agent_assets` package-data directory. Use `cognirelay assets path`
to print that installed path, `cognirelay assets list` to inspect the bundled
allowlist, or `cognirelay assets copy --to <dir>` to write `<dir>/agent-assets`
for local agent configuration.

Wheel installs do not bundle the full source documentation in this slice. The
`/ui/docs` page may show degraded or unavailable doc entries unless
`COGNIRELAY_DOCS_SOURCE_ROOT` points at a source checkout.

Production deployments can use the templates under `deploy/` with either a
source checkout or an installed package. Docker is not the default deployment
path.

## Documentation

- [System Overview](system-overview.md): implemented product shape, runtime
  model, and agent usage guidance.
- [Agent Onboarding](agent-onboarding.md): practical integration guide for
  cold-start and already-running agents.
- [Last-mile Adapter Kit](../agent-assets/README.md): copyable skill and hook
  assets for agent-authored continuity integration.
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

- [Latest release notes: v1.4.10](releases/v1.4.10.md)
- [v1.4.9 release notes](releases/v1.4.9.md)
- [v1.4.8 release notes](releases/v1.4.8.md)
- [v1.4.7 release notes](releases/v1.4.7.md)
- [v1.4.6 release notes](releases/v1.4.6.md)
- [v1.4.5 release notes](releases/v1.4.5.md)
- [v1.4.4 release notes](releases/v1.4.4.md)
- [v1.4.3 release notes](releases/v1.4.3.md)
- [v1.4.2 release notes](releases/v1.4.2.md)
- [v1.4.1 release notes](releases/v1.4.1.md)
- [v1.4.0 release notes](releases/v1.4.0.md)
- [Changelog](https://github.com/stef-k/CogniRelay/blob/main/CHANGELOG.md)
- [GitHub releases](https://github.com/stef-k/CogniRelay/releases)
