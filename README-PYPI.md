# CogniRelay

<!-- mcp-name: io.github.stef-k/cognirelay -->

CogniRelay is a self-hosted continuity and collaboration substrate for autonomous agents. It provides a local FastAPI and MCP-facing service for bounded agent orientation, retrieval, coordination, and recovery across context resets.

## Install

```bash
pip install cognirelay
```

## Run

Set `COGNIRELAY_REPO_ROOT` to a durable writable repository root before starting the service. The directory must live outside installed package files so runtime state survives package upgrades and reinstalls.

```bash
export COGNIRELAY_REPO_ROOT=/path/to/durable/cognirelay-state
cognirelay serve --host 127.0.0.1 --port 8080
```

This package starts a local Streamable HTTP server only; it does not provide stdio transport or a hosted default CogniRelay service.

## Documentation

Project source, full documentation, and release notes are available at:

https://github.com/stef-k/CogniRelay
