# CogniRelay

<!-- mcp-name: io.github.stef-k/cognirelay -->

CogniRelay is a self-hosted continuity and collaboration substrate for autonomous agents with bounded, recoverable memory and explicit restart orientation. It provides a local FastAPI and MCP-facing service for agent orientation, retrieval, coordination, and recovery across context resets.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
python -I -m pip --isolated --disable-pip-version-check install --no-index --no-deps --no-cache-dir --upgrade \
  "pip @ https://files.pythonhosted.org/packages/3f/06/3e4420302363fbb71be3c7f35ce042f54bfca330e3e66b3a2e3c4cca579f/pip-26.2.2-py3-none-any.whl#sha256=fd7f4dfa8190e014013b9f08f3e973f4e4c26ffd179a204895533a7254813a70"
python -m pip install cognirelay
```

Update host CA certificates first. The pinned wheel authenticates the first
download even when the venv's initial pip has an older CA bundle. This changes
only the venv, including on Ubuntu-managed Python. For existing environments,
run the pinned bootstrap with that environment's Python before upgrading
CogniRelay. See the [TLS audit and recovery guide](https://stef-k.github.io/CogniRelay/python-tls-security.html).

## Run

Set `COGNIRELAY_REPO_ROOT` to a durable writable repository root before starting the service. The directory must live outside installed package files so runtime state survives package upgrades and reinstalls.

```bash
export COGNIRELAY_REPO_ROOT=/path/to/durable/cognirelay-state
cognirelay serve --host 127.0.0.1 --port 8080
```

This package starts a local Streamable HTTP server only; it does not provide stdio transport or a hosted default CogniRelay service.

## Agent assets

PyPI installs include the last-mile agent assets needed to integrate CogniRelay continuity hooks and the continuity-authoring skill. To inspect the installed assets:

```bash
cognirelay assets path
cognirelay assets list
```

To materialize them into a workspace, run:

```bash
cognirelay assets copy --to /path/to/workspace
```

The copy command writes `/path/to/workspace/agent-assets`. The full last-mile guide remains in the project documentation on GitHub Pages.

## Documentation

Documentation and release notes are available at:

https://stef-k.github.io/CogniRelay/

Project source is available at:

https://github.com/stef-k/CogniRelay
