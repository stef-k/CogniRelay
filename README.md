# CogniRelay

CogniRelay is a small self-hosted memory and collaboration service for autonomous agents. It uses a local git repository as durable state, exposes a machine-first FastAPI interface, stores content as Markdown and JSON/JSONL, and keeps dependencies minimal.

This is not a Git forge. It is an AI-native substrate for memory, retrieval, messaging, task coordination, and controlled collaboration.

The current implementation should be read as a bounded orientation-preservation substrate, not as a claim of perfect persistence across context boundaries.

## What It Offers

- Git-backed read, write, and append operations with commit-on-change behavior
- Derived indexing and local search with JSON indexes and SQLite FTS5
- Context retrieval, continuity capsules, and deterministic snapshots for continuation-safe agent loops
- Peer registry, federation metadata, direct messaging, and relay transport
- Shared task records, patch proposal/apply flows, and code check/merge workflows
- Token lifecycle management, signed message verification, replication, backup, and host-local ops automation

## Canonical Docs

- [Reviewer Guide](docs/reviewer-guide.md): system thesis, boundaries, recovery model, and authority limits
- [System Overview](docs/system-overview.md): implemented product shape and agent usage guidance
- [API Surface](docs/api-surface.md): currently implemented HTTP behavior grouped by domain
- [MCP Guide](docs/mcp.md): MCP bootstrap flow and tool mapping
- [Design Doc](DESIGN_DOC.md): architecture rationale and background framing
- [Go-live Runbook](deploy/GO_LIVE_RUNBOOK.md): operator deployment and go-live workflow
- [Production Signoff Checklist](deploy/PRODUCTION_SIGNOFF_CHECKLIST.md): production verification and data-safety checks

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --host 127.0.0.1 --port 8080 --reload
```

If you want git history under `data_repo/` and it is not already initialized:

```bash
cd data_repo
git init
```

For non-local exposure, prefer file-based peer tokens in `data_repo/config/peer_tokens.json` instead of the plaintext development token in `.env`.

## Runtime Shape

- API framework: FastAPI
- Storage model: git-backed repo plus Markdown and JSON/JSONL files
- Search layer: stdlib `sqlite3` FTS5 with JSON-index fallback
- Auth model: bearer tokens with scopes and split read/write namespace restrictions
- Machine discoverability: `/v1/manifest`, `/v1/discovery/*`, and `POST /v1/mcp`

For agent integration details, including the MCP bootstrap flow and tool mapping, see [docs/mcp.md](docs/mcp.md).

## Development

Tests are in `tests/`. Discovery and manifest behavior are covered in `tests/test_discovery.py`.

Install development-only tooling with:

```bash
pip install -r requirements-dev.txt
```

Local quality commands:

```bash
./.venv/bin/python -m unittest discover -s tests -v
./.venv/bin/python -m ruff check app tests tools_hash_token.py
```
