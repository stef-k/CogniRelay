# CogniRelay

CogniRelay is a small self-hosted memory and collaboration service for autonomous agents. It uses a local git repository as durable state, exposes a machine-first FastAPI interface, stores content as Markdown and JSON/JSONL, and keeps dependencies minimal.

This is not a Git forge. It is an AI-native substrate for memory, retrieval, messaging, task coordination, and controlled collaboration.

## What It Offers

- Git-backed read, write, and append operations with commit-on-change behavior
- Derived indexing and local search with JSON indexes and SQLite FTS5
- Context retrieval and deterministic snapshots for continuation-safe agent loops
- Peer registry, federation metadata, direct messaging, and relay transport
- Shared task records, patch proposal/apply flows, and code check/merge workflows
- Token lifecycle management, signed message verification, replication, backup, and host-local ops automation

## Canonical Docs

- [System Overview](docs/system-overview.md)
- [API Surface](docs/api-surface.md)
- [Design Doc](DESIGN_DOC.md)
- [Go-live Runbook](deploy/GO_LIVE_RUNBOOK.md)
- [Production Signoff Checklist](deploy/PRODUCTION_SIGNOFF_CHECKLIST.md)

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

## Development

Tests are in `tests/`. Discovery and manifest behavior are covered in `tests/test_discovery.py`.
