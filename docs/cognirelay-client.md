# CogniRelay CLI Client

`tools/cognirelay_client.py` is a single-file, stdlib-only command-line client for CogniRelay's continuity endpoints. It exists so that agents and operators can read, write, and prepare continuity data without a third-party HTTP library or a full SDK.

The client is a thin transport tool. It does not validate capsule contents, inject fields, or make decisions about what to persist — that remains the agent's responsibility. It sends what you give it, returns what the server gives back, and surfaces errors through exit codes and stderr.

## Requirements

- Python 3.12+
- No third-party dependencies (uses `urllib.request`, `argparse`, `hashlib` from stdlib)

## Subcommands

The client exposes three subcommands:

| Subcommand | Purpose |
|---|---|
| `read` | Read a continuity capsule |
| `upsert` | Write or update a continuity capsule |
| `token hash` | Compute the SHA-256 hex digest of a token (for config files) |

Running with no subcommand prints usage to stderr and exits 2.

## Connection Arguments

`read` and `upsert` share these arguments (not present on `token hash`):

| Argument | Default | Notes |
|---|---|---|
| `--base-url` | `COGNIRELAY_BASE_URL` env, else required | One trailing slash stripped before use |
| `--token` | — | Raw bearer token (visible in process listings; prefer `--token-file` or `--token-env` in production) |
| `--token-file` | — | Path to file containing token (stripped of trailing whitespace) |
| `--token-env` | — | Name of env var containing token (stripped of trailing whitespace) |
| `--timeout` | `30.0` | HTTP timeout in seconds |

### Token resolution order

The client resolves the bearer token using the first non-empty source in this order:

1. `--token` (explicit value on command line)
2. `--token-file` (read file, strip trailing whitespace)
3. `--token-env` (read the named env var)
4. `COGNIRELAY_TOKEN` env var (implicit fallback)

If none resolve, the client exits 3.

## Reading a Capsule

```bash
python tools/cognirelay_client.py read \
  --base-url http://localhost:8080 \
  --token-file /run/secrets/agent_token \
  --subject-kind user \
  --subject-id agent-1 \
  --format startup
```

`--subject-kind` accepts: `user`, `peer`, `thread`, `task`. This calls `POST /v1/continuity/read` with `allow_fallback: true` and prints the response.

### Output formats

- `--format json` (default): pretty-printed JSON response body
- `--format startup`: compact section-based text showing source state, recovery warnings, top priorities, active constraints, open loops, negative decisions, and session trajectory

Use `--output <path>` to write to a file instead of stdout.

### Startup format example

```
=== Source State ===
active

=== Recovery Warnings ===
(none)

=== Top Priorities ===
- Complete authentication refactor
- Update deployment runbook

=== Active Constraints ===
- No breaking API changes before v2

=== Open Loops ===
(none)

=== Negative Decisions ===
- No caching layer: adds complexity without current need

=== Session Trajectory ===
- Reviewed PR #42
- Started auth module extraction
```

If there is no capsule, only Source State and Recovery Warnings are printed, followed by `(no capsule available)`.

## Upserting a Capsule

```bash
python tools/cognirelay_client.py upsert \
  --base-url http://localhost:8080 \
  --token-file /run/secrets/agent_token \
  --input capsule.json
```

The JSON file is the complete `POST /v1/continuity/upsert` request body (must include `subject_kind`, `subject_id`, `capsule`). The client sends it verbatim — no field injection or validation.

Use `--stdin` instead of `--input` to pipe JSON from another process. Exactly one of the two is required. Payloads over 256 KiB are rejected client-side.

## Hashing a Token

```bash
python tools/cognirelay_client.py token hash --value "my-secret-token"
```

Prints the lowercase hex SHA-256 digest to stdout. Use `--file` or `--env` instead of `--value` to read the token from a file or environment variable. Exactly one source is required.

This produces the same SHA-256 hex digest as the existing `tools_hash_token.py` for the same input string, but uses named flags (`--value`/`--file`/`--env`) rather than a positional argument. Either tool can be used to generate token hashes for `peer_tokens.json` and other config files.

## Exit Codes

| Code | Meaning |
|---|---|
| 0 | Success (including degraded/fallback reads) |
| 1 | HTTP error (4xx/5xx) — stderr shows `Error: HTTP {code}: {body}` |
| 2 | Usage or argument error |
| 3 | Token resolution failed (no source, unreadable `--token-file`, or empty token file) |
| 4 | Connection or network error (timeout, DNS, refused) |
| 5 | Response parse error (non-JSON body) |
| 6 | Token source unreadable (`token hash` only — file not found, env var unset) |

## Environment Variables

| Variable | Used by | Purpose |
|---|---|---|
| `COGNIRELAY_BASE_URL` | `read`, `upsert` | Default for `--base-url` |
| `COGNIRELAY_TOKEN` | `read`, `upsert` | Implicit token fallback (lowest precedence) |

## Usage Patterns

**Agent startup hook** — restore orientation after a context reset:

```bash
python tools/cognirelay_client.py read \
  --subject-kind user --subject-id "$AGENT_ID" \
  --format startup
```

**Pre-compaction hook** — persist orientation before context loss:

```bash
python tools/cognirelay_client.py upsert --input /tmp/capsule.json
```

**Operator token preparation** — generate a hash for config:

```bash
python tools/cognirelay_client.py token hash --value "$NEW_TOKEN"
```
