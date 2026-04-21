# CogniRelay CLI Client

`tools/cognirelay_client.py` is a single-file, stdlib-only command-line client for CogniRelay's continuity endpoints. It exists so that agents and operators can read, write, and prepare continuity data without a third-party HTTP library or a full SDK.

The client is a thin transport tool. It does not validate capsule contents, inject fields, or make decisions about what to persist — that remains the agent's responsibility. It sends what you give it, returns what the server gives back, and surfaces errors through exit codes and stderr.

## Requirements

- Python 3.12+
- No third-party dependencies (uses `urllib.request`, `argparse`, `hashlib` from stdlib)

## Subcommands

The client exposes five subcommands:

| Subcommand | Purpose |
|---|---|
| `read` | Read a continuity capsule |
| `upsert` | Write or update a continuity capsule |
| `list` | List and discover continuity capsules |
| `capabilities` | Show server capabilities (feature map) |
| `token hash` | Compute the SHA-256 hex digest of a token (for config files) |

Running with no subcommand prints usage to stderr and exits 2.

## Connection Arguments

`read`, `upsert`, `list`, and `capabilities` share these arguments (not present on `token hash`):

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

- `--format json` (default): pretty-printed JSON response body. Does not send `view` in the request — returns the full server response unchanged.
- `--format startup`: sends `view="startup"` to the server and renders the `startup_summary` block as compact section-based text. If the server predates `view="startup"` (response lacks `startup_summary`), the client falls back to a legacy renderer that extracts fields from `capsule.continuity` directly.

The client tolerates both continuity schema `1.0` and `1.1` payloads on read surfaces. For issue #194, newly written continuity capsules and continuity archive/fallback/cold artifacts move to schema `1.1`; stabilized-shape legacy `1.0` continuity payloads remain readable and upgrade safely where supported. Truly pre-stabilization payloads missing required modern continuity fields are still outside the automatic migration boundary.

Use `--output <path>` to write to a file instead of stdout.

### Startup format sections

Sections are divided into **always-shown** (rendered with `(none)` when empty) and **conditional** (suppressed entirely when empty/null):

**Always shown:** Source State, Top Priorities, Active Constraints, Open Loops.

**Conditional:** Recovery Warnings (with health status), Trust Signals (4-line digest), Thread Identity (from `capsule.thread_descriptor`), Negative Decisions, Rationale Entries, Session Trajectory, Stable Preferences.

**Not shown:** `updated_at`, `stance_summary`, `active_concerns` — available in `--format json`.

The startup text format is a presentation convenience. Its exact layout is not a stable contract. Scripts that need stable field access must use `--format json`.

### Startup format example

```
=== Source State ===
active

=== Top Priorities ===
- Complete authentication refactor
- Update deployment runbook

=== Active Constraints ===
- No breaking API changes before v2

=== Open Loops ===
(none)

=== Trust Signals ===
Recency: current (fresh)
Completeness: orientation adequate
Integrity: healthy, verified
Scope: exact

=== Thread Identity ===
Auth refactor [active]
Keywords: auth, security

=== Negative Decisions ===
- No caching layer: adds complexity without current need

=== Rationale Entries ===
- [constraint] perf: Latency budget is 200ms

=== Session Trajectory ===
- Reviewed PR #42
- Started auth module extraction

=== Stable Preferences ===
- [communication] Prefer concise responses
```

If there is no capsule and no `startup_summary`, only Source State (and Recovery Warnings if present) is printed, followed by `(no capsule available)`.

## Upserting a Capsule

```bash
python tools/cognirelay_client.py upsert \
  --base-url http://localhost:8080 \
  --token-file /run/secrets/agent_token \
  --input capsule.json
```

The JSON file is the complete `POST /v1/continuity/upsert` request body (must include `subject_kind`, `subject_id`, `capsule`). The client sends it verbatim — no field injection or validation.

The request body may include an optional `session_end_snapshot` to merge the fixed startup-critical snapshot field set into the capsule before persistence — see [Payload Reference](payload-reference.md#session-end-snapshot-helper) for the merge algorithm and field constraints. It may also include `lifecycle_transition` and `superseded_by` to atomically transition a thread capsule's lifecycle — see [Payload Reference](payload-reference.md#upsert--post-v1continuityupsert).

Use `--stdin` instead of `--input` to pipe JSON from another process. Exactly one of the two is required. Payloads over 256 KiB are rejected client-side.

### Session-End Snapshot

Include `session_end_snapshot` in the upsert request body to merge the fixed startup-critical snapshot field set into the capsule before persistence — the server applies the merge, the client sends it verbatim.

```json
{
  "subject_kind": "user",
  "subject_id": "agent-1",
  "capsule": { "...": "..." },
  "session_end_snapshot": {
    "open_loops": ["finish auth refactor"],
    "top_priorities": ["ship v2 API"],
    "active_constraints": ["no breaking changes before release"],
    "stance_summary": "Wrapping up auth work, blocked on review."
  }
}
```

The P0 fields (`open_loops`, `top_priorities`, `active_constraints`, `stance_summary`) are required when `session_end_snapshot` is present. P1 fields (`negative_decisions`, `session_trajectory`, `rationale_entries`) are optional — null means preserve the existing capsule value. See [Payload Reference](payload-reference.md#session-end-snapshot-helper) for the full merge algorithm.

## Listing Capsules

```bash
python tools/cognirelay_client.py list \
  --base-url http://localhost:8080 \
  --token-file /run/secrets/agent_token \
  --subject-kind thread \
  --sort salience \
  --limit 10
```

Sends `POST /v1/continuity/list`. All flags map 1:1 to request body fields — omitted flags produce omitted fields (server defaults apply).

| Flag | Maps to | Type |
|---|---|---|
| `--subject-kind` | `subject_kind` | Choice: user, peer, thread, task |
| `--limit` | `limit` | int |
| `--include-fallback` | `include_fallback` | flag |
| `--include-archived` | `include_archived` | flag |
| `--include-cold` | `include_cold` | flag |
| `--lifecycle` | `lifecycle` | Choice: active, suspended, concluded, superseded |
| `--scope-anchor` | `scope_anchor` | str |
| `--keyword` | `keyword` | str |
| `--label-exact` | `label_exact` | str |
| `--anchor-kind` | `anchor_kind` | str |
| `--anchor-value` | `anchor_value` | str |
| `--sort` | `sort` | Choice: default, salience |

Output is always pretty-printed JSON. No `--format` flag. No `--output` flag — pipe to a file if needed.

## Capabilities

```bash
python tools/cognirelay_client.py capabilities \
  --base-url http://localhost:8080 \
  --token-file /run/secrets/agent_token
```

Sends `GET /v1/capabilities`. Prints the server's feature map as pretty-printed JSON. No additional arguments beyond connection args. Output is always JSON — no `--format` or `--output` flag.

## Hashing a Token

```bash
python tools/cognirelay_client.py token hash --value "my-secret-token"
```

Prints the lowercase hex SHA-256 digest to stdout. Use `--file` or `--env` instead of `--value` to read the token from a file or environment variable. Exactly one source is required.

Use this to generate token hashes for `peer_tokens.json` and other config files.

## Exit Codes

| Code | Meaning |
|---|---|
| 0 | Success (including degraded/fallback reads) |
| 1 | HTTP error (4xx/5xx) — stderr shows `Error: HTTP {code}: {body}` |
| 2 | Usage or argument error |
| 3 | Token resolution failed (no source, empty `--token`, unreadable or empty `--token-file`, or `--token-env` naming an unset/empty variable) |
| 4 | Connection or network error (timeout, DNS, refused) |
| 5 | Response parse error (non-JSON body) |
| 6 | Token source unreadable or empty (`token hash` only — file not found, empty file, env var unset or empty, empty `--value`) |

## Environment Variables

| Variable | Used by | Purpose |
|---|---|---|
| `COGNIRELAY_BASE_URL` | `read`, `upsert`, `list`, `capabilities` | Default for `--base-url` |
| `COGNIRELAY_TOKEN` | `read`, `upsert`, `list`, `capabilities` | Implicit token fallback (lowest precedence) |

## Usage Patterns

**Canonical `startup` hook** — restore orientation after a context reset:

```bash
python tools/cognirelay_client.py read \
  --subject-kind user --subject-id "$AGENT_ID" \
  --format startup
```

This client read path sends `allow_fallback: true`. Under the canonical `startup` contract, pair it with `--format startup` so the request uses `view: "startup"` and the runtime forwards the response unchanged.

**Canonical `pre_compaction_or_handoff` hook** — persist orientation before context loss:

```bash
python tools/cognirelay_client.py upsert --input /tmp/capsule.json
```

**Operator token preparation** — generate a hash for config:

```bash
python tools/cognirelay_client.py token hash --value "$NEW_TOKEN"
```

## Feature Discovery

Use the `capabilities` subcommand to discover what the current CogniRelay instance supports (including features like startup view, trust signals, and salience ranking):

```bash
python tools/cognirelay_client.py capabilities --base-url http://localhost:8080 --token-file /run/secrets/agent_token
```

See [API Surface](api-surface.md#get-v1capabilities--versioned-feature-map) for the endpoint contract.
