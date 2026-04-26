# CogniRelay Hooks

These hooks are copyable Python 3 scripts for generic agent-runtime integration. They use only the Python standard library and emit one JSON object to stdout for normal operation. Human/debug logs belong on stderr.

## Configuration

Required by CLI flag or environment for networked modes:

- `COGNIRELAY_BASE_URL`
- `COGNIRELAY_TOKEN`

Subject values are required for retrieval and may be used by `facts`, `template`, `readback`, and `doctor`:

- `COGNIRELAY_SUBJECT_KIND`
- `COGNIRELAY_SUBJECT_ID`

Optional:

- `COGNIRELAY_RETRIEVAL_TASK`
- `COGNIRELAY_CONTEXT_RETRIEVE`
- `COGNIRELAY_TIMEOUT_SECONDS`

CLI flags override environment variables. `--no-context-retrieve` wins over all context-retrieve enablement.

## Retrieval

```bash
python agent-assets/hooks/cognirelay_retrieval_hook.py \
  --base-url http://127.0.0.1:8000 \
  --token "$COGNIRELAY_TOKEN" \
  --subject-kind thread \
  --subject-id issue-289 \
  --context-retrieve \
  --task "Continue issue 289"
```

The retrieval hook is read-only. It calls `POST /v1/continuity/read` with `view="startup"` and `allow_fallback=true`, then optionally calls `POST /v1/context/retrieve` when enabled and a non-empty task is present.

## Save

```bash
python agent-assets/hooks/cognirelay_continuity_save_hook.py facts --subject-kind thread --subject-id issue-289
python agent-assets/hooks/cognirelay_continuity_save_hook.py template --subject-kind thread --subject-id issue-289
python agent-assets/hooks/cognirelay_continuity_save_hook.py dry-run --input payload.json
python agent-assets/hooks/cognirelay_continuity_save_hook.py write --input payload.json
python agent-assets/hooks/cognirelay_continuity_save_hook.py readback --subject-kind thread --subject-id issue-289
python agent-assets/hooks/cognirelay_continuity_save_hook.py doctor --subject-kind thread --subject-id issue-289
```

Use `--input -` for stdin payloads in `dry-run` and `write`.

`dry-run` is local-only by default and does not contact CogniRelay. `--server-compare` is intentionally unsupported in this shipped hook until implemented by a future slice.

## Stdout Envelope

Success or degraded success:

```json
{"ok": true, "mode": "dry-run", "warnings": [], "errors": [], "result": {}}
```

Failure:

```json
{"ok": false, "mode": "dry-run", "warnings": [], "errors": [{"code": "validation", "message": "Invalid input."}], "result": {}}
```

Exit codes:

- `0`: success or degraded success with warnings
- `2`: usage, config, input validation, placeholder rejection, or unsupported offline mode
- `3`: transport failure without an HTTP response
- `4`: HTTP response received outside the expected success range

## Local Glue Boundary

Local glue supplies base URL, token, subject identity, task text, and any runtime-specific file/stdin wiring. The hook never prints the token and never creates schedules, mutates tasks, acknowledges schedules, or infers continuity semantics. The agent authors semantic fields explicitly.
