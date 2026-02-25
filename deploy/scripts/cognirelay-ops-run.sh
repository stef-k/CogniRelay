#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <job_id> [arguments_json]" >&2
  exit 1
fi

JOB_ID="$1"
ARGS_JSON="${2:-{}}"
BASE_URL="${COGNIRELAY_BASE_URL:-http://127.0.0.1:8080}"
TOKEN_FILE="${COGNIRELAY_OPS_TOKEN_FILE:-/etc/cognirelay/ops.token}"

if [[ ! -r "$TOKEN_FILE" ]]; then
  echo "Ops token file is missing or not readable: $TOKEN_FILE" >&2
  exit 2
fi

TOKEN="$(tr -d '[:space:]' < "$TOKEN_FILE")"
if [[ -z "$TOKEN" ]]; then
  echo "Ops token is empty: $TOKEN_FILE" >&2
  exit 3
fi

PAYLOAD="$(python3 - "$JOB_ID" "$ARGS_JSON" <<'PY'
import json
import sys

job_id = sys.argv[1]
arguments_raw = sys.argv[2]
try:
    arguments = json.loads(arguments_raw)
except json.JSONDecodeError as exc:
    raise SystemExit(f"Invalid arguments JSON: {exc}")

body = {
    "job_id": job_id,
    "arguments": arguments,
}
print(json.dumps(body, separators=(",", ":"), sort_keys=True))
PY
)"

curl --fail --show-error --silent \
  -X POST "${BASE_URL}/v1/ops/run" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD"
