#!/usr/bin/env python3
"""CogniRelay continuity save helper hook."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

HELP_LINKS = {
    "onboarding": "/v1/help/onboarding",
    "last_mile_topic": "/v1/help/topics/last-mile-adapter",
    "continuity_read_tool": "/v1/help/tools/continuity.read",
    "continuity_upsert_tool": "/v1/help/tools/continuity.upsert",
    "context_retrieve_tool": "/v1/help/tools/context.retrieve",
    "limits_index": "/v1/help/limits",
}

SEMANTIC_PREFIXES = (
    "/capsule/continuity",
    "/capsule/stable_preferences",
    "/capsule/thread_descriptor",
    "/capsule/source",
    "/capsule/attention_policy",
    "/capsule/canonical_sources",
    "/capsule/metadata",
    "/session_end_snapshot",
)
EXCLUDED_DIFF_PREFIXES = (
    "/subject_kind",
    "/subject_id",
    "/merge_mode",
    "/capsule/schema_version",
    "/capsule/subject_kind",
    "/capsule/subject_id",
    "/capsule/updated_at",
    "/capsule/verified_at",
    "/capsule/confidence",
)
PLACEHOLDER_RE = re.compile(r"\b(?:TODO|TBD|PLACEHOLDER|FILL ME|REPLACE ME)\b", re.IGNORECASE)
BRACKET_PLACEHOLDER_RE = re.compile(r"^(?:<[^<>]+>|\[[^\[\]]+\])$")
UTC_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")


def envelope(ok: bool, mode: str, result: dict[str, Any] | None = None, errors: list[dict[str, str]] | None = None, warnings: list[Any] | None = None) -> dict[str, Any]:
    return {"ok": ok, "mode": mode, "warnings": warnings or [], "errors": errors or [], "result": result or {}}


def emit(payload: dict[str, Any], code: int) -> int:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return code


def pointer(parts: list[str]) -> str:
    return "/" + "/".join(part.replace("~", "~0").replace("/", "~1") for part in parts)


def under_prefix(path: str, prefixes: tuple[str, ...]) -> bool:
    return any(path == prefix or path.startswith(f"{prefix}/") for prefix in prefixes)


def read_input(path: str) -> Any:
    raw = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8")
    return json.loads(raw)


def post_json(base_url: str, token: str, path: str, payload: dict[str, Any], timeout: float) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(
        f"{base_url}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
        return response.status, json.loads(body) if body else {}


def current_git_facts() -> dict[str, str]:
    facts: dict[str, str] = {}
    for key, cmd in {
        "branch": ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        "head": ["git", "rev-parse", "HEAD"],
    }.items():
        try:
            completed = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=2)
        except (OSError, subprocess.SubprocessError):
            continue
        if completed.returncode == 0:
            facts[key] = completed.stdout.strip()
    return facts


def template_payload(subject_kind: str, subject_id: str) -> dict[str, Any]:
    subject_kind = subject_kind or "thread"
    subject_id = subject_id or "<agent-authored-subject-id>"
    timestamp = "<UTC-timestamp-filled-by-agent-or-local-glue>"
    return {
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "merge_mode": "preserve",
        "capsule": {
            "schema_version": "1.1",
            "subject_kind": subject_kind,
            "subject_id": subject_id,
            "updated_at": timestamp,
            "verified_at": timestamp,
            "continuity": {
                "top_priorities": ["<agent-authored-priority>"],
                "active_concerns": [],
                "active_constraints": ["<agent-authored-constraint>"],
                "open_loops": ["<agent-authored-open-loop>"],
                "drift_signals": [],
                "stance_summary": "<agent-authored-stance-summary>",
                "negative_decisions": [],
                "rationale_entries": [],
                "retrieval_hints": {"must_include": [], "load_next": [], "avoid": []},
            },
            "source": {"producer": "<agent-authored-or-adapter-identifier>", "update_reason": "manual", "inputs": []},
            "confidence": {"continuity": 0.8, "relationship_model": 0.5},
        },
    }


def validate_payload_shape(payload: Any) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    if not isinstance(payload, dict):
        return [{"code": "invalid_input", "message": "Input must be a JSON object."}]
    for field in ("subject_kind", "subject_id", "capsule"):
        if field not in payload:
            errors.append({"code": "invalid_input", "message": f"Missing required field {field}.", "field": field})
    if "capsule" in payload and not isinstance(payload["capsule"], dict):
        errors.append({"code": "invalid_input", "message": "capsule must be an object.", "field": "capsule"})
    return errors


def timestamp_errors(payload: dict[str, Any]) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    capsule = payload.get("capsule", {})
    if not isinstance(capsule, dict):
        return errors
    for key in ("updated_at", "verified_at"):
        path = f"/capsule/{key}"
        value = capsule.get(key)
        if not isinstance(value, str) or not value.strip():
            errors.append({"code": "invalid_input", "message": "Timestamp must be a non-empty UTC string.", "field": path})
            continue
        stripped = value.strip()
        if PLACEHOLDER_RE.search(stripped) or BRACKET_PLACEHOLDER_RE.match(stripped):
            errors.append({"code": "placeholder_rejected", "message": "Timestamp placeholder is not allowed.", "field": path})
        elif not UTC_TIMESTAMP_RE.match(stripped):
            errors.append({"code": "invalid_input", "message": "Timestamp must use UTC Z format.", "field": path})
    return errors


def placeholder_errors(value: Any, parts: list[str] | None = None) -> list[dict[str, str]]:
    parts = parts or []
    path = pointer(parts) if parts else ""
    errors: list[dict[str, str]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            errors.extend(placeholder_errors(child, [*parts, str(key)]))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            errors.extend(placeholder_errors(child, [*parts, str(index)]))
    elif isinstance(value, str) and under_prefix(path, SEMANTIC_PREFIXES):
        stripped = value.strip()
        if not stripped:
            errors.append({"code": "placeholder_rejected", "message": "Empty semantic string is not allowed.", "field": path})
        elif PLACEHOLDER_RE.search(stripped) or BRACKET_PLACEHOLDER_RE.match(stripped):
            errors.append({"code": "placeholder_rejected", "message": "Unresolved placeholder or TODO marker is not allowed.", "field": path})
    return errors


def write_semantic_requirements(payload: dict[str, Any]) -> list[dict[str, str]]:
    continuity = payload.get("capsule", {}).get("continuity", {})
    if not isinstance(continuity, dict):
        return [{"code": "invalid_input", "message": "capsule.continuity must be an object.", "field": "capsule.continuity"}]
    errors: list[dict[str, str]] = []
    stance = continuity.get("stance_summary")
    if not isinstance(stance, str) or not stance.strip():
        errors.append({"code": "invalid_input", "message": "stance_summary must be non-empty.", "field": "capsule.continuity.stance_summary"})
    hints = continuity.get("retrieval_hints", {})
    carriers = [
        continuity.get("top_priorities"),
        continuity.get("open_loops"),
        continuity.get("active_constraints"),
        continuity.get("negative_decisions"),
        continuity.get("rationale_entries"),
        hints.get("must_include") if isinstance(hints, dict) else None,
        hints.get("load_next") if isinstance(hints, dict) else None,
    ]
    if not any(non_empty_carrier(carrier) for carrier in carriers):
        errors.append({"code": "invalid_input", "message": "At least one semantic update carrier must be non-empty.", "field": "capsule.continuity"})
    return errors


def non_empty_carrier(value: Any) -> bool:
    if isinstance(value, list):
        return any(bool(item.strip()) if isinstance(item, str) else item not in (None, {}, []) for item in value)
    return bool(value)


def flatten(value: Any, parts: list[str] | None = None) -> list[tuple[str, Any]]:
    parts = parts or []
    path = pointer(parts) if parts else ""
    if isinstance(value, dict):
        items: list[tuple[str, Any]] = []
        for key in sorted(value):
            items.extend(flatten(value[key], [*parts, str(key)]))
        return items
    if isinstance(value, list):
        items = []
        for index, child in enumerate(value):
            items.extend(flatten(child, [*parts, str(index)]))
        return items
    return [(path, value)]


def semantic_diff(payload: dict[str, Any]) -> dict[str, Any]:
    added = []
    for path, value in flatten(payload):
        if under_prefix(path, SEMANTIC_PREFIXES) and not under_prefix(path, EXCLUDED_DIFF_PREFIXES):
            if value not in ("", [], {}, None):
                added.append({"path": path, "after": value})
    added.sort(key=lambda item: item["path"])
    return {"current_available": False, "added": added, "removed": [], "changed": []}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate, write, and read back CogniRelay continuity payloads.")
    parser.add_argument("mode", choices=["facts", "template", "dry-run", "write", "readback", "doctor"])
    parser.add_argument("--base-url")
    parser.add_argument("--token")
    parser.add_argument("--subject-kind", choices=["thread", "task", "user", "peer"])
    parser.add_argument("--subject-id")
    parser.add_argument("--input")
    parser.add_argument("--timeout", type=float)
    parser.add_argument("--server-compare", action="store_true")
    return parser


def normalize_base_url(value: str) -> str:
    return value[:-1] if value.endswith("/") else value


def resolve_timeout(value: float | None) -> tuple[float, dict[str, str] | None]:
    if value is not None:
        return value, None
    raw = os.getenv("COGNIRELAY_TIMEOUT_SECONDS", "10")
    try:
        return float(raw), None
    except ValueError:
        return 0.0, {"code": "invalid_config", "message": "Timeout must be numeric seconds.", "field": "timeout"}


def network_config(args: argparse.Namespace) -> tuple[str, str, float, dict[str, str] | None]:
    base_url = normalize_base_url(args.base_url or os.getenv("COGNIRELAY_BASE_URL") or "")
    token = args.token or os.getenv("COGNIRELAY_TOKEN") or ""
    timeout, timeout_error = resolve_timeout(args.timeout)
    return base_url, token, timeout, timeout_error


def readback(mode: str, args: argparse.Namespace) -> int:
    base_url, token, timeout, timeout_error = network_config(args)
    subject_kind = args.subject_kind or os.getenv("COGNIRELAY_SUBJECT_KIND") or ""
    subject_id = args.subject_id or os.getenv("COGNIRELAY_SUBJECT_ID") or ""
    if timeout_error is not None:
        return emit(envelope(False, mode, errors=[timeout_error]), 2)
    if not base_url or not token:
        return emit(envelope(False, mode, errors=[{"code": "missing_config", "message": "Missing CogniRelay base URL or token."}]), 2)
    if not subject_kind or not subject_id:
        return emit(envelope(False, mode, errors=[{"code": "missing_subject", "message": "Missing subject kind or subject id.", "field": "subject"}]), 2)
    try:
        status, response = post_json(base_url, token, "/v1/continuity/read", {"subject_kind": subject_kind, "subject_id": subject_id, "view": "startup", "allow_fallback": True}, timeout)
        if status < 200 or status >= 300:
            return emit(envelope(False, mode, result={"status": status}, errors=[{"code": "http_error", "message": "Continuity read returned an unsuccessful status."}]), 4)
        return emit(envelope(True, mode, result={"readback": response, "trust_signals": response.get("trust_signals", {}), "recovery_warnings": response.get("recovery_warnings", [])}), 0)
    except urllib.error.HTTPError as exc:
        return emit(envelope(False, mode, result={"status": exc.code}, errors=[{"code": "http_error", "message": "CogniRelay returned an unsuccessful status."}]), 4)
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        print(f"CogniRelay save-hook transport failure: {exc.__class__.__name__}", file=sys.stderr)
        return emit(envelope(False, mode, errors=[{"code": "transport_failure", "message": "No usable HTTP response was received."}]), 3)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    mode = args.mode
    subject_kind = args.subject_kind or os.getenv("COGNIRELAY_SUBJECT_KIND") or ""
    subject_id = args.subject_id or os.getenv("COGNIRELAY_SUBJECT_ID") or ""

    if mode == "facts":
        result = {
            "subject": {"kind": subject_kind or None, "id": subject_id or None},
            "config": {
                "base_url_present": bool(args.base_url or os.getenv("COGNIRELAY_BASE_URL")),
                "token_present": bool(args.token or os.getenv("COGNIRELAY_TOKEN")),
            },
            "mechanical_facts": {"utc_now": datetime.now(UTC).isoformat().replace("+00:00", "Z"), "git": current_git_facts()},
            "help_links": HELP_LINKS,
        }
        return emit(envelope(True, mode, result=result), 0)
    if mode == "template":
        return emit(envelope(True, mode, result={"payload": template_payload(subject_kind, subject_id)}), 0)
    if mode in {"readback", "doctor"}:
        return readback(mode, args)
    if mode in {"dry-run", "write"}:
        if args.server_compare:
            return emit(
                envelope(
                    False,
                    mode,
                    result={"diff": {"current_available": False, "added": [], "removed": [], "changed": []}},
                    errors=[{"code": "server_compare_not_implemented", "message": "Server compare is not implemented by this hook."}],
                ),
                2,
            )
        if not args.input:
            return emit(envelope(False, mode, errors=[{"code": "missing_input", "message": "--input PATH|- is required.", "field": "input"}]), 2)
        try:
            payload = read_input(args.input)
        except (OSError, json.JSONDecodeError) as exc:
            return emit(envelope(False, mode, errors=[{"code": "invalid_input", "message": f"Could not read input payload: {exc.__class__.__name__}", "field": "input"}]), 2)
        errors = validate_payload_shape(payload)
        if not errors:
            errors.extend(timestamp_errors(payload))
            errors.extend(placeholder_errors(payload))
            errors.extend(write_semantic_requirements(payload))
        diff = semantic_diff(payload) if isinstance(payload, dict) else {"current_available": False, "added": [], "removed": [], "changed": []}
        if errors:
            return emit(envelope(False, mode, result={"valid": False, "diff": diff, "placeholder_errors": errors}, errors=errors), 2)
        if mode == "dry-run":
            return emit(envelope(True, mode, result={"valid": True, "diff": diff, "placeholder_errors": []}), 0)
        base_url, token, timeout, timeout_error = network_config(args)
        if timeout_error is not None:
            return emit(envelope(False, mode, errors=[timeout_error]), 2)
        if not base_url or not token:
            return emit(envelope(False, mode, errors=[{"code": "missing_config", "message": "Missing CogniRelay base URL or token."}]), 2)
        try:
            status, response = post_json(base_url, token, "/v1/continuity/upsert", payload, timeout)
            if status < 200 or status >= 300:
                return emit(envelope(False, mode, result={"status": status}, errors=[{"code": "http_error", "message": "Continuity upsert returned an unsuccessful status."}]), 4)
            result = {"upsert_response": response}
            return emit(envelope(True, mode, result=result), 0)
        except urllib.error.HTTPError as exc:
            return emit(envelope(False, mode, result={"status": exc.code}, errors=[{"code": "http_error", "message": "CogniRelay returned an unsuccessful status."}]), 4)
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            print(f"CogniRelay write transport failure: {exc.__class__.__name__}", file=sys.stderr)
            return emit(envelope(False, mode, errors=[{"code": "transport_failure", "message": "No usable HTTP response was received."}]), 3)
    return emit(envelope(False, mode, errors=[{"code": "invalid_mode", "message": "Unsupported mode."}]), 2)


if __name__ == "__main__":
    raise SystemExit(main())
