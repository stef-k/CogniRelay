#!/usr/bin/env python3
"""Read-only CogniRelay startup/context retrieval hook."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any


class InvalidArgsError(Exception):
    """Raised when argparse should emit the hook JSON failure envelope."""

    def __init__(self, message: str, field: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.field = field


class HookArgumentParser(argparse.ArgumentParser):
    """Argument parser that preserves --help and defers errors to JSON output."""

    def error(self, message: str) -> None:
        field = None
        if message.startswith("argument --"):
            field = message.split(":", 1)[0].removeprefix("argument --").replace("-", "_")
        raise InvalidArgsError(message, field)


def envelope(ok: bool, mode: str, result: dict[str, Any] | None = None, errors: list[dict[str, str]] | None = None, warnings: list[Any] | None = None) -> dict[str, Any]:
    return {"ok": ok, "mode": mode, "warnings": warnings or [], "errors": errors or [], "result": result or {}}


def emit(payload: dict[str, Any], code: int) -> int:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return code


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


def env_bool(value: str | None) -> bool:
    return value is not None and value.strip().lower() in {"1", "true", "yes", "on"}


def normalize_base_url(value: str) -> str:
    return value[:-1] if value.endswith("/") else value


def http_error_result(exc: urllib.error.HTTPError) -> dict[str, Any]:
    return {"status": exc.code}


def resolve_timeout(value: float | None) -> tuple[float, dict[str, str] | None]:
    if value is not None:
        return value, None
    raw = os.getenv("COGNIRELAY_TIMEOUT_SECONDS", "10")
    try:
        return float(raw), None
    except ValueError:
        return 0.0, {"code": "invalid_config", "message": "Timeout must be numeric seconds.", "field": "timeout"}


def build_parser() -> argparse.ArgumentParser:
    parser = HookArgumentParser(description="Read CogniRelay startup continuity and optional bounded context.")
    parser.add_argument("--base-url")
    parser.add_argument("--token")
    parser.add_argument("--subject-kind", choices=["thread", "task", "user", "peer"])
    parser.add_argument("--subject-id")
    parser.add_argument("--task")
    parser.add_argument("--context-retrieve", action="store_true")
    parser.add_argument("--no-context-retrieve", action="store_true")
    parser.add_argument("--timeout", type=float)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except InvalidArgsError as exc:
        error = {"code": "invalid_args", "message": exc.message}
        if exc.field:
            error["field"] = exc.field
        return emit(envelope(False, "retrieval", errors=[error]), 2)
    base_url = normalize_base_url(args.base_url or os.getenv("COGNIRELAY_BASE_URL") or "")
    token = args.token or os.getenv("COGNIRELAY_TOKEN") or ""
    subject_kind = args.subject_kind or os.getenv("COGNIRELAY_SUBJECT_KIND") or ""
    subject_id = args.subject_id or os.getenv("COGNIRELAY_SUBJECT_ID") or ""
    task = args.task if args.task is not None else os.getenv("COGNIRELAY_RETRIEVAL_TASK", "")
    timeout, timeout_error = resolve_timeout(args.timeout)
    context_enabled = (args.context_retrieve or env_bool(os.getenv("COGNIRELAY_CONTEXT_RETRIEVE"))) and not args.no_context_retrieve

    if timeout_error is not None:
        return emit(envelope(False, "retrieval", errors=[timeout_error]), 2)
    if not base_url:
        return emit(envelope(False, "retrieval", errors=[{"code": "missing_config", "message": "Missing CogniRelay base URL.", "field": "base_url"}]), 2)
    if not token:
        return emit(envelope(False, "retrieval", errors=[{"code": "missing_config", "message": "Missing CogniRelay token.", "field": "token"}]), 2)
    if not subject_kind or not subject_id:
        field = "subject" if not subject_kind and not subject_id else ("subject_kind" if not subject_kind else "subject_id")
        return emit(envelope(False, "retrieval", errors=[{"code": "missing_subject", "message": "Missing subject kind or subject id.", "field": field}]), 2)

    read_payload = {"subject_kind": subject_kind, "subject_id": subject_id, "view": "startup", "allow_fallback": True}
    try:
        read_status, startup = post_json(base_url, token, "/v1/continuity/read", read_payload, timeout)
        if read_status < 200 or read_status >= 300:
            return emit(envelope(False, "retrieval", result={"status": read_status}, errors=[{"code": "http_error", "message": "Continuity read returned an unsuccessful status."}]), 4)
        result: dict[str, Any] = {"startup": startup}
        if context_enabled and task.strip():
            context_payload = {"task": task, "subject_kind": subject_kind, "subject_id": subject_id}
            context_status, context = post_json(base_url, token, "/v1/context/retrieve", context_payload, timeout)
            if context_status < 200 or context_status >= 300:
                return emit(
                    envelope(
                        False,
                        "retrieval",
                        result={"startup": startup, "status": context_status},
                        errors=[{"code": "http_error", "message": "Context retrieve returned an unsuccessful status."}],
                    ),
                    4,
                )
            result["context"] = context
        return emit(envelope(True, "retrieval", result=result), 0)
    except urllib.error.HTTPError as exc:
        return emit(envelope(False, "retrieval", result=http_error_result(exc), errors=[{"code": "http_error", "message": "HTTP request failed."}]), 4)
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        print(f"CogniRelay retrieval transport failure: {exc.__class__.__name__}", file=sys.stderr)
        return emit(envelope(False, "retrieval", errors=[{"code": "transport_failure", "message": "No usable HTTP response was received."}]), 3)


if __name__ == "__main__":
    raise SystemExit(main())
