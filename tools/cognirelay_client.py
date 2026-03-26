#!/usr/bin/env python3
"""CogniRelay CLI client — stdlib-only single-file tool for continuity operations."""

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request

MAX_PAYLOAD_BYTES = 262_144  # 256 KiB


# ---------------------------------------------------------------------------
# Token resolution
# ---------------------------------------------------------------------------

def resolve_token(args):
    """Resolve bearer token from CLI args / env with deterministic precedence.

    Order: --token > --token-file > --token-env > COGNIRELAY_TOKEN env var.
    Returns the token string or calls sys.exit(3) on failure.
    """
    # 1. Explicit --token
    if args.token:
        return args.token

    # 2. --token-file
    if args.token_file:
        try:
            with open(args.token_file) as f:
                value = f.read().rstrip()
        except OSError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(3)
        if value:
            return value
        print(f"Error: token file '{args.token_file}' is empty", file=sys.stderr)
        sys.exit(3)

    # 3. --token-env
    if args.token_env:
        value = os.environ.get(args.token_env, "").rstrip()
        if value:
            return value
        print(
            f"Error: environment variable '{args.token_env}' is not set or empty",
            file=sys.stderr,
        )
        sys.exit(3)

    # 4. Implicit fallback: COGNIRELAY_TOKEN
    value = os.environ.get("COGNIRELAY_TOKEN", "").rstrip()
    if value:
        return value

    print("Error: no token provided", file=sys.stderr)
    sys.exit(3)


# ---------------------------------------------------------------------------
# Base URL resolution
# ---------------------------------------------------------------------------

def resolve_base_url(args):
    """Return the normalized base URL or exit 2 if missing."""
    url = args.base_url or os.environ.get("COGNIRELAY_BASE_URL")
    if not url:
        print("Error: --base-url is required (or set COGNIRELAY_BASE_URL)", file=sys.stderr)
        sys.exit(2)
    # Strip exactly one trailing slash
    if url.endswith("/"):
        url = url[:-1]
    return url


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def do_request(base_url, path, token, body_bytes, timeout):
    """Send a POST request and return the response body string.

    On HTTP error exits 1, on connection error exits 4.
    """
    url = f"{base_url}{path}"
    req = urllib.request.Request(
        url,
        data=body_bytes,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8")
        except Exception:
            pass
        print(f"Error: HTTP {exc.code}: {body}", file=sys.stderr)
        sys.exit(1)
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        reason = getattr(exc, "reason", exc)
        print(f"Error: {reason}", file=sys.stderr)
        sys.exit(4)


# ---------------------------------------------------------------------------
# Startup formatter
# ---------------------------------------------------------------------------

def format_startup(data):
    """Format a continuity read response as startup-friendly plain text."""
    lines = []

    source_state = data.get("source_state", "unknown")
    lines.append("=== Source State ===")
    lines.append(source_state)

    warnings = data.get("recovery_warnings", []) or []
    lines.append("")
    lines.append("=== Recovery Warnings ===")
    if warnings:
        for w in warnings:
            lines.append(f"- {w}")
    else:
        lines.append("(none)")

    capsule = data.get("capsule")
    if capsule is None:
        lines.append("")
        lines.append("(no capsule available)")
        return "\n".join(lines) + "\n"

    # Capsule sections
    section_map = [
        ("top_priorities", "Top Priorities"),
        ("active_constraints", "Active Constraints"),
        ("open_loops", "Open Loops"),
        ("negative_decisions", "Negative Decisions"),
        ("session_trajectory", "Session Trajectory"),
    ]

    for field, header in section_map:
        lines.append("")
        lines.append(f"=== {header} ===")
        items = capsule.get(field) or []
        if not items:
            lines.append("(none)")
        elif field == "negative_decisions":
            for nd in items:
                decision = nd.get("decision", "") if isinstance(nd, dict) else str(nd)
                rationale = nd.get("rationale", "") if isinstance(nd, dict) else ""
                lines.append(f"- {decision}: {rationale}")
        else:
            for item in items:
                lines.append(f"- {item}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

def cmd_read(args):
    """Handle the 'read' subcommand."""
    base_url = resolve_base_url(args)
    token = resolve_token(args)

    payload = json.dumps({
        "subject_kind": args.subject_kind,
        "subject_id": args.subject_id,
        "allow_fallback": True,
    }).encode("utf-8")

    resp_text = do_request(base_url, "/v1/continuity/read", token, payload, args.timeout)

    # Parse to verify it's JSON
    try:
        resp_data = json.loads(resp_text)
    except (json.JSONDecodeError, ValueError):
        print("Error: unexpected response format", file=sys.stderr)
        sys.exit(5)

    if args.format == "json":
        output = json.dumps(resp_data, indent=4) + "\n"
    else:
        output = format_startup(resp_data)

    if args.output:
        try:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(output)
        except OSError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(2)
    else:
        sys.stdout.write(output)


def cmd_upsert(args):
    """Handle the 'upsert' subcommand."""
    base_url = resolve_base_url(args)
    token = resolve_token(args)

    # Validate mutual exclusion
    has_input = args.input is not None
    has_stdin = args.stdin
    if has_input == has_stdin:
        print(
            "Error: exactly one of --input or --stdin is required",
            file=sys.stderr,
        )
        sys.exit(2)

    # Read payload
    if has_stdin:
        raw = sys.stdin.buffer.read(MAX_PAYLOAD_BYTES + 1)
    else:
        try:
            with open(args.input, "rb") as f:
                raw = f.read(MAX_PAYLOAD_BYTES + 1)
        except OSError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(2)

    if len(raw) > MAX_PAYLOAD_BYTES:
        print("Error: payload exceeds 256 KiB limit", file=sys.stderr)
        sys.exit(2)

    resp_text = do_request(base_url, "/v1/continuity/upsert", token, raw, args.timeout)

    try:
        resp_data = json.loads(resp_text)
    except (json.JSONDecodeError, ValueError):
        print("Error: unexpected response format", file=sys.stderr)
        sys.exit(5)

    sys.stdout.write(json.dumps(resp_data, indent=4) + "\n")


def cmd_token_hash(args):
    """Handle the 'token hash' subcommand."""
    # Exactly one source required
    sources = [args.value, args.file, args.env]
    provided = sum(1 for s in sources if s is not None)
    if provided != 1:
        print(
            "Error: exactly one of --value, --file, or --env is required",
            file=sys.stderr,
        )
        sys.exit(2)

    if args.value is not None:
        token = args.value
    elif args.file is not None:
        try:
            with open(args.file) as f:
                token = f.read().rstrip()
        except OSError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(6)
    else:
        token = os.environ.get(args.env, "").rstrip()
        if not token:
            print(
                f"Error: environment variable '{args.env}' is not set or empty",
                file=sys.stderr,
            )
            sys.exit(6)

    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
    sys.stdout.write(digest + "\n")


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser():
    """Build the top-level argparse parser with subcommands.

    Returns (parser, token_parser) so callers can print token-specific
    usage when 'token' is invoked without a sub-subcommand.
    """
    parser = argparse.ArgumentParser(
        prog="cognirelay-client",
        description="CogniRelay CLI client",
    )
    subparsers = parser.add_subparsers(dest="command")

    # -- Shared connection/token arguments --
    def add_connection_args(p):
        p.add_argument("--base-url", dest="base_url", default=None)
        p.add_argument("--token", default=None)
        p.add_argument("--token-file", dest="token_file", default=None)
        p.add_argument("--token-env", dest="token_env", default=None)
        p.add_argument("--timeout", type=float, default=30.0)

    # -- read --
    read_parser = subparsers.add_parser("read", help="Read continuity capsule")
    add_connection_args(read_parser)
    read_parser.add_argument("--subject-kind", dest="subject_kind", required=True,
                             choices=["user", "peer", "thread", "task"])
    read_parser.add_argument("--subject-id", dest="subject_id", required=True)
    read_parser.add_argument("--format", default="json", choices=["json", "startup"])
    read_parser.add_argument("--output", default=None)

    # -- upsert --
    upsert_parser = subparsers.add_parser("upsert", help="Upsert continuity capsule")
    add_connection_args(upsert_parser)
    upsert_parser.add_argument("--input", default=None)
    upsert_parser.add_argument("--stdin", action="store_true", default=False)
    upsert_parser.add_argument("--format", default="json", choices=["json"])

    # -- token (parent) -> hash (child) --
    token_parser = subparsers.add_parser("token", help="Token utilities")
    token_sub = token_parser.add_subparsers(dest="token_command")

    hash_parser = token_sub.add_parser("hash", help="Hash a token to SHA-256")
    hash_parser.add_argument("--value", default=None)
    hash_parser.add_argument("--file", default=None)
    hash_parser.add_argument("--env", default=None)

    return parser, token_parser


def main():
    """CLI entry point."""
    parser, token_parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_usage(sys.stderr)
        sys.exit(2)

    if args.command == "read":
        cmd_read(args)
    elif args.command == "upsert":
        cmd_upsert(args)
    elif args.command == "token":
        if not getattr(args, "token_command", None):
            token_parser.print_usage(sys.stderr)
            sys.exit(2)
        elif args.token_command == "hash":
            cmd_token_hash(args)
    else:
        parser.print_usage(sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
