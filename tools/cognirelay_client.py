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
    if args.token is not None:
        if not args.token:
            print("Error: --token value is empty", file=sys.stderr)
            sys.exit(3)
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

def do_request(base_url, path, token, body_bytes, timeout, *, method="POST"):
    """Send an HTTP request and return the response body string.

    When *method* is ``GET``, *body_bytes* is ignored and no
    ``Content-Type`` header is sent.

    On HTTP error exits 1, on connection error exits 4.
    """
    url = f"{base_url}{path}"
    headers = {"Authorization": f"Bearer {token}"}
    if method == "POST":
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        url,
        data=body_bytes if method == "POST" else None,
        headers=headers,
        method=method,
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

    # Continuity fields are nested under capsule.continuity
    continuity = capsule.get("continuity") or {}

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
        items = continuity.get(field) or []
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
# Startup-summary formatter (server-side startup_summary)
# ---------------------------------------------------------------------------

def format_startup_summary(data):
    """Format a ``view=startup`` response as compact startup text.

    Renders the server's ``startup_summary`` block.  Falls back to
    :func:`format_startup` when ``startup_summary`` is absent (older
    servers).  When both ``startup_summary`` and ``capsule`` are null,
    shows only source state and a placeholder.
    """
    summary = data.get("startup_summary")
    capsule = data.get("capsule")

    # Fallback: no startup_summary in response — delegate to legacy renderer
    if summary is None:
        if capsule is not None:
            return format_startup(data)
        # Both null — minimal output, but still surface recovery warnings
        lines = [
            "=== Source State ===",
            data.get("source_state", "unknown"),
        ]
        warnings = data.get("recovery_warnings") or []
        if warnings:
            lines.append("")
            lines.append("=== Recovery Warnings ===")
            for w in warnings:
                lines.append(f"- {w}")
        lines.append("")
        lines.append("(no capsule available)")
        return "\n".join(lines) + "\n"

    lines: list[str] = []

    # --- recovery (always shown) ---
    recovery = summary.get("recovery") or {}
    lines.append("=== Source State ===")
    lines.append(recovery.get("source_state") or data.get("source_state", "unknown"))

    # --- recovery warnings (conditional) ---
    warnings = recovery.get("recovery_warnings") or []
    health_reasons = recovery.get("capsule_health_reasons") or []
    health_status = recovery.get("capsule_health_status")
    if warnings or health_reasons:
        lines.append("")
        lines.append("=== Recovery Warnings ===")
        if health_status:
            lines.append(f"Health: {health_status}")
        for w in warnings:
            lines.append(f"- {w}")
        for r in health_reasons:
            lines.append(f"- {r}")

    # --- orientation: always-shown sections ---
    orientation = summary.get("orientation") or {}

    for field, header in [
        ("top_priorities", "Top Priorities"),
        ("active_constraints", "Active Constraints"),
        ("open_loops", "Open Loops"),
    ]:
        lines.append("")
        lines.append(f"=== {header} ===")
        items = orientation.get(field) or []
        if not items:
            lines.append("(none)")
        else:
            for item in items:
                lines.append(f"- {item}")

    # --- trust signals (conditional) ---
    trust = summary.get("trust_signals")
    if trust is not None:
        recency = trust.get("recency") or {}
        completeness = trust.get("completeness") or {}
        integrity = trust.get("integrity") or {}
        scope = trust.get("scope_match") or {}

        phase = recency.get("phase", "unknown")
        fc = recency.get("freshness_class", "unknown")
        adequate = "adequate" if completeness.get("orientation_adequate") else "inadequate"
        trimmed_note = ", trimmed" if completeness.get("trimmed") else ""
        h_status = integrity.get("health_status", "unknown")
        v_status = integrity.get("verification_status", "unknown")
        scope_label = "exact" if scope.get("exact") else "fallback"

        lines.append("")
        lines.append("=== Trust Signals ===")
        lines.append(f"Recency: {phase} ({fc})")
        lines.append(f"Completeness: orientation {adequate}{trimmed_note}")
        lines.append(f"Integrity: {h_status}, {v_status}")
        lines.append(f"Scope: {scope_label}")

    # --- thread identity (conditional, from capsule) ---
    thread_desc = (capsule or {}).get("thread_descriptor")
    if thread_desc is not None:
        label = thread_desc.get("label", "")
        lifecycle = thread_desc.get("lifecycle", "")
        keywords = thread_desc.get("keywords") or []

        lines.append("")
        lines.append("=== Thread Identity ===")
        lines.append(f"{label} [{lifecycle}]")
        if keywords:
            lines.append(f"Keywords: {', '.join(str(k) for k in keywords)}")

    # --- negative decisions (conditional) ---
    neg = orientation.get("negative_decisions") or []
    if neg:
        lines.append("")
        lines.append("=== Negative Decisions ===")
        for nd in neg:
            decision = nd.get("decision", "") if isinstance(nd, dict) else str(nd)
            rationale = nd.get("rationale", "") if isinstance(nd, dict) else ""
            lines.append(f"- {decision}: {rationale}")

    # --- rationale entries (conditional) ---
    rationale_entries = orientation.get("rationale_entries") or []
    if rationale_entries:
        lines.append("")
        lines.append("=== Rationale Entries ===")
        for entry in rationale_entries:
            if isinstance(entry, dict):
                kind = entry.get("kind", "")
                tag = entry.get("tag", "")
                entry_summary = entry.get("summary", "")
                lines.append(f"- [{kind}] {tag}: {entry_summary}")
            else:
                lines.append(f"- {entry}")

    # --- session trajectory (conditional) ---
    context = summary.get("context") or {}
    trajectory = context.get("session_trajectory") or []
    if trajectory:
        lines.append("")
        lines.append("=== Session Trajectory ===")
        for item in trajectory:
            lines.append(f"- {item}")

    # --- stable preferences (conditional) ---
    prefs = summary.get("stable_preferences") or []
    if prefs:
        lines.append("")
        lines.append("=== Stable Preferences ===")
        for pref in prefs:
            if isinstance(pref, dict):
                tag = pref.get("tag", "")
                content = pref.get("content", "")
                lines.append(f"- [{tag}] {content}")
            else:
                lines.append(f"- {pref}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

def cmd_read(args):
    """Handle the 'read' subcommand."""
    base_url = resolve_base_url(args)
    token = resolve_token(args)

    body = {
        "subject_kind": args.subject_kind,
        "subject_id": args.subject_id,
        "allow_fallback": True,
    }
    if args.format == "startup":
        body["view"] = "startup"

    payload = json.dumps(body).encode("utf-8")

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
        output = format_startup_summary(resp_data)

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


def cmd_capabilities(args):
    """Handle the 'capabilities' subcommand."""
    base_url = resolve_base_url(args)
    token = resolve_token(args)

    resp_text = do_request(
        base_url, "/v1/capabilities", token, None, args.timeout, method="GET",
    )

    try:
        resp_data = json.loads(resp_text)
    except (json.JSONDecodeError, ValueError):
        print("Error: unexpected response format", file=sys.stderr)
        sys.exit(5)

    sys.stdout.write(json.dumps(resp_data, indent=4) + "\n")


def cmd_list(args):
    """Handle the 'list' subcommand."""
    base_url = resolve_base_url(args)
    token = resolve_token(args)

    # Build request body — only include explicitly-provided flags.
    body = {}
    if args.subject_kind is not None:
        body["subject_kind"] = args.subject_kind
    if args.limit is not None:
        body["limit"] = args.limit
    if args.include_fallback:
        body["include_fallback"] = True
    if args.include_archived:
        body["include_archived"] = True
    if args.include_cold:
        body["include_cold"] = True
    if args.lifecycle is not None:
        body["lifecycle"] = args.lifecycle
    if args.scope_anchor is not None:
        body["scope_anchor"] = args.scope_anchor
    if args.keyword is not None:
        body["keyword"] = args.keyword
    if args.label_exact is not None:
        body["label_exact"] = args.label_exact
    if args.anchor_kind is not None:
        body["anchor_kind"] = args.anchor_kind
    if args.anchor_value is not None:
        body["anchor_value"] = args.anchor_value
    if args.sort is not None:
        body["sort"] = args.sort

    payload = json.dumps(body).encode("utf-8")
    resp_text = do_request(base_url, "/v1/continuity/list", token, payload, args.timeout)

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
        if not args.value:
            print("Error: --value is empty", file=sys.stderr)
            sys.exit(6)
        token = args.value
    elif args.file is not None:
        try:
            with open(args.file) as f:
                token = f.read().rstrip()
        except OSError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(6)
        if not token:
            print(f"Error: token file '{args.file}' is empty", file=sys.stderr)
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

    # -- list --
    list_parser = subparsers.add_parser("list", help="List continuity capsules")
    add_connection_args(list_parser)
    list_parser.add_argument("--subject-kind", dest="subject_kind", default=None,
                             choices=["user", "peer", "thread", "task"])
    list_parser.add_argument("--limit", type=int, default=None)
    list_parser.add_argument("--include-fallback", dest="include_fallback",
                             action="store_true", default=False)
    list_parser.add_argument("--include-archived", dest="include_archived",
                             action="store_true", default=False)
    list_parser.add_argument("--include-cold", dest="include_cold",
                             action="store_true", default=False)
    list_parser.add_argument("--lifecycle", default=None,
                             choices=["active", "suspended", "concluded", "superseded"])
    list_parser.add_argument("--scope-anchor", dest="scope_anchor", default=None)
    list_parser.add_argument("--keyword", default=None)
    list_parser.add_argument("--label-exact", dest="label_exact", default=None)
    list_parser.add_argument("--anchor-kind", dest="anchor_kind", default=None)
    list_parser.add_argument("--anchor-value", dest="anchor_value", default=None)
    list_parser.add_argument("--sort", default=None, choices=["default", "salience"])

    # -- capabilities --
    capabilities_parser = subparsers.add_parser(
        "capabilities", help="Show server capabilities",
    )
    add_connection_args(capabilities_parser)

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
    elif args.command == "list":
        cmd_list(args)
    elif args.command == "capabilities":
        cmd_capabilities(args)
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
