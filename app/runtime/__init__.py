"""Shared runtime helpers for routing, auditing, and rate limiting."""

from .hooks import (
    HookExecutionDependencies,
    HookLocalStep,
    HookWriteResult,
    execute_post_prompt_hook,
    execute_pre_compaction_or_handoff_hook,
    execute_pre_prompt_hook,
    execute_startup_hook,
)
from .service import (
    RATE_LIMIT_STATE_REL,
    audit_event,
    enforce_payload_limit,
    enforce_rate_limit,
    handle_mcp_request,
    load_rate_limit_state,
    parse_iso,
    read_commit_file,
    record_verification_failure,
    resolve_auth_context,
    run_git,
    scope_for_path,
    verification_failure_count,
)

__all__ = [
    "RATE_LIMIT_STATE_REL",
    "HookExecutionDependencies",
    "HookLocalStep",
    "HookWriteResult",
    "audit_event",
    "enforce_payload_limit",
    "enforce_rate_limit",
    "execute_post_prompt_hook",
    "execute_pre_compaction_or_handoff_hook",
    "execute_pre_prompt_hook",
    "execute_startup_hook",
    "handle_mcp_request",
    "load_rate_limit_state",
    "parse_iso",
    "read_commit_file",
    "record_verification_failure",
    "resolve_auth_context",
    "run_git",
    "scope_for_path",
    "verification_failure_count",
]
