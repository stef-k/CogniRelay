"""Deterministic machine-facing help payloads for issue #214 slice 1."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from fastapi.responses import JSONResponse

_TOOL_IDS = [
    "continuity.read",
    "continuity.upsert",
    "context.retrieve",
]

_TOPIC_IDS = [
    "continuity.read.startup_view",
    "continuity.read.trust_signals",
    "continuity.upsert.session_end_snapshot",
]

_ERROR_CODES = [
    "validation",
    "tool_not_found",
    "unknown_help_topic",
]

_EXACT_HELP_PATHS = frozenset(
    {
        "/v1/help",
        "/v1/help/hooks",
    }
)

_PARAMETERIZED_HELP_PREFIXES = (
    "/v1/help/tools/",
    "/v1/help/topics/",
    "/v1/help/errors/",
)

_ROOT_BODY = {
    "http_endpoints": [
        "GET /v1/help",
        "GET /v1/help/tools/{name}",
        "GET /v1/help/topics/{id}",
        "GET /v1/help/hooks",
        "GET /v1/help/errors/{code}",
    ],
    "mcp_tools": [
        "system.help",
        "system.tool_usage",
        "system.topic_help",
        "system.hook_guide",
        "system.error_guide",
    ],
    "tool_topics": _TOOL_IDS,
    "non_tool_topics": _TOPIC_IDS,
    "hook_ids": [
        "startup",
        "pre_prompt",
        "post_prompt",
        "pre_compaction_or_handoff",
    ],
    "errors": _ERROR_CODES,
}

_TOOLS = {
    "continuity.read": {
        "kind": "tool",
        "id": "continuity.read",
        "purpose": "Read continuity state for a subject.",
        "when_to_use": [
            "Use when the runtime needs persisted orientation for a subject.",
            "Use at session start when continuity is needed before prompting.",
        ],
        "read_operations": [
            "POST /v1/continuity/read",
            "continuity.read",
        ],
        "write_operations": [],
        "minimal_payload": {
            "subject_kind": "thread",
            "subject_id": "issue-214",
            "view": "startup",
            "allow_fallback": True,
        },
        "common_mistakes": [
            "Using a view value that is not defined by the continuity.read contract.",
            "Omitting subject_kind or subject_id.",
        ],
        "correction_hints": [
            "Use view: startup and allow_fallback: true for startup continuity guidance.",
            "Provide both subject_kind and subject_id.",
        ],
    },
    "continuity.upsert": {
        "kind": "tool",
        "id": "continuity.upsert",
        "purpose": "Create or update continuity state for a subject.",
        "when_to_use": [
            "Use when the runtime needs to persist an updated continuity capsule.",
            "Use at session end when storing a bounded snapshot for the next startup read.",
        ],
        "read_operations": [
            "POST /v1/continuity/read",
            "continuity.read",
        ],
        "write_operations": [
            "POST /v1/continuity/upsert",
            "continuity.upsert",
        ],
        "minimal_payload": {
            "subject_kind": "thread",
            "subject_id": "issue-214",
            "capsule": {
                "updated_at": "2026-04-21T12:00:00Z",
                "open_loops": [],
                "top_priorities": [],
                "active_constraints": [],
                "stance_summary": "Ready to continue issue 214 work.",
            },
        },
        "common_mistakes": [
            "Sending a capsule without updated_at.",
            "Sending session_end_snapshot with fields outside the closed field set in this issue.",
        ],
        "correction_hints": [
            "Include updated_at in the capsule using an explicit UTC timestamp.",
            "Use only the session_end_snapshot fields closed by this issue.",
        ],
    },
    "context.retrieve": {
        "kind": "tool",
        "id": "context.retrieve",
        "purpose": "Retrieve a bounded context package for a task, thread, or subject.",
        "when_to_use": [
            "Use when the runtime needs a compact context package instead of a raw continuity capsule.",
            "Use before prompting when context retrieval is the contract-defined entrypoint.",
        ],
        "read_operations": [
            "POST /v1/context/retrieve",
            "context.retrieve",
        ],
        "write_operations": [],
        "minimal_payload": {
            "task": "Address determinism findings on issue #214 only.",
            "subject_kind": "thread",
            "subject_id": "issue-214",
            "continuity_mode": "required",
        },
        "common_mistakes": [
            "Using continuity.read fields as if they were context.retrieve fields.",
            "Persisting prompt text, retrieved snippets, or transcript material through context.retrieve.",
        ],
        "correction_hints": [
            "Use exactly task, subject_kind, subject_id, and continuity_mode in the minimal payload shape defined by this issue.",
            "Keep context.retrieve read-only and do not persist prompt or retrieval transcript material.",
        ],
    },
}

_TOPICS = {
    "continuity.read.startup_view": {
        "kind": "topic",
        "id": "continuity.read.startup_view",
        "purpose": "Explain the startup continuity view used to re-establish orientation.",
        "when_to_use": [
            "Use when selecting the startup view for continuity.read.",
            "Use when a runtime needs startup-oriented continuity guidance rather than a full raw capsule.",
        ],
        "read_operations": [
            "POST /v1/continuity/read",
            "continuity.read",
        ],
        "write_operations": [],
        "minimal_payload": {
            "subject_kind": "thread",
            "subject_id": "issue-214",
            "view": "startup",
            "allow_fallback": True,
        },
        "common_mistakes": [
            "Using startup_view as if it were a literal request value.",
            "Disabling allow_fallback when degraded startup continuity is acceptable.",
        ],
        "correction_hints": [
            "Use view: startup in the request payload.",
            "Set allow_fallback to true when fallback continuity is acceptable.",
        ],
    },
    "continuity.read.trust_signals": {
        "kind": "topic",
        "id": "continuity.read.trust_signals",
        "purpose": "Explain trust-oriented continuity signals surfaced by continuity.read.",
        "when_to_use": [
            "Use when interpreting trust signals returned with a continuity read.",
            "Use when a caller needs to distinguish healthy continuity from degraded continuity.",
        ],
        "read_operations": [
            "POST /v1/continuity/read",
            "continuity.read",
        ],
        "write_operations": [],
        "minimal_payload": {
            "subject_kind": "thread",
            "subject_id": "issue-214",
            "view": "startup",
            "allow_fallback": True,
        },
        "common_mistakes": [
            "Treating trust signals as a separate request field.",
            "Ignoring degraded trust signals when choosing the next runtime action.",
        ],
        "correction_hints": [
            "Read trust signals from the continuity.read response rather than inventing a request field.",
            "Use degraded trust signals to trigger a cautious or recovery-oriented next step.",
        ],
    },
    "continuity.upsert.session_end_snapshot": {
        "kind": "topic",
        "id": "continuity.upsert.session_end_snapshot",
        "purpose": "Explain the bounded session_end_snapshot helper for continuity.upsert.",
        "when_to_use": [
            "Use when persisting a startup-focused summary at session end.",
            "Use when the runtime needs to update startup-critical continuity fields without rebuilding the full capsule.",
        ],
        "read_operations": [
            "POST /v1/continuity/read",
            "continuity.read",
        ],
        "write_operations": [
            "POST /v1/continuity/upsert",
            "continuity.upsert",
        ],
        "minimal_payload": {
            "subject_kind": "thread",
            "subject_id": "issue-214",
            "capsule": {
                "updated_at": "2026-04-21T12:00:00Z",
                "open_loops": [],
                "top_priorities": [],
                "active_constraints": [],
                "stance_summary": "Ready to continue issue 214 work.",
            },
            "session_end_snapshot": {
                "open_loops": [],
                "top_priorities": [],
                "active_constraints": [],
                "stance_summary": "Ready to continue issue 214 work.",
                "negative_decisions": [],
                "session_trajectory": [],
                "rationale_entries": [],
            },
        },
        "common_mistakes": [
            "Sending session_end_snapshot without a base capsule.",
            "Sending fields in session_end_snapshot that are outside the closed field set in this issue.",
        ],
        "correction_hints": [
            "Include the base capsule and then provide session_end_snapshot as a bounded helper.",
            "Use only open_loops, top_priorities, active_constraints, stance_summary, negative_decisions, session_trajectory, and rationale_entries in session_end_snapshot.",
        ],
    },
}

_HOOKS = {
    "hooks": [
        {
            "id": "startup",
            "purpose": "Re-establish orientation at session start or agent re-entry.",
            "when_to_use": [
                "Use when a runtime is about to begin work and needs startup continuity guidance.",
            ],
            "read_operations": [
                "POST /v1/continuity/read",
                "continuity.read",
            ],
            "write_operations": [],
            "minimal_payload": {
                "subject_kind": "thread",
                "subject_id": "issue-214",
                "view": "startup",
                "allow_fallback": True,
            },
            "common_mistakes": [
                "Using a hook ID that is not one of the four canonical hook IDs in this issue.",
            ],
            "correction_hints": [
                "Use startup exactly for the startup hook.",
            ],
        },
        {
            "id": "pre_prompt",
            "purpose": "Retrieve bounded working context before a major work step.",
            "when_to_use": [
                "Use when the runtime is about to start a major work step and needs bounded retrieval.",
            ],
            "read_operations": [
                "POST /v1/context/retrieve",
                "context.retrieve",
            ],
            "write_operations": [],
            "minimal_payload": {
                "task": "Address determinism findings on issue #214 only.",
                "subject_kind": "thread",
                "subject_id": "issue-214",
                "continuity_mode": "required",
            },
            "common_mistakes": [
                "Using continuity.read fields as if pre_prompt were bound to continuity.read.",
                "Persisting prompt text, retrieved snippets, or transcript material through pre_prompt.",
            ],
            "correction_hints": [
                "Use exactly task, subject_kind, subject_id, and continuity_mode in the minimal payload shape defined by this issue.",
                "Keep pre_prompt read-only and do not persist prompt or retrieval transcript material.",
            ],
        },
        {
            "id": "post_prompt",
            "purpose": "Persist durable orientation changes caused by the completed work step.",
            "when_to_use": [
                "Use when a completed work step changed durable continuity state that should persist.",
            ],
            "read_operations": [],
            "write_operations": [
                "POST /v1/continuity/upsert",
                "continuity.upsert",
            ],
            "minimal_payload": {
                "subject_kind": "thread",
                "subject_id": "issue-214",
                "capsule": {
                    "updated_at": "2026-04-21T12:00:00Z",
                    "open_loops": [],
                    "top_priorities": [],
                    "active_constraints": [],
                    "stance_summary": "Ready to continue issue 214 work.",
                },
            },
            "common_mistakes": [
                "Treating post_prompt as read-oriented guidance.",
                "Using post_prompt as an interaction log or prompt/response summary sink.",
            ],
            "correction_hints": [
                "Use continuity.upsert only when a completed work step produced durable orientation state that should persist.",
                "Keep post_prompt focused on durable continuity rather than transcript material.",
            ],
        },
        {
            "id": "pre_compaction_or_handoff",
            "purpose": "Persist a bounded savepoint immediately before context loss, compaction, or a real inter-agent handoff boundary.",
            "when_to_use": [
                "Use when a runtime is about to compact local context or cross a real inter-agent handoff boundary.",
            ],
            "read_operations": [],
            "write_operations": [
                "POST /v1/continuity/upsert",
                "continuity.upsert",
            ],
            "additional_operations_for_real_handoff": [
                "POST /v1/coordination/handoff/create",
                "coordination.handoff_create",
            ],
            "minimal_payload": {
                "subject_kind": "thread",
                "subject_id": "issue-214",
                "capsule": {
                    "updated_at": "2026-04-21T12:00:00Z",
                    "open_loops": [],
                    "top_priorities": [],
                    "active_constraints": [],
                    "stance_summary": "Ready to continue issue 214 work.",
                },
                "session_end_snapshot": {
                    "open_loops": [],
                    "top_priorities": [],
                    "active_constraints": [],
                    "stance_summary": "Ready to continue issue 214 work.",
                    "negative_decisions": [],
                    "session_trajectory": [],
                    "rationale_entries": [],
                },
            },
            "common_mistakes": [
                "Using the deprecated hook spelling pre_compaction_handoff.",
                "Sending session_end_snapshot with fields outside the closed field set in this issue.",
                "Calling handoff creation before the local continuity step completes.",
            ],
            "correction_hints": [
                "Use pre_compaction_or_handoff exactly.",
                "Use only open_loops, top_priorities, active_constraints, stance_summary, negative_decisions, session_trajectory, and rationale_entries in session_end_snapshot.",
                "For a real inter-agent handoff, call coordination.handoff_create only after the local continuity step completes.",
            ],
        },
    ],
}

_ERRORS = {
    "validation": {
        "kind": "error",
        "id": "validation",
        "purpose": "Explain how to correct a contract-validation failure.",
        "when_to_use": [
            "Use when a request failed contract validation.",
        ],
        "common_mistakes": [
            "Guessing field names or allowed values from the error detail string alone.",
        ],
        "correction_hints": [
            "Inspect validation_hints and correct the named field directly.",
        ],
    },
    "tool_not_found": {
        "kind": "error",
        "id": "tool_not_found",
        "purpose": "Explain the meaning of the tool_not_found error-guide target.",
        "when_to_use": [
            "Use when reading help about the tool_not_found error class itself.",
        ],
        "common_mistakes": [
            "Treating tool_not_found as the rejection contract for unsupported tool names on the help surface.",
        ],
        "correction_hints": [
            "For unsupported tool names on the help surface, use the validation rejection contract defined in this issue.",
        ],
    },
    "unknown_help_topic": {
        "kind": "error",
        "id": "unknown_help_topic",
        "purpose": "Explain the meaning of the unknown_help_topic error-guide target.",
        "when_to_use": [
            "Use when reading help about the unknown_help_topic error class itself.",
        ],
        "common_mistakes": [
            "Treating unknown_help_topic as the rejection contract for unsupported topic IDs on the help surface.",
        ],
        "correction_hints": [
            "For unsupported topic IDs on the help surface, use the validation rejection contract defined in this issue.",
        ],
    },
}


def _copy(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a defensive deep copy of a frozen help payload."""
    return deepcopy(payload)


def _validation_error(
    *,
    field: str,
    detail: str,
    allowed_values: list[str],
    correction_hint: str,
) -> JSONResponse:
    """Build the exact slice-1 HTTP validation error body."""
    return JSONResponse(
        status_code=400,
        content={
            "error": {
                "code": "validation",
                "detail": detail,
                "validation_hints": [
                    {
                        "field": field,
                        "area": "request.path",
                        "reason": "unsupported_value",
                        "limit": None,
                        "allowed_values": allowed_values,
                        "correction_hint": correction_hint,
                    }
                ],
            }
        },
    )


def help_root_payload() -> dict[str, Any]:
    """Return the exact slice-1 HTTP help root body."""
    return _copy(_ROOT_BODY)


def is_forbidden_help_alias_path(path: str) -> bool:
    """Return ``True`` when a slash-suffixed alias targets the closed help surface."""
    if not path.startswith("/v1/help") or not path.endswith("/"):
        return False

    canonical_path = path[:-1]
    if canonical_path in _EXACT_HELP_PATHS:
        return True

    return any(
        canonical_path.startswith(prefix) and len(canonical_path) > len(prefix)
        for prefix in _PARAMETERIZED_HELP_PREFIXES
    )


def help_tool_payload(name: str) -> dict[str, Any] | JSONResponse:
    """Return the exact tool help body or the exact unsupported-tool validation error."""
    payload = _TOOLS.get(name)
    if payload is not None:
        return _copy(payload)
    return _validation_error(
        field="name",
        detail="Unsupported tool name.",
        allowed_values=list(_TOOL_IDS),
        correction_hint="Use one of: continuity.read, continuity.upsert, context.retrieve.",
    )


def help_topic_payload(topic_id: str) -> dict[str, Any] | JSONResponse:
    """Return the exact topic help body or the exact unsupported-topic validation error."""
    payload = _TOPICS.get(topic_id)
    if payload is not None:
        return _copy(payload)
    return _validation_error(
        field="id",
        detail="Unsupported topic id.",
        allowed_values=list(_TOPIC_IDS),
        correction_hint="Use one of: continuity.read.startup_view, continuity.read.trust_signals, continuity.upsert.session_end_snapshot.",
    )


def help_hooks_payload() -> dict[str, Any]:
    """Return the exact slice-1 HTTP hook guidance body."""
    return _copy(_HOOKS)


def help_error_payload(code: str) -> dict[str, Any] | JSONResponse:
    """Return the exact error help body or the exact unsupported-error validation error."""
    payload = _ERRORS.get(code)
    if payload is not None:
        return _copy(payload)
    return _validation_error(
        field="code",
        detail="Unsupported error code.",
        allowed_values=list(_ERROR_CODES),
        correction_hint="Use one of: validation, tool_not_found, unknown_help_topic.",
    )


_MCP_HELP_METHODS = (
    "system.help",
    "system.tool_usage",
    "system.topic_help",
    "system.hook_guide",
    "system.error_guide",
)

_MCP_ERROR_GUIDES = {
    -32700: {
        "title": "Parse error",
        "summary": "The request body must contain valid JSON before the server can apply JSON-RPC or MCP validation.",
    },
    -32600: {
        "title": "Invalid Request",
        "summary": "The JSON-RPC envelope itself is malformed, so fix the top-level request shape before retrying.",
    },
    -32601: {
        "title": "Method not found",
        "summary": "The method name is outside the recognized `#216` surface; correct the method string instead of retrying bootstrap.",
    },
    -32602: {
        "title": "Invalid params",
        "summary": "The method name is recognized, but the params shape or target value is invalid for that specific request.",
    },
    -32000: {
        "title": "Server not initialized",
        "summary": "Bootstrap is incomplete; complete `initialize` and then `notifications/initialized` before calling normal-operation methods.",
    },
    -32001: {
        "title": "Unauthorized",
        "summary": "The request needs valid bearer authentication before the addressed operation can proceed.",
    },
    -32002: {
        "title": "Forbidden",
        "summary": "The caller is authenticated or identified, but local policy, scope, or origin rules still block the requested operation.",
    },
    -32003: {
        "title": "Tool execution failed",
        "summary": "Request validation passed, but the tool failed during execution and should be retried only after correcting the underlying runtime issue.",
    },
    -32004: {
        "title": "Not Found",
        "summary": "The addressed object was missing only after request validation, auth, and target resolution had already succeeded.",
    },
}


def mcp_help_method_names() -> list[str]:
    """Return the exact slice-3 MCP help/reference request methods."""
    return list(_MCP_HELP_METHODS)


def is_mcp_help_method(name: str) -> bool:
    """Return whether *name* is one of the five slice-3 MCP help methods."""
    return name in _MCP_HELP_METHODS


def _mcp_result(structured_content: dict[str, Any]) -> dict[str, Any]:
    summary = str(structured_content["summary"])
    return {
        "content": [{"type": "text", "text": summary}],
        "structuredContent": structured_content,
    }


def _mcp_invalid_params(reason: str, *, field: str | None = None, **extra: Any) -> dict[str, Any]:
    data: dict[str, Any] = {"reason": reason}
    if field is not None:
        data["field"] = field
    data.update(extra)
    return data


def _ascii_whitespace_only(value: str) -> bool:
    return bool(value) and all(ch in {" ", "\t", "\n", "\r"} for ch in value)


def _validate_zero_param_method(name: str, params_present: bool, params: Any) -> dict[str, Any] | None:
    if not params_present:
        return None
    if not isinstance(params, dict):
        return _mcp_invalid_params("params must be an object")
    for key in params:
        return _mcp_invalid_params(f"unexpected {name} param", field=key)
    return None


def _validate_targeted_string_param(
    method: str,
    field_name: str,
    params_present: bool,
    params: Any,
) -> tuple[str | None, dict[str, Any] | None]:
    if not params_present or not isinstance(params, dict):
        return None, _mcp_invalid_params("params must be an object")
    for key in params:
        if key != field_name:
            return None, _mcp_invalid_params(f"unexpected {method} param", field=key)
    if field_name not in params:
        return None, _mcp_invalid_params(f"{field_name} is required")
    value = params[field_name]
    if not isinstance(value, str):
        return None, _mcp_invalid_params(f"{field_name} must be a non-empty string")
    if value == "" or _ascii_whitespace_only(value):
        return None, _mcp_invalid_params(f"{field_name} is required")
    return value, None


def _validate_error_code_param(params_present: bool, params: Any) -> tuple[int | None, dict[str, Any] | None]:
    if not params_present or not isinstance(params, dict):
        return None, _mcp_invalid_params("params must be an object")
    for key in params:
        if key != "code":
            return None, _mcp_invalid_params("unexpected system.error_guide param", field=key)
    if "code" not in params:
        return None, _mcp_invalid_params("code is required")
    code = params["code"]
    if isinstance(code, bool) or not isinstance(code, int):
        return None, _mcp_invalid_params("code must be an integer")
    return code, None


def resolve_mcp_help_method(
    name: str,
    *,
    params_present: bool,
    params: Any,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Resolve one slice-3 MCP help method into a result or an exact invalid-params body."""
    if name == "system.help":
        error = _validate_zero_param_method(name, params_present, params)
        if error is not None:
            return None, error
        return _mcp_result(
            {
                "surface": "help_index",
                "httpEquivalent": "/v1/help",
                "title": "CogniRelay Help Index",
                "summary": "Browse the canonical machine-facing help surfaces for tools, topics, hooks, and MCP error guidance.",
            }
        ), None

    if name == "system.hook_guide":
        error = _validate_zero_param_method(name, params_present, params)
        if error is not None:
            return None, error
        return _mcp_result(
            {
                "surface": "hook_guide",
                "httpEquivalent": "/v1/help/hooks",
                "title": "CogniRelay Hook Guide",
                "summary": "Review the canonical startup, prompt, persistence, and handoff hook guidance exposed by the HTTP help surface.",
            }
        ), None

    if name == "system.tool_usage":
        tool_name, error = _validate_targeted_string_param(name, "name", params_present, params)
        if error is not None:
            return None, error
        payload = help_tool_payload(tool_name or "")
        if isinstance(payload, JSONResponse):
            return None, _mcp_invalid_params("unknown tool", name=tool_name)
        return _mcp_result(
            {
                "surface": "tool_help",
                "httpEquivalent": f"/v1/help/tools/{tool_name}",
                "name": tool_name,
                "summary": str(payload["purpose"]),
            }
        ), None

    if name == "system.topic_help":
        topic_id, error = _validate_targeted_string_param(name, "id", params_present, params)
        if error is not None:
            return None, error
        payload = help_topic_payload(topic_id or "")
        if isinstance(payload, JSONResponse):
            return None, _mcp_invalid_params("unknown topic", id=topic_id)
        return _mcp_result(
            {
                "surface": "topic_help",
                "httpEquivalent": f"/v1/help/topics/{topic_id}",
                "id": topic_id,
                "title": topic_id,
                "summary": str(payload["purpose"]),
            }
        ), None

    code, error = _validate_error_code_param(params_present, params)
    if error is not None:
        return None, error
    guide = _MCP_ERROR_GUIDES.get(code)
    if guide is None:
        return None, _mcp_invalid_params("unknown error code", code=code)
    return _mcp_result(
        {
            "surface": "error_guide",
            "httpEquivalent": f"/v1/help/errors/{code}",
            "code": code,
            "title": guide["title"],
            "summary": guide["summary"],
        }
    ), None
