"""Exact MCP slice-2 runtime handling for POST /v1/mcp."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urlsplit

from fastapi import HTTPException
from pydantic import ValidationError

from app.auth import AuthContext
from app.help import is_mcp_help_method, resolve_mcp_help_method
from app.timestamps import format_iso, iso_now

SUPPORTED_PROTOCOL_VERSION = "2025-11-25"

_RECOGNIZED_METHODS = {
    "initialize",
    "ping",
    "notifications/initialized",
    "tools/list",
    "tools/call",
    "system.help",
    "system.tool_usage",
    "system.topic_help",
    "system.hook_guide",
    "system.error_guide",
}
_REQUEST_METHODS = _RECOGNIZED_METHODS - {"notifications/initialized"}
_NORMAL_OPERATION_METHODS = {
    "tools/list",
    "tools/call",
    "system.help",
    "system.tool_usage",
    "system.topic_help",
    "system.hook_guide",
    "system.error_guide",
}
_PLACEHOLDER_DESCRIPTIONS = {"", "tbd", "todo", "coming soon"}

_BOOTSTRAP_LOCK = threading.Lock()
_BOOTSTRAP_NONE = "pre_initialize"
_BOOTSTRAP_INITIALIZED = "post_initialize_pre_notification"
_BOOTSTRAP_READY = "ready"
_bootstrap_state: dict[str, str] = {}


@dataclass(frozen=True)
class McpHttpResponse:
    """HTTP-ready MCP response payload."""

    status_code: int
    body: dict[str, Any] | None = None
    headers: dict[str, str] = field(default_factory=dict)


def reset_bootstrap_state() -> None:
    """Reset the in-memory bootstrap state used by the MCP runtime."""
    with _BOOTSTRAP_LOCK:
        _bootstrap_state.clear()


def _response(status_code: int, body: dict[str, Any] | None = None, *, headers: dict[str, str] | None = None) -> McpHttpResponse:
    merged_headers = dict(headers or {})
    return McpHttpResponse(status_code=status_code, body=body, headers=merged_headers)


def _jsonrpc_success(request_id: Any, result: dict[str, Any]) -> McpHttpResponse:
    return _response(200, {"jsonrpc": "2.0", "id": request_id, "result": result})


def _jsonrpc_error(status_code: int, request_id: Any, code: int, message: str, data: dict[str, Any]) -> McpHttpResponse:
    return _response(
        status_code,
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": code,
                "message": message,
                "data": data,
            },
        },
    )


def _invalid_request(reason: str) -> McpHttpResponse:
    return _jsonrpc_error(400, None, -32600, "Invalid Request", {"reason": reason})


def _invalid_params(request_id: Any, reason: str, *, field: str | None = None) -> McpHttpResponse:
    data: dict[str, Any] = {"reason": reason}
    if field is not None:
        data["field"] = field
    return _jsonrpc_error(200, request_id, -32602, "Invalid params", data)


def _server_not_initialized(request_id: Any, required_step: str) -> McpHttpResponse:
    return _jsonrpc_error(
        200,
        request_id,
        -32000,
        "Server not initialized",
        {"required_step": required_step},
    )


def _method_not_found(request_id: Any, method: str) -> McpHttpResponse:
    return _jsonrpc_error(200, request_id, -32601, "Method not found", {"method": method})


def _bootstrap_key(
    authorization: str | None,
    x_forwarded_for: str | None,
    x_real_ip: str | None,
    request: Any,
) -> str | None:
    """Derive the narrowest bootstrap key allowed by slice 2.

    The hardened issue body closes the state machine but does not define a
    session carrier for anonymous callers in slice 2. Persist bootstrap state
    only when the caller presents an explicit Authorization identity; otherwise
    treat the request as anonymous and stateless.
    """

    auth_key = (authorization or "").strip()
    if auth_key:
        return f"auth:{auth_key}"

    return None


def _bootstrap_phase(key: str | None) -> str:
    if key is None:
        return _BOOTSTRAP_NONE
    with _BOOTSTRAP_LOCK:
        return _bootstrap_state.get(key, _BOOTSTRAP_NONE)


def _set_bootstrap_phase(key: str | None, phase: str) -> None:
    if key is None:
        return
    with _BOOTSTRAP_LOCK:
        _bootstrap_state[key] = phase


def _is_valid_request_id(value: Any) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, str):
        return True
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return value.is_integer()
    return False


def _ascii_whitespace_only(value: str) -> bool:
    return bool(value) and all(ch in {" ", "\t", "\n", "\r"} for ch in value)


def _origin_allowed(origin: str | None) -> bool:
    if origin is None:
        return True
    try:
        parts = urlsplit(origin)
    except ValueError:
        return False
    if parts.scheme not in {"http", "https"}:
        return False
    host = parts.hostname
    return host in {"localhost", "127.0.0.1", "::1"}


def _resolve_ref(schema: dict[str, Any], root_schema: dict[str, Any]) -> dict[str, Any]:
    ref = schema.get("$ref")
    if not isinstance(ref, str) or not ref.startswith("#/$defs/"):
        return schema
    name = ref[len("#/$defs/") :]
    defs = root_schema.get("$defs", {})
    if isinstance(defs, dict) and isinstance(defs.get(name), dict):
        return defs[name]
    return schema


def _detail(path: str, keyword: str, message: str) -> dict[str, str]:
    return {"path": path or "", "keyword": keyword, "message": message}


def _validate_schema_value(
    value: Any,
    schema: dict[str, Any],
    *,
    path: str,
    root_schema: dict[str, Any],
) -> list[dict[str, str]]:
    schema = _resolve_ref(schema, root_schema)
    errors: list[dict[str, str]] = []

    if "anyOf" in schema and isinstance(schema["anyOf"], list):
        variants = schema["anyOf"]
        for variant in variants:
            if isinstance(variant, dict) and not _validate_schema_value(value, variant, path=path, root_schema=root_schema):
                return []
        return [_detail(path, "anyOf", "value does not match any allowed schema")]

    if "oneOf" in schema and isinstance(schema["oneOf"], list):
        matches = 0
        for variant in schema["oneOf"]:
            if isinstance(variant, dict) and not _validate_schema_value(value, variant, path=path, root_schema=root_schema):
                matches += 1
        if matches == 1:
            return []
        return [_detail(path, "oneOf", "value must match exactly one allowed schema")]

    if "enum" in schema and isinstance(schema["enum"], list) and value not in schema["enum"]:
        return [_detail(path, "enum", "value is not one of the allowed choices")]

    if "const" in schema and value != schema["const"]:
        return [_detail(path, "const", "value does not match the required constant")]

    expected_type = schema.get("type")
    if expected_type == "object":
        if not isinstance(value, dict):
            return [_detail(path, "type", "value must be an object")]
        required = schema.get("required", [])
        if isinstance(required, list):
            for key in required:
                if isinstance(key, str) and key not in value:
                    errors.append(_detail(f"{path}/{key}", "required", f"'{key}' is required"))
        properties = schema.get("properties", {})
        if isinstance(properties, dict):
            for key, subschema in properties.items():
                if key in value and isinstance(subschema, dict):
                    errors.extend(
                        _validate_schema_value(
                            value[key],
                            subschema,
                            path=f"{path}/{key}",
                            root_schema=root_schema,
                        )
                    )
        if schema.get("additionalProperties") is False and isinstance(properties, dict):
            for key in value:
                if key not in properties:
                    errors.append(_detail(f"{path}/{key}", "additionalProperties", "additional property is not allowed"))
        return errors

    if expected_type == "array":
        if not isinstance(value, list):
            return [_detail(path, "type", "value must be an array")]
        min_items = schema.get("minItems")
        if isinstance(min_items, int) and len(value) < min_items:
            errors.append(_detail(path, "minItems", f"array must contain at least {min_items} item(s)"))
        max_items = schema.get("maxItems")
        if isinstance(max_items, int) and len(value) > max_items:
            errors.append(_detail(path, "maxItems", f"array must contain at most {max_items} item(s)"))
        items = schema.get("items")
        if isinstance(items, dict):
            for index, item in enumerate(value):
                errors.extend(
                    _validate_schema_value(
                        item,
                        items,
                        path=f"{path}/{index}",
                        root_schema=root_schema,
                    )
                )
        return errors

    if expected_type == "string":
        if not isinstance(value, str):
            return [_detail(path, "type", "value must be a string")]
        min_length = schema.get("minLength")
        if isinstance(min_length, int) and len(value) < min_length:
            errors.append(_detail(path, "minLength", f"string must be at least {min_length} character(s)"))
        max_length = schema.get("maxLength")
        if isinstance(max_length, int) and len(value) > max_length:
            errors.append(_detail(path, "maxLength", f"string must be at most {max_length} character(s)"))
        return errors

    if expected_type == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            return [_detail(path, "type", "value must be an integer")]
        return []

    if expected_type == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return [_detail(path, "type", "value must be a number")]
        return []

    if expected_type == "boolean":
        if not isinstance(value, bool):
            return [_detail(path, "type", "value must be a boolean")]
        return []

    if expected_type == "null":
        if value is not None:
            return [_detail(path, "type", "value must be null")]
        return []

    return []


def _validation_details_from_pydantic(exc: ValidationError) -> list[dict[str, str]]:
    details: list[dict[str, str]] = []
    for err in exc.errors():
        loc = err.get("loc", ())
        pointer = ""
        if isinstance(loc, tuple):
            pointer = "/" + "/".join(str(part) for part in loc)
        error_type = str(err.get("type", "validation_error"))
        keyword = {
            "missing": "required",
            "extra_forbidden": "additionalProperties",
            "literal_error": "enum",
        }.get(error_type, "type" if "type" in error_type or "parsing" in error_type else error_type)
        details.append(_detail(pointer, keyword, str(err.get("msg", "validation failed"))))
    return details


def _ensure_metadata_minimums(tools: list[dict[str, Any]]) -> None:
    seen_names: set[str] = set()
    for tool in tools:
        name = tool.get("name")
        description = tool.get("description")
        schema = tool.get("input_schema")
        if not isinstance(name, str) or not name:
            raise ValueError("tool name must be a non-empty string")
        if name in seen_names:
            raise ValueError(f"duplicate tool name: {name}")
        seen_names.add(name)
        if not isinstance(description, str) or description.strip().lower() in _PLACEHOLDER_DESCRIPTIONS:
            raise ValueError(f"tool description must be specific for {name}")
        if not isinstance(schema, dict):
            raise ValueError(f"tool input schema must be an object for {name}")
        if schema.get("type") != "object":
            raise ValueError(f"tool input schema must declare type=object for {name}")
        if not isinstance(schema.get("properties", {}), dict):
            raise ValueError(f"tool input schema must define properties for {name}")
        if "required" in schema and not isinstance(schema.get("required"), list):
            raise ValueError(f"tool input schema required must be a list for {name}")


def _tools_list_result(tools: list[dict[str, Any]]) -> dict[str, Any]:
    _ensure_metadata_minimums(tools)
    return {
        "tools": [
            {
                "name": tool["name"],
                "description": tool["description"],
                "inputSchema": tool["input_schema"],
                "metadata": {
                    "method": tool["method"],
                    "path": tool["path"],
                    "scopes": tool["scopes"],
                    "idempotent": tool["idempotent"],
                    "local_only": bool(tool.get("local_only", False)),
                },
            }
            for tool in tools
        ]
    }


def _tool_by_name(name: str, tools: list[dict[str, Any]]) -> dict[str, Any] | None:
    for tool in tools:
        if tool.get("name") == name:
            return tool
    return None


def _validate_initialize(request_id: Any, params: Any, server_version: str) -> McpHttpResponse | dict[str, Any]:
    if not isinstance(params, dict):
        return _invalid_params(request_id, "params must be an object")

    allowed_keys = {"protocolVersion", "capabilities", "clientInfo"}
    for key in params:
        if key not in allowed_keys:
            return _invalid_params(request_id, "unexpected initialize param", field=key)

    if "protocolVersion" not in params:
        return _invalid_params(request_id, "protocolVersion is required")
    protocol_version = params["protocolVersion"]
    if not isinstance(protocol_version, str):
        return _invalid_params(request_id, "protocolVersion must be a string")
    if protocol_version != SUPPORTED_PROTOCOL_VERSION:
        return _jsonrpc_error(
            200,
            request_id,
            -32602,
            "Unsupported protocol version",
            {"supported": [SUPPORTED_PROTOCOL_VERSION], "requested": protocol_version},
        )

    if "capabilities" in params and not isinstance(params["capabilities"], dict):
        return _invalid_params(request_id, "capabilities must be an object")

    if "clientInfo" in params:
        client_info = params["clientInfo"]
        if not isinstance(client_info, dict):
            return _invalid_params(request_id, "clientInfo must be an object")
        for key in client_info:
            if key not in {"name", "version"}:
                return _invalid_params(request_id, "unexpected clientInfo field", field=key)
        if "name" not in client_info:
            return _invalid_params(request_id, "clientInfo.name is required")
        if not isinstance(client_info.get("name"), str) or not str(client_info["name"]).strip():
            return _invalid_params(request_id, "clientInfo.name must be a non-empty string")
        if "version" in client_info and (
            not isinstance(client_info.get("version"), str) or not str(client_info["version"]).strip()
        ):
            return _invalid_params(request_id, "clientInfo.version must be a non-empty string")

    return {
        "protocolVersion": SUPPORTED_PROTOCOL_VERSION,
        "capabilities": {"tools": {"listChanged": False}},
        "serverInfo": {"name": "cognirelay", "version": server_version},
    }


def _validate_tools_list_params(request_id: Any, params: Any, *, params_present: bool) -> McpHttpResponse | dict[str, Any]:
    if not params_present:
        effective: dict[str, Any] = {}
    else:
        effective = params
    if not isinstance(effective, dict):
        return _invalid_params(request_id, "params must be an object")
    for key in effective:
        if key != "cursor":
            return _invalid_params(request_id, "unexpected tools/list param", field=key)
    cursor = effective.get("cursor")
    if cursor is None or cursor == "":
        return {}
    if not isinstance(cursor, str):
        return _invalid_params(request_id, "cursor must be a string or null")
    return _invalid_params(request_id, "cursor pagination is not supported in slice 2")


def _validate_tools_call_params(request_id: Any, params: Any) -> McpHttpResponse | tuple[str, bool, Any]:
    if not isinstance(params, dict):
        return _invalid_params(request_id, "params must be an object")
    for key in params:
        if key not in {"name", "arguments"}:
            return _invalid_params(request_id, "unexpected tools/call param", field=key)
    if "name" not in params:
        return _invalid_params(request_id, "name is required")
    name = params.get("name")
    if not isinstance(name, str):
        return _invalid_params(request_id, "name must be a non-empty string")
    if name == "" or _ascii_whitespace_only(name):
        return _invalid_params(request_id, "name is required")
    return name, "arguments" in params, params.get("arguments")


def handle_mcp_request_payload(
    request_payload: Any,
    *,
    origin: str | None,
    authorization: str | None,
    x_forwarded_for: str | None,
    x_real_ip: str | None,
    request: Any,
    server_version: str,
    tools: list[dict[str, Any]],
    resolve_auth_context: Callable[..., AuthContext | None],
    invoke_tool_by_name: Callable[[str, dict[str, Any], AuthContext | None], dict[str, Any]],
) -> McpHttpResponse:
    """Handle one already-parsed MCP request payload."""
    if not _origin_allowed(origin):
        return _jsonrpc_error(
            403,
            None,
            -32002,
            "Forbidden",
            {"reason": "origin not allowed", "origin": str(origin)},
        )

    if isinstance(request_payload, list):
        return _invalid_request("batch requests are not supported")
    if not isinstance(request_payload, dict):
        return _invalid_request("request body must be a JSON object")

    if request_payload.get("jsonrpc") != "2.0":
        return _invalid_request('jsonrpc must be exactly "2.0"')

    method = request_payload.get("method")
    if not isinstance(method, str):
        return _invalid_request("method must be a string")

    if method == "notifications/initialized":
        if "id" in request_payload:
            return _invalid_request("notifications/initialized is notification-only")
        request_id = None
    else:
        if "id" not in request_payload:
            return _invalid_request("id is required for this method")
        request_id = request_payload.get("id")
        if not _is_valid_request_id(request_id):
            return _invalid_request("id must be a string or integer")

    if method not in _RECOGNIZED_METHODS:
        return _method_not_found(request_id, method)

    bootstrap_key = _bootstrap_key(authorization, x_forwarded_for, x_real_ip, request)
    phase = _bootstrap_phase(bootstrap_key)

    if method == "initialize":
        if phase != _BOOTSTRAP_NONE:
            return _server_not_initialized(request_id, "notifications/initialized")
        result = _validate_initialize(request_id, request_payload.get("params"), server_version)
        if isinstance(result, McpHttpResponse):
            return result
        _set_bootstrap_phase(bootstrap_key, _BOOTSTRAP_INITIALIZED)
        return _jsonrpc_success(request_id, result)

    if method == "notifications/initialized":
        if phase == _BOOTSTRAP_INITIALIZED:
            _set_bootstrap_phase(bootstrap_key, _BOOTSTRAP_READY)
        return _response(204, None)

    if method == "ping":
        return _jsonrpc_success(request_id, {"ok": True, "ts": format_iso(iso_now())})

    if method in _NORMAL_OPERATION_METHODS:
        if phase == _BOOTSTRAP_NONE:
            return _server_not_initialized(request_id, "initialize")
        if phase == _BOOTSTRAP_INITIALIZED:
            return _server_not_initialized(request_id, "notifications/initialized")

    if is_mcp_help_method(method):
        result, validation_error = resolve_mcp_help_method(
            method,
            params_present="params" in request_payload,
            params=request_payload.get("params"),
        )
        if validation_error is not None:
            return _jsonrpc_error(200, request_id, -32602, "Invalid params", validation_error)
        return _jsonrpc_success(request_id, result or {})

    if method != "tools/list" and method != "tools/call":
        return _method_not_found(request_id, method)

    if method == "tools/list":
        params_result = _validate_tools_list_params(
            request_id,
            request_payload.get("params"),
            params_present="params" in request_payload,
        )
        if isinstance(params_result, McpHttpResponse):
            return params_result
        return _jsonrpc_success(request_id, _tools_list_result(tools))

    call_params = _validate_tools_call_params(request_id, request_payload.get("params"))
    if isinstance(call_params, McpHttpResponse):
        return call_params
    tool_name, arguments_present, raw_arguments = call_params

    tool = _tool_by_name(tool_name, tools)
    if tool is None:
        return _jsonrpc_error(200, request_id, -32602, "Invalid params", {"reason": "unknown tool", "name": tool_name})

    arguments = raw_arguments if arguments_present else {}
    if not isinstance(arguments, dict):
        return _invalid_params(request_id, "arguments must be an object")

    schema = tool.get("input_schema")
    if isinstance(schema, dict):
        schema_errors = _validate_schema_value(arguments, schema, path="", root_schema=schema)
        if schema_errors:
            return _jsonrpc_error(
                200,
                request_id,
                -32602,
                "Invalid params",
                {"reason": "schema validation failed", "details": schema_errors},
            )

    auth_required = bool(tool.get("scopes"))
    try:
        auth = resolve_auth_context(
            authorization,
            required=auth_required,
            x_forwarded_for=x_forwarded_for,
            x_real_ip=x_real_ip,
            request=request,
        )
        result = invoke_tool_by_name(tool_name, arguments, auth)
    except ValidationError as exc:
        details = _validation_details_from_pydantic(exc)
        return _jsonrpc_error(
            200,
            request_id,
            -32602,
            "Invalid params",
            {"reason": "schema validation failed", "details": details or [_detail("", "validation", "validation failed")]},
        )
    except HTTPException as exc:
        if exc.status_code == 401:
            return _jsonrpc_error(200, request_id, -32001, "Unauthorized", {"reason": "authentication required"})
        if exc.status_code == 403:
            return _jsonrpc_error(200, request_id, -32002, "Forbidden", {"reason": "forbidden"})
        if exc.status_code == 404:
            return _jsonrpc_error(200, request_id, -32004, "Not Found", {"reason": "not found"})
        return _jsonrpc_error(200, request_id, -32003, "Tool execution failed", {"reason": "tool execution failed"})
    except Exception:
        return _jsonrpc_error(200, request_id, -32003, "Tool execution failed", {"reason": "tool execution failed"})

    return _jsonrpc_success(
        request_id,
        {
            "content": [{"type": "text", "text": f"Executed {tool_name}"}],
            "structuredContent": result if isinstance(result, dict) else {},
        },
    )


def handle_mcp_http_request(
    raw_body: bytes,
    *,
    origin: str | None,
    authorization: str | None,
    x_forwarded_for: str | None,
    x_real_ip: str | None,
    request: Any,
    server_version: str,
    tools: list[dict[str, Any]],
    resolve_auth_context: Callable[..., AuthContext | None],
    invoke_tool_by_name: Callable[[str, dict[str, Any], AuthContext | None], dict[str, Any]],
) -> McpHttpResponse:
    """Parse and dispatch one raw HTTP MCP request body."""
    if not _origin_allowed(origin):
        return _jsonrpc_error(
            403,
            None,
            -32002,
            "Forbidden",
            {"reason": "origin not allowed", "origin": str(origin)},
        )
    try:
        parsed = json.loads(raw_body)
    except json.JSONDecodeError:
        return _jsonrpc_error(400, None, -32700, "Parse error", {"reason": "request body must be valid JSON"})
    return handle_mcp_request_payload(
        parsed,
        origin=origin,
        authorization=authorization,
        x_forwarded_for=x_forwarded_for,
        x_real_ip=x_real_ip,
        request=request,
        server_version=server_version,
        tools=tools,
        resolve_auth_context=resolve_auth_context,
        invoke_tool_by_name=invoke_tool_by_name,
    )
