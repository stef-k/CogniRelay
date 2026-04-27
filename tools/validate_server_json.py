"""Validate root server.json against the MCP Registry JSON Schema."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import urlopen

try:
    import jsonschema
    from jsonschema.exceptions import SchemaError
except ImportError:  # pragma: no cover - exercised only when dev deps are absent
    jsonschema = None  # type: ignore[assignment]
    SchemaError = ValueError  # type: ignore[assignment,misc]


DEFAULT_TIMEOUT_SECONDS = 20


def json_pointer(parts: tuple[Any, ...]) -> str:
    """Return a JSON Pointer for a jsonschema error path."""
    if not parts:
        return "/"
    escaped = [str(part).replace("~", "~0").replace("/", "~1") for part in parts]
    return "/" + "/".join(escaped)


def load_json_path(path: Path, label: str) -> dict[str, Any]:
    """Load a JSON object from a local file."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"{label}: cannot read") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label}: invalid JSON at line {exc.lineno} column {exc.colno}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label}: root must be an object")
    return payload


def load_json_url(url: str, *, timeout: int) -> dict[str, Any]:
    """Load a JSON object from an HTTP(S) URL."""
    try:
        with urlopen(url, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise ValueError(f"{url}: HTTP {exc.code}") from exc
    except URLError as exc:
        raise ValueError(f"{url}: cannot fetch") from exc
    except TimeoutError as exc:
        raise ValueError(f"{url}: fetch timed out") from exc
    except UnicodeDecodeError as exc:
        raise ValueError(f"{url}: response is not UTF-8") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"{url}: invalid JSON at line {exc.lineno} column {exc.colno}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{url}: root must be an object")
    return payload


def load_schema(schema_source: str, *, root: Path, timeout: int) -> dict[str, Any]:
    """Load a JSON Schema from a local path or HTTP(S) URL."""
    parsed = urlparse(schema_source)
    if parsed.scheme in {"http", "https"}:
        return load_json_url(schema_source, timeout=timeout)
    path = Path(schema_source)
    if not path.is_absolute():
        path = root / path
    return load_json_path(path, schema_source)


def validate_server_json(server_json: dict[str, Any], schema: dict[str, Any]) -> list[str]:
    """Return deterministic, path-only validation error lines."""
    if jsonschema is None:
        raise ValueError("jsonschema is not installed; run pip install -r requirements-dev.txt")

    validator_cls = jsonschema.validators.validator_for(schema)
    validator_cls.check_schema(schema)
    validator = validator_cls(schema, format_checker=jsonschema.FormatChecker())
    errors = sorted(validator.iter_errors(server_json), key=lambda error: (tuple(error.path), tuple(error.schema_path)))
    return [f"server.json: schema validation failed at {json_pointer(tuple(error.path))}" for error in errors]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1], help="repository root containing server.json")
    parser.add_argument("--schema", help="schema path or URL; defaults to server.json $schema")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP schema fetch timeout in seconds")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    root = args.root.resolve()
    try:
        server_json = load_json_path(root / "server.json", "server.json")
        schema_source = args.schema or server_json.get("$schema")
        if not isinstance(schema_source, str) or not schema_source:
            raise ValueError("server.json: missing $schema")
        schema = load_schema(schema_source, root=root, timeout=args.timeout)
        errors = validate_server_json(server_json, schema)
    except (SchemaError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
