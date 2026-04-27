#!/usr/bin/env python3
"""Validate and update local CogniRelay release/version surfaces."""

from __future__ import annotations

import argparse
import datetime as _datetime
import json
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SURFACES = (
    "app_version",
    "pyproject_version",
    "pyproject_dependencies",
    "server_json_version",
    "server_json_description",
    "server_json_package_version",
    "server_json_package_identifier",
    "server_json_package_arguments",
    "server_json_transport",
    "server_json_environment_variables",
    "mcp_ownership_marker",
    "changelog",
    "release_notes",
    "docs_index",
    "publishable_tree_safety",
)
VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
CHANGELOG_HEADING_RE = re.compile(r"^## \[([0-9]+\.[0-9]+\.[0-9]+)\] - ([0-9]{4}-[0-9]{2}-[0-9]{2})$", re.MULTILINE)
APP_VERSION_RE = re.compile(r"(FastAPI\([^)]*\bversion=)([\"'])([^\"']+)(\2)", re.DOTALL)
PROJECT_VERSION_RE = re.compile(r"(^\[project\]\s*?.*?^version\s*=\s*)([\"'])([^\"']+)(\2)", re.MULTILINE | re.DOTALL)
LATEST_RE = re.compile(r"^- \[Latest release notes: v([0-9]+\.[0-9]+\.[0-9]+)\]\(releases/v\1\.md\)$")
NORMAL_RELEASE_RE = re.compile(r"^- \[v([0-9]+\.[0-9]+\.[0-9]+) release notes\]\(releases/v\1\.md\)$")
SERVER_JSON_DESCRIPTION = "Self-hosted agent continuity server with bounded, recoverable memory and restart orientation."
SERVER_JSON_PACKAGE_IDENTIFIER = "cognirelay"
SERVER_JSON_PACKAGE_ARGUMENTS = [{"type": "positional", "value": "serve"}]
SERVER_JSON_TRANSPORT = {"type": "streamable-http", "url": "http://127.0.0.1:8080/v1/mcp"}
SERVER_JSON_ENVIRONMENT_VARIABLES = [
    {
        "name": "COGNIRELAY_REPO_ROOT",
        "description": "Path to a durable writable CogniRelay repository root for runtime state.",
        "isRequired": True,
        "format": "filepath",
        "isSecret": False,
    }
]
MCP_OWNERSHIP_MARKER = "mcp-name: io.github.stef-k/cognirelay"
ALLOWED_ENV_TEMPLATES = {".env.example", "deploy/systemd/cognirelay.env.example"}
FORBIDDEN_DIR_PREFIXES = ("data_repo/", "memory/", "logs/", ".locks/", "dist/", "build/")
FORBIDDEN_DB_SUFFIXES = (
    ".db",
    ".db-wal",
    ".db-shm",
    ".db-journal",
    ".sqlite",
    ".sqlite-wal",
    ".sqlite-shm",
    ".sqlite-journal",
    ".sqlite3",
    ".sqlite3-wal",
    ".sqlite3-shm",
    ".sqlite3-journal",
)
FORBIDDEN_RUNTIME_SUFFIXES = (".log", ".jsonl", ".bak", ".tmp")
FORBIDDEN_SECRET_SUFFIXES = (".pem", ".key", ".token")
FORBIDDEN_RUNTIME_FILENAMES = {"api_audit.jsonl", "peer_tokens.json"}
FORBIDDEN_BUILD_RESIDUE_NAMES = {"dist", "build"}
RESIDUE_SCAN_PRUNE_DIRS = {".git", ".venv", ".pytest_cache", ".ruff_cache", ".mypy_cache", "__pycache__"}


CONTENT_ERROR_CODES = {
    "version_mismatch",
    "changelog_release_missing",
    "changelog_date_mismatch",
    "changelog_release_duplicate",
    "changelog_release_out_of_order",
    "release_notes_missing",
    "release_notes_conflict",
    "docs_latest_mismatch",
    "docs_previous_latest_missing",
    "docs_duplicate_release_link",
    "docs_release_link_order",
    "pyproject_version_mismatch",
    "pyproject_dependency_mismatch",
    "server_json_version_mismatch",
    "server_json_package_version_mismatch",
    "server_json_package_identifier_mismatch",
    "server_json_transport_mismatch",
    "server_json_package_arguments_mismatch",
    "server_json_environment_variables_mismatch",
    "server_json_description_mismatch",
    "mcp_ownership_marker_missing",
    "publishable_tree_forbidden_file",
}
VALIDATION_ERROR_CODES = {
    "invalid_args",
    "invalid_version",
    "invalid_date",
    "invalid_title",
    "dirty_worktree",
    "not_git_worktree",
}


@dataclass(frozen=True)
class SurfaceError(Exception):
    """Structured release helper error for one local surface."""

    code: str
    message: str
    surface: str | None = None
    path: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "surface": self.surface, "path": self.path}


@dataclass(frozen=True)
class PlannedWrite:
    """A validated file update to apply after all surfaces have been checked."""

    path: Path
    surface: str
    content: str


@dataclass(frozen=True)
class WriteSnapshot:
    """Prior file state captured before applying planned writes."""

    existed: bool
    content: str | None


@dataclass(frozen=True)
class ChangelogReleaseHeading:
    """A changelog release heading outside fenced code blocks."""

    version: str
    date: str
    start: int
    end: int


class JsonArgumentParser(argparse.ArgumentParser):
    """ArgumentParser variant that raises instead of exiting."""

    def error(self, message: str) -> None:
        raise SurfaceError("invalid_args", message)


def standard_result(
    *,
    ok: bool,
    mode: str | None,
    version: str | None,
    date: str | None,
    dry_run: bool = False,
    checked: list[dict[str, Any]] | None = None,
    updated: list[dict[str, Any]] | None = None,
    warnings: list[dict[str, Any]] | None = None,
    errors: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return the standard JSON-compatible result object."""
    tag = f"v{version}" if version else None
    return {
        "ok": ok,
        "mode": mode,
        "version": version,
        "tag": tag,
        "date": date,
        "dry_run": dry_run,
        "checked": checked or [],
        "updated": updated or [],
        "warnings": warnings or [],
        "errors": errors or [],
    }


def error_result(error: SurfaceError, *, mode: str | None = None, version: str | None = None, date: str | None = None, dry_run: bool = False) -> dict[str, Any]:
    """Return a standard failure result containing a single error."""
    return standard_result(ok=False, mode=mode, version=version, date=date, dry_run=dry_run, errors=[error.as_dict()])


def update_entry(surface: str, path: str, action: str, *, dry_run: bool, would_write: bool) -> dict[str, Any]:
    """Return an ordered update entry, including dry-run write intent when needed."""
    entry: dict[str, Any] = {"surface": surface, "path": path, "action": action}
    if dry_run:
        entry["would_write"] = would_write
    return entry


def exit_code_for(result: dict[str, Any]) -> int:
    """Classify the process exit code for a standard result."""
    if result["ok"]:
        return 0
    codes = {error["code"] for error in result["errors"]}
    if codes and codes <= VALIDATION_ERROR_CODES:
        return 2
    if codes and all(code in CONTENT_ERROR_CODES for code in codes):
        return 1
    return 3


def validate_version(value: str) -> str:
    """Return a normalized version or raise a validation error."""
    if not VERSION_RE.fullmatch(value):
        raise SurfaceError("invalid_version", "--version must match X.Y.Z")
    return value


def validate_date(value: str) -> str:
    """Return an ISO release date after calendar validation."""
    try:
        return _datetime.date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise SurfaceError("invalid_date", "--date must be a valid YYYY-MM-DD date") from exc


def validate_title(value: str | None) -> str:
    """Return a stripped single-line title or raise a validation error."""
    title = (value or "").strip()
    if not title or "\n" in title or "\r" in title or CONTROL_RE.search(title):
        raise SurfaceError("invalid_title", "--title must be non-empty, single-line, and contain no control characters")
    return title


def today_utc() -> str:
    """Return the current UTC calendar date."""
    return _datetime.datetime.now(_datetime.timezone.utc).date().isoformat()


def rel_path(path: Path, root: Path) -> str:
    """Return a POSIX path relative to the repository root."""
    return path.relative_to(root).as_posix()


def read_text(path: Path, root: Path, surface: str) -> str:
    """Read a required UTF-8 file or raise a structured IO error."""
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SurfaceError("read_failed", "cannot read required file", surface, rel_path(path, root)) from exc


def write_text(path: Path, root: Path, surface: str, content: str) -> None:
    """Write a UTF-8 file or raise a structured IO error."""
    try:
        path.write_text(content, encoding="utf-8")
    except OSError as exc:
        raise SurfaceError("write_failed", "cannot write required file", surface, rel_path(path, root)) from exc


def ensure_parent(path: Path, root: Path, surface: str) -> list[Path]:
    """Create a file parent directory when the contract allows it."""
    parent = path.parent
    missing: list[Path] = []
    current = parent
    while current != root and not current.exists():
        missing.append(current)
        current = current.parent
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise SurfaceError("write_failed", "cannot create required parent directory", surface, rel_path(parent, root)) from exc
    return missing


def check_app_version(root: Path, version: str) -> dict[str, Any] | SurfaceError:
    """Validate the FastAPI app version assignment."""
    path = root / "app" / "main.py"
    text = read_text(path, root, "app_version")
    match = APP_VERSION_RE.search(text)
    if not match:
        raise SurfaceError("app_version_parse_failed", "FastAPI app version assignment was not found", "app_version", rel_path(path, root))
    if match.group(3) != version:
        return SurfaceError("version_mismatch", f"FastAPI app version is {match.group(3)}, expected {version}", "app_version", rel_path(path, root))
    return {"surface": "app_version", "path": rel_path(path, root), "ok": True}


def update_app_version(root: Path, version: str, *, dry_run: bool) -> tuple[dict[str, Any], dict[str, Any] | SurfaceError, PlannedWrite | None]:
    """Plan a FastAPI app version assignment update if needed."""
    path = root / "app" / "main.py"
    text = read_text(path, root, "app_version")
    match = APP_VERSION_RE.search(text)
    if not match:
        raise SurfaceError("app_version_parse_failed", "FastAPI app version assignment was not found", "app_version", rel_path(path, root))
    checked = {"surface": "app_version", "path": rel_path(path, root), "ok": match.group(3) == version}
    if match.group(3) == version:
        return checked, update_entry("app_version", rel_path(path, root), "unchanged", dry_run=dry_run, would_write=False), None
    updated_text = text[: match.start(3)] + version + text[match.end(3) :]
    return checked, update_entry("app_version", rel_path(path, root), "updated", dry_run=dry_run, would_write=True), PlannedWrite(path, "app_version", updated_text)


def read_toml(path: Path, root: Path, surface: str) -> dict[str, Any]:
    """Read a TOML file or raise a structured surface error."""
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise SurfaceError("read_failed", f"cannot read {rel_path(path, root)}", surface, rel_path(path, root)) from exc


def runtime_requirements(root: Path) -> list[str] | SurfaceError:
    """Return runtime dependencies from requirements.txt using the #290 sync rule."""
    path = root / "requirements.txt"
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise SurfaceError("read_failed", f"cannot read {rel_path(path, root)}", "pyproject_dependencies", rel_path(path, root)) from exc
    dependencies: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith(("-r", "--", "-e")) or "#" in stripped:
            return SurfaceError("pyproject_dependency_mismatch", "runtime requirements contain unsupported packaging syntax", "pyproject_dependencies", rel_path(path, root))
        dependencies.append(stripped)
    return dependencies


def pyproject_project(root: Path) -> tuple[Path, dict[str, Any]]:
    """Return the pyproject path and parsed project table."""
    path = root / "pyproject.toml"
    data = read_toml(path, root, "pyproject_version")
    project = data.get("project")
    if not isinstance(project, dict):
        raise SurfaceError("pyproject_version_mismatch", "pyproject project metadata is missing", "pyproject_version", rel_path(path, root))
    return path, project


def pyproject_readme_path(root: Path) -> Path:
    """Return the validated project.readme file path from pyproject.toml."""
    path, project = pyproject_project(root)
    readme = project.get("readme")
    if not isinstance(readme, str) or not readme:
        raise SurfaceError("mcp_ownership_marker_missing", "invalid PyPI long-description source", "mcp_ownership_marker", rel_path(path, root))
    readme_path = Path(readme)
    if readme_path.is_absolute():
        raise SurfaceError("mcp_ownership_marker_missing", "invalid PyPI long-description source", "mcp_ownership_marker", rel_path(path, root))
    root_resolved = root.resolve()
    resolved = (root_resolved / readme_path).resolve()
    try:
        normalized_readme = resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise SurfaceError("mcp_ownership_marker_missing", "invalid PyPI long-description source", "mcp_ownership_marker", rel_path(path, root)) from exc
    return root / normalized_readme


def check_pyproject_version(root: Path, version: str) -> dict[str, Any] | SurfaceError:
    """Validate pyproject project.version."""
    path, project = pyproject_project(root)
    if project.get("version") != version:
        return SurfaceError("pyproject_version_mismatch", "pyproject version does not match requested version", "pyproject_version", rel_path(path, root))
    return {"surface": "pyproject_version", "path": rel_path(path, root), "ok": True}


def check_pyproject_dependencies(root: Path) -> dict[str, Any] | SurfaceError:
    """Validate pyproject dependencies against requirements.txt."""
    path, project = pyproject_project(root)
    expected = runtime_requirements(root)
    if isinstance(expected, SurfaceError):
        return expected
    actual = project.get("dependencies")
    if actual != expected:
        return SurfaceError("pyproject_dependency_mismatch", "pyproject dependencies do not match runtime requirements", "pyproject_dependencies", rel_path(path, root))
    return {"surface": "pyproject_dependencies", "path": rel_path(path, root), "ok": True}


def update_pyproject_version(root: Path, version: str, *, dry_run: bool) -> tuple[dict[str, Any], dict[str, Any] | SurfaceError, PlannedWrite | None]:
    """Plan a pyproject project.version update if needed."""
    path = root / "pyproject.toml"
    text = read_text(path, root, "pyproject_version")
    match = PROJECT_VERSION_RE.search(text)
    if not match:
        raise SurfaceError("pyproject_version_mismatch", "pyproject version assignment was not found", "pyproject_version", rel_path(path, root))
    checked = {"surface": "pyproject_version", "path": rel_path(path, root), "ok": match.group(3) == version}
    if match.group(3) == version:
        return checked, update_entry("pyproject_version", rel_path(path, root), "unchanged", dry_run=dry_run, would_write=False), None
    updated_text = text[: match.start(3)] + version + text[match.end(3) :]
    return checked, update_entry("pyproject_version", rel_path(path, root), "updated", dry_run=dry_run, would_write=True), PlannedWrite(path, "pyproject_version", updated_text)


def read_server_json(root: Path) -> tuple[Path, dict[str, Any]]:
    """Return server.json path and parsed payload."""
    path = root / "server.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SurfaceError("read_failed", f"cannot read {rel_path(path, root)}", "server_json_version", rel_path(path, root)) from exc
    if not isinstance(data, dict):
        raise SurfaceError("server_json_version_mismatch", "server.json root object is invalid", "server_json_version", rel_path(path, root))
    return path, data


def server_json_package(data: dict[str, Any], root: Path, path: Path, surface: str, code: str) -> dict[str, Any] | SurfaceError:
    """Return the single PyPI package entry from server.json."""
    packages = data.get("packages")
    if not isinstance(packages, list) or len(packages) != 1 or not isinstance(packages[0], dict):
        return SurfaceError(code, "server.json package metadata is invalid", surface, rel_path(path, root))
    return packages[0]


def check_server_json_version(root: Path, version: str) -> dict[str, Any] | SurfaceError:
    path, data = read_server_json(root)
    if data.get("version") != version:
        return SurfaceError("server_json_version_mismatch", "server.json version does not match requested version", "server_json_version", rel_path(path, root))
    return {"surface": "server_json_version", "path": rel_path(path, root), "ok": True}


def check_server_json_description(root: Path) -> dict[str, Any] | SurfaceError:
    path, data = read_server_json(root)
    if data.get("description") != SERVER_JSON_DESCRIPTION:
        return SurfaceError("server_json_description_mismatch", "server.json description is not the expected public value", "server_json_description", rel_path(path, root))
    return {"surface": "server_json_description", "path": rel_path(path, root), "ok": True}


def check_server_json_package_version(root: Path, version: str) -> dict[str, Any] | SurfaceError:
    path, data = read_server_json(root)
    package = server_json_package(data, root, path, "server_json_package_version", "server_json_package_version_mismatch")
    if isinstance(package, SurfaceError):
        return package
    if package.get("version") != version:
        return SurfaceError("server_json_package_version_mismatch", "server.json package version does not match requested version", "server_json_package_version", rel_path(path, root))
    return {"surface": "server_json_package_version", "path": rel_path(path, root), "ok": True}


def check_server_json_package_identifier(root: Path) -> dict[str, Any] | SurfaceError:
    path, data = read_server_json(root)
    package = server_json_package(data, root, path, "server_json_package_identifier", "server_json_package_identifier_mismatch")
    if isinstance(package, SurfaceError):
        return package
    if package.get("identifier") != SERVER_JSON_PACKAGE_IDENTIFIER:
        return SurfaceError("server_json_package_identifier_mismatch", "server.json package identifier is not expected", "server_json_package_identifier", rel_path(path, root))
    return {"surface": "server_json_package_identifier", "path": rel_path(path, root), "ok": True}


def check_server_json_package_arguments(root: Path) -> dict[str, Any] | SurfaceError:
    path, data = read_server_json(root)
    package = server_json_package(data, root, path, "server_json_package_arguments", "server_json_package_arguments_mismatch")
    if isinstance(package, SurfaceError):
        return package
    if package.get("packageArguments") != SERVER_JSON_PACKAGE_ARGUMENTS:
        return SurfaceError("server_json_package_arguments_mismatch", "server.json package arguments are not expected", "server_json_package_arguments", rel_path(path, root))
    return {"surface": "server_json_package_arguments", "path": rel_path(path, root), "ok": True}


def check_server_json_transport(root: Path) -> dict[str, Any] | SurfaceError:
    path, data = read_server_json(root)
    package = server_json_package(data, root, path, "server_json_transport", "server_json_transport_mismatch")
    if isinstance(package, SurfaceError):
        return package
    if package.get("transport") != SERVER_JSON_TRANSPORT:
        return SurfaceError("server_json_transport_mismatch", "server.json transport is not expected", "server_json_transport", rel_path(path, root))
    return {"surface": "server_json_transport", "path": rel_path(path, root), "ok": True}


def check_server_json_environment_variables(root: Path) -> dict[str, Any] | SurfaceError:
    path, data = read_server_json(root)
    package = server_json_package(data, root, path, "server_json_environment_variables", "server_json_environment_variables_mismatch")
    if isinstance(package, SurfaceError):
        return package
    if package.get("environmentVariables") != SERVER_JSON_ENVIRONMENT_VARIABLES:
        return SurfaceError("server_json_environment_variables_mismatch", "server.json environment variables are not expected", "server_json_environment_variables", rel_path(path, root))
    return {"surface": "server_json_environment_variables", "path": rel_path(path, root), "ok": True}


def check_mcp_ownership_marker(root: Path) -> dict[str, Any] | SurfaceError:
    """Validate the public MCP ownership marker in the PyPI long description."""
    path = pyproject_readme_path(root)
    text = read_text(path, root, "mcp_ownership_marker")
    if MCP_OWNERSHIP_MARKER not in text:
        return SurfaceError("mcp_ownership_marker_missing", "MCP ownership marker is missing", "mcp_ownership_marker", rel_path(path, root))
    return {"surface": "mcp_ownership_marker", "path": rel_path(path, root), "ok": True}


def update_server_json_versions(root: Path, version: str, *, dry_run: bool) -> tuple[dict[str, Any], dict[str, Any] | SurfaceError, PlannedWrite | None]:
    """Plan server.json top-level and package version updates if needed."""
    path, data = read_server_json(root)
    package = server_json_package(data, root, path, "server_json_version", "server_json_version_mismatch")
    if isinstance(package, SurfaceError):
        return {"surface": "server_json_version", "path": rel_path(path, root), "ok": False}, package, None
    ok = data.get("version") == version and package.get("version") == version
    checked = {"surface": "server_json_version", "path": rel_path(path, root), "ok": ok}
    if ok:
        return checked, update_entry("server_json_version", rel_path(path, root), "unchanged", dry_run=dry_run, would_write=False), None
    updated = dict(data)
    updated["version"] = version
    updated_packages = list(updated["packages"])
    updated_package = dict(updated_packages[0])
    updated_package["version"] = version
    updated_packages[0] = updated_package
    updated["packages"] = updated_packages
    content = json.dumps(updated, indent=2) + "\n"
    return checked, update_entry("server_json_version", rel_path(path, root), "updated", dry_run=dry_run, would_write=True), PlannedWrite(path, "server_json_version", content)


def changelog_release_headings(text: str, start: int = 0) -> list[ChangelogReleaseHeading]:
    """Return changelog release headings outside fenced code blocks."""
    headings: list[ChangelogReleaseHeading] = []
    in_fence = False
    line_start = 0
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
        elif not in_fence and line_start >= start:
            match = CHANGELOG_HEADING_RE.fullmatch(line.rstrip("\r\n"))
            if match:
                headings.append(ChangelogReleaseHeading(match.group(1), match.group(2), line_start, line_start + len(line)))
        line_start += len(line)
    return headings


def changelog_sections(text: str, root: Path, path: Path) -> tuple[re.Match[str], ChangelogReleaseHeading]:
    """Return the Unreleased heading and first older release heading."""
    unreleased = re.search(r"^## \[Unreleased\]$", text, re.MULTILINE)
    if not unreleased:
        raise SurfaceError("changelog_parse_failed", "missing ## [Unreleased] heading", "changelog", rel_path(path, root))
    older_headings = changelog_release_headings(text, unreleased.end())
    if not older_headings:
        raise SurfaceError("changelog_parse_failed", "missing older release heading after ## [Unreleased]", "changelog", rel_path(path, root))
    return unreleased, older_headings[0]


def check_changelog(root: Path, version: str, date: str) -> dict[str, Any] | SurfaceError:
    """Validate the requested changelog release heading."""
    path = root / "CHANGELOG.md"
    text = read_text(path, root, "changelog")
    unreleased, first_release = changelog_sections(text, root, path)
    release_headings = changelog_release_headings(text, unreleased.end())
    heading = f"## [{version}] - {date}"
    if first_release.version == version and first_release.date == date:
        if any(release.version == version for release in release_headings[1:]):
            return SurfaceError("changelog_release_duplicate", f"changelog release {version} appears more than once", "changelog", rel_path(path, root))
        return {"surface": "changelog", "path": rel_path(path, root), "ok": True}
    if first_release.version == version:
        return SurfaceError("changelog_date_mismatch", f"changelog release {version} has date {first_release.date}, expected {date}", "changelog", rel_path(path, root))
    return SurfaceError("changelog_release_missing", f"changelog release heading {heading} is missing", "changelog", rel_path(path, root))


def update_changelog(root: Path, version: str, date: str, title: str, *, dry_run: bool) -> tuple[dict[str, Any], dict[str, Any] | SurfaceError, PlannedWrite | None]:
    """Plan insertion of the generated changelog section if it is absent."""
    path = root / "CHANGELOG.md"
    text = read_text(path, root, "changelog")
    unreleased, older = changelog_sections(text, root, path)
    release_headings = changelog_release_headings(text, unreleased.end())
    existing = check_changelog(root, version, date)
    if isinstance(existing, dict):
        return existing, update_entry("changelog", rel_path(path, root), "unchanged", dry_run=dry_run, would_write=False), None
    if existing.code in {"changelog_date_mismatch", "changelog_release_duplicate"}:
        return {"surface": "changelog", "path": rel_path(path, root), "ok": False}, existing, None
    if any(heading.version == version for heading in release_headings[1:]):
        return {"surface": "changelog", "path": rel_path(path, root), "ok": False}, SurfaceError(
            "changelog_release_out_of_order",
            f"changelog release {version} exists after an older release heading",
            "changelog",
            rel_path(path, root),
        ), None

    section = f"## [{version}] - {date}\n\n### Changed\n\n- {title}\n\n"
    unreleased_body = text[unreleased.end() : older.start]
    insert_at = older.start
    if not unreleased_body.strip():
        insert_at = unreleased.end()
        if text[insert_at:].startswith("\n\n"):
            insert_at += 2
        elif text[insert_at:].startswith("\n"):
            insert_at += 1
    new_text = text[:insert_at] + section + text[insert_at:]
    return {"surface": "changelog", "path": rel_path(path, root), "ok": False}, update_entry(
        "changelog",
        rel_path(path, root),
        "updated",
        dry_run=dry_run,
        would_write=True,
    ), PlannedWrite(path, "changelog", new_text)


def release_notes_path(root: Path, version: str) -> Path:
    """Return the release notes path for a version."""
    return root / "docs" / "releases" / f"v{version}.md"


def check_release_notes(root: Path, version: str, date: str) -> dict[str, Any] | SurfaceError:
    """Validate release notes heading and date line."""
    path = release_notes_path(root, version)
    if not path.exists():
        return SurfaceError("release_notes_missing", f"{rel_path(path, root)} is missing", "release_notes", rel_path(path, root))
    text = read_text(path, root, "release_notes")
    lines = text.splitlines()
    expected_heading = f"# CogniRelay v{version} Release Notes"
    expected_date = f"Release date: {date}"
    if not lines or lines[0] != expected_heading:
        return SurfaceError("release_notes_conflict", f"release notes heading must be {expected_heading}", "release_notes", rel_path(path, root))
    if len(lines) < 3 or lines[2] != expected_date:
        return SurfaceError("release_notes_conflict", f"release notes date line must be {expected_date}", "release_notes", rel_path(path, root))
    return {"surface": "release_notes", "path": rel_path(path, root), "ok": True}


def release_notes_template(version: str, date: str, title: str) -> str:
    """Return the deterministic release notes template."""
    return (
        f"# CogniRelay v{version} Release Notes\n\n"
        f"Release date: {date}\n\n"
        f"{title}\n\n"
        "## Verification\n\n"
        "Release preparation should pass:\n\n"
        "```bash\n"
        "git diff --check\n"
        "./.venv/bin/python -m ruff check app tests tools agent-assets\n"
        "./.venv/bin/python -m unittest discover -s tests -v\n"
        "```\n"
    )


def update_release_notes(root: Path, version: str, date: str, title: str, *, dry_run: bool) -> tuple[dict[str, Any], dict[str, Any] | SurfaceError, PlannedWrite | None]:
    """Plan release note creation or preserve matching existing notes."""
    path = release_notes_path(root, version)
    existing = check_release_notes(root, version, date)
    if isinstance(existing, dict):
        return existing, update_entry("release_notes", rel_path(path, root), "unchanged", dry_run=dry_run, would_write=False), None
    if existing.code != "release_notes_missing":
        return {"surface": "release_notes", "path": rel_path(path, root), "ok": False}, existing, None
    return {"surface": "release_notes", "path": rel_path(path, root), "ok": False}, update_entry(
        "release_notes",
        rel_path(path, root),
        "created",
        dry_run=dry_run,
        would_write=True,
    ), PlannedWrite(path, "release_notes", release_notes_template(version, date, title))


def release_list_bounds(text: str, root: Path, path: Path) -> tuple[int, int]:
    """Return start/end offsets for the bounded release bullet list."""
    heading = re.search(r"^## Releases$", text, re.MULTILINE)
    if not heading:
        raise SurfaceError("docs_index_parse_failed", "missing ## Releases heading", "docs_index", rel_path(path, root))
    pos = heading.end()
    if text[pos:].startswith("\n"):
        pos += 1
    if text[pos:].startswith("\n"):
        pos += 1
    line_start = pos
    end = pos
    saw_list = False
    for line in text[pos:].splitlines(keepends=True):
        if line.startswith("## "):
            break
        if line.startswith("- "):
            saw_list = True
            end = line_start + len(line)
        elif saw_list and line.strip():
            break
        elif not saw_list and line.strip():
            raise SurfaceError("docs_index_parse_failed", "release list must begin with Markdown bullets", "docs_index", rel_path(path, root))
        line_start += len(line)
    if not saw_list:
        raise SurfaceError("docs_index_parse_failed", "release list is missing after ## Releases", "docs_index", rel_path(path, root))
    return pos, end


def release_list_lines(root: Path) -> tuple[Path, str, int, int, list[str]]:
    """Return docs index text and bounded release list lines."""
    path = root / "docs" / "index.md"
    text = read_text(path, root, "docs_index")
    start, end = release_list_bounds(text, root, path)
    return path, text, start, end, text[start:end].splitlines()


def changelog_release_order(root: Path) -> list[str]:
    """Return release versions in changelog order after Unreleased."""
    path = root / "CHANGELOG.md"
    text = read_text(path, root, "changelog")
    unreleased, _older = changelog_sections(text, root, path)
    return [heading.version for heading in changelog_release_headings(text, unreleased.end())]


def previous_release_from_changelog(root: Path, version: str) -> str | None:
    """Return the release heading immediately after version in CHANGELOG.md."""
    releases = changelog_release_order(root)
    try:
        index = releases.index(version)
    except ValueError:
        return None
    if index + 1 >= len(releases):
        return None
    return releases[index + 1]


def check_docs_index(root: Path, version: str) -> dict[str, Any] | SurfaceError:
    """Validate the latest release pointer and previous latest list entry."""
    path, _text, _start, _end, lines = release_list_lines(root)
    expected_latest = f"- [Latest release notes: v{version}](releases/v{version}.md)"
    if not lines or lines[0] != expected_latest:
        return SurfaceError("docs_latest_mismatch", f"latest release pointer must be {expected_latest}", "docs_index", rel_path(path, root))
    previous_latest = previous_release_from_changelog(root, version)
    expected_previous = f"- [v{previous_latest} release notes](releases/v{previous_latest}.md)" if previous_latest else None
    if len(lines) < 2 or expected_previous is None or lines[1] != expected_previous:
        expected = expected_previous or "the previous changelog release link"
        return SurfaceError("docs_previous_latest_missing", f"previous latest release link must be immediately below latest pointer as {expected}", "docs_index", rel_path(path, root))
    seen: set[str] = set()
    for line in lines[1:]:
        match = NORMAL_RELEASE_RE.fullmatch(line)
        if not match:
            continue
        release_version = match.group(1)
        if release_version in seen or release_version == version:
            return SurfaceError("docs_duplicate_release_link", f"duplicate or current normal release link for v{release_version}", "docs_index", rel_path(path, root))
        seen.add(release_version)
    return {"surface": "docs_index", "path": rel_path(path, root), "ok": True}


def update_docs_index(root: Path, version: str, *, dry_run: bool) -> tuple[dict[str, Any], dict[str, Any] | SurfaceError, PlannedWrite | None]:
    """Plan an update only to the bounded release list in docs/index.md."""
    path, text, start, end, lines = release_list_lines(root)
    if not lines:
        raise SurfaceError("docs_index_parse_failed", "release list is missing after ## Releases", "docs_index", rel_path(path, root))
    latest_match = LATEST_RE.fullmatch(lines[0])
    if not latest_match:
        raise SurfaceError("docs_index_parse_failed", "first release list item must be the latest release pointer", "docs_index", rel_path(path, root))
    existing_latest = latest_match.group(1)
    checked = {"surface": "docs_index", "path": rel_path(path, root), "ok": lines[0] == f"- [Latest release notes: v{version}](releases/v{version}.md)"}
    if checked["ok"]:
        checked_result = check_docs_index(root, version)
        if isinstance(checked_result, dict):
            return checked_result, update_entry("docs_index", rel_path(path, root), "unchanged", dry_run=dry_run, would_write=False), None
        checked = {"surface": "docs_index", "path": rel_path(path, root), "ok": False}

    new_latest = f"- [Latest release notes: v{version}](releases/v{version}.md)"
    previous_latest = previous_release_from_changelog(root, version) if existing_latest == version else existing_latest
    if previous_latest is None or previous_latest == version:
        return checked, SurfaceError(
            "docs_previous_latest_missing",
            "previous latest release link must be available from CHANGELOG.md before updating docs/index.md",
            "docs_index",
            rel_path(path, root),
        ), None
    previous_normal = f"- [v{previous_latest} release notes](releases/v{previous_latest}.md)"
    remaining: list[str] = []
    seen_normal_versions = {previous_latest}
    for line in lines[1:]:
        normal = NORMAL_RELEASE_RE.fullmatch(line)
        if normal:
            release_version = normal.group(1)
            if release_version == version or release_version in seen_normal_versions:
                continue
            seen_normal_versions.add(release_version)
        remaining.append(line)
    new_lines = [new_latest, previous_normal, *remaining]
    new_block = "\n".join(new_lines) + "\n"
    new_text = text[:start] + new_block + text[end:]
    return {"surface": "docs_index", "path": rel_path(path, root), "ok": False}, update_entry(
        "docs_index",
        rel_path(path, root),
        "updated",
        dry_run=dry_run,
        would_write=True,
    ), PlannedWrite(path, "docs_index", new_text)


def is_forbidden_publishable_path(path: str) -> bool:
    """Return whether a tracked repo-relative path is forbidden for publication."""
    normalized = path.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    if not normalized or normalized in ALLOWED_ENV_TEMPLATES:
        return False
    name = normalized.rsplit("/", 1)[-1]
    if normalized.startswith(FORBIDDEN_DIR_PREFIXES):
        return True
    if any(part.endswith(".egg-info") for part in normalized.split("/")[:-1]):
        return True
    if normalized.endswith(FORBIDDEN_DB_SUFFIXES):
        return True
    if normalized.endswith(FORBIDDEN_RUNTIME_SUFFIXES):
        return True
    if normalized.endswith(FORBIDDEN_SECRET_SUFFIXES):
        return True
    if name in FORBIDDEN_RUNTIME_FILENAMES:
        return True
    if name == ".env" or name.startswith(".env."):
        return True
    if name.endswith(".env.example"):
        return True
    return False


def git_tracked_paths(root: Path) -> list[str] | None:
    """Return git tracked paths for root, or None when root is not a git worktree."""
    try:
        proc = subprocess.run(["git", "ls-files", "-z"], cwd=root, check=False, capture_output=True)
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    return [item.decode("utf-8", errors="replace") for item in proc.stdout.split(b"\0") if item]


def publishable_tree_residue_paths(root: Path) -> list[str]:
    """Return release/build residue paths from the filesystem without reading contents."""
    residue: list[str] = []
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            children = list(current.iterdir())
        except OSError:
            continue
        for path in children:
            name = path.name
            if path.is_dir() and name in RESIDUE_SCAN_PRUNE_DIRS:
                continue
            try:
                relative = rel_path(path, root)
            except ValueError:
                continue
            if name in FORBIDDEN_BUILD_RESIDUE_NAMES or name.endswith(".egg-info"):
                residue.append(relative)
                continue
            if path.is_dir():
                stack.append(path)
    priority = {"dist": 0, "build": 1}
    return sorted(residue, key=lambda path: (priority.get(path, 2), path))


def check_publishable_tree_safety(root: Path) -> dict[str, Any] | SurfaceError:
    """Validate that tracked files and local residue contain no publish-blocking state."""
    tracked = git_tracked_paths(root)
    if tracked is None:
        tracked = []
    for path in sorted(tracked):
        if is_forbidden_publishable_path(path):
            return SurfaceError(
                "publishable_tree_forbidden_file",
                "tracked file is not allowed in publishable tree",
                "publishable_tree_safety",
                path,
            )
    for path in publishable_tree_residue_paths(root):
        return SurfaceError(
            "publishable_tree_forbidden_file",
            "release/build residue is not allowed in publishable tree",
            "publishable_tree_safety",
            path,
        )
    return {"surface": "publishable_tree_safety", "path": ".", "ok": True}


def check_release(root: Path, version: str, date: str) -> dict[str, Any]:
    """Check local release surfaces for a requested version."""
    checked: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    checks = (
        lambda: check_app_version(root, version),
        lambda: check_pyproject_version(root, version),
        lambda: check_pyproject_dependencies(root),
        lambda: check_server_json_version(root, version),
        lambda: check_server_json_description(root),
        lambda: check_server_json_package_version(root, version),
        lambda: check_server_json_package_identifier(root),
        lambda: check_server_json_package_arguments(root),
        lambda: check_server_json_transport(root),
        lambda: check_server_json_environment_variables(root),
        lambda: check_mcp_ownership_marker(root),
        lambda: check_changelog(root, version, date),
        lambda: check_release_notes(root, version, date),
        lambda: check_docs_index(root, version),
        lambda: check_publishable_tree_safety(root),
    )
    for func in checks:
        try:
            result = func()
        except SurfaceError as exc:
            errors.append(exc.as_dict())
            continue
        if isinstance(result, SurfaceError):
            errors.append(result.as_dict())
        else:
            checked.append(result)
    return standard_result(ok=not errors, mode="check", version=version, date=date, checked=checked, errors=errors)


def update_release(root: Path, version: str, date: str, title: str, *, dry_run: bool) -> dict[str, Any]:
    """Update local release surfaces for a requested version."""
    checked: list[dict[str, Any]] = []
    updated: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    planned_writes: list[PlannedWrite] = []
    for func in (update_app_version, update_pyproject_version, update_server_json_versions, update_changelog, update_release_notes, update_docs_index):
        try:
            if func in {update_app_version, update_pyproject_version, update_server_json_versions, update_docs_index}:
                check_entry, update_result, planned_write = func(root, version, dry_run=dry_run)
            else:
                check_entry, update_result, planned_write = func(root, version, date, title, dry_run=dry_run)
        except SurfaceError as exc:
            errors.append(exc.as_dict())
            continue
        checked.append(check_entry)
        if isinstance(update_result, SurfaceError):
            errors.append(update_result.as_dict())
        else:
            updated.append(update_result)
        if planned_write is not None:
            planned_writes.append(planned_write)
    if errors or dry_run:
        return standard_result(ok=not errors, mode="update", version=version, date=date, dry_run=dry_run, checked=checked, updated=updated, errors=errors)
    try:
        snapshots = snapshot_write_targets(planned_writes, root)
    except SurfaceError as exc:
        errors.append(exc.as_dict())
        return standard_result(ok=False, mode="update", version=version, date=date, dry_run=dry_run, checked=checked, updated=updated, errors=errors)
    written_paths: list[Path] = []
    created_dirs: list[Path] = []
    for planned_write in planned_writes:
        try:
            created_dirs.extend(ensure_parent(planned_write.path, root, planned_write.surface))
            write_text(planned_write.path, root, planned_write.surface, planned_write.content)
        except SurfaceError as exc:
            rollback_written_targets([*written_paths, planned_write.path], snapshots)
            remove_created_dirs(created_dirs)
            errors.append(exc.as_dict())
            return standard_result(ok=False, mode="update", version=version, date=date, dry_run=dry_run, checked=checked, updated=updated, errors=errors)
        written_paths.append(planned_write.path)
    return standard_result(ok=not errors, mode="update", version=version, date=date, dry_run=dry_run, checked=checked, updated=updated, errors=errors)


def snapshot_write_targets(planned_writes: list[PlannedWrite], root: Path) -> dict[Path, WriteSnapshot]:
    """Capture existing target file state before the write phase."""
    snapshots: dict[Path, WriteSnapshot] = {}
    for planned_write in planned_writes:
        path = planned_write.path
        if path in snapshots:
            continue
        try:
            existed = path.exists()
            snapshots[path] = WriteSnapshot(existed, path.read_text(encoding="utf-8") if existed else None)
        except OSError as exc:
            raise SurfaceError("read_failed", "cannot snapshot planned write target", planned_write.surface, rel_path(path, root)) from exc
    return snapshots


def rollback_written_targets(written_paths: list[Path], snapshots: dict[Path, WriteSnapshot]) -> None:
    """Restore targets already changed by a failed write phase."""
    restored: set[Path] = set()
    for path in reversed(written_paths):
        if path in restored:
            continue
        restored.add(path)
        snapshot = snapshots[path]
        try:
            if snapshot.existed:
                path.write_text(snapshot.content or "", encoding="utf-8")
            elif path.exists():
                path.unlink()
        except OSError:
            continue


def remove_created_dirs(created_dirs: list[Path]) -> None:
    """Remove newly-created empty directories deepest first."""
    removed: set[Path] = set()
    for path in sorted(created_dirs, key=lambda item: len(item.parts), reverse=True):
        if path in removed:
            continue
        removed.add(path)
        try:
            path.rmdir()
        except OSError:
            continue


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse CLI arguments or raise a structured validation error."""
    parser = JsonArgumentParser(prog="prepare_release.py", add_help=True)
    subparsers = parser.add_subparsers(dest="mode", required=True)
    for mode in ("check", "update"):
        sub = subparsers.add_parser(mode)
        sub.add_argument("--version", required=True)
        sub.add_argument("--date", default=today_utc())
        sub.add_argument("--allow-dirty", action="store_true")
        if mode == "update":
            sub.add_argument("--title", required=True)
            sub.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def resolve_repo_root() -> Path:
    """Resolve the git repository root from the current working directory."""
    try:
        proc = subprocess.run(["git", "rev-parse", "--show-toplevel"], check=False, capture_output=True, text=True)
    except OSError as exc:
        raise SurfaceError("not_git_worktree", f"cannot run git rev-parse: {exc}") from exc
    if proc.returncode != 0:
        raise SurfaceError("not_git_worktree", "current directory is not inside a git worktree")
    return Path(proc.stdout.strip()).resolve()


def ensure_clean_worktree(root: Path) -> None:
    """Fail when tracked or untracked non-ignored files are present."""
    try:
        proc = subprocess.run(["git", "status", "--porcelain=v1"], cwd=root, check=False, capture_output=True, text=True)
    except OSError as exc:
        raise SurfaceError("dirty_worktree", f"cannot inspect git status: {exc}") from exc
    if proc.returncode != 0:
        raise SurfaceError("dirty_worktree", "cannot inspect git status")
    if proc.stdout:
        raise SurfaceError("dirty_worktree", "worktree has tracked or untracked non-ignored changes")


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    argv = list(sys.argv[1:] if argv is None else argv)
    mode = version = date = None
    dry_run = False
    try:
        args = parse_args(argv)
        mode = args.mode
        dry_run = bool(getattr(args, "dry_run", False))
        version = validate_version(args.version)
        date = validate_date(args.date)
        title = validate_title(getattr(args, "title", None)) if mode == "update" else None
        root = resolve_repo_root()
        if not args.allow_dirty:
            ensure_clean_worktree(root)
        result = check_release(root, version, date) if mode == "check" else update_release(root, version, date, title or "", dry_run=dry_run)
    except SurfaceError as exc:
        result = error_result(exc, mode=mode, version=version, date=date, dry_run=dry_run)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return exit_code_for(result)


if __name__ == "__main__":
    raise SystemExit(main())
