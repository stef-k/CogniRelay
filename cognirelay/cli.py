"""Command-line entry point for the packaged CogniRelay server."""

from __future__ import annotations

import argparse
import os
import sys
import sysconfig
from pathlib import Path
from typing import Sequence

LOG_LEVELS = ("debug", "info", "warning", "error", "critical")


class RuntimeStateError(Exception):
    """Raised when a package startup would place runtime state in install files."""


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _site_package_roots() -> tuple[Path, ...]:
    roots: set[Path] = set()
    for key in ("purelib", "platlib"):
        value = sysconfig.get_paths().get(key)
        if value:
            roots.add(Path(value).resolve())
    return tuple(sorted(roots))


def _installed_package_root(package_file: Path) -> Path | None:
    package_dir = package_file.resolve().parent
    for root in _site_package_roots():
        if _is_relative_to(package_dir, root):
            return root
    return None


def validate_runtime_state_root(repo_root_raw: str | None, *, cwd: Path | None = None, package_file: Path | None = None) -> Path:
    """Return a safe runtime state root or raise before startup can create state."""
    working_dir = (cwd or Path.cwd()).resolve()
    package_path = (package_file or Path(__file__)).resolve()
    package_dir = package_path.parent
    site_root = _installed_package_root(package_path)
    raw = repo_root_raw if repo_root_raw else "./data_repo"
    repo_root = Path(raw).expanduser()
    if not repo_root.is_absolute():
        repo_root = working_dir / repo_root
    repo_root = repo_root.resolve()

    unsafe_roots = [package_dir]
    if site_root is not None:
        unsafe_roots.append(site_root)
        if not repo_root_raw:
            raise RuntimeStateError("COGNIRELAY_REPO_ROOT must point to a durable writable path outside the Python install")

    if any(_is_relative_to(repo_root, root) for root in unsafe_roots):
        raise RuntimeStateError("COGNIRELAY_REPO_ROOT must be outside package and site-packages paths")
    return repo_root


def build_parser() -> argparse.ArgumentParser:
    """Build the CogniRelay CLI parser."""
    parser = argparse.ArgumentParser(prog="cognirelay")
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument("--log-level", choices=LOG_LEVELS, default="info")
    serve.add_argument("--reload", action="store_true")
    return parser


def _serve(args: argparse.Namespace) -> int:
    validate_runtime_state_root(os.getenv("COGNIRELAY_REPO_ROOT"))
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        reload=args.reload,
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CogniRelay CLI."""
    parser = build_parser()
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    try:
        if args.command == "serve":
            return _serve(args)
    except RuntimeStateError as exc:
        print(f"cognirelay: {exc}", file=sys.stderr)
        return 1
    parser.error("unknown command")
    return 2
