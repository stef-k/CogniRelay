"""Command-line entry point for the packaged CogniRelay server."""

from __future__ import annotations

import argparse
import os
import shutil
import stat
import sys
import sysconfig
from pathlib import Path
from typing import Sequence

LOG_LEVELS = ("debug", "info", "warning", "error", "critical")
AGENT_ASSET_FILES = (
    "README.md",
    "hooks/README.md",
    "hooks/cognirelay_continuity_save_hook.py",
    "hooks/cognirelay_retrieval_hook.py",
    "skills/cognirelay-continuity-authoring/SKILL.md",
)
AGENT_ASSET_DIRS = {
    "hooks",
    "hooks/__pycache__",
    "skills",
    "skills/cognirelay-continuity-authoring",
}
AGENT_ASSET_INSTALLER_CACHE_PREFIXES = (
    "hooks/__pycache__/cognirelay_continuity_save_hook.",
    "hooks/__pycache__/cognirelay_retrieval_hook.",
)


class RuntimeStateError(Exception):
    """Raised when a package startup would place runtime state in install files."""


class AgentAssetsError(Exception):
    """Raised when installed agent assets are unavailable or cannot be copied."""


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


def _installed_agent_assets_root(package_file: Path | None = None) -> Path:
    package_path = (package_file or Path(__file__)).resolve()
    return package_path.parent / "agent_assets"


def _readable_regular_file(path: Path) -> bool:
    try:
        file_stat = path.stat()
        if not stat.S_ISREG(file_stat.st_mode):
            return False
        with path.open("rb") as handle:
            handle.read(1)
    except OSError:
        return False
    return True


def validate_installed_agent_assets(*, package_file: Path | None = None) -> Path:
    """Return the installed agent-assets root after strict allowlist validation."""
    root = _installed_agent_assets_root(package_file)
    if not root.is_dir():
        raise AgentAssetsError(f"installed agent assets are unavailable at {root}")

    allowed = set(AGENT_ASSET_FILES)
    for relative in AGENT_ASSET_FILES:
        if not _readable_regular_file(root / relative):
            raise AgentAssetsError(f"installed agent assets are unavailable at {root}")

    try:
        discovered = sorted(path.relative_to(root).as_posix() for path in root.rglob("*"))
    except OSError as exc:
        raise AgentAssetsError(f"installed agent assets are unavailable at {root}") from exc

    for relative in discovered:
        path = root / relative
        if relative in allowed:
            continue
        if relative.endswith(".pyc") and any(relative.startswith(prefix) for prefix in AGENT_ASSET_INSTALLER_CACHE_PREFIXES):
            continue
        if relative in AGENT_ASSET_DIRS and path.is_dir():
            continue
        raise AgentAssetsError(f"installed agent assets contain unexpected entries at {root}")
    return root


def build_parser() -> argparse.ArgumentParser:
    """Build the CogniRelay CLI parser."""
    parser = argparse.ArgumentParser(prog="cognirelay")
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument("--log-level", choices=LOG_LEVELS, default="info")
    serve.add_argument("--reload", action="store_true")
    assets = subparsers.add_parser("assets")
    asset_subparsers = assets.add_subparsers(dest="asset_command", required=True)
    asset_subparsers.add_parser("path")
    asset_subparsers.add_parser("list")
    copy = asset_subparsers.add_parser("copy")
    copy.add_argument("--to", required=True)
    copy.add_argument("--force", action="store_true")
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


def _assets_path(_args: argparse.Namespace) -> int:
    root = validate_installed_agent_assets()
    print(root)
    return 0


def _assets_list(_args: argparse.Namespace) -> int:
    validate_installed_agent_assets()
    for relative in sorted(AGENT_ASSET_FILES):
        print(relative)
    return 0


def _copy_agent_assets(source: Path, destination: Path, *, force: bool) -> None:
    if destination.is_symlink():
        raise AgentAssetsError(f"target path is not a real directory: {destination}")
    if destination.exists():
        if not destination.is_dir():
            raise AgentAssetsError(f"target path is not a directory: {destination}")
        try:
            has_entries = any(destination.iterdir())
        except OSError as exc:
            raise AgentAssetsError(f"could not inspect target directory: {destination}") from exc
        if has_entries:
            if not force:
                raise AgentAssetsError(f"target directory is not empty: {destination}")
            try:
                shutil.rmtree(destination)
            except OSError as exc:
                raise AgentAssetsError(f"could not replace target directory: {destination}") from exc

    try:
        shutil.copytree(source, destination, dirs_exist_ok=True)
    except OSError as exc:
        raise AgentAssetsError(f"could not copy agent assets to {destination}") from exc


def _assets_copy(args: argparse.Namespace) -> int:
    root = validate_installed_agent_assets()
    requested_parent = Path(args.to).expanduser()
    try:
        requested_parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise AgentAssetsError(f"could not create destination directory: {requested_parent}") from exc
    destination = requested_parent / "agent-assets"
    _copy_agent_assets(root, destination, force=args.force)
    print(destination.resolve())
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CogniRelay CLI."""
    parser = build_parser()
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    try:
        if args.command == "serve":
            return _serve(args)
        if args.command == "assets":
            if args.asset_command == "path":
                return _assets_path(args)
            if args.asset_command == "list":
                return _assets_list(args)
            if args.asset_command == "copy":
                return _assets_copy(args)
    except RuntimeStateError as exc:
        print(f"cognirelay: {exc}", file=sys.stderr)
        return 1
    except AgentAssetsError as exc:
        print(f"cognirelay assets: {exc}", file=sys.stderr)
        return 1
    parser.error("unknown command")
    return 2
