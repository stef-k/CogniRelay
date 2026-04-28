"""Packaging, registry metadata, and artifact checks for issue #290."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import unittest
import venv
import zipfile
from pathlib import Path
from unittest import mock

from app.mcp.service import SUPPORTED_PROTOCOL_VERSIONS
from app.ui.docs import UI_DOCS
from tests.test_prepare_release_293 import _git_env
from tools import prepare_release


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_VERSION = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]
SDIST_PREFIX = f"cognirelay-{PACKAGE_VERSION}/"
GITHUB_REPOSITORY_TOPICS = [
    "mcp",
    "ai-collaboration",
    "agent-infrastructure",
    "continuity",
    "agent-memory",
    "autonomous-agents",
    "context-recovery",
    "fastapi",
    "long-horizon-agents",
    "multi-agent-systems",
    "self-hosted",
    "session-recovery",
    "agent-continuity",
    "continuity-infrastructure",
    "recoverable-memory",
]
FORBIDDEN_ARTIFACT_SUFFIXES = (
    ".pyc",
    ".pyo",
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
    ".pem",
    ".key",
    ".token",
    ".log",
    ".jsonl",
    ".bak",
    ".tmp",
)
ALLOWED_ENV_TEMPLATE_ARTIFACTS = {
    f"{SDIST_PREFIX}.env.example",
    f"{SDIST_PREFIX}deploy/systemd/cognirelay.env.example",
}
FORBIDDEN_METADATA_TOKENS = (
    "data_repo/",
    "peer_tokens.json",
    "api_audit.jsonl",
    ".env",
    ".token",
    "*.token",
    "/home/",
    "/Users/",
)
WHEEL_AGENT_ASSET_FILES = {
    "cognirelay/agent_assets/README.md",
    "cognirelay/agent_assets/hooks/README.md",
    "cognirelay/agent_assets/hooks/cognirelay_continuity_save_hook.py",
    "cognirelay/agent_assets/hooks/cognirelay_retrieval_hook.py",
    "cognirelay/agent_assets/skills/cognirelay-continuity-authoring/SKILL.md",
}
CLI_AGENT_ASSET_FILES = sorted(name.removeprefix("cognirelay/agent_assets/") for name in WHEEL_AGENT_ASSET_FILES)


def _forbidden_artifact_name(name: str) -> bool:
    parts = name.split("/")
    basename = parts[-1]
    return (
        any(part in {".git", ".venv", "memory", "logs", "dist", "build", "data_repo", ".locks", ".pytest_cache", ".ruff_cache", ".mypy_cache", "__pycache__"} for part in parts)
        or any(part.endswith(".egg-info") for part in parts)
        or basename.endswith(FORBIDDEN_ARTIFACT_SUFFIXES)
        or basename in {"api_audit.jsonl", "peer_tokens.json"}
        or ((basename == ".env" or basename.startswith(".env.") or basename.endswith(".env.example")) and name not in ALLOWED_ENV_TEMPLATE_ARTIFACTS)
    )


class Packaging290Tests(unittest.TestCase):
    def test_pyproject_dependencies_match_requirements_and_exclude_dev_tools(self) -> None:
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        expected = prepare_release.runtime_requirements(ROOT)

        self.assertEqual(pyproject["project"]["readme"], "README-PYPI.md")
        self.assertEqual(pyproject["project"]["keywords"], GITHUB_REPOSITORY_TOPICS)
        self.assertEqual(pyproject["project"]["dependencies"], expected)
        self.assertNotIn("build", "\n".join(pyproject["project"]["dependencies"]))
        self.assertNotIn("twine", "\n".join(pyproject["project"]["dependencies"]))
        self.assertIn("build", (ROOT / "requirements-dev.txt").read_text(encoding="utf-8"))
        self.assertIn("twine", (ROOT / "requirements-dev.txt").read_text(encoding="utf-8"))
        self.assertIn("jsonschema", (ROOT / "requirements-dev.txt").read_text(encoding="utf-8"))

    def test_pypi_readme_is_sanitized_and_carries_mcp_marker(self) -> None:
        text = (ROOT / "README-PYPI.md").read_text(encoding="utf-8")

        self.assertIn("self-hosted", text)
        self.assertIn("pip install cognirelay", text)
        self.assertIn("cognirelay serve", text)
        self.assertIn("COGNIRELAY_REPO_ROOT", text)
        self.assertIn("https://github.com/stef-k/CogniRelay", text)
        self.assertIn("<!-- mcp-name: io.github.stef-k/cognirelay -->", text)
        for token in FORBIDDEN_METADATA_TOKENS:
            self.assertNotIn(token, text)

    def test_server_json_shape_and_runtime_protocol_versions(self) -> None:
        payload = json.loads((ROOT / "server.json").read_text(encoding="utf-8"))
        package = payload["packages"][0]

        self.assertEqual(payload["$schema"], "https://static.modelcontextprotocol.io/schemas/2025-12-11/server.schema.json")
        self.assertEqual(payload["name"], "io.github.stef-k/cognirelay")
        self.assertEqual(payload["description"], prepare_release.SERVER_JSON_DESCRIPTION)
        self.assertEqual(package["identifier"], "cognirelay")
        self.assertEqual(package["packageArguments"], [{"type": "positional", "value": "serve"}])
        self.assertEqual(package["transport"], {"type": "streamable-http", "url": "http://127.0.0.1:8080/v1/mcp"})
        self.assertEqual(package["environmentVariables"], prepare_release.SERVER_JSON_ENVIRONMENT_VARIABLES)
        self.assertEqual(SUPPORTED_PROTOCOL_VERSIONS, ("2025-06-18", "2025-11-25"))

    def test_server_json_schema_validator_supports_local_schema_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["$schema", "name"],
                "properties": {
                    "$schema": {"type": "string"},
                    "name": {"type": "string", "const": "io.github.stef-k/cognirelay"},
                },
                "additionalProperties": True,
            }
            (root / "schema.json").write_text(json.dumps(schema), encoding="utf-8")
            shutil.copy2(ROOT / "server.json", root / "server.json")

            proc = subprocess.run(
                [sys.executable, str(ROOT / "tools" / "validate_server_json.py"), "--root", str(root), "--schema", "schema.json"],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=30,
            )

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout, "")
        self.assertEqual(proc.stderr, "")

    def test_server_json_schema_validator_reports_path_only_errors(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["$schema", "name"],
                "properties": {
                    "$schema": {"type": "string"},
                    "name": {"type": "string", "const": "io.github.stef-k/cognirelay"},
                },
                "additionalProperties": True,
            }
            payload = json.loads((ROOT / "server.json").read_text(encoding="utf-8"))
            payload["name"] = "private-wrong-name"
            (root / "schema.json").write_text(json.dumps(schema), encoding="utf-8")
            (root / "server.json").write_text(json.dumps(payload), encoding="utf-8")

            proc = subprocess.run(
                [sys.executable, str(ROOT / "tools" / "validate_server_json.py"), "--root", str(root), "--schema", "schema.json"],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=30,
            )

        self.assertEqual(proc.returncode, 1)
        self.assertEqual(proc.stdout, "")
        self.assertIn("server.json: schema validation failed at /name", proc.stderr)
        self.assertNotIn("private-wrong-name", proc.stderr)

    def test_built_wheel_and_sdist_contents_are_publishable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            dist = Path(td) / "dist"
            subprocess.run([sys.executable, "-m", "build", "--outdir", str(dist)], cwd=ROOT, check=True, capture_output=True, text=True, timeout=120)
            wheel = next(dist.glob("*.whl"))
            sdist = next(dist.glob("*.tar.gz"))

            with zipfile.ZipFile(wheel) as archive:
                wheel_names = set(archive.namelist())
                metadata_name = next(name for name in wheel_names if name.endswith(".dist-info/METADATA"))
                wheel_metadata = archive.read(metadata_name).decode("utf-8")
            self.assertIn("app/main.py", wheel_names)
            self.assertIn("cognirelay/cli.py", wheel_names)
            for source in (ROOT / "app" / "ui" / "templates").glob("*.html"):
                self.assertIn(f"app/ui/templates/{source.name}", wheel_names)
            for source in (ROOT / "app" / "ui" / "static").rglob("*"):
                if source.is_file():
                    self.assertIn(source.relative_to(ROOT).as_posix(), wheel_names)
            self.assertEqual({name for name in wheel_names if name.startswith("cognirelay/agent_assets/")}, WHEEL_AGENT_ASSET_FILES)
            self.assertFalse(any(name.startswith(("docs/", "agent-assets/", "deploy/", "tests/", "data_repo/", "dist/", "build/")) for name in wheel_names))
            self.assertFalse(any(_forbidden_artifact_name(name) for name in wheel_names))
            self.assertIn("<!-- mcp-name: io.github.stef-k/cognirelay -->", wheel_metadata)
            for token in FORBIDDEN_METADATA_TOKENS:
                self.assertNotIn(token, wheel_metadata)

            with tarfile.open(sdist) as archive:
                sdist_names = set(archive.getnames())
                pkg_info = archive.extractfile(f"{SDIST_PREFIX}PKG-INFO")
                self.assertIsNotNone(pkg_info)
                sdist_metadata = pkg_info.read().decode("utf-8")  # type: ignore[union-attr]
            for expected in (
                "README.md",
                "README-PYPI.md",
                ".env.example",
                "server.json",
                "docs/index.md",
                "agent-assets/README.md",
                "deploy/systemd/cognirelay.env.example",
            ):
                self.assertIn(SDIST_PREFIX + expected, sdist_names)
            self.assertFalse(any(_forbidden_artifact_name(name) for name in sdist_names))
            self.assertIn("<!-- mcp-name: io.github.stef-k/cognirelay -->", sdist_metadata)
            for token in FORBIDDEN_METADATA_TOKENS:
                self.assertNotIn(token, sdist_metadata)
            self.assertFalse((ROOT / "cognirelay" / "agent_assets").exists())

    def test_built_wheel_prunes_stale_agent_asset_build_residue(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            copied = root / "copy"
            shutil.copytree(ROOT, copied, ignore=shutil.ignore_patterns(".git", ".venv", "dist", "*.egg-info", ".pytest_cache", ".ruff_cache", ".mypy_cache"))
            stale = copied / "build" / "lib" / "cognirelay" / "agent_assets" / "leak.token"
            stale.parent.mkdir(parents=True, exist_ok=True)
            stale.write_text("private-token\n", encoding="utf-8")
            dist = root / "dist"

            subprocess.run([sys.executable, "-m", "build", "--wheel", "--outdir", str(dist)], cwd=copied, check=True, capture_output=True, text=True, timeout=120)

            wheel = next(dist.glob("*.whl"))
            with zipfile.ZipFile(wheel) as archive:
                wheel_names = set(archive.namelist())
            self.assertEqual({name for name in wheel_names if name.startswith("cognirelay/agent_assets/")}, WHEEL_AGENT_ASSET_FILES)
            self.assertNotIn("cognirelay/agent_assets/leak.token", wheel_names)
            self.assertFalse((ROOT / "cognirelay" / "agent_assets").exists())

    def test_built_wheel_installed_cli_can_manage_agent_assets(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            dist = root / "dist"
            subprocess.run([sys.executable, "-m", "build", "--wheel", "--outdir", str(dist)], cwd=ROOT, check=True, capture_output=True, text=True, timeout=120)
            wheel = next(dist.glob("*.whl"))
            venv_dir = root / "venv"
            venv.EnvBuilder(with_pip=True).create(venv_dir)
            venv_python = venv_dir / "bin" / "python"
            subprocess.run([str(venv_python), "-m", "pip", "install", "--no-deps", "--no-compile", str(wheel)], check=True, capture_output=True, text=True, timeout=120)
            console_script = venv_dir / "bin" / "cognirelay"

            path_proc = subprocess.run([str(console_script), "assets", "path"], cwd=root, check=False, capture_output=True, text=True, timeout=30)
            list_proc = subprocess.run([str(console_script), "assets", "list"], cwd=root, check=False, capture_output=True, text=True, timeout=30)
            copy_parent = root / "copied"
            copy_proc = subprocess.run([str(console_script), "assets", "copy", "--to", str(copy_parent)], cwd=root, check=False, capture_output=True, text=True, timeout=30)
            module_proc = subprocess.run([str(venv_python), "-m", "cognirelay", "assets", "path"], cwd=root, check=False, capture_output=True, text=True, timeout=30)

            self.assertEqual(path_proc.returncode, 0, path_proc.stderr)
            self.assertEqual(path_proc.stderr, "")
            installed_assets = Path(path_proc.stdout.strip())
            self.assertTrue(installed_assets.is_dir())
            self.assertTrue(installed_assets.is_absolute())
            self.assertEqual(list_proc.returncode, 0, list_proc.stderr)
            self.assertEqual(list_proc.stdout.splitlines(), CLI_AGENT_ASSET_FILES)
            self.assertEqual(list_proc.stderr, "")
            self.assertEqual(copy_proc.returncode, 0, copy_proc.stderr)
            self.assertEqual(copy_proc.stderr, "")
            copied_assets = copy_parent / "agent-assets"
            self.assertEqual(Path(copy_proc.stdout.strip()), copied_assets.resolve())
            for relative in CLI_AGENT_ASSET_FILES:
                self.assertTrue((copied_assets / relative).is_file())
            self.assertTrue((copied_assets / "hooks" / "cognirelay_retrieval_hook.py").is_file())
            self.assertTrue((copied_assets / "hooks" / "cognirelay_continuity_save_hook.py").is_file())
            self.assertTrue((copied_assets / "skills" / "cognirelay-continuity-authoring" / "SKILL.md").is_file())
            self.assertEqual(module_proc.returncode, 0, module_proc.stderr)
            self.assertEqual(module_proc.stdout, path_proc.stdout)
            self.assertEqual(module_proc.stderr, "")
            self.assertFalse((ROOT / "cognirelay" / "agent_assets").exists())

    def test_sdist_prunes_temp_only_runtime_fixture_paths(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            copied = Path(td) / "copy"
            shutil.copytree(ROOT, copied, ignore=shutil.ignore_patterns(".git", ".venv", "dist", "build", "*.egg-info", ".pytest_cache", ".ruff_cache", ".mypy_cache"))
            for relative in ("data_repo/x", ".locks/x.lock", "nested/secret.token", "nested/state.sqlite-wal", "nested/api_audit.jsonl"):
                path = copied / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("private-fixture\n", encoding="utf-8")
            subprocess.run(["git", "init"], cwd=copied, check=True, capture_output=True, text=True)
            subprocess.run(["git", "add", "."], cwd=copied, check=True, capture_output=True, text=True)
            subprocess.run(["git", "commit", "-m", "fixture"], cwd=copied, check=True, capture_output=True, text=True, env=_git_env())
            dist = Path(td) / "dist"

            subprocess.run([sys.executable, "-m", "build", "--sdist", "--outdir", str(dist)], cwd=copied, check=True, capture_output=True, text=True, timeout=120)

            sdist = next(dist.glob("*.tar.gz"))
            with tarfile.open(sdist) as archive:
                names = set(archive.getnames())
            self.assertFalse(any(_forbidden_artifact_name(name) for name in names))
            for relative in ("data_repo/x", ".locks/x.lock", "nested/secret.token", "nested/state.sqlite-wal", "nested/api_audit.jsonl"):
                self.assertFalse((ROOT / relative).exists())

    def test_release_helper_reports_new_surface_error_codes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            shutil.copytree(ROOT, root, dirs_exist_ok=True, ignore=shutil.ignore_patterns(".git", ".venv", "dist", "build", "*.egg-info"))
            (root / "pyproject.toml").write_text((root / "pyproject.toml").read_text(encoding="utf-8").replace('"bleach>=6,<7",', '"bleach>=6,<7",\n    "twine>=5,<7",'), encoding="utf-8")
            server = json.loads((root / "server.json").read_text(encoding="utf-8"))
            server["packages"][0]["identifier"] = "wrong"
            (root / "server.json").write_text(json.dumps(server, indent=2) + "\n", encoding="utf-8")
            (root / "README-PYPI.md").write_text("# CogniRelay\n", encoding="utf-8")
            with mock.patch.object(prepare_release, "git_tracked_paths", return_value=["data_repo/x"]):
                result = prepare_release.check_release(root, "1.4.8", "2026-04-26")

        codes = {error["code"] for error in result["errors"]}
        self.assertIn("pyproject_dependency_mismatch", codes)
        self.assertIn("server_json_package_identifier_mismatch", codes)
        self.assertIn("mcp_ownership_marker_missing", codes)
        self.assertIn("publishable_tree_forbidden_file", codes)
        self.assertEqual(prepare_release.exit_code_for(result), 1)

    def test_ui_docs_degrades_when_docs_root_missing(self) -> None:
        from tests.test_ui_docs import _ui_response

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            docs_root = root / "missing-docs"
            index = _ui_response(root, route_path="/ui/docs", request_path="/ui/docs", docs_source_root=docs_root)
            detail = _ui_response(root, route_path="/ui/docs/{doc_id}", request_path="/ui/docs/readme", docs_source_root=docs_root, endpoint_kwargs={"doc_id": "readme"})

        self.assertEqual(index.status_code, 200)
        self.assertEqual(detail.status_code, 200)
        self.assertIn("doc_missing:readme", index.text)
        self.assertIn("doc_missing:readme", detail.text)
        for doc in UI_DOCS:
            self.assertIn(doc.title, index.text)

    def test_source_and_deploy_docs_do_not_contain_private_state_or_operator_paths(self) -> None:
        proc = subprocess.run(["git", "ls-files", "README.md", "README-PYPI.md", "docs", "deploy"], cwd=ROOT, check=True, capture_output=True, text=True)
        scanned = "\n".join((ROOT / path).read_text(encoding="utf-8", errors="replace") for path in proc.stdout.splitlines() if (ROOT / path).is_file())

        self.assertNotIn("/home/", scanned)
        self.assertNotIn("/Users/", scanned)
        self.assertNotIn("super-secret", scanned)
        self.assertNotIn("private-fixture", scanned)
        self.assertNotIn("BEGIN PRIVATE KEY", scanned)


if __name__ == "__main__":
    unittest.main()
