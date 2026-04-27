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
import zipfile
from pathlib import Path
from unittest import mock

from app.mcp.service import SUPPORTED_PROTOCOL_VERSIONS
from app.ui.docs import UI_DOCS
from tests.test_prepare_release_293 import _git_env
from tools import prepare_release


ROOT = Path(__file__).resolve().parents[1]
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
    "cognirelay-1.4.8/.env.example",
    "cognirelay-1.4.8/deploy/systemd/cognirelay.env.example",
}


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

        self.assertEqual(pyproject["project"]["dependencies"], expected)
        self.assertNotIn("build", "\n".join(pyproject["project"]["dependencies"]))
        self.assertNotIn("twine", "\n".join(pyproject["project"]["dependencies"]))
        self.assertIn("build", (ROOT / "requirements-dev.txt").read_text(encoding="utf-8"))
        self.assertIn("twine", (ROOT / "requirements-dev.txt").read_text(encoding="utf-8"))

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

    def test_built_wheel_and_sdist_contents_are_publishable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            dist = Path(td) / "dist"
            subprocess.run([sys.executable, "-m", "build", "--outdir", str(dist)], cwd=ROOT, check=True, capture_output=True, text=True, timeout=120)
            wheel = next(dist.glob("*.whl"))
            sdist = next(dist.glob("*.tar.gz"))

            with zipfile.ZipFile(wheel) as archive:
                wheel_names = set(archive.namelist())
            self.assertIn("app/main.py", wheel_names)
            self.assertIn("cognirelay/cli.py", wheel_names)
            for source in (ROOT / "app" / "ui" / "templates").glob("*.html"):
                self.assertIn(f"app/ui/templates/{source.name}", wheel_names)
            for source in (ROOT / "app" / "ui" / "static").rglob("*"):
                if source.is_file():
                    self.assertIn(source.relative_to(ROOT).as_posix(), wheel_names)
            self.assertFalse(any(name.startswith(("docs/", "agent-assets/", "deploy/", "data_repo/")) for name in wheel_names))
            self.assertFalse(any(_forbidden_artifact_name(name) for name in wheel_names))

            with tarfile.open(sdist) as archive:
                sdist_names = set(archive.getnames())
            prefix = "cognirelay-1.4.8/"
            for expected in (
                "README.md",
                ".env.example",
                "server.json",
                "docs/index.md",
                "agent-assets/README.md",
                "deploy/systemd/cognirelay.env.example",
            ):
                self.assertIn(prefix + expected, sdist_names)
            self.assertFalse(any(_forbidden_artifact_name(name) for name in sdist_names))

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
            (root / "README.md").write_text("# CogniRelay\n", encoding="utf-8")
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


if __name__ == "__main__":
    unittest.main()
