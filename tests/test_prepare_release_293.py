"""Tests for the local release preparation helper from issue #293."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


_helper_path = Path(__file__).resolve().parents[1] / "tools" / "prepare_release.py"
_spec = importlib.util.spec_from_file_location("prepare_release", _helper_path)
prepare_release = importlib.util.module_from_spec(_spec)
sys.modules["prepare_release"] = prepare_release
_spec.loader.exec_module(prepare_release)


def _write_fixture(root: Path, *, version: str = "1.4.8", latest: str = "1.4.8") -> None:
    """Create the release surfaces used by the helper tests."""
    (root / "app").mkdir(parents=True)
    (root / "docs" / "releases").mkdir(parents=True)
    (root / "tools").mkdir(parents=True)
    (root / "app" / "main.py").write_text(
        f'from fastapi import FastAPI\n\napp = FastAPI(title="CogniRelay", version="{version}")\n',
        encoding="utf-8",
    )
    (root / "CHANGELOG.md").write_text(
        "# Changelog\n\n"
        "## [Unreleased]\n\n"
        f"## [{latest}] - 2026-04-26\n\n"
        "### Fixed\n\n"
        "- Previous release.\n\n"
        "## [1.4.7] - 2026-04-25\n\n"
        "### Fixed\n\n"
        "- Older release.\n",
        encoding="utf-8",
    )
    (root / "docs" / "index.md").write_text(
        "# CogniRelay Documentation\n\n"
        "## Releases\n\n"
        f"- [Latest release notes: v{latest}](releases/v{latest}.md)\n"
        "- [v1.4.7 release notes](releases/v1.4.7.md)\n"
        "- [Changelog](https://github.com/stef-k/CogniRelay/blob/main/CHANGELOG.md)\n\n"
        "## Other Links\n\n"
        f"- [Latest release notes: v{latest}](releases/v{latest}.md)\n",
        encoding="utf-8",
    )
    (root / "docs" / "releases" / f"v{latest}.md").write_text(
        f"# CogniRelay v{latest} Release Notes\n\nRelease date: 2026-04-26\n",
        encoding="utf-8",
    )
    (root / "requirements.txt").write_text(
        "fastapi>=0.115,<1\n"
        "uvicorn>=0.30,<1\n",
        encoding="utf-8",
    )
    (root / "pyproject.toml").write_text(
        "[project]\n"
        'name = "cognirelay"\n'
        f'version = "{version}"\n'
        "dependencies = [\n"
        '    "fastapi>=0.115,<1",\n'
        '    "uvicorn>=0.30,<1",\n'
        "]\n",
        encoding="utf-8",
    )
    (root / "server.json").write_text(
        json.dumps(
            {
                "$schema": "https://static.modelcontextprotocol.io/schemas/2025-12-11/server.schema.json",
                "name": "io.github.stef-k/cognirelay",
                "title": "CogniRelay",
                "description": "Self-hosted continuity and collaboration substrate for autonomous agents.",
                "version": version,
                "packages": [
                    {
                        "registryType": "pypi",
                        "identifier": "cognirelay",
                        "version": version,
                        "packageArguments": [{"type": "positional", "value": "serve"}],
                        "transport": {"type": "streamable-http", "url": "http://127.0.0.1:8080/v1/mcp"},
                        "environmentVariables": [
                            {
                                "name": "COGNIRELAY_REPO_ROOT",
                                "description": "Path to a durable writable CogniRelay repository root for runtime state.",
                                "isRequired": True,
                                "format": "filepath",
                                "isSecret": False,
                            }
                        ],
                    }
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "README.md").write_text(
        "# CogniRelay\n\n<!-- mcp-name: io.github.stef-k/cognirelay -->\n",
        encoding="utf-8",
    )


def _git_env() -> dict[str, str]:
    """Return deterministic git identity for temporary repositories."""
    return {
        **os.environ,
        "GIT_AUTHOR_NAME": "Test",
        "GIT_AUTHOR_EMAIL": "test@example.com",
        "GIT_COMMITTER_NAME": "Test",
        "GIT_COMMITTER_EMAIL": "test@example.com",
    }


def _init_git_repo(root: Path) -> None:
    """Initialize and commit a temporary git repository."""
    subprocess.run(["git", "init"], cwd=root, check=True, capture_output=True, text=True)
    subprocess.run(["git", "add", "."], cwd=root, check=True, capture_output=True, text=True)
    subprocess.run(["git", "commit", "-m", "fixture"], cwd=root, check=True, capture_output=True, text=True, env=_git_env())


class PrepareReleaseTests(unittest.TestCase):
    def test_check_mode_passes_on_matching_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root)

            result = prepare_release.check_release(root, "1.4.8", "2026-04-26")

            self.assertTrue(result["ok"])
            self.assertEqual(
                [entry["surface"] for entry in result["checked"]],
                [
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
                ],
            )
            self.assertEqual(result["updated"], [])

    def test_publishable_tree_safety_allows_only_exact_env_templates(self) -> None:
        allowed = [".env.example", "deploy/systemd/cognirelay.env.example"]
        forbidden = [
            "foo.env.example",
            "docs/foo.env.example",
            ".env.local",
            "nested/.env",
            "foo.token",
            "api_audit.jsonl",
            "data_repo/x",
        ]

        for path in allowed:
            with self.subTest(path=path):
                self.assertFalse(prepare_release.is_forbidden_publishable_path(path))
        for path in forbidden:
            with self.subTest(path=path):
                self.assertTrue(prepare_release.is_forbidden_publishable_path(path))

    def test_publishable_tree_safety_rejects_tracked_runtime_state_path_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root)
            secret_text = "super-secret-token-value"
            (root / "data_repo").mkdir()
            (root / "data_repo" / "x").write_text(secret_text, encoding="utf-8")
            _init_git_repo(root)

            result = prepare_release.check_release(root, "1.4.8", "2026-04-26")

            self.assertFalse(result["ok"])
            self.assertEqual(prepare_release.exit_code_for(result), 1)
            errors = [error for error in result["errors"] if error["surface"] == "publishable_tree_safety"]
            self.assertEqual(len(errors), 1)
            self.assertEqual(errors[0]["code"], "publishable_tree_forbidden_file")
            self.assertEqual(errors[0]["path"], "data_repo/x")
            encoded = json.dumps(result, sort_keys=True)
            self.assertNotIn(secret_text, encoded)
            self.assertNotIn(str(root), encoded)

    def test_check_mode_reports_content_mismatch_as_exit_1_class(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root, version="1.4.7")

            result = prepare_release.check_release(root, "1.4.8", "2026-04-26")

            self.assertFalse(result["ok"])
            self.assertEqual(prepare_release.exit_code_for(result), 1)
            self.assertEqual(result["errors"][0]["code"], "version_mismatch")
            self.assertEqual(result["errors"][0]["surface"], "app_version")

    def test_update_mode_edits_only_allowed_surfaces(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root)
            untouched = root / "docs" / "outside.md"
            untouched.write_text("outside\n", encoding="utf-8")

            result = prepare_release.update_release(root, "1.4.9", "2026-04-27", "Release helper", dry_run=False)

            self.assertTrue(result["ok"], result["errors"])
            self.assertEqual(untouched.read_text(encoding="utf-8"), "outside\n")
            self.assertIn('version="1.4.9"', (root / "app" / "main.py").read_text(encoding="utf-8"))
            self.assertTrue((root / "docs" / "releases" / "v1.4.9.md").exists())
            paths = {entry["path"] for entry in result["updated"]}
            self.assertEqual(paths, {"app/main.py", "pyproject.toml", "server.json", "CHANGELOG.md", "docs/releases/v1.4.9.md", "docs/index.md"})

    def test_update_preserves_non_empty_unreleased_content(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root)
            changelog = root / "CHANGELOG.md"
            changelog.write_text(
                "# Changelog\n\n"
                "## [Unreleased]\n\n"
                "### Changed\n\n"
                "- Keep this.\n\n"
                "## [1.4.8] - 2026-04-26\n\n"
                "### Fixed\n\n"
                "- Previous release.\n",
                encoding="utf-8",
            )

            result = prepare_release.update_release(root, "1.4.9", "2026-04-27", "Release helper", dry_run=False)

            self.assertTrue(result["ok"], result["errors"])
            text = changelog.read_text(encoding="utf-8")
            self.assertLess(text.index("- Keep this."), text.index("## [1.4.9] - 2026-04-27"))
            self.assertLess(text.index("## [1.4.9] - 2026-04-27"), text.index("## [1.4.8] - 2026-04-26"))

    def test_update_does_not_duplicate_changelog_version_with_wrong_date(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root)
            changelog = root / "CHANGELOG.md"
            changelog.write_text(
                "# Changelog\n\n"
                "## [Unreleased]\n\n"
                "## [1.4.9] - 2026-04-26\n\n"
                "### Changed\n\n"
                "- Existing.\n\n"
                "## [1.4.8] - 2026-04-26\n\n"
                "### Fixed\n\n"
                "- Previous release.\n",
                encoding="utf-8",
            )

            result = prepare_release.update_release(root, "1.4.9", "2026-04-27", "Release helper", dry_run=False)

            self.assertFalse(result["ok"])
            self.assertEqual(prepare_release.exit_code_for(result), 1)
            self.assertEqual(result["errors"][0]["code"], "changelog_date_mismatch")
            self.assertEqual(changelog.read_text(encoding="utf-8").count("## [1.4.9]"), 1)

    def test_update_rejects_out_of_order_existing_changelog_release_without_writes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root)
            changelog = root / "CHANGELOG.md"
            changelog.write_text(
                "# Changelog\n\n"
                "## [Unreleased]\n\n"
                "## [1.4.8] - 2026-04-26\n\n"
                "### Fixed\n\n"
                "- Previous release.\n\n"
                "## [1.4.9] - 2026-04-27\n\n"
                "### Changed\n\n"
                "- Current release in the wrong position.\n",
                encoding="utf-8",
            )
            tracked_paths = [
                root / "app" / "main.py",
                root / "CHANGELOG.md",
                root / "docs" / "index.md",
                root / "docs" / "releases" / "v1.4.8.md",
            ]
            before = {path: path.read_bytes() for path in tracked_paths}
            new_notes = root / "docs" / "releases" / "v1.4.9.md"

            result = prepare_release.update_release(root, "1.4.9", "2026-04-27", "Release helper", dry_run=False)

            self.assertFalse(result["ok"])
            self.assertEqual(prepare_release.exit_code_for(result), 1)
            self.assertIn("changelog_release_out_of_order", {error["code"] for error in result["errors"]})
            self.assertEqual({path: path.read_bytes() for path in tracked_paths}, before)
            self.assertFalse(new_notes.exists())
            text = changelog.read_text(encoding="utf-8")
            self.assertEqual(text.count("## [1.4.9]"), 1)

    def test_check_rejects_duplicate_target_changelog_release(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root, version="1.4.9", latest="1.4.9")
            (root / "CHANGELOG.md").write_text(
                "# Changelog\n\n"
                "## [Unreleased]\n\n"
                "## [1.4.9] - 2026-04-27\n\n"
                "### Changed\n\n"
                "- Current release.\n\n"
                "## [1.4.8] - 2026-04-26\n\n"
                "### Fixed\n\n"
                "- Previous release.\n\n"
                "## [1.4.9] - 2026-04-27\n\n"
                "### Changed\n\n"
                "- Duplicate release.\n",
                encoding="utf-8",
            )
            (root / "docs" / "releases" / "v1.4.9.md").write_text(
                "# CogniRelay v1.4.9 Release Notes\n\nRelease date: 2026-04-27\n",
                encoding="utf-8",
            )
            (root / "docs" / "index.md").write_text(
                "# CogniRelay Documentation\n\n"
                "## Releases\n\n"
                "- [Latest release notes: v1.4.9](releases/v1.4.9.md)\n"
                "- [v1.4.8 release notes](releases/v1.4.8.md)\n",
                encoding="utf-8",
            )

            result = prepare_release.check_release(root, "1.4.9", "2026-04-27")

            self.assertFalse(result["ok"])
            self.assertEqual(prepare_release.exit_code_for(result), 1)
            self.assertIn("changelog_release_duplicate", {error["code"] for error in result["errors"]})

    def test_update_rejects_duplicate_target_changelog_release_without_writes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root, version="1.4.9", latest="1.4.9")
            changelog = root / "CHANGELOG.md"
            changelog.write_text(
                "# Changelog\n\n"
                "## [Unreleased]\n\n"
                "## [1.4.9] - 2026-04-27\n\n"
                "### Changed\n\n"
                "- Current release.\n\n"
                "## [1.4.8] - 2026-04-26\n\n"
                "### Fixed\n\n"
                "- Previous release.\n\n"
                "## [1.4.9] - 2026-04-27\n\n"
                "### Changed\n\n"
                "- Duplicate release.\n",
                encoding="utf-8",
            )
            (root / "docs" / "releases" / "v1.4.9.md").write_text(
                "# CogniRelay v1.4.9 Release Notes\n\nRelease date: 2026-04-27\n",
                encoding="utf-8",
            )
            tracked_paths = [
                root / "app" / "main.py",
                root / "CHANGELOG.md",
                root / "docs" / "index.md",
                root / "docs" / "releases" / "v1.4.9.md",
            ]
            before = {path: path.read_bytes() for path in tracked_paths}

            result = prepare_release.update_release(root, "1.4.9", "2026-04-27", "Release helper", dry_run=False)

            self.assertFalse(result["ok"])
            self.assertEqual(prepare_release.exit_code_for(result), 1)
            self.assertIn("changelog_release_duplicate", {error["code"] for error in result["errors"]})
            self.assertEqual({path: path.read_bytes() for path in tracked_paths}, before)
            self.assertEqual(changelog.read_text(encoding="utf-8").count("## [1.4.9]"), 2)

    def test_update_conflict_writes_no_release_surfaces(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root)
            notes = root / "docs" / "releases" / "v1.4.9.md"
            notes.write_text(
                "# CogniRelay v1.4.9 Release Notes\n\n"
                "Release date: 2026-04-26\n\n"
                "Conflicting existing notes.\n",
                encoding="utf-8",
            )
            tracked_paths = [
                root / "app" / "main.py",
                root / "CHANGELOG.md",
                root / "docs" / "index.md",
                notes,
            ]
            before = {path: path.read_text(encoding="utf-8") for path in tracked_paths}

            result = prepare_release.update_release(root, "1.4.9", "2026-04-27", "Release helper", dry_run=False)

            self.assertFalse(result["ok"])
            self.assertEqual(prepare_release.exit_code_for(result), 1)
            self.assertIn("release_notes_conflict", {error["code"] for error in result["errors"]})
            self.assertEqual({path: path.read_text(encoding="utf-8") for path in tracked_paths}, before)

    def test_changelog_check_requires_exact_heading_line(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root, version="1.4.9", latest="1.4.8")
            (root / "CHANGELOG.md").write_text(
                "# Changelog\n\n"
                "## [Unreleased]\n\n"
                "### Notes\n\n"
                "- Mention `## [1.4.9] - 2026-04-27` in prose, not as a heading.\n\n"
                "```text\n"
                "## [1.4.9] - 2026-04-27\n"
                "```\n\n"
                "## [1.4.8] - 2026-04-26\n\n"
                "### Fixed\n\n"
                "- Previous release.\n\n"
                "## [1.4.7] - 2026-04-25\n\n"
                "### Fixed\n\n"
                "- Older release.\n",
                encoding="utf-8",
            )
            (root / "docs" / "releases" / "v1.4.9.md").write_text(
                "# CogniRelay v1.4.9 Release Notes\n\nRelease date: 2026-04-27\n",
                encoding="utf-8",
            )
            (root / "docs" / "index.md").write_text(
                "# CogniRelay Documentation\n\n"
                "## Releases\n\n"
                "- [Latest release notes: v1.4.9](releases/v1.4.9.md)\n"
                "- [v1.4.8 release notes](releases/v1.4.8.md)\n",
                encoding="utf-8",
            )

            result = prepare_release.check_release(root, "1.4.9", "2026-04-27")

            self.assertFalse(result["ok"])
            self.assertIn("changelog_release_missing", {error["code"] for error in result["errors"]})

    def test_changelog_check_requires_requested_release_before_older_releases(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root, version="1.4.9", latest="1.4.9")
            (root / "CHANGELOG.md").write_text(
                "# Changelog\n\n"
                "## [Unreleased]\n\n"
                "## [1.4.8] - 2026-04-26\n\n"
                "### Fixed\n\n"
                "- Previous release.\n\n"
                "## [1.4.9] - 2026-04-27\n\n"
                "### Changed\n\n"
                "- Current release in the wrong position.\n",
                encoding="utf-8",
            )
            (root / "docs" / "releases" / "v1.4.9.md").write_text(
                "# CogniRelay v1.4.9 Release Notes\n\nRelease date: 2026-04-27\n",
                encoding="utf-8",
            )
            (root / "docs" / "index.md").write_text(
                "# CogniRelay Documentation\n\n"
                "## Releases\n\n"
                "- [Latest release notes: v1.4.9](releases/v1.4.9.md)\n"
                "- [v1.4.8 release notes](releases/v1.4.8.md)\n",
                encoding="utf-8",
            )

            result = prepare_release.check_release(root, "1.4.9", "2026-04-27")

            self.assertFalse(result["ok"])
            self.assertIn("changelog_release_missing", {error["code"] for error in result["errors"]})

    def test_docs_index_check_requires_exact_previous_latest_from_changelog(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root, version="1.4.9", latest="1.4.9")
            (root / "CHANGELOG.md").write_text(
                "# Changelog\n\n"
                "## [Unreleased]\n\n"
                "## [1.4.9] - 2026-04-27\n\n"
                "### Changed\n\n"
                "- Current release.\n\n"
                "## [1.4.8] - 2026-04-26\n\n"
                "### Fixed\n\n"
                "- Previous release.\n\n"
                "## [1.4.7] - 2026-04-25\n\n"
                "### Fixed\n\n"
                "- Older release.\n",
                encoding="utf-8",
            )
            (root / "docs" / "releases" / "v1.4.9.md").write_text(
                "# CogniRelay v1.4.9 Release Notes\n\nRelease date: 2026-04-27\n",
                encoding="utf-8",
            )
            (root / "docs" / "index.md").write_text(
                "# CogniRelay Documentation\n\n"
                "## Releases\n\n"
                "- [Latest release notes: v1.4.9](releases/v1.4.9.md)\n"
                "- [v1.4.7 release notes](releases/v1.4.7.md)\n",
                encoding="utf-8",
            )

            result = prepare_release.check_release(root, "1.4.9", "2026-04-27")

            self.assertFalse(result["ok"])
            self.assertIn("docs_previous_latest_missing", {error["code"] for error in result["errors"]})

    def test_docs_index_update_repairs_current_latest_with_wrong_previous_link(self) -> None:
        cases = {
            "missing": "",
            "wrong": "- [v1.4.7 release notes](releases/v1.4.7.md)\n",
        }
        for name, existing_previous in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as td:
                root = Path(td)
                _write_fixture(root, version="1.4.9", latest="1.4.9")
                (root / "CHANGELOG.md").write_text(
                    "# Changelog\n\n"
                    "## [Unreleased]\n\n"
                    "## [1.4.9] - 2026-04-27\n\n"
                    "### Changed\n\n"
                    "- Current release.\n\n"
                    "## [1.4.8] - 2026-04-26\n\n"
                    "### Fixed\n\n"
                    "- Previous release.\n\n"
                    "## [1.4.7] - 2026-04-25\n\n"
                    "### Fixed\n\n"
                    "- Older release.\n",
                    encoding="utf-8",
                )
                (root / "docs" / "releases" / "v1.4.9.md").write_text(
                    "# CogniRelay v1.4.9 Release Notes\n\nRelease date: 2026-04-27\n",
                    encoding="utf-8",
                )
                (root / "docs" / "index.md").write_text(
                    "# CogniRelay Documentation\n\n"
                    "## Releases\n\n"
                    "- [Latest release notes: v1.4.9](releases/v1.4.9.md)\n"
                    f"{existing_previous}"
                    "- [v1.4.9 release notes](releases/v1.4.9.md)\n"
                    "- [Changelog](https://github.com/stef-k/CogniRelay/blob/main/CHANGELOG.md)\n\n"
                    "## Other Links\n\n"
                    "- [Latest release notes: v1.4.9](releases/v1.4.9.md)\n",
                    encoding="utf-8",
                )

                result = prepare_release.update_release(root, "1.4.9", "2026-04-27", "Release helper", dry_run=False)

                self.assertTrue(result["ok"], result["errors"])
                text = (root / "docs" / "index.md").read_text(encoding="utf-8")
                releases_block = text.split("## Releases", 1)[1].split("## Other Links", 1)[0]
                release_lines = releases_block.strip().splitlines()
                self.assertEqual(release_lines[0], "- [Latest release notes: v1.4.9](releases/v1.4.9.md)")
                self.assertEqual(release_lines[1], "- [v1.4.8 release notes](releases/v1.4.8.md)")
                self.assertNotIn("- [v1.4.9 release notes](releases/v1.4.9.md)", releases_block)
                check = prepare_release.check_release(root, "1.4.9", "2026-04-27")
                self.assertTrue(check["ok"], check["errors"])

    def test_existing_matching_release_notes_are_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root)
            notes = root / "docs" / "releases" / "v1.4.9.md"
            notes.write_text(
                "# CogniRelay v1.4.9 Release Notes\n\n"
                "Release date: 2026-04-27\n\n"
                "Custom prose.\n",
                encoding="utf-8",
            )

            result = prepare_release.update_release(root, "1.4.9", "2026-04-27", "Release helper", dry_run=False)

            self.assertTrue(result["ok"], result["errors"])
            self.assertIn("Custom prose.", notes.read_text(encoding="utf-8"))
            release_entry = next(entry for entry in result["updated"] if entry["surface"] == "release_notes")
            self.assertEqual(release_entry["action"], "unchanged")

    def test_docs_index_update_is_bounded_to_release_list(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root)

            result = prepare_release.update_release(root, "1.4.9", "2026-04-27", "Release helper", dry_run=False)

            self.assertTrue(result["ok"], result["errors"])
            text = (root / "docs" / "index.md").read_text(encoding="utf-8")
            releases_block = text.split("## Releases", 1)[1].split("## Other Links", 1)[0]
            self.assertIn("- [Latest release notes: v1.4.9](releases/v1.4.9.md)", releases_block)
            self.assertIn("- [v1.4.8 release notes](releases/v1.4.8.md)", releases_block)
            self.assertIn("- [Latest release notes: v1.4.8](releases/v1.4.8.md)", text.split("## Other Links", 1)[1])

    def test_dry_run_writes_no_files_and_reports_would_write(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root)
            before = (root / "app" / "main.py").read_text(encoding="utf-8")

            result = prepare_release.update_release(root, "1.4.9", "2026-04-27", "Release helper", dry_run=True)

            self.assertTrue(result["ok"], result["errors"])
            self.assertTrue(result["dry_run"])
            self.assertEqual((root / "app" / "main.py").read_text(encoding="utf-8"), before)
            self.assertTrue(all("would_write" in entry for entry in result["updated"]))
            self.assertFalse((root / "docs" / "releases" / "v1.4.9.md").exists())

    def test_update_rolls_back_earlier_writes_when_later_write_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root)
            tracked_paths = [
                root / "app" / "main.py",
                root / "CHANGELOG.md",
                root / "docs" / "index.md",
            ]
            before = {path: path.read_bytes() for path in tracked_paths}
            notes = root / "docs" / "releases" / "v1.4.9.md"
            original_write_text = prepare_release.write_text

            def fail_docs_index(path: Path, root: Path, surface: str, content: str) -> None:
                if surface == "docs_index":
                    raise prepare_release.SurfaceError("write_failed", "cannot write docs/index.md", surface, "docs/index.md")
                original_write_text(path, root, surface, content)

            with mock.patch.object(prepare_release, "write_text", side_effect=fail_docs_index):
                result = prepare_release.update_release(root, "1.4.9", "2026-04-27", "Release helper", dry_run=False)

            self.assertFalse(result["ok"])
            self.assertEqual(prepare_release.exit_code_for(result), 3)
            self.assertEqual(result["errors"][0]["code"], "write_failed")
            self.assertEqual({path: path.read_bytes() for path in tracked_paths}, before)
            self.assertFalse(notes.exists())

    def test_update_rolls_back_new_release_notes_parent_directory(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root)
            releases_dir = root / "docs" / "releases"
            for child in releases_dir.iterdir():
                child.unlink()
            releases_dir.rmdir()
            tracked_paths = [
                root / "app" / "main.py",
                root / "CHANGELOG.md",
                root / "docs" / "index.md",
            ]
            before = {path: path.read_bytes() for path in tracked_paths}
            original_write_text = prepare_release.write_text

            def fail_docs_index(path: Path, root: Path, surface: str, content: str) -> None:
                if surface == "docs_index":
                    raise prepare_release.SurfaceError("write_failed", "cannot write docs/index.md", surface, "docs/index.md")
                original_write_text(path, root, surface, content)

            with mock.patch.object(prepare_release, "write_text", side_effect=fail_docs_index):
                result = prepare_release.update_release(root, "1.4.9", "2026-04-27", "Release helper", dry_run=False)

            self.assertFalse(result["ok"])
            self.assertEqual(prepare_release.exit_code_for(result), 3)
            self.assertEqual({path: path.read_bytes() for path in tracked_paths}, before)
            self.assertFalse((root / "docs" / "releases" / "v1.4.9.md").exists())
            self.assertFalse(releases_dir.exists())


class PrepareReleaseCliTests(unittest.TestCase):
    def _run_cli(self, *args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(_helper_path), *args],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def test_parser_validation_failures_emit_single_json_object(self) -> None:
        proc = self._run_cli()

        self.assertEqual(proc.returncode, 2)
        payload = json.loads(proc.stdout)
        self.assertFalse(payload["ok"])
        self.assertIsNone(payload["mode"])
        self.assertIsNone(payload["version"])
        self.assertEqual(payload["checked"], [])
        self.assertEqual(payload["updated"], [])
        self.assertEqual(len(payload["errors"]), 1)
        self.assertFalse(payload["dry_run"])

    def test_invalid_version_date_and_title_exit_2(self) -> None:
        cases = [
            ("check", "--version", "1.2"),
            ("check", "--version", "1.2.3", "--date", "2026-02-30"),
            ("update", "--version", "1.2.3", "--title", "bad\ntitle"),
        ]
        for args in cases:
            with self.subTest(args=args):
                proc = self._run_cli(*args)
                self.assertEqual(proc.returncode, 2)
                self.assertFalse(json.loads(proc.stdout)["ok"])

    def test_update_dry_run_validation_failures_preserve_dry_run_flag(self) -> None:
        cases = [
            ("update", "--version", "bad", "--title", "T", "--dry-run"),
            ("update", "--version", "1.4.9", "--title", "T", "--date", "bad-date", "--dry-run"),
        ]
        for args in cases:
            with self.subTest(args=args):
                proc = self._run_cli(*args)
                payload = json.loads(proc.stdout)

                self.assertEqual(proc.returncode, 2)
                self.assertFalse(payload["ok"])
                self.assertTrue(payload["dry_run"])

    def test_dirty_worktree_exits_2_unless_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _write_fixture(root)
            subprocess.run(["git", "init"], cwd=root, check=True, capture_output=True, text=True)
            subprocess.run(["git", "add", "."], cwd=root, check=True, capture_output=True, text=True)
            git_env = {
                **os.environ,
                "GIT_AUTHOR_NAME": "Test",
                "GIT_AUTHOR_EMAIL": "test@example.com",
                "GIT_COMMITTER_NAME": "Test",
                "GIT_COMMITTER_EMAIL": "test@example.com",
            }
            subprocess.run(["git", "commit", "-m", "fixture"], cwd=root, check=True, capture_output=True, text=True, env=git_env)
            (root / "untracked.txt").write_text("dirty\n", encoding="utf-8")

            dirty = self._run_cli("check", "--version", "1.4.8", "--date", "2026-04-26", cwd=root)
            allowed = self._run_cli("check", "--version", "1.4.8", "--date", "2026-04-26", "--allow-dirty", cwd=root)

            self.assertEqual(dirty.returncode, 2)
            self.assertEqual(json.loads(dirty.stdout)["errors"][0]["code"], "dirty_worktree")
            self.assertEqual(allowed.returncode, 0)


if __name__ == "__main__":
    unittest.main()
