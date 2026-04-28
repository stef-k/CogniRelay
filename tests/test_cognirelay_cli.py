"""Tests for the thin CogniRelay package CLI."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

from cognirelay import cli

AGENT_ASSET_FILES = (
    "README.md",
    "hooks/README.md",
    "hooks/cognirelay_continuity_save_hook.py",
    "hooks/cognirelay_retrieval_hook.py",
    "skills/cognirelay-continuity-authoring/SKILL.md",
)


def _write_installed_assets(root: Path) -> None:
    for relative in AGENT_ASSET_FILES:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{relative}\n", encoding="utf-8")


def _run_cli(argv: list[str]) -> tuple[int, str, str]:
    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        result = cli.main(argv)
    return result, stdout.getvalue(), stderr.getvalue()


class CogniRelayCliTests(unittest.TestCase):
    def test_help_commands_work_from_temp_cwd_without_state(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as td:
            cwd = Path(td)
            env = {**os.environ, "PYTHONPATH": str(repo_root)}
            env.pop("COGNIRELAY_REPO_ROOT", None)
            cases = (
                [sys.executable, "-m", "cognirelay", "--help"],
                [sys.executable, "-m", "cognirelay", "serve", "--help"],
            )
            for command in cases:
                with self.subTest(command=command):
                    proc = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True, timeout=10)
                    self.assertEqual(proc.returncode, 0, proc.stderr)
                    self.assertIn("usage: cognirelay", proc.stdout)
                    self.assertFalse((cwd / "data_repo").exists())

    def test_invalid_args_exit_2(self) -> None:
        proc = subprocess.run(
            [sys.executable, "-m", "cognirelay", "serve", "--log-level", "verbose"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        self.assertEqual(proc.returncode, 2)

    def test_serve_calls_uvicorn_with_existing_app_import_path(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch("uvicorn.run") as run:
            result = cli.main(["serve", "--host", "0.0.0.0", "--port", "9000", "--log-level", "debug", "--reload"])

        self.assertEqual(result, 0)
        run.assert_called_once_with(
            "app.main:app",
            host="0.0.0.0",
            port=9000,
            log_level="debug",
            reload=True,
        )

    def test_runtime_state_guard_rejects_site_packages_default_before_creation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            site_packages = Path(td) / "lib" / "python3.12" / "site-packages"
            package_dir = site_packages / "cognirelay"
            package_dir.mkdir(parents=True)
            package_file = package_dir / "cli.py"
            package_file.write_text("", encoding="utf-8")

            with mock.patch.object(cli, "_site_package_roots", return_value=(site_packages.resolve(),)):
                with self.assertRaises(cli.RuntimeStateError):
                    cli.validate_runtime_state_root(None, cwd=site_packages, package_file=package_file)

            self.assertFalse((site_packages / "data_repo").exists())

    def test_runtime_state_guard_rejects_explicit_package_path_before_creation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            site_packages = Path(td) / "site-packages"
            package_dir = site_packages / "cognirelay"
            package_dir.mkdir(parents=True)
            package_file = package_dir / "cli.py"
            package_file.write_text("", encoding="utf-8")
            unsafe_root = package_dir / "data_repo"

            with mock.patch.object(cli, "_site_package_roots", return_value=(site_packages.resolve(),)):
                with self.assertRaises(cli.RuntimeStateError):
                    cli.validate_runtime_state_root(str(unsafe_root), cwd=Path(td), package_file=package_file)

            self.assertFalse(unsafe_root.exists())

    def test_assets_path_and_list_validate_installed_assets(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            assets_root = Path(td) / "site-packages" / "cognirelay" / "agent_assets"
            _write_installed_assets(assets_root)

            with mock.patch.object(cli, "_installed_agent_assets_root", return_value=assets_root):
                path_result, path_stdout, path_stderr = _run_cli(["assets", "path"])
                list_result, list_stdout, list_stderr = _run_cli(["assets", "list"])

            self.assertEqual(path_result, 0)
            self.assertEqual(path_stdout, f"{assets_root}\n")
            self.assertEqual(path_stderr, "")
            self.assertEqual(list_result, 0)
            self.assertEqual(list_stdout.splitlines(), sorted(AGENT_ASSET_FILES))
            self.assertEqual(list_stderr, "")

    def test_assets_copy_target_behaviors(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            assets_root = root / "site-packages" / "cognirelay" / "agent_assets"
            _write_installed_assets(assets_root)

            cases_root = root / "cases"
            with mock.patch.object(cli, "_installed_agent_assets_root", return_value=assets_root):
                absent_result, absent_stdout, absent_stderr = _run_cli(["assets", "copy", "--to", str(cases_root / "absent")])
                empty_target = cases_root / "empty" / "agent-assets"
                empty_target.mkdir(parents=True)
                empty_result, empty_stdout, empty_stderr = _run_cli(["assets", "copy", "--to", str(cases_root / "empty")])
                non_empty_target = cases_root / "non-empty" / "agent-assets"
                non_empty_target.mkdir(parents=True)
                (non_empty_target / "old.txt").write_text("old\n", encoding="utf-8")
                non_empty_result, non_empty_stdout, non_empty_stderr = _run_cli(["assets", "copy", "--to", str(cases_root / "non-empty")])
                force_result, force_stdout, force_stderr = _run_cli(["assets", "copy", "--to", str(cases_root / "non-empty"), "--force"])
                file_parent = cases_root / "file"
                file_parent.mkdir()
                (file_parent / "agent-assets").write_text("not a directory\n", encoding="utf-8")
                file_result, file_stdout, file_stderr = _run_cli(["assets", "copy", "--to", str(file_parent), "--force"])
                symlink_parent = cases_root / "symlink"
                symlink_parent.mkdir()
                real_target = cases_root / "real-target"
                real_target.mkdir()
                (symlink_parent / "agent-assets").symlink_to(real_target, target_is_directory=True)
                symlink_result, symlink_stdout, symlink_stderr = _run_cli(["assets", "copy", "--to", str(symlink_parent), "--force"])

            self.assertEqual(absent_result, 0, absent_stderr)
            self.assertEqual(absent_stdout, f"{(cases_root / 'absent' / 'agent-assets').resolve()}\n")
            self.assertEqual(absent_stderr, "")
            self.assertEqual(empty_result, 0, empty_stderr)
            self.assertEqual(empty_stdout, f"{empty_target.resolve()}\n")
            self.assertEqual(empty_stderr, "")
            self.assertEqual(non_empty_result, 1)
            self.assertEqual(non_empty_stdout, "")
            self.assertIn("target directory is not empty", non_empty_stderr)
            self.assertEqual(force_result, 0, force_stderr)
            self.assertEqual(force_stdout, f"{non_empty_target.resolve()}\n")
            self.assertEqual(force_stderr, "")
            self.assertFalse((non_empty_target / "old.txt").exists())
            self.assertEqual(file_result, 1)
            self.assertEqual(file_stdout, "")
            self.assertIn("target path is not a directory", file_stderr)
            self.assertEqual(symlink_result, 1)
            self.assertEqual(symlink_stdout, "")
            self.assertIn("target path is not a real directory", symlink_stderr)

            copied = cases_root / "absent" / "agent-assets"
            for relative in AGENT_ASSET_FILES:
                self.assertTrue((copied / relative).is_file())

    def test_assets_copy_allows_symlinked_parent_directory(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            assets_root = root / "site-packages" / "cognirelay" / "agent_assets"
            _write_installed_assets(assets_root)
            real_parent = root / "real-parent"
            real_parent.mkdir()
            linked_parent = root / "linked-parent"
            linked_parent.symlink_to(real_parent, target_is_directory=True)

            with mock.patch.object(cli, "_installed_agent_assets_root", return_value=assets_root):
                result, stdout, stderr = _run_cli(["assets", "copy", "--to", str(linked_parent)])

            self.assertEqual(result, 0, stderr)
            self.assertEqual(stdout, f"{(real_parent / 'agent-assets').resolve()}\n")
            self.assertEqual(stderr, "")

    def test_assets_commands_fail_when_installed_assets_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            assets_root = Path(td) / "site-packages" / "cognirelay" / "agent_assets"
            _write_installed_assets(assets_root)
            (assets_root / "hooks" / "cognirelay_retrieval_hook.py").unlink()

            with mock.patch.object(cli, "_installed_agent_assets_root", return_value=assets_root):
                cases = (
                    ["assets", "path"],
                    ["assets", "list"],
                    ["assets", "copy", "--to", str(Path(td) / "out")],
                )
                for argv in cases:
                    with self.subTest(argv=argv):
                        result, stdout, stderr = _run_cli(argv)
                        self.assertEqual(result, 1)
                        self.assertEqual(stdout, "")
                        self.assertIn("installed agent assets are unavailable", stderr)

    def test_assets_commands_reject_extra_installed_bytecode_and_cache_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cases = {
                "hook-name-pyc": ("hooks/cognirelay_retrieval_hook.pyc", b"cache"),
                "pycache-directory": ("hooks/__pycache__", None),
            }
            commands = (
                ["assets", "path"],
                ["assets", "list"],
                ["assets", "copy", "--to", str(root / "out")],
            )
            for case, (extra_relative, payload) in cases.items():
                for argv in commands:
                    with self.subTest(case=case, argv=argv):
                        assets_root = root / case / "-".join(argv[:2]) / "site-packages" / "cognirelay" / "agent_assets"
                        _write_installed_assets(assets_root)
                        extra = assets_root / extra_relative
                        if payload is None:
                            extra.mkdir(parents=True)
                        else:
                            extra.parent.mkdir(parents=True, exist_ok=True)
                            extra.write_bytes(payload)

                        with mock.patch.object(cli, "_installed_agent_assets_root", return_value=assets_root):
                            result, stdout, stderr = _run_cli(argv)

                        self.assertEqual(result, 1)
                        self.assertEqual(stdout, "")
                        self.assertIn("unexpected entries", stderr)

    def test_assets_copy_from_source_tree_does_not_create_package_mirror(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        package_asset_root = repo_root / "cognirelay" / "agent_assets"
        self.assertFalse(package_asset_root.exists())


if __name__ == "__main__":
    unittest.main()
