"""Tests for the thin CogniRelay package CLI."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from cognirelay import cli


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


if __name__ == "__main__":
    unittest.main()
