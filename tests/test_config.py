"""Tests for token configuration loading and environment fallback behavior."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.config import get_settings


class TestConfigTokens(unittest.TestCase):
    """Cover token loading behavior for config file and environment sources."""

    def test_no_implicit_dev_token_when_env_tokens_unset(self) -> None:
        """Ensure no synthetic dev token is created when env tokens are absent."""
        with tempfile.TemporaryDirectory() as td:
            with patch.dict(os.environ, {"COGNIRELAY_REPO_ROOT": td}, clear=True):
                settings = get_settings(force_reload=True)
                self.assertEqual(settings.tokens, {})
                self.assertNotIn("change-me-local-dev-token", settings.tokens)

    def test_tokens_load_from_file_without_env_tokens(self) -> None:
        """Ensure token config is loaded from disk when env tokens are not supplied."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            config_dir = repo_root / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            (config_dir / "peer_tokens.json").write_text(
                json.dumps(
                    {
                        "tokens": [
                            {
                                "peer_id": "peer-a",
                                "token": "file-token-a",
                                "scopes": ["read:files"],
                                "read_namespaces": ["memory"],
                                "write_namespaces": ["messages"],
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"COGNIRELAY_REPO_ROOT": td}, clear=True):
                settings = get_settings(force_reload=True)

            self.assertIn("file-token-a", settings.tokens)
            self.assertNotIn("change-me-local-dev-token", settings.tokens)

    def test_continuity_retention_archive_days_reads_env_and_clamps_minimum(self) -> None:
        """Retention archive days should come from central env config with minimum clamping."""
        with tempfile.TemporaryDirectory() as td:
            with patch.dict(
                os.environ,
                {
                    "COGNIRELAY_REPO_ROOT": td,
                    "COGNIRELAY_CONTINUITY_RETENTION_ARCHIVE_DAYS": "0",
                },
                clear=True,
            ):
                settings = get_settings(force_reload=True)

            self.assertEqual(settings.continuity_retention_archive_days, 1)

    def test_ui_flags_load_from_env_with_expected_defaults(self) -> None:
        """UI flags should load from env and default to safe read-only posture."""
        with tempfile.TemporaryDirectory() as td:
            with patch.dict(os.environ, {"COGNIRELAY_REPO_ROOT": td}, clear=True):
                defaults = get_settings(force_reload=True)

            self.assertFalse(defaults.ui_enabled)
            self.assertTrue(defaults.ui_require_localhost)
            self.assertTrue(defaults.ui_read_only)

            with patch.dict(
                os.environ,
                {
                    "COGNIRELAY_REPO_ROOT": td,
                    "COGNIRELAY_UI_ENABLED": "true",
                    "COGNIRELAY_UI_REQUIRE_LOCALHOST": "false",
                    "COGNIRELAY_UI_READ_ONLY": "false",
                },
                clear=True,
            ):
                configured = get_settings(force_reload=True)

            self.assertTrue(configured.ui_enabled)
            self.assertFalse(configured.ui_require_localhost)
            self.assertFalse(configured.ui_read_only)


if __name__ == "__main__":
    unittest.main()
