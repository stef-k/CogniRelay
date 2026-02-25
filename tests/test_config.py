import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.config import get_settings


class TestConfigTokens(unittest.TestCase):
    def test_no_implicit_dev_token_when_env_tokens_unset(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with patch.dict(os.environ, {"COGNIRELAY_REPO_ROOT": td}, clear=True):
                settings = get_settings(force_reload=True)
                self.assertEqual(settings.tokens, {})
                self.assertNotIn("change-me-local-dev-token", settings.tokens)

    def test_tokens_load_from_file_without_env_tokens(self) -> None:
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


if __name__ == "__main__":
    unittest.main()
