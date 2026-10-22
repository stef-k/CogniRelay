"""Verify that deployment authenticates pip before downloading dependencies."""

from __future__ import annotations

import shlex
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PIP_SHA256 = "fd7f4dfa8190e014013b9f08f3e973f4e4c26ffd179a204895533a7254813a70"


class PipBootstrap315Tests(unittest.TestCase):
    """Exercise the first deployment download and its failure boundary."""

    def bootstrap_command(self) -> list[str]:
        """Read the first pip invocation after the offline venv creation."""
        script = (ROOT / "deploy/scripts/bootstrap-debian-nginx.sh").read_text(encoding="utf-8").replace("\\\n", " ")
        line = next(line for line in script.splitlines() if line.startswith('sudo -u "$SERVICE_USER" "$APP_DIR/.venv/bin/'))
        return shlex.split(line)[3:]

    def test_first_download_is_only_the_authenticated_pip_wheel(self) -> None:
        """Keep the first install independent of package indexes and old CA trust."""
        command = self.bootstrap_command()
        self.assertTrue(command[0].endswith("/.venv/bin/python"))
        self.assertEqual(command[1:7], ["-I", "-m", "pip", "--isolated", "--disable-pip-version-check", "install"])
        for option in ("--no-index", "--no-deps", "--no-cache-dir", "--upgrade"):
            self.assertIn(option, command)
        self.assertEqual(command[-1], "pip @ https://files.pythonhosted.org/packages/3f/06/3e4420302363fbb71be3c7f35ce042f54bfca330e3e66b3a2e3c4cca579f/"
                         f"pip-26.2.2-py3-none-any.whl#sha256={PIP_SHA256}")

    def test_tampered_bootstrap_aborts_before_next_install(self) -> None:
        """Reject substituted wheel bytes before any subsequent installation."""
        command = self.bootstrap_command()
        self.assertIn(f"#sha256={PIP_SHA256}", command[-1])
        with tempfile.TemporaryDirectory() as td:
            wheel = Path(td) / "pip-26.2.2-py3-none-any.whl"
            wheel.write_bytes(b"tampered bootstrap download")
            command[0] = sys.executable
            command[-1] = f"pip @ {wheel.as_uri()}#sha256={PIP_SHA256}"
            marker = Path(td) / "next-install"
            result = subprocess.run(["bash", "-c", f"set -e\n{shlex.join(command)}\ntouch {shlex.quote(str(marker))}"], capture_output=True, text=True, timeout=30)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("DO NOT MATCH THE HASHES", result.stderr)
            self.assertFalse(marker.exists())
