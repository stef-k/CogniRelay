"""Tests for host-local operations endpoints and scheduling helpers."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.main import ops_catalog, ops_run, ops_schedule_export, ops_status
from app.models import OpsRunRequest


class _AuthStub:
    """Auth stub carrying the caller IP used by host-local ops tests."""

    def __init__(self, client_ip: str | None) -> None:
        """Store the client identity and address for the test request."""
        self.peer_id = "peer-host"
        self.client_ip = client_ip

    def require(self, _scope: str) -> None:
        """Accept any requested scope for test purposes."""
        return None

    def require_write_path(self, _path: str) -> None:
        """Accept any requested write path for test purposes."""
        return None

    def require_read_path(self, _path: str) -> None:
        """Accept any requested read path for test purposes."""
        return None


class _GitManagerStub:
    """Git manager stub that pretends every file commit succeeds."""

    def commit_file(self, _path: Path, _message: str) -> bool:
        """Report a successful commit without touching git."""
        return True

    def latest_commit(self) -> str:
        """Return a stable fake commit hash."""
        return "test-sha"


class TestOpsHostLocal(unittest.TestCase):
    """Validate local-only ops access, status, and schedule rendering."""

    def _settings(self, repo_root: Path) -> Settings:
        """Build a settings object rooted at the temporary repository."""
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def test_ops_catalog_local_only_enforcement(self) -> None:
        """Ops catalog should reject non-local callers and allow loopback callers."""
        with tempfile.TemporaryDirectory() as td:
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                local = ops_catalog(auth=_AuthStub(client_ip="127.0.0.1"))
                with self.assertRaises(HTTPException) as err:
                    ops_catalog(auth=_AuthStub(client_ip="10.1.2.3"))

        self.assertTrue(local["ok"])
        self.assertTrue(local["local_only"])
        self.assertTrue(any(job["job_id"] == "backup.restore_test" for job in local["jobs"]))
        self.assertEqual(err.exception.status_code, 403)

    def test_ops_run_writes_run_record_and_status(self) -> None:
        """Running an ops job should write a run record and show up in status."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                out = ops_run(
                    req=OpsRunRequest(job_id="metrics.poll_and_alarm_eval"),
                    auth=_AuthStub(client_ip="127.0.0.1"),
                )
                status = ops_status(limit=20, auth=_AuthStub(client_ip="127.0.0.1"))

            runs_path = repo_root / "logs" / "ops_runs.jsonl"
            rows = [json.loads(x) for x in runs_path.read_text(encoding="utf-8").splitlines() if x.strip()]

        self.assertTrue(out["ok"])
        self.assertEqual(out["status"], "succeeded")
        self.assertTrue(status["ok"])
        self.assertGreaterEqual(len(status["recent_runs"]), 1)
        self.assertEqual(rows[-1]["job_id"], "metrics.poll_and_alarm_eval")
        self.assertEqual(rows[-1]["status"], "succeeded")

    def test_ops_run_lock_conflict(self) -> None:
        """Ops run should fail when a lockfile already exists for the same job."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            lock_path = repo_root / "logs" / "ops_locks" / "metrics.poll_and_alarm_eval.lock"
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            lock_path.write_text("{}", encoding="utf-8")

            settings = self._settings(repo_root)
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                with self.assertRaises(HTTPException) as err:
                    ops_run(
                        req=OpsRunRequest(job_id="metrics.poll_and_alarm_eval"),
                        auth=_AuthStub(client_ip="127.0.0.1"),
                    )

        self.assertEqual(err.exception.status_code, 409)
        self.assertIn("already running", str(err.exception.detail))

    def test_ops_schedule_export(self) -> None:
        """Schedule export should render both systemd and cron examples."""
        with tempfile.TemporaryDirectory() as td:
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                systemd = ops_schedule_export(format="systemd", auth=_AuthStub(client_ip="127.0.0.1"))
                cron = ops_schedule_export(format="cron", auth=_AuthStub(client_ip="127.0.0.1"))

        self.assertTrue(systemd["ok"])
        self.assertEqual(systemd["format"], "systemd")
        self.assertIn("service_unit", systemd["examples"])
        self.assertIn('{"job_id":"metrics.poll_and_alarm_eval"}', systemd["examples"]["service_unit"]["ExecStart"])
        self.assertTrue(cron["ok"])
        self.assertEqual(cron["format"], "cron")
        self.assertIn("cron_examples", cron["examples"])
        self.assertIn('{"job_id":"index.rebuild_incremental"}', cron["examples"]["cron_examples"][0])


class TestReleaseOpsLock(unittest.TestCase):
    """Tests for _release_ops_lock error handling."""

    def test_release_ops_lock_logs_on_oserror(self) -> None:
        """OSError during lock release must be logged, not silently swallowed."""
        from app.ops.service import _release_ops_lock

        with tempfile.TemporaryDirectory() as td:
            lock = Path(td) / "test.lock"
            lock.touch()
            with patch.object(Path, "unlink", side_effect=PermissionError("denied")):
                with self.assertLogs("app.ops.service", level="WARNING") as cm:
                    _release_ops_lock(lock)
            self.assertIn("failed to release ops lock", cm.output[0])

    def test_release_ops_lock_succeeds_normally(self) -> None:
        """Lock release should delete the file without logging."""
        from app.ops.service import _release_ops_lock

        with tempfile.TemporaryDirectory() as td:
            lock = Path(td) / "test.lock"
            lock.touch()
            _release_ops_lock(lock)
            self.assertFalse(lock.exists())


if __name__ == "__main__":
    unittest.main()
