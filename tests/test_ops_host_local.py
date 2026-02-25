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
    def __init__(self, client_ip: str | None) -> None:
        self.peer_id = "peer-host"
        self.client_ip = client_ip

    def require(self, _scope: str) -> None:
        return None

    def require_write_path(self, _path: str) -> None:
        return None

    def require_read_path(self, _path: str) -> None:
        return None


class _GitManagerStub:
    def commit_file(self, _path: Path, _message: str) -> bool:
        return True

    def latest_commit(self) -> str:
        return "test-sha"


class TestOpsHostLocal(unittest.TestCase):
    def _settings(self, repo_root: Path) -> Settings:
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def test_ops_catalog_local_only_enforcement(self) -> None:
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
        with tempfile.TemporaryDirectory() as td:
            settings = self._settings(Path(td))
            with patch("app.main._services", return_value=(settings, _GitManagerStub())):
                systemd = ops_schedule_export(format="systemd", auth=_AuthStub(client_ip="127.0.0.1"))
                cron = ops_schedule_export(format="cron", auth=_AuthStub(client_ip="127.0.0.1"))

        self.assertTrue(systemd["ok"])
        self.assertEqual(systemd["format"], "systemd")
        self.assertIn("service_unit", systemd["examples"])
        self.assertTrue(cron["ok"])
        self.assertEqual(cron["format"], "cron")
        self.assertIn("cron_examples", cron["examples"])


if __name__ == "__main__":
    unittest.main()
