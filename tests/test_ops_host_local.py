"""Tests for host-local operations endpoints and scheduling helpers."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from app.config import Settings
from app.main import ops_catalog, ops_run, ops_schedule_export, ops_status
from app.models import OpsRunRequest
from app.ops.service import _load_ops_runs, _release_ops_lock


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


def _make_settings(repo_root: Path) -> Settings:
    """Build a settings object rooted at a temporary repository."""
    return Settings(
        repo_root=repo_root,
        auto_init_git=False,
        git_author_name="n/a",
        git_author_email="n/a",
        tokens={},
        audit_log_enabled=False,
    )


class TestOpsHostLocal(unittest.TestCase):
    """Validate local-only ops access, status, and schedule rendering."""

    def test_ops_catalog_local_only_enforcement(self) -> None:
        """Ops catalog should reject non-local callers and allow loopback callers."""
        with tempfile.TemporaryDirectory() as td:
            settings = _make_settings(Path(td))
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
            settings = _make_settings(repo_root)
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

            settings = _make_settings(repo_root)
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
            settings = _make_settings(Path(td))
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


class TestOpsLockReleasedOnAppendFailure(unittest.TestCase):
    """Lock must be released and audit must fire even when _append_ops_run raises."""

    def test_lock_released_when_append_ops_run_raises(self) -> None:
        """If _append_ops_run raises OSError, the lock file must still be removed."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _make_settings(repo_root)
            lock_dir = repo_root / "logs" / "ops_locks"
            lock_dir.mkdir(parents=True, exist_ok=True)
            lock_file = lock_dir / "metrics.poll_and_alarm_eval.lock"

            append_called = False

            def _fail_append(*_args: object, **_kwargs: object) -> None:
                nonlocal append_called
                append_called = True
                assert lock_file.exists(), "Lock must exist when _append_ops_run is called"
                raise OSError("disk full")

            with (
                patch("app.main._services", return_value=(settings, _GitManagerStub())),
                patch("app.ops.service._append_ops_run", side_effect=_fail_append),
            ):
                with self.assertLogs("app.ops.service", level="WARNING"):
                    result = ops_run(
                        req=OpsRunRequest(job_id="metrics.poll_and_alarm_eval"),
                        auth=_AuthStub(client_ip="127.0.0.1"),
                    )

            self.assertTrue(append_called, "_append_ops_run must have been called")
            self.assertFalse(
                lock_file.exists(),
                "Lock file must be removed even when _append_ops_run raises",
            )
            self.assertTrue(result["ok"])

    def test_audit_called_when_append_ops_run_raises(self) -> None:
        """Audit must still fire even when _append_ops_run raises."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _make_settings(repo_root)
            lock_dir = repo_root / "logs" / "ops_locks"
            lock_dir.mkdir(parents=True, exist_ok=True)
            audit_spy = MagicMock()

            with (
                patch("app.main._services", return_value=(settings, _GitManagerStub())),
                patch(
                    "app.ops.service._append_ops_run",
                    side_effect=OSError("disk full"),
                ),
                patch("app.main._audit", audit_spy),
            ):
                with self.assertLogs("app.ops.service", level="WARNING"):
                    ops_run(
                        req=OpsRunRequest(job_id="metrics.poll_and_alarm_eval"),
                        auth=_AuthStub(client_ip="127.0.0.1"),
                    )

            audit_spy.assert_called_once()
            call_args = audit_spy.call_args
            # _audit is called as _audit(settings, auth, event, detail)
            self.assertEqual(call_args[0][2], "ops_run")
            self.assertIn("run_id", call_args[0][3])
            self.assertEqual(call_args[0][3]["job_id"], "metrics.poll_and_alarm_eval")
            self.assertEqual(call_args[0][3]["status"], "succeeded")

    def test_audit_failure_does_not_break_ops_run(self) -> None:
        """If audit raises, ops_run must still return ok and release the lock."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _make_settings(repo_root)
            lock_dir = repo_root / "logs" / "ops_locks"
            lock_dir.mkdir(parents=True, exist_ok=True)
            lock_file = lock_dir / "metrics.poll_and_alarm_eval.lock"

            with (
                patch("app.main._services", return_value=(settings, _GitManagerStub())),
                patch("app.main._audit", side_effect=RuntimeError("audit boom")),
            ):
                with self.assertLogs("app.ops.service", level="WARNING") as cm:
                    result = ops_run(
                        req=OpsRunRequest(job_id="metrics.poll_and_alarm_eval"),
                        auth=_AuthStub(client_ip="127.0.0.1"),
                    )

            self.assertTrue(result["ok"])
            self.assertFalse(lock_file.exists())
            self.assertTrue(any("audit event failed" in msg for msg in cm.output))

    def test_triple_failure_preserves_original_exception(self) -> None:
        """When job, append, and audit all fail, the original HTTPException must propagate."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _make_settings(repo_root)
            lock_dir = repo_root / "logs" / "ops_locks"
            lock_dir.mkdir(parents=True, exist_ok=True)
            lock_file = lock_dir / "metrics.poll_and_alarm_eval.lock"

            with (
                patch("app.main._services", return_value=(settings, _GitManagerStub())),
                patch(
                    "app.ops.service._ops_execute_job",
                    side_effect=HTTPException(status_code=500, detail="job exploded"),
                ),
                patch(
                    "app.ops.service._append_ops_run",
                    side_effect=OSError("disk full"),
                ),
                patch("app.main._audit", side_effect=RuntimeError("audit boom")),
            ):
                with self.assertRaises(HTTPException) as ctx:
                    with self.assertLogs("app.ops.service", level="WARNING") as cm:
                        ops_run(
                            req=OpsRunRequest(job_id="metrics.poll_and_alarm_eval"),
                            auth=_AuthStub(client_ip="127.0.0.1"),
                        )

            self.assertEqual(ctx.exception.status_code, 500)
            self.assertIn("job exploded", str(ctx.exception.detail))
            self.assertFalse(lock_file.exists())
            self.assertTrue(any("failed to append ops run log" in msg for msg in cm.output))
            self.assertTrue(any("audit event failed" in msg for msg in cm.output))

    def test_lock_release_failure_in_ops_run_still_succeeds(self) -> None:
        """If lock release fails inside ops_run, the run must still return ok."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _make_settings(repo_root)
            lock_dir = repo_root / "logs" / "ops_locks"
            lock_dir.mkdir(parents=True, exist_ok=True)

            original_unlink = Path.unlink

            def _selective_unlink(self_path: Path, *, missing_ok: bool = False) -> None:
                if self_path.suffix == ".lock":
                    raise PermissionError("denied")
                original_unlink(self_path, missing_ok=missing_ok)

            with (
                patch("app.main._services", return_value=(settings, _GitManagerStub())),
                patch.object(Path, "unlink", _selective_unlink),
            ):
                with self.assertLogs("app.ops.service", level="ERROR") as cm:
                    result = ops_run(
                        req=OpsRunRequest(job_id="metrics.poll_and_alarm_eval"),
                        auth=_AuthStub(client_ip="127.0.0.1"),
                    )

            self.assertTrue(result["ok"])
            self.assertTrue(any("failed to release ops lock" in msg for msg in cm.output))

    def test_original_exception_preserved_on_double_failure(self) -> None:
        """When both job execution and _append_ops_run fail, the original job exception must propagate."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            settings = _make_settings(repo_root)
            lock_dir = repo_root / "logs" / "ops_locks"
            lock_dir.mkdir(parents=True, exist_ok=True)
            lock_file = lock_dir / "metrics.poll_and_alarm_eval.lock"

            with (
                patch("app.main._services", return_value=(settings, _GitManagerStub())),
                patch(
                    "app.ops.service._append_ops_run",
                    side_effect=OSError("disk full"),
                ),
                patch(
                    "app.ops.service._ops_execute_job",
                    side_effect=HTTPException(status_code=500, detail="job exploded"),
                ),
            ):
                with self.assertRaises(HTTPException) as ctx:
                    with self.assertLogs("app.ops.service", level="WARNING"):
                        ops_run(
                            req=OpsRunRequest(job_id="metrics.poll_and_alarm_eval"),
                            auth=_AuthStub(client_ip="127.0.0.1"),
                        )

            self.assertEqual(ctx.exception.status_code, 500)
            self.assertIn("job exploded", str(ctx.exception.detail))
            self.assertFalse(lock_file.exists())


class TestReleaseOpsLock(unittest.TestCase):
    """Tests for _release_ops_lock error handling."""

    def test_release_ops_lock_logs_on_oserror(self) -> None:
        """OSError during lock release must be logged at ERROR, not silently swallowed."""
        with tempfile.TemporaryDirectory() as td:
            lock = Path(td) / "test.lock"
            lock.touch()
            with patch.object(Path, "unlink", side_effect=PermissionError("denied")):
                with self.assertLogs("app.ops.service", level="ERROR") as cm:
                    _release_ops_lock(lock)
            self.assertIn("failed to release ops lock", cm.output[0])
            self.assertIn("manual cleanup", cm.output[0])

    def test_release_ops_lock_succeeds_normally(self) -> None:
        """Lock release should delete the file without logging."""
        with tempfile.TemporaryDirectory() as td:
            lock = Path(td) / "test.lock"
            lock.touch()
            _release_ops_lock(lock)
            self.assertFalse(lock.exists())


class TestLoadOpsRunsMalformed(unittest.TestCase):
    """Tests for _load_ops_runs handling of malformed JSONL lines."""

    def test_load_ops_runs_logs_malformed_lines(self) -> None:
        """Malformed JSONL lines must be skipped and logged as warnings."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            runs_path = repo / "logs" / "ops_runs.jsonl"
            runs_path.parent.mkdir(parents=True, exist_ok=True)
            runs_path.write_text(
                '{"job_id":"a","status":"ok"}\n'
                "not-valid-json\n"
                '{"job_id":"b","status":"ok"}\n',
                encoding="utf-8",
            )

            with self.assertLogs("app.ops.service", level="WARNING") as cm:
                result = _load_ops_runs(repo)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["job_id"], "a")
        self.assertEqual(result[1]["job_id"], "b")
        self.assertTrue(any("malformed JSONL" in msg for msg in cm.output))
        self.assertTrue(any("not-valid-json" in msg for msg in cm.output))
        self.assertTrue(any("file line 2" in msg for msg in cm.output))

    def test_load_ops_runs_logs_non_dict_lines(self) -> None:
        """Valid JSON that is not a dict should be skipped with a debug log."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            runs_path = repo / "logs" / "ops_runs.jsonl"
            runs_path.parent.mkdir(parents=True, exist_ok=True)
            runs_path.write_text(
                '[1,2,3]\n'
                '"just a string"\n'
                '{"job_id":"valid","status":"ok"}\n',
                encoding="utf-8",
            )

            with self.assertLogs("app.ops.service", level="DEBUG") as cm:
                result = _load_ops_runs(repo)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["job_id"], "valid")
        self.assertTrue(any("non-dict JSON" in msg for msg in cm.output))

    def test_load_ops_runs_handles_empty_lines(self) -> None:
        """Empty lines in a JSONL file should be skipped and logged as malformed."""
        with tempfile.TemporaryDirectory() as td:
            repo = Path(td)
            runs_path = repo / "logs" / "ops_runs.jsonl"
            runs_path.parent.mkdir(parents=True, exist_ok=True)
            runs_path.write_text(
                '{"job_id":"a","status":"ok"}\n'
                "\n"
                '{"job_id":"b","status":"ok"}\n',
                encoding="utf-8",
            )

            with self.assertLogs("app.ops.service", level="WARNING") as cm:
                result = _load_ops_runs(repo)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["job_id"], "a")
        self.assertEqual(result[1]["job_id"], "b")
        self.assertTrue(any("malformed JSONL" in msg for msg in cm.output))


if __name__ == "__main__":
    unittest.main()
