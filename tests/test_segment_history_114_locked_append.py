"""Direct tests for locked_append_jsonl, locked_append_jsonl_multi, and _check_and_rollover_locked.

These functions are the primary write path for all 6 segment-history families.
Tests cover the composed lock → rollover check → append → error translation
pipeline, including concurrency, partial failure, and graceful degradation.
"""

from __future__ import annotations

import fcntl
import json
import shutil
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.segment_history.append import (
    SegmentHistoryAppendError,
    _check_and_rollover_locked,
    locked_append_jsonl,
    locked_append_jsonl_multi,
)
from app.segment_history.locking import (
    LockInfrastructureError,
    _safe_lock_filename,
)
from tests.helpers import SimpleGitManagerStub


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _RepoMixin:
    """Provide a temporary directory as a minimal repo root."""

    def setUp(self) -> None:
        self._td = tempfile.mkdtemp()
        self.repo = Path(self._td)
        # Ensure lock dir exists so lock infrastructure doesn't fail
        (self.repo / ".locks" / "segment_history").mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self._td, ignore_errors=True)


class _FakeSettings:
    """Minimal settings stub for rollover checks."""

    audit_log_rollover_bytes: int = 1_000_000
    ops_run_rollover_bytes: int = 1_000_000
    message_stream_rollover_bytes: int = 1_000_000
    message_stream_max_hot_days: int = 14
    message_thread_rollover_bytes: int = 1_000_000
    message_thread_inactivity_days: int = 30
    episodic_rollover_bytes: int = 1_000_000


def _hold_lock(repo: Path, lock_key: str, ready: threading.Event, release: threading.Event) -> None:
    """Thread target: hold a source lock until *release* is set."""
    lock_dir = repo / ".locks" / "segment_history"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_file = lock_dir / _safe_lock_filename(lock_key)
    fd = lock_file.open("a")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        ready.set()
        release.wait(timeout=10)
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        fd.close()


# ===========================================================================
# TestLockedAppendJsonl
# ===========================================================================
class TestLockedAppendJsonl(_RepoMixin, unittest.TestCase):
    """Tests for locked_append_jsonl — single-path locked append."""

    def test_non_sh_path_plain_append(self) -> None:
        """Non-segment-history path bypasses locking, writes record."""
        p = self.repo / "tasks" / "work.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        record = {"task": "test"}
        locked_append_jsonl(p, record, repo_root=self.repo)
        content = p.read_text(encoding="utf-8")
        self.assertEqual(json.loads(content.strip()), record)

    def test_sh_path_writes_record(self) -> None:
        """Segment-history path acquires lock and writes JSON line."""
        p = self.repo / "logs" / "api_audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        record = {"ts": "2026-03-20T10:00:00Z", "event": "test"}
        locked_append_jsonl(p, record, repo_root=self.repo)
        content = p.read_text(encoding="utf-8")
        self.assertEqual(json.loads(content.strip()), record)

    def test_sh_path_creates_parent_dirs(self) -> None:
        """Parent directories created when missing."""
        p = self.repo / "messages" / "inbox" / "new.jsonl"
        record = {"msg": "hello"}
        locked_append_jsonl(p, record, repo_root=self.repo)
        self.assertTrue(p.is_file())
        self.assertEqual(json.loads(p.read_text(encoding="utf-8").strip()), record)

    def test_lock_timeout_translates_to_append_error(self) -> None:
        """Lock contention raises SegmentHistoryAppendError with timeout code."""
        p = self.repo / "logs" / "api_audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)

        from app.segment_history.utils import _derive_stream_key

        lock_key = f"segment_history:api_audit:{_derive_stream_key('api_audit', 'logs/api_audit.jsonl')}"
        ready = threading.Event()
        release = threading.Event()
        t = threading.Thread(target=_hold_lock, args=(self.repo, lock_key, ready, release))
        t.start()
        ready.wait(timeout=5)
        try:
            with self.assertRaises(SegmentHistoryAppendError) as ctx:
                locked_append_jsonl(
                    p,
                    {"event": "test"},
                    repo_root=self.repo,
                    lock_timeout=0.15,
                )
            self.assertEqual(ctx.exception.code, "segment_history_source_lock_timeout")
        finally:
            release.set()
            t.join(timeout=5)

    def test_write_time_rollover_error_translated(self) -> None:
        """WriteTimeRolloverError from rollover check translates to SegmentHistoryAppendError."""
        from app.audit import WriteTimeRolloverError

        p = self.repo / "logs" / "api_audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)

        with patch(
            "app.segment_history.append._check_and_rollover_locked",
            side_effect=WriteTimeRolloverError("test_rollover_code", "test detail"),
        ):
            with self.assertRaises(SegmentHistoryAppendError) as ctx:
                locked_append_jsonl(
                    p,
                    {"event": "test"},
                    repo_root=self.repo,
                    family="api_audit",
                )
            self.assertEqual(ctx.exception.code, "test_rollover_code")
            self.assertIn("test detail", ctx.exception.detail)

    def test_lock_infrastructure_error_translated(self) -> None:
        """LockInfrastructureError translates to SegmentHistoryAppendError."""
        p = self.repo / "logs" / "api_audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)

        with patch(
            "app.segment_history.locking._ensure_lock_dir",
            side_effect=LockInfrastructureError("disk full"),
        ):
            with self.assertRaises(SegmentHistoryAppendError) as ctx:
                locked_append_jsonl(
                    p,
                    {"event": "test"},
                    repo_root=self.repo,
                    family="api_audit",
                )
            self.assertEqual(ctx.exception.code, "segment_history_lock_infrastructure_unavailable")
            self.assertIn("disk full", ctx.exception.detail)

    def test_journal_non_current_day_rejected(self) -> None:
        """Past-day journal path raises SegmentHistoryAppendError."""
        p = self.repo / "journal" / "2026" / "2020-01-01.md"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("old entry\n")
        with self.assertRaises(SegmentHistoryAppendError) as ctx:
            locked_append_jsonl(
                p,
                {"entry": "test"},
                repo_root=self.repo,
                family="journal",
            )
        self.assertEqual(ctx.exception.code, "segment_history_journal_non_current_day")

    def test_journal_current_day_accepted(self) -> None:
        """Today's journal path writes successfully."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        year = datetime.now(timezone.utc).strftime("%Y")
        p = self.repo / "journal" / year / f"{today}.md"
        p.parent.mkdir(parents=True, exist_ok=True)
        record = {"entry": "today"}
        locked_append_jsonl(p, record, repo_root=self.repo, family="journal")
        self.assertTrue(p.is_file())
        content = p.read_text(encoding="utf-8")
        self.assertIn("today", content)

    def test_rollover_check_skipped_without_gm(self) -> None:
        """Append works with gm=None — rollover check skipped."""
        p = self.repo / "logs" / "api_audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        with patch(
            "app.segment_history.append._check_and_rollover_locked",
            wraps=_check_and_rollover_locked,
        ) as mock_check:
            locked_append_jsonl(
                p,
                {"event": "test"},
                repo_root=self.repo,
                gm=None,
                settings=_FakeSettings(),
            )
            # _check_and_rollover_locked was called but should have returned
            # early (gm=None). Verify append still succeeded.
            mock_check.assert_called_once()
        self.assertTrue(p.is_file())


# ===========================================================================
# TestLockedAppendJsonlMulti
# ===========================================================================
class TestLockedAppendJsonlMulti(_RepoMixin, unittest.TestCase):
    """Tests for locked_append_jsonl_multi — multi-path locked append."""

    def test_empty_paths_noop(self) -> None:
        """Empty path list returns without error."""
        locked_append_jsonl_multi([], {"x": 1}, repo_root=self.repo)

    def test_mixed_sh_and_non_sh_writes_all(self) -> None:
        """Both SH and non-SH paths receive the record."""
        sh_path = self.repo / "logs" / "api_audit.jsonl"
        non_sh = self.repo / "tasks" / "work.jsonl"
        sh_path.parent.mkdir(parents=True, exist_ok=True)
        non_sh.parent.mkdir(parents=True, exist_ok=True)
        record = {"event": "mixed"}
        locked_append_jsonl_multi(
            [sh_path, non_sh],
            record,
            repo_root=self.repo,
        )
        for p in [sh_path, non_sh]:
            content = p.read_text(encoding="utf-8")
            self.assertEqual(json.loads(content.strip()), record)

    def test_all_non_sh_no_locking(self) -> None:
        """Only non-SH paths → no lock acquired, record written."""
        p1 = self.repo / "tasks" / "a.jsonl"
        p2 = self.repo / "tasks" / "b.jsonl"
        p1.parent.mkdir(parents=True, exist_ok=True)
        record = {"task": "test"}
        locked_append_jsonl_multi([p1, p2], record, repo_root=self.repo)
        for p in [p1, p2]:
            self.assertTrue(p.is_file())

    def test_sorted_lock_acquisition_order(self) -> None:
        """Lock keys are acquired in sorted order regardless of input order."""
        # Create 3 SH paths in different families to get distinct lock keys
        p_ops = self.repo / "logs" / "ops_runs.jsonl"
        p_audit = self.repo / "logs" / "api_audit.jsonl"
        p_episodic = self.repo / "memory" / "episodic" / "observations.jsonl"
        for p in [p_ops, p_audit, p_episodic]:
            p.parent.mkdir(parents=True, exist_ok=True)

        acquired_keys: list[str] = []

        from app.segment_history.locking import acquire_sorted_source_locks as real_acquire

        def capturing_acquire(keys, **kwargs):
            acquired_keys.extend(keys)
            return real_acquire(keys, **kwargs)

        with patch(
            "app.segment_history.locking.acquire_sorted_source_locks",
            side_effect=capturing_acquire,
        ):
            locked_append_jsonl_multi(
                [p_ops, p_audit, p_episodic],
                {"event": "test"},
                repo_root=self.repo,
            )
        # Keys must be sorted
        self.assertEqual(acquired_keys, sorted(acquired_keys))

    def test_partial_failure_reports_written_paths(self) -> None:
        """OSError on 2nd write reports partial success in error detail."""
        p1 = self.repo / "logs" / "api_audit.jsonl"
        p2 = self.repo / "logs" / "ops_runs.jsonl"
        p1.parent.mkdir(parents=True, exist_ok=True)
        p2.parent.mkdir(parents=True, exist_ok=True)

        call_count = 0
        _real_open = Path.open

        def failing_open(self_path, *args, **kwargs):
            nonlocal call_count
            # The locking layer opens lock files; only count source file opens
            if str(self_path).endswith(".jsonl") and "a" in args:
                call_count += 1
                if call_count >= 2:
                    raise OSError("simulated disk full")
            return _real_open(self_path, *args, **kwargs)

        with patch.object(Path, "open", failing_open):
            with self.assertRaises(SegmentHistoryAppendError) as ctx:
                locked_append_jsonl_multi(
                    [p1, p2],
                    {"event": "test"},
                    repo_root=self.repo,
                )
            self.assertEqual(ctx.exception.code, "segment_history_partial_multi_append")
            # First write should have succeeded
            self.assertIn("1 of 2", ctx.exception.detail)

    def test_lock_timeout_multi(self) -> None:
        """Lock contention on one path raises SegmentHistoryAppendError."""
        p = self.repo / "logs" / "api_audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)

        from app.segment_history.utils import _derive_stream_key

        lock_key = f"segment_history:api_audit:{_derive_stream_key('api_audit', 'logs/api_audit.jsonl')}"
        ready = threading.Event()
        release = threading.Event()
        t = threading.Thread(target=_hold_lock, args=(self.repo, lock_key, ready, release))
        t.start()
        ready.wait(timeout=5)
        try:
            with self.assertRaises(SegmentHistoryAppendError) as ctx:
                locked_append_jsonl_multi(
                    [p],
                    {"event": "test"},
                    repo_root=self.repo,
                    lock_timeout=0.15,
                )
            self.assertEqual(ctx.exception.code, "segment_history_source_lock_timeout")
        finally:
            release.set()
            t.join(timeout=5)

    def test_rollover_error_multi(self) -> None:
        """WriteTimeRolloverError during multi-append translates correctly."""
        from app.audit import WriteTimeRolloverError

        p = self.repo / "logs" / "api_audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)

        with patch(
            "app.segment_history.append._check_and_rollover_locked",
            side_effect=WriteTimeRolloverError("multi_rollover_code", "multi detail"),
        ):
            with self.assertRaises(SegmentHistoryAppendError) as ctx:
                locked_append_jsonl_multi(
                    [p],
                    {"event": "test"},
                    repo_root=self.repo,
                )
            self.assertEqual(ctx.exception.code, "multi_rollover_code")

    def test_lock_infrastructure_error_multi(self) -> None:
        """LockInfrastructureError during multi-append translates correctly."""
        p = self.repo / "logs" / "api_audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)

        with patch(
            "app.segment_history.locking._ensure_lock_dir",
            side_effect=LockInfrastructureError("permission denied"),
        ):
            with self.assertRaises(SegmentHistoryAppendError) as ctx:
                locked_append_jsonl_multi(
                    [p],
                    {"event": "test"},
                    repo_root=self.repo,
                )
            self.assertEqual(ctx.exception.code, "segment_history_lock_infrastructure_unavailable")


# ===========================================================================
# TestCheckAndRolloverLocked
# ===========================================================================
class TestCheckAndRolloverLocked(_RepoMixin, unittest.TestCase):
    """Tests for _check_and_rollover_locked — internal rollover helper."""

    def test_gm_none_skips_rollover(self) -> None:
        """gm=None causes early return without rollover check."""
        p = self.repo / "logs" / "api_audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text('{"x":1}\n')
        with patch("app.segment_history.families.check_rollover_eligible") as mock_elig:
            _check_and_rollover_locked(p, "api_audit", self.repo, None, _FakeSettings())
            mock_elig.assert_not_called()

    def test_settings_none_skips_rollover(self) -> None:
        """settings=None causes early return without rollover check."""
        p = self.repo / "logs" / "api_audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text('{"x":1}\n')
        gm = SimpleGitManagerStub(self.repo)
        with patch("app.segment_history.families.check_rollover_eligible") as mock_elig:
            _check_and_rollover_locked(p, "api_audit", self.repo, gm, None)
            mock_elig.assert_not_called()

    def test_journal_skips_rollover(self) -> None:
        """Journal family skips write-time rollover (maintenance-only)."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        year = datetime.now(timezone.utc).strftime("%Y")
        p = self.repo / "journal" / year / f"{today}.md"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("entry\n")
        gm = SimpleGitManagerStub(self.repo)
        with patch("app.segment_history.families.check_rollover_eligible") as mock_elig:
            _check_and_rollover_locked(p, "journal", self.repo, gm, _FakeSettings())
            mock_elig.assert_not_called()

    def test_nonexistent_path_skips(self) -> None:
        """Non-existent path causes early return."""
        p = self.repo / "logs" / "api_audit.jsonl"
        gm = SimpleGitManagerStub(self.repo)
        # Path doesn't exist — should return without error
        _check_and_rollover_locked(p, "api_audit", self.repo, gm, _FakeSettings())

    def test_not_eligible_skips(self) -> None:
        """Ineligible file skips rollover."""
        p = self.repo / "logs" / "api_audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text('{"x":1}\n')
        gm = SimpleGitManagerStub(self.repo)
        with patch(
            "app.segment_history.families.check_rollover_eligible",
            return_value=False,
        ):
            with patch("app.audit._check_write_time_rollover_locked") as mock_rollover:
                _check_and_rollover_locked(p, "api_audit", self.repo, gm, _FakeSettings())
                mock_rollover.assert_not_called()

    def test_eligible_triggers_rollover(self) -> None:
        """Eligible file triggers _check_write_time_rollover_locked."""
        p = self.repo / "logs" / "api_audit.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text('{"x":1}\n')
        gm = SimpleGitManagerStub(self.repo)
        with patch(
            "app.segment_history.families.check_rollover_eligible",
            return_value=True,
        ):
            with patch("app.audit._check_write_time_rollover_locked") as mock_rollover:
                _check_and_rollover_locked(
                    p, "api_audit", self.repo, gm, _FakeSettings(),
                )
                mock_rollover.assert_called_once()
                args = mock_rollover.call_args
                self.assertEqual(args[0][0], p)  # path
                self.assertEqual(args[0][2], self.repo)  # repo_root
                self.assertEqual(args[0][3], gm)  # gm
                self.assertEqual(args[1]["family"], "api_audit")

    def test_unknown_family_skips(self) -> None:
        """Unknown family returns early without error."""
        p = self.repo / "unknown" / "data.jsonl"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text('{"x":1}\n')
        gm = SimpleGitManagerStub(self.repo)
        # Should not raise — unknown families are silently skipped
        _check_and_rollover_locked(p, "nonexistent_family", self.repo, gm, _FakeSettings())


if __name__ == "__main__":
    unittest.main()
