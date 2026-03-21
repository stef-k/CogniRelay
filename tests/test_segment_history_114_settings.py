"""Tests for segment-history lifecycle settings (issue #114, Phase 1)."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch


class TestSegmentHistorySettingsDefaults(unittest.TestCase):
    """Verify default values for the 20 new segment-history settings."""

    def setUp(self) -> None:
        import app.config as cfg

        cfg._cached = None

    def tearDown(self) -> None:
        import app.config as cfg

        cfg._cached = None

    @patch.dict(os.environ, {"COGNIRELAY_TOKENS": "test-token"}, clear=False)
    def test_defaults(self) -> None:
        from app.config import get_settings

        s = get_settings(force_reload=True)
        self.assertEqual(s.journal_cold_after_days, 30)
        self.assertEqual(s.journal_retention_days, 365)
        self.assertEqual(s.audit_log_rollover_bytes, 1_048_576)
        self.assertEqual(s.audit_log_cold_after_days, 30)
        self.assertEqual(s.audit_log_retention_days, 365)
        self.assertEqual(s.ops_run_rollover_bytes, 1_048_576)
        self.assertEqual(s.ops_run_cold_after_days, 30)
        self.assertEqual(s.ops_run_retention_days, 365)
        self.assertEqual(s.message_stream_rollover_bytes, 1_048_576)
        self.assertEqual(s.message_stream_max_hot_days, 14)
        self.assertEqual(s.message_stream_cold_after_days, 30)
        self.assertEqual(s.message_stream_retention_days, 180)
        self.assertEqual(s.message_thread_rollover_bytes, 2_097_152)
        self.assertEqual(s.message_thread_inactivity_days, 30)
        self.assertEqual(s.message_thread_cold_after_days, 60)
        self.assertEqual(s.message_thread_retention_days, 365)
        self.assertEqual(s.episodic_rollover_bytes, 1_048_576)
        self.assertEqual(s.episodic_cold_after_days, 30)
        self.assertEqual(s.episodic_retention_days, 180)
        self.assertEqual(s.segment_history_batch_limit, 500)


class TestSegmentHistorySettingsEnvOverride(unittest.TestCase):
    """Verify environment variable overrides are respected."""

    def setUp(self) -> None:
        import app.config as cfg

        cfg._cached = None

    def tearDown(self) -> None:
        import app.config as cfg

        cfg._cached = None

    @patch.dict(
        os.environ,
        {
            "COGNIRELAY_TOKENS": "test-token",
            "COGNIRELAY_JOURNAL_COLD_AFTER_DAYS": "60",
            "COGNIRELAY_JOURNAL_RETENTION_DAYS": "730",
            "COGNIRELAY_AUDIT_LOG_ROLLOVER_BYTES": "2097152",
            "COGNIRELAY_SEGMENT_HISTORY_BATCH_LIMIT": "100",
        },
        clear=False,
    )
    def test_env_override(self) -> None:
        from app.config import get_settings

        s = get_settings(force_reload=True)
        self.assertEqual(s.journal_cold_after_days, 60)
        self.assertEqual(s.journal_retention_days, 730)
        self.assertEqual(s.audit_log_rollover_bytes, 2_097_152)
        self.assertEqual(s.segment_history_batch_limit, 100)


class TestSegmentHistorySettingsValidation(unittest.TestCase):
    """Verify cross-field validation raises SystemExit on invalid config."""

    def setUp(self) -> None:
        import app.config as cfg

        cfg._cached = None

    def tearDown(self) -> None:
        import app.config as cfg

        cfg._cached = None

    @patch.dict(
        os.environ,
        {
            "COGNIRELAY_TOKENS": "test-token",
            "COGNIRELAY_JOURNAL_COLD_AFTER_DAYS": "400",
            "COGNIRELAY_JOURNAL_RETENTION_DAYS": "365",
        },
        clear=False,
    )
    def test_cold_after_exceeds_retention_raises(self) -> None:
        from app.config import get_settings

        with self.assertRaises(SystemExit) as ctx:
            get_settings(force_reload=True)
        self.assertIn("journal_cold_after_days", str(ctx.exception))
        self.assertIn("journal_retention_days", str(ctx.exception))

    @patch.dict(
        os.environ,
        {
            "COGNIRELAY_TOKENS": "test-token",
            "COGNIRELAY_EPISODIC_COLD_AFTER_DAYS": "200",
            "COGNIRELAY_EPISODIC_RETENTION_DAYS": "180",
        },
        clear=False,
    )
    def test_episodic_cold_exceeds_retention_raises(self) -> None:
        from app.config import get_settings

        with self.assertRaises(SystemExit) as ctx:
            get_settings(force_reload=True)
        self.assertIn("episodic_cold_after_days", str(ctx.exception))

    @patch.dict(
        os.environ,
        {
            "COGNIRELAY_TOKENS": "test-token",
            "COGNIRELAY_MESSAGE_THREAD_COLD_AFTER_DAYS": "500",
            "COGNIRELAY_MESSAGE_THREAD_RETENTION_DAYS": "365",
            "COGNIRELAY_AUDIT_LOG_COLD_AFTER_DAYS": "400",
            "COGNIRELAY_AUDIT_LOG_RETENTION_DAYS": "365",
        },
        clear=False,
    )
    def test_multiple_violations_all_reported(self) -> None:
        from app.config import get_settings

        with self.assertRaises(SystemExit) as ctx:
            get_settings(force_reload=True)
        msg = str(ctx.exception)
        self.assertIn("message_thread_cold_after_days", msg)
        self.assertIn("audit_log_cold_after_days", msg)

    @patch.dict(
        os.environ,
        {
            "COGNIRELAY_TOKENS": "test-token",
            "COGNIRELAY_JOURNAL_COLD_AFTER_DAYS": "30",
            "COGNIRELAY_JOURNAL_RETENTION_DAYS": "30",
        },
        clear=False,
    )
    def test_cold_equals_retention_is_valid(self) -> None:
        from app.config import get_settings

        s = get_settings(force_reload=True)
        self.assertEqual(s.journal_cold_after_days, 30)
        self.assertEqual(s.journal_retention_days, 30)

    @patch.dict(
        os.environ,
        {
            "COGNIRELAY_TOKENS": "test-token",
            "COGNIRELAY_AUDIT_LOG_ROLLOVER_BYTES": "-5",
        },
        clear=False,
    )
    def test_negative_value_causes_startup_refusal(self) -> None:
        from app.config import get_settings

        with self.assertRaises(SystemExit):
            get_settings(force_reload=True)


if __name__ == "__main__":
    unittest.main()
