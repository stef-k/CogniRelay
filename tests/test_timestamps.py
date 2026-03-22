"""Tests for the canonical datetime utility module ``app.timestamps``."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone, timedelta


from app.timestamps import (
    format_compact,
    format_iso,
    is_iso_timestamp,
    iso_now,
    iso_to_posix,
    parse_iso,
    require_utc_iso,
)


class TestParseIso(unittest.TestCase):
    """Tests for ``parse_iso``."""

    def test_z_suffix(self) -> None:
        dt = parse_iso("2026-03-20T12:00:00Z")
        assert dt is not None
        self.assertEqual(dt, datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc))

    def test_plus_zero_offset(self) -> None:
        dt = parse_iso("2026-03-20T12:00:00+00:00")
        assert dt is not None
        self.assertEqual(dt, datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc))

    def test_non_utc_offset_preserved(self) -> None:
        dt = parse_iso("2026-03-20T12:00:00+05:30")
        assert dt is not None
        self.assertEqual(dt.utctimetuple()[:6], (2026, 3, 20, 6, 30, 0))

    def test_naive_assumes_utc(self) -> None:
        dt = parse_iso("2026-03-20T12:00:00")
        assert dt is not None
        self.assertEqual(dt.tzinfo, timezone.utc)
        self.assertEqual(dt, datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc))

    def test_none_returns_none(self) -> None:
        self.assertIsNone(parse_iso(None))

    def test_empty_returns_none(self) -> None:
        self.assertIsNone(parse_iso(""))

    def test_malformed_returns_none(self) -> None:
        self.assertIsNone(parse_iso("not-a-date"))

    def test_non_string_coercion(self) -> None:
        """Numeric or other types that str() can convert should not crash."""
        self.assertIsNone(parse_iso("12345"))  # type: ignore[arg-type]

    def test_with_microseconds(self) -> None:
        dt = parse_iso("2026-03-20T12:00:00.123456Z")
        assert dt is not None
        self.assertEqual(dt.microsecond, 123456)

    def test_date_only(self) -> None:
        dt = parse_iso("2026-03-20")
        assert dt is not None
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 3)
        self.assertEqual(dt.day, 20)
        self.assertEqual(dt.tzinfo, timezone.utc)


class TestRequireUtcIso(unittest.TestCase):
    """Tests for ``require_utc_iso``."""

    def test_valid_z_suffix(self) -> None:
        dt = require_utc_iso("2026-03-20T12:00:00Z", "test_field")
        self.assertEqual(dt, datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc))

    def test_valid_plus_zero(self) -> None:
        dt = require_utc_iso("2026-03-20T12:00:00+00:00", "test_field")
        self.assertEqual(dt, datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc))

    def test_rejects_naive(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            require_utc_iso("2026-03-20T12:00:00", "updated_at")
        self.assertIn("updated_at", str(ctx.exception))
        self.assertIn("explicit UTC", str(ctx.exception))

    def test_rejects_non_utc_offset(self) -> None:
        with self.assertRaises(ValueError):
            require_utc_iso("2026-03-20T12:00:00+05:30", "field")

    def test_rejects_none(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            require_utc_iso(None, "field")
        self.assertIn("missing", str(ctx.exception))

    def test_rejects_empty(self) -> None:
        with self.assertRaises(ValueError):
            require_utc_iso("", "field")

    def test_rejects_malformed(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            require_utc_iso("not-a-dateZ", "field")
        self.assertIn("malformed", str(ctx.exception))


class TestIsoNow(unittest.TestCase):
    """Tests for ``iso_now``."""

    def test_returns_utc_aware(self) -> None:
        now = iso_now()
        self.assertEqual(now.tzinfo, timezone.utc)

    def test_no_microseconds(self) -> None:
        now = iso_now()
        self.assertEqual(now.microsecond, 0)

    def test_close_to_real_time(self) -> None:
        before = datetime.now(timezone.utc).replace(microsecond=0)
        now = iso_now()
        after = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=1)
        self.assertGreaterEqual(now, before)
        self.assertLessEqual(now, after)


class TestFormatIso(unittest.TestCase):
    """Tests for ``format_iso``."""

    def test_produces_z_suffix(self) -> None:
        dt = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(format_iso(dt), "2026-03-20T12:00:00Z")

    def test_no_plus_zero(self) -> None:
        result = format_iso(datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc))
        self.assertNotIn("+00:00", result)

    def test_strips_microseconds(self) -> None:
        dt = datetime(2026, 3, 20, 12, 0, 0, 123456, tzinfo=timezone.utc)
        self.assertEqual(format_iso(dt), "2026-03-20T12:00:00Z")

    def test_converts_non_utc_to_utc(self) -> None:
        ist = timezone(timedelta(hours=5, minutes=30))
        dt = datetime(2026, 3, 20, 17, 30, 0, tzinfo=ist)
        self.assertEqual(format_iso(dt), "2026-03-20T12:00:00Z")

    def test_roundtrip_with_parse_iso(self) -> None:
        original = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
        formatted = format_iso(original)
        parsed = parse_iso(formatted)
        self.assertEqual(parsed, original)


class TestFormatCompact(unittest.TestCase):
    """Tests for ``format_compact``."""

    def test_correct_format(self) -> None:
        dt = datetime(2026, 3, 20, 12, 30, 45, tzinfo=timezone.utc)
        self.assertEqual(format_compact(dt), "20260320T123045Z")

    def test_converts_to_utc(self) -> None:
        ist = timezone(timedelta(hours=5, minutes=30))
        dt = datetime(2026, 3, 20, 17, 30, 0, tzinfo=ist)
        self.assertEqual(format_compact(dt), "20260320T120000Z")

    def test_strips_microseconds(self) -> None:
        dt = datetime(2026, 3, 20, 12, 0, 0, 999999, tzinfo=timezone.utc)
        self.assertEqual(format_compact(dt), "20260320T120000Z")


class TestIsIsoTimestamp(unittest.TestCase):
    """Tests for ``is_iso_timestamp``."""

    def test_valid_z(self) -> None:
        self.assertTrue(is_iso_timestamp("2026-03-20T12:00:00Z"))

    def test_valid_offset(self) -> None:
        self.assertTrue(is_iso_timestamp("2026-03-20T12:00:00+00:00"))

    def test_valid_date_only(self) -> None:
        self.assertTrue(is_iso_timestamp("2026-03-20"))

    def test_invalid_string(self) -> None:
        self.assertFalse(is_iso_timestamp("not-a-date"))

    def test_none(self) -> None:
        self.assertFalse(is_iso_timestamp(None))

    def test_empty_string(self) -> None:
        self.assertFalse(is_iso_timestamp(""))

    def test_whitespace_only(self) -> None:
        self.assertFalse(is_iso_timestamp("   "))

    def test_integer(self) -> None:
        self.assertFalse(is_iso_timestamp(12345))


class TestIsoToPosix(unittest.TestCase):
    """Tests for ``iso_to_posix``."""

    def test_valid_timestamp(self) -> None:
        dt = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
        expected = dt.timestamp()
        self.assertAlmostEqual(iso_to_posix("2026-03-20T12:00:00Z"), expected)

    def test_none_returns_zero(self) -> None:
        self.assertEqual(iso_to_posix(None), 0.0)

    def test_empty_returns_zero(self) -> None:
        self.assertEqual(iso_to_posix(""), 0.0)

    def test_malformed_returns_zero(self) -> None:
        self.assertEqual(iso_to_posix("not-a-date"), 0.0)

    def test_z_and_offset_equal(self) -> None:
        a = iso_to_posix("2026-03-20T12:00:00Z")
        b = iso_to_posix("2026-03-20T12:00:00+00:00")
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
