"""Unit tests for app.lifecycle_warnings shared helpers."""

from __future__ import annotations

import unittest

from fastapi import HTTPException

from app.lifecycle_warnings import make_error_detail, make_lock_error, make_warning


class TestMakeWarning(unittest.TestCase):
    """Tests for make_warning."""

    def test_minimal(self) -> None:
        w = make_warning("code_a", "something happened")
        self.assertEqual(w["code"], "code_a")
        self.assertEqual(w["detail"], "something happened")
        self.assertIsNone(w["path"])
        self.assertIsNone(w["segment_id"])

    def test_with_path_and_segment_id(self) -> None:
        w = make_warning("code_b", "detail", path="/foo", segment_id="seg-1")
        self.assertEqual(w["path"], "/foo")
        self.assertEqual(w["segment_id"], "seg-1")

    def test_extra_kwargs(self) -> None:
        w = make_warning("code_c", "detail", extra_field="value")
        self.assertEqual(w["extra_field"], "value")

    def test_shape_matches_segment_history(self) -> None:
        w = make_warning("c", "d")
        self.assertEqual(set(w.keys()), {"code", "detail", "path", "segment_id"})


class TestMakeErrorDetail(unittest.TestCase):
    """Tests for make_error_detail."""

    def test_minimal(self) -> None:
        d = make_error_detail(
            operation="test_op",
            error_code="test_error",
            error_detail="bad thing",
        )
        self.assertFalse(d["ok"])
        self.assertEqual(d["operation"], "test_op")
        self.assertEqual(d["error"]["code"], "test_error")
        self.assertEqual(d["error"]["detail"], "bad thing")
        self.assertNotIn("family", d)

    def test_with_family(self) -> None:
        d = make_error_detail(
            operation="op",
            family="delivery",
            error_code="e",
            error_detail="d",
        )
        self.assertEqual(d["family"], "delivery")

    def test_extra_kwargs(self) -> None:
        d = make_error_detail(
            operation="op",
            error_code="e",
            error_detail="d",
            rollback_errors=["a", "b"],
        )
        self.assertEqual(d["rollback_errors"], ["a", "b"])


class TestMakeLockError(unittest.TestCase):
    """Tests for make_lock_error."""

    def test_timeout_returns_409(self) -> None:
        exc = make_lock_error("myop", "fam", RuntimeError("timed out"), is_timeout=True)
        self.assertIsInstance(exc, HTTPException)
        self.assertEqual(exc.status_code, 409)
        self.assertIn("source_lock_timeout", exc.detail["error"]["code"])

    def test_infra_returns_503(self) -> None:
        exc = make_lock_error("myop", "fam", RuntimeError("no dir"), is_timeout=False)
        self.assertIsInstance(exc, HTTPException)
        self.assertEqual(exc.status_code, 503)
        self.assertIn("lock_infrastructure_unavailable", exc.detail["error"]["code"])

    def test_family_none(self) -> None:
        exc = make_lock_error("op", None, RuntimeError("x"), is_timeout=True)
        self.assertNotIn("family", exc.detail)


if __name__ == "__main__":
    unittest.main()
