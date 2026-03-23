"""Tests for Issue #143: in-process locking for rate-limit state mutations.

Validates that concurrent read-modify-write cycles on rate_limit_state.json
are serialized by the module-level threading.Lock, preventing lost updates.
"""

from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from fastapi import HTTPException

from app.config import Settings
from app.runtime.service import (
    _rate_limit_lock,
    enforce_rate_limit,
    load_rate_limit_state,
    record_verification_failure,
)
from tests.helpers import AllowAllAuthStub


class _AuthStub(AllowAllAuthStub):
    """Auth stub that exposes a token attribute for _auth_refs()."""

    def __init__(self, *, peer_id: str = "peer-test", token: str = "test-token", client_ip: str | None = "127.0.0.1") -> None:
        super().__init__(peer_id=peer_id, client_ip=client_ip)
        self.token = token


def _make_settings(repo_root: Path, **overrides: Any) -> Settings:
    """Build a minimal Settings for rate-limit testing."""
    defaults: dict[str, Any] = {
        "repo_root": repo_root,
        "auto_init_git": False,
        "git_author_name": "test",
        "git_author_email": "test@test",
        "tokens": {},
        "audit_log_enabled": False,
        "token_rate_limit_per_minute": 1000,
        "ip_rate_limit_per_minute": 2000,
        "verify_failure_window_seconds": 600,
    }
    defaults.update(overrides)
    return Settings(**defaults)


class TestConcurrentEnforceNoLostEvents(unittest.TestCase):
    """Concurrent enforce_rate_limit calls must not lose events."""

    def test_concurrent_enforce_no_lost_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            (repo_root / "logs").mkdir()
            settings = _make_settings(repo_root)
            n_threads = 8
            barrier = threading.Barrier(n_threads, timeout=10)
            errors: list[Exception] = []

            def worker(idx: int) -> None:
                try:
                    barrier.wait()
                    auth = _AuthStub(token=f"token-{idx}", client_ip=f"10.0.0.{idx}")
                    enforce_rate_limit(settings, auth, "test_bucket")
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)

            self.assertEqual(errors, [], f"Unexpected errors: {errors}")
            state = load_rate_limit_state(repo_root)
            self.assertEqual(
                len(state["events"]),
                n_threads,
                f"Expected {n_threads} events, got {len(state['events'])}. Lost events indicate a race condition in the read-modify-write cycle.",
            )


class TestConcurrentRecordFailureNoLostEntries(unittest.TestCase):
    """Concurrent record_verification_failure calls must not lose entries."""

    def test_concurrent_record_failure_no_lost_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            (repo_root / "logs").mkdir()
            settings = _make_settings(repo_root)
            n_threads = 8
            barrier = threading.Barrier(n_threads, timeout=10)
            errors: list[Exception] = []

            def worker(idx: int) -> None:
                try:
                    barrier.wait()
                    auth = _AuthStub(token=f"token-{idx}", client_ip=f"10.0.0.{idx}")
                    record_verification_failure(settings, auth, f"reason-{idx}")
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)

            self.assertEqual(errors, [], f"Unexpected errors: {errors}")
            state = load_rate_limit_state(repo_root)
            self.assertEqual(
                len(state["verification_failures"]),
                n_threads,
                f"Expected {n_threads} failure records, got {len(state['verification_failures'])}. Lost entries indicate a race condition.",
            )


class TestMixedConcurrentMutationsSerialized(unittest.TestCase):
    """Mixed enforce + record calls must not lose any writes."""

    def test_mixed_concurrent_mutations_serialized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            (repo_root / "logs").mkdir()
            settings = _make_settings(repo_root)
            n_enforce = 4
            n_record = 4
            total = n_enforce + n_record
            barrier = threading.Barrier(total, timeout=10)
            errors: list[Exception] = []

            def enforce_worker(idx: int) -> None:
                try:
                    barrier.wait()
                    auth = _AuthStub(token=f"enforce-{idx}", client_ip=f"10.0.0.{idx}")
                    enforce_rate_limit(settings, auth, "mixed_bucket")
                except Exception as exc:
                    errors.append(exc)

            def record_worker(idx: int) -> None:
                try:
                    barrier.wait()
                    auth = _AuthStub(token=f"record-{idx}", client_ip=f"10.0.1.{idx}")
                    record_verification_failure(settings, auth, f"reason-{idx}")
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=enforce_worker, args=(i,)) for i in range(n_enforce)]
            threads += [threading.Thread(target=record_worker, args=(i,)) for i in range(n_record)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)

            self.assertEqual(errors, [], f"Unexpected errors: {errors}")
            state = load_rate_limit_state(repo_root)
            self.assertEqual(len(state["events"]), n_enforce)
            self.assertEqual(len(state["verification_failures"]), n_record)


class TestLockReleasedOn429(unittest.TestCase):
    """The lock must be released when enforce_rate_limit raises HTTPException(429)."""

    def test_lock_released_on_429(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            (repo_root / "logs").mkdir()
            settings = _make_settings(repo_root, token_rate_limit_per_minute=1)
            auth = _AuthStub(token="same-token")

            # Consume the single allowed request.
            enforce_rate_limit(settings, auth, "limited_bucket")

            # Two concurrent threads should both get 429 without deadlocking.
            barrier = threading.Barrier(2, timeout=10)
            results: list[int] = []

            def worker() -> None:
                barrier.wait()
                try:
                    enforce_rate_limit(settings, auth, "limited_bucket")
                except HTTPException as exc:
                    results.append(exc.status_code)

            t1 = threading.Thread(target=worker)
            t2 = threading.Thread(target=worker)
            t1.start()
            t2.start()
            t1.join(timeout=5)
            t2.join(timeout=5)

            self.assertEqual(sorted(results), [429, 429], "Both threads should receive 429")
            # Verify the lock is not held after the 429 exceptions.
            self.assertTrue(_rate_limit_lock.acquire(timeout=1), "Lock should be available after 429")
            _rate_limit_lock.release()


class TestLockReleasedOnUnexpectedException(unittest.TestCase):
    """The lock must be released when an unexpected error occurs during write."""

    def test_lock_released_on_write_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            (repo_root / "logs").mkdir()
            settings = _make_settings(repo_root)
            auth = _AuthStub()

            # Patch _write_rate_limit_state to fail on the first call.
            with patch("app.runtime.service._write_rate_limit_state", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    enforce_rate_limit(settings, auth, "bucket")

            # Lock must be available — the context manager released it despite the error.
            self.assertTrue(_rate_limit_lock.acquire(timeout=1), "Lock should be available after exception")
            _rate_limit_lock.release()

            # A subsequent call should succeed normally (no deadlock).
            record_verification_failure(settings, auth, "after-error")
            state = load_rate_limit_state(repo_root)
            self.assertEqual(len(state["verification_failures"]), 1)


if __name__ == "__main__":
    unittest.main()
