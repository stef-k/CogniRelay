"""Focused coverage for one-shot schedule/reminder slice #255."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from app.models import ContextRetrieveRequest, ContinuitySelector
from app.schedule import (
    SCHEDULE_DB_REL,
    schedule_acknowledge_service,
    schedule_context_for_context_retrieve,
    schedule_create_service,
    schedule_get_service,
    schedule_list_service,
    schedule_retire_service,
    schedule_update_service,
    validate_schedule_mcp_arguments,
)
from tests.helpers import AllowAllAuthStub


class _Row(dict):
    def keys(self):
        return super().keys()


class _Result:
    def __init__(self, *, one=None, many=None):
        self.one = one
        self.many = many or []

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.many


def _valid_row(**overrides):
    row = _Row(
        schedule_id="sched_existing",
        kind="reminder",
        status="pending",
        title="Existing",
        note=None,
        due_at="2026-05-01T12:00:00Z",
        due_at_ts=1777636800,
        created_at="2026-04-25T09:00:00Z",
        updated_at="2026-04-25T09:00:00Z",
        created_by="peer-alpha",
        updated_by="peer-alpha",
        terminal_at=None,
        terminal_by=None,
        terminal_reason=None,
        task_id=None,
        thread_id="thread-1",
        subject_kind="thread",
        subject_id="thread-1",
        idempotency_key=None,
        create_identity_hash="a" * 64,
        create_identity_json="{}",
        metadata_json="{}",
        version=1,
    )
    row.update(overrides)
    return row


class _FakeConn:
    def __init__(self, *, failures=None, row=None, rows=None):
        self.failures = list(failures or [])
        self.row = row
        self.rows = rows
        self.rollback_called = False
        self.closed = False

    def execute(self, sql, _params=()):
        for marker, exc in list(self.failures):
            if marker in sql:
                self.failures.remove((marker, exc))
                raise exc
        if "SELECT * FROM scheduled_items WHERE schedule_id" in sql:
            return _Result(one=self.row)
        if "SELECT * FROM scheduled_items WHERE idempotency_key" in sql:
            return _Result(one=None)
        if "SELECT * FROM scheduled_items" in sql:
            return _Result(many=self.rows if self.rows is not None else [])
        return _Result()

    def commit(self):
        for marker, exc in list(self.failures):
            if marker == "COMMIT":
                self.failures.remove((marker, exc))
                raise exc

    def rollback(self):
        self.rollback_called = True

    def close(self):
        self.closed = True


class _DenyAuth(AllowAllAuthStub):
    def __init__(self, *, deny_scope: bool = False, deny_read: bool = False, deny_write: bool = False) -> None:
        super().__init__(peer_id="peer-alpha")
        self.deny_scope = deny_scope
        self.deny_read = deny_read
        self.deny_write = deny_write

    def require(self, _scope: str) -> None:
        if self.deny_scope:
            raise HTTPException(status_code=403, detail={"code": "forbidden"})

    def require_read_path(self, _path: str) -> None:
        if self.deny_read:
            raise HTTPException(status_code=403, detail={"code": "forbidden"})

    def require_write_path(self, _path: str) -> None:
        if self.deny_write:
            raise HTTPException(status_code=403, detail={"code": "forbidden"})


class Schedule255Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.repo_root = Path(self.tmp.name)
        self.auth = AllowAllAuthStub(peer_id="peer-alpha")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _create(self, **overrides):
        payload = {
            "kind": "reminder",
            "title": "Check build",
            "due_at": "2026-05-01T12:00:00Z",
            "thread_id": "thread-1",
            "subject_kind": "thread",
            "subject_id": "thread-1",
            "metadata": {"source": "test"},
        }
        payload.update(overrides)
        return schedule_create_service(repo_root=self.repo_root, auth=self.auth, payload=payload)

    def test_bootstrap_schema_and_create_identity_are_durable(self) -> None:
        status, body = self._create()
        self.assertEqual(status, 201)
        self.assertTrue((self.repo_root / SCHEDULE_DB_REL).exists())
        self.assertTrue(body["created"])
        item = body["item"]
        self.assertEqual(item["created_at"], item["updated_at"])
        self.assertEqual(item["created_by"], "peer-alpha")
        self.assertEqual(item["metadata"], {"source": "test"})
        self.assertEqual(item["version"], 1)

        conn = sqlite3.connect(self.repo_root / SCHEDULE_DB_REL)
        row = conn.execute("SELECT create_identity_hash, create_identity_json FROM scheduled_items").fetchone()
        migration = conn.execute("SELECT version FROM schedule_schema_migrations").fetchone()
        self.assertEqual(migration[0], 1)
        self.assertEqual(len(row[0]), 64)
        self.assertIn('"created_by":"peer-alpha"', row[1])

    def test_create_replay_uses_original_identity_after_patch(self) -> None:
        status, body = self._create(idempotency_key="idem-1")
        self.assertEqual(status, 201)
        schedule_id = body["item"]["schedule_id"]
        updated = schedule_update_service(
            repo_root=self.repo_root,
            auth=self.auth,
            schedule_id=schedule_id,
            payload={"expected_version": 1, "title": "Changed title"},
        )
        self.assertTrue(updated["updated"])

        replay_status, replay = self._create(idempotency_key="idem-1")
        self.assertEqual(replay_status, 200)
        self.assertFalse(replay["created"])
        self.assertEqual(replay["item"]["title"], "Changed title")

    def test_conflict_and_timestamp_validation_precedence(self) -> None:
        self._create(schedule_id="sched_conflict1", idempotency_key="idem-a")
        self._create(schedule_id="sched_conflict2", idempotency_key="idem-b", title="Other")
        with self.assertRaises(HTTPException) as ctx:
            self._create(schedule_id="sched_conflict1", idempotency_key="idem-b", due_at="2025-01-01T00:00:00Z")
        self.assertEqual(ctx.exception.status_code, 422)
        self.assertEqual(ctx.exception.detail["code"], "due_at_not_future")

        with self.assertRaises(HTTPException) as ctx2:
            self._create(schedule_id="sched_conflict1", idempotency_key="idem-b", due_at="2026-06-01T00:00:00Z")
        self.assertEqual(ctx2.exception.status_code, 409)
        self.assertEqual(ctx2.exception.detail["code"], "idempotency_key_conflict")

    def test_list_update_acknowledge_retire_lifecycle(self) -> None:
        _status, body = self._create(schedule_id="sched_lifecycle")
        listed = schedule_list_service(repo_root=self.repo_root, auth=self.auth, query={"thread_id": "thread-1"})
        self.assertEqual(listed["count"], 1)
        self.assertEqual(listed["total"], 1)

        no_op = schedule_update_service(
            repo_root=self.repo_root,
            auth=self.auth,
            schedule_id="sched_lifecycle",
            payload={"expected_version": 1, "title": body["item"]["title"]},
        )
        self.assertFalse(no_op["updated"])
        self.assertEqual(no_op["item"]["version"], 1)

        acked = schedule_acknowledge_service(
            repo_root=self.repo_root,
            auth=self.auth,
            schedule_id="sched_lifecycle",
            payload={"expected_version": 1, "status": "done", "reason": " completed "},
        )
        self.assertTrue(acked["updated"])
        self.assertEqual(acked["item"]["status"], "done")
        self.assertEqual(acked["item"]["terminal_reason"], "completed")

        retired = schedule_retire_service(
            repo_root=self.repo_root,
            auth=self.auth,
            schedule_id="sched_lifecycle",
            payload={"expected_version": 2, "reason": "obsolete"},
        )
        self.assertEqual(retired["item"]["status"], "retired")

    def test_missing_db_read_degrades_with_schedule_db_missing_warning(self) -> None:
        with self.assertRaises(HTTPException) as ctx:
            schedule_get_service(repo_root=self.repo_root, auth=self.auth, schedule_id="sched_missing")
        self.assertEqual(ctx.exception.status_code, 404)
        self.assertEqual(ctx.exception.detail["warnings"], ["schedule_db_missing"])

    def test_due_context_is_scoped_to_context_retrieve_selectors(self) -> None:
        with patch("app.schedule.service.iso_now") as mocked_now:
            mocked_now.return_value = datetime(2026, 4, 25, 9, 0, 0, tzinfo=timezone.utc)
            self._create(schedule_id="sched_due", due_at="2026-04-25T09:00:01Z", thread_id="thread-1")
            self._create(
                schedule_id="sched_other",
                due_at="2026-04-25T09:00:01Z",
                thread_id="thread-2",
                subject_id="thread-2",
            )
        with patch("app.schedule.service.iso_now") as mocked_now:
            mocked_now.return_value = datetime(2026, 4, 25, 9, 0, 2, tzinfo=timezone.utc)
            context = schedule_context_for_context_retrieve(
                repo_root=self.repo_root,
                auth=self.auth,
                req=ContextRetrieveRequest(
                    task="resume",
                    continuity_selectors=[ContinuitySelector(subject_kind="thread", subject_id="thread-1")],
                ),
                due_limit=10,
                upcoming_limit=5,
                upcoming_window_hours=72,
            )
        self.assertEqual(context["due"]["count"], 1)
        self.assertEqual(context["due"]["items"][0]["schedule_id"], "sched_due")

    def test_mcp_validation_returns_single_schedule_detail(self) -> None:
        detail = validate_schedule_mcp_arguments(
            "schedule.create",
            {"kind": "reminder", "title": "ok", "due_at": "2026-05-01T12:00:00+00:00"},
        )
        self.assertEqual(detail["code"], "invalid_schedule_due_at")
        self.assertEqual(detail["field"], "due_at")

    def test_sqlite_select_failures_degrade_with_specific_codes(self) -> None:
        cases = [
            (sqlite3.OperationalError("database is locked"), "schedule_db_locked"),
            (sqlite3.DatabaseError("database disk image is malformed"), "schedule_db_corrupt"),
            (sqlite3.OperationalError("disk I/O error"), "schedule_db_unavailable"),
        ]
        for exc, code in cases:
            with self.subTest(code=code):
                fakes = [_FakeConn(failures=[("SELECT", exc)]) for _ in range(3)]
                with patch("app.schedule.service._connect_once", side_effect=fakes):
                    body = schedule_get_service(repo_root=self.repo_root, auth=self.auth, schedule_id="sched_missing")
                self.assertFalse(body["ok"])
                self.assertEqual(body["warnings"], [code])

    def test_sqlite_insert_update_and_commit_failures_rollback_and_return_503(self) -> None:
        cases = [
            ("INSERT", lambda fake: schedule_create_service(repo_root=self.repo_root, auth=self.auth, payload={
                "kind": "reminder",
                "title": "Check build",
                "due_at": "2026-05-01T12:00:00Z",
            })),
            ("UPDATE", lambda fake: schedule_update_service(
                repo_root=self.repo_root,
                auth=self.auth,
                schedule_id="sched_existing",
                payload={"expected_version": 1, "title": "Changed"},
            )),
            ("COMMIT", lambda fake: schedule_acknowledge_service(
                repo_root=self.repo_root,
                auth=self.auth,
                schedule_id="sched_existing",
                payload={"expected_version": 1},
            )),
        ]
        for marker, call in cases:
            with self.subTest(marker=marker):
                row = None if marker == "INSERT" else _valid_row(title="Existing")
                fakes = [_FakeConn(failures=[(marker, sqlite3.OperationalError("database is locked"))], row=row) for _ in range(3)]
                with patch("app.schedule.service._connect_once", side_effect=fakes):
                    with self.assertRaises(HTTPException) as ctx:
                        call(fakes[-1])
                self.assertEqual(ctx.exception.status_code, 503)
                self.assertEqual(ctx.exception.detail["code"], "schedule_db_locked")
                self.assertTrue(fakes[-1].rollback_called)

    def test_locked_select_retries_full_operation(self) -> None:
        first = _FakeConn(failures=[("SELECT", sqlite3.OperationalError("database is locked"))])
        second = _FakeConn(row=_valid_row(schedule_id="sched_retry"))
        with patch("app.schedule.service._connect_once", side_effect=[first, second]) as mocked_connect:
            body = schedule_get_service(repo_root=self.repo_root, auth=self.auth, schedule_id="sched_retry")
        self.assertTrue(body["ok"])
        self.assertEqual(body["item"]["schedule_id"], "sched_retry")
        self.assertEqual(mocked_connect.call_count, 2)

    def test_malformed_rows_degrade_get_list_and_context_without_repair(self) -> None:
        _status, body = self._create(schedule_id="sched_badrow")
        conn = sqlite3.connect(self.repo_root / SCHEDULE_DB_REL)
        conn.execute("UPDATE scheduled_items SET metadata_json = ? WHERE schedule_id = ?", ('{"nested":{"bad":true}}', body["item"]["schedule_id"]))
        conn.commit()
        conn.close()

        got = schedule_get_service(repo_root=self.repo_root, auth=self.auth, schedule_id="sched_badrow")
        self.assertFalse(got["ok"])
        self.assertIsNone(got["item"])
        self.assertIn("schedule_row_invalid:sched_badrow", got["warnings"])

        listed = schedule_list_service(repo_root=self.repo_root, auth=self.auth, query={})
        self.assertTrue(listed["ok"])
        self.assertEqual(listed["count"], 0)
        self.assertIn("schedule_rows_skipped", listed["warnings"])
        self.assertIn("schedule_row_invalid:sched_badrow", listed["warnings"])

        context = schedule_context_for_context_retrieve(
            repo_root=self.repo_root,
            auth=self.auth,
            req=ContextRetrieveRequest(
                task="resume",
                continuity_selectors=[ContinuitySelector(subject_kind="thread", subject_id="thread-1")],
            ),
            due_limit=10,
            upcoming_limit=5,
            upcoming_window_hours=72,
        )
        self.assertEqual(context["due"]["count"], 0)
        self.assertIn("schedule_rows_skipped", context["warnings"])
        self.assertIn("schedule_row_invalid:sched_badrow", context["warnings"])

    def test_mcp_get_list_unknown_keys_and_strict_types(self) -> None:
        self.assertEqual(
            validate_schedule_mcp_arguments("schedule.get", {"schedule_id": "sched_ok", "extra": True})["field"],
            "extra",
        )
        self.assertEqual(
            validate_schedule_mcp_arguments("schedule.list", {"task_id": 123})["field"],
            "query.task_id",
        )
        self.assertEqual(
            validate_schedule_mcp_arguments("schedule.list", {"due": "true"})["field"],
            "query.due",
        )
        self.assertEqual(
            validate_schedule_mcp_arguments("schedule.list", {"limit": "10"})["field"],
            "query.limit",
        )

    def test_mcp_extra_field_precedence_for_item_id_tools(self) -> None:
        for tool in ("schedule.update", "schedule.acknowledge", "schedule.retire"):
            with self.subTest(tool=tool):
                detail = validate_schedule_mcp_arguments(tool, {"schedule_id": "bad id", "extra": "wins"})
                self.assertEqual(detail["code"], "invalid_schedule_payload")
                self.assertEqual(detail["field"], "extra")

    def test_terminal_idempotency_and_stale_expected_version_cases(self) -> None:
        self._create(schedule_id="sched_terminal")
        acked = schedule_acknowledge_service(
            repo_root=self.repo_root,
            auth=self.auth,
            schedule_id="sched_terminal",
            payload={"expected_version": 1, "status": "done", "reason": "complete"},
        )
        replay = schedule_acknowledge_service(
            repo_root=self.repo_root,
            auth=self.auth,
            schedule_id="sched_terminal",
            payload={"status": "done", "reason": "complete"},
        )
        self.assertFalse(replay["updated"])
        self.assertEqual(replay["item"]["version"], acked["item"]["version"])
        with self.assertRaises(HTTPException) as ctx:
            schedule_acknowledge_service(
                repo_root=self.repo_root,
                auth=self.auth,
                schedule_id="sched_terminal",
                payload={"expected_version": 1, "status": "done", "reason": "complete"},
            )
        self.assertEqual(ctx.exception.status_code, 409)

        self._create(schedule_id="sched_retired")
        retired = schedule_retire_service(
            repo_root=self.repo_root,
            auth=self.auth,
            schedule_id="sched_retired",
            payload={"expected_version": 1, "reason": "obsolete"},
        )
        retire_replay = schedule_retire_service(
            repo_root=self.repo_root,
            auth=self.auth,
            schedule_id="sched_retired",
            payload={"reason": "obsolete"},
        )
        self.assertFalse(retire_replay["updated"])
        self.assertEqual(retire_replay["item"]["version"], retired["item"]["version"])

    def test_operation_clock_snapshot_for_written_timestamps(self) -> None:
        self._create(schedule_id="sched_clock")
        with patch("app.schedule.service.iso_now", return_value=datetime(2026, 4, 25, 10, 0, 0, tzinfo=timezone.utc)):
            acked = schedule_acknowledge_service(
                repo_root=self.repo_root,
                auth=self.auth,
                schedule_id="sched_clock",
                payload={"expected_version": 1, "reason": "same instant"},
            )
        self.assertEqual(acked["item"]["updated_at"], "2026-04-25T10:00:00Z")
        self.assertEqual(acked["item"]["terminal_at"], "2026-04-25T10:00:00Z")

    def test_metadata_utf8_byte_limit_and_non_finite_numbers_rejected(self) -> None:
        with self.assertRaises(HTTPException) as too_large:
            self._create(metadata={"x": "é" * 1100})
        self.assertEqual(too_large.exception.detail["code"], "invalid_schedule_metadata")
        with self.assertRaises(HTTPException) as non_finite:
            self._create(metadata={"x": float("inf")})
        self.assertEqual(non_finite.exception.detail["code"], "invalid_schedule_metadata")

    def test_auth_scope_and_namespace_checks_are_enforced(self) -> None:
        with self.assertRaises(HTTPException):
            schedule_list_service(repo_root=self.repo_root, auth=_DenyAuth(deny_scope=True), query={})
        with self.assertRaises(HTTPException):
            schedule_list_service(repo_root=self.repo_root, auth=_DenyAuth(deny_read=True), query={})
        with self.assertRaises(HTTPException):
            schedule_create_service(
                repo_root=self.repo_root,
                auth=_DenyAuth(deny_write=True),
                payload={"kind": "reminder", "title": "Check", "due_at": "2026-05-01T12:00:00Z"},
            )

    def test_all_six_service_success_paths(self) -> None:
        status, created = self._create(schedule_id="sched_success")
        self.assertEqual(status, 201)
        self.assertTrue(schedule_get_service(repo_root=self.repo_root, auth=self.auth, schedule_id="sched_success")["ok"])
        self.assertGreaterEqual(schedule_list_service(repo_root=self.repo_root, auth=self.auth, query={})["count"], 1)
        updated = schedule_update_service(
            repo_root=self.repo_root,
            auth=self.auth,
            schedule_id="sched_success",
            payload={"expected_version": created["item"]["version"], "title": "Updated"},
        )
        self.assertTrue(updated["updated"])
        acked = schedule_acknowledge_service(
            repo_root=self.repo_root,
            auth=self.auth,
            schedule_id="sched_success",
            payload={"expected_version": updated["item"]["version"]},
        )
        self.assertTrue(acked["updated"])
        self._create(schedule_id="sched_retire_success")
        retired = schedule_retire_service(
            repo_root=self.repo_root,
            auth=self.auth,
            schedule_id="sched_retire_success",
            payload={"expected_version": 1},
        )
        self.assertTrue(retired["updated"])
