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
