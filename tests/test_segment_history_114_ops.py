"""Tests for segment-history ops wiring (issue #114, Phase 9)."""

from __future__ import annotations

import unittest

from app.models import (
    SegmentHistoryColdRehydrateRequest,
    SegmentHistoryColdStoreRequest,
    SegmentHistoryMaintenanceRequest,
)
from app.ops.service import OPS_JOBS


class TestOpsJobRegistry(unittest.TestCase):
    def test_segment_history_maintenance_registered(self) -> None:
        self.assertIn("segment_history_maintenance", OPS_JOBS)

    def test_segment_history_cold_store_registered(self) -> None:
        self.assertIn("segment_history_cold_store", OPS_JOBS)

    def test_segment_history_cold_rehydrate_registered(self) -> None:
        self.assertIn("segment_history_cold_rehydrate", OPS_JOBS)


class TestRequestModels(unittest.TestCase):
    def test_maintenance_request(self) -> None:
        req = SegmentHistoryMaintenanceRequest(family="journal")
        self.assertEqual(req.family, "journal")

    def test_cold_store_request(self) -> None:
        req = SegmentHistoryColdStoreRequest(family="api_audit")
        self.assertIsNone(req.segment_ids)

    def test_cold_store_request_with_ids(self) -> None:
        req = SegmentHistoryColdStoreRequest(
            family="api_audit",
            segment_ids=["api_audit__20260320T120000Z__0001"],
        )
        self.assertEqual(len(req.segment_ids), 1)

    def test_cold_rehydrate_request(self) -> None:
        req = SegmentHistoryColdRehydrateRequest(
            family="journal",
            segment_id="journal__20260320T120000Z__0001",
        )
        self.assertEqual(req.family, "journal")


if __name__ == "__main__":
    unittest.main()
