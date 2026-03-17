"""Tests for Issue #45 — CoordinationQueryIndex SQLite sidecar.

Validates schema creation, rebuild from filesystem, upsert operations,
query filtering, sort order, pagination, junction table queries, and
graceful handling of invalid files.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app.coordination.query_index import CoordinationQueryIndex


def _write_json(directory: Path, filename: str, data: dict) -> Path:
    """Write a JSON artifact fixture to the given directory."""
    path = directory / filename
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


class TestCoordinationQueryIndex(unittest.TestCase):
    """Validate the SQLite sidecar index for coordination query services."""

    def _make_index(self, tmp: Path) -> CoordinationQueryIndex:
        """Create a fresh index at a temporary path."""
        return CoordinationQueryIndex(tmp / ".query_index.db")

    # -- schema / availability ---------------------------------------------

    def test_schema_creation_on_fresh_db(self) -> None:
        """A freshly created index should report is_available=True."""
        with tempfile.TemporaryDirectory() as tmp:
            idx = self._make_index(Path(tmp))
            self.assertTrue(idx.is_available)

    def test_unavailable_on_bad_path(self) -> None:
        """Index at an invalid path should degrade gracefully."""
        idx = CoordinationQueryIndex(Path("/nonexistent/dir/.query_index.db"))
        self.assertFalse(idx.is_available)

    # -- rebuild handoffs --------------------------------------------------

    def test_rebuild_handoffs_from_valid_files(self) -> None:
        """Rebuild should index all valid handoff JSON files."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            idx = self._make_index(tmp_path)
            hdir = tmp_path / "handoffs"
            hdir.mkdir()

            _write_json(hdir, "handoff_aaa.json", {
                "handoff_id": "handoff_aaa",
                "sender_peer": "alice",
                "recipient_peer": "bob",
                "recipient_status": "pending",
                "created_at": "2026-03-01T10:00:00Z",
                "task_id": "t1",
                "thread_id": None,
            })
            _write_json(hdir, "handoff_bbb.json", {
                "handoff_id": "handoff_bbb",
                "sender_peer": "bob",
                "recipient_peer": "alice",
                "recipient_status": "accepted_advisory",
                "created_at": "2026-03-02T10:00:00Z",
                "task_id": None,
                "thread_id": "th1",
            })

            count = idx.rebuild_handoffs(hdir)
            self.assertEqual(count, 2)

            ids, total = idx.query_handoffs()
            self.assertEqual(total, 2)
            # bbb is newer, should come first
            self.assertEqual(ids, ["handoff_bbb", "handoff_aaa"])

    def test_rebuild_handoffs_skips_invalid_files(self) -> None:
        """Invalid JSON and non-handoff files are silently skipped."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            idx = self._make_index(tmp_path)
            hdir = tmp_path / "handoffs"
            hdir.mkdir()

            # Valid
            _write_json(hdir, "handoff_ok.json", {
                "handoff_id": "handoff_ok",
                "sender_peer": "alice",
                "recipient_peer": "bob",
                "created_at": "2026-03-01T10:00:00Z",
            })
            # Invalid JSON
            (hdir / "broken.json").write_text("{bad json", encoding="utf-8")
            # Missing handoff_id
            _write_json(hdir, "noid.json", {"sender_peer": "alice"})
            # Not a JSON file
            (hdir / "readme.txt").write_text("ignore me", encoding="utf-8")
            # Subdirectory
            (hdir / "subdir").mkdir()

            count = idx.rebuild_handoffs(hdir)
            self.assertEqual(count, 1)

    def test_rebuild_handoffs_empty_directory(self) -> None:
        """Rebuild on an empty directory returns 0."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            idx = self._make_index(tmp_path)
            hdir = tmp_path / "handoffs"
            hdir.mkdir()

            count = idx.rebuild_handoffs(hdir)
            self.assertEqual(count, 0)
            ids, total = idx.query_handoffs()
            self.assertEqual(ids, [])
            self.assertEqual(total, 0)

    def test_rebuild_handoffs_nonexistent_directory(self) -> None:
        """Rebuild on a non-existent directory returns 0 without error."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            idx = self._make_index(tmp_path)
            count = idx.rebuild_handoffs(tmp_path / "does_not_exist")
            self.assertEqual(count, 0)

    # -- rebuild shared ----------------------------------------------------

    def test_rebuild_shared_with_participants(self) -> None:
        """Shared rebuild should populate main table and participant junction."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            idx = self._make_index(tmp_path)
            sdir = tmp_path / "shared"
            sdir.mkdir()

            _write_json(sdir, "shared_aaa.json", {
                "shared_id": "shared_aaa",
                "owner_peer": "alice",
                "participant_peers": ["bob", "charlie"],
                "task_id": "t1",
                "thread_id": None,
                "updated_at": "2026-03-01T12:00:00Z",
            })

            count = idx.rebuild_shared(sdir)
            self.assertEqual(count, 1)

            # Query by participant should find it
            ids, total = idx.query_shared(participant_peer="bob")
            self.assertEqual(total, 1)
            self.assertEqual(ids, ["shared_aaa"])

            # Query by non-participant should not
            ids, total = idx.query_shared(participant_peer="dave")
            self.assertEqual(total, 0)

    # -- rebuild reconciliations -------------------------------------------

    def test_rebuild_reconciliations_with_claimants(self) -> None:
        """Reconciliation rebuild should populate claimant junction table."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            idx = self._make_index(tmp_path)
            rdir = tmp_path / "reconciliations"
            rdir.mkdir()

            _write_json(rdir, "recon_aaa.json", {
                "reconciliation_id": "recon_aaa",
                "owner_peer": "alice",
                "status": "open",
                "classification": "contradictory",
                "task_id": "t1",
                "thread_id": None,
                "updated_at": "2026-03-01T12:00:00Z",
                "claims": [
                    {"claimant_peer": "bob", "claim_summary": "x"},
                    {"claimant_peer": "charlie", "claim_summary": "y"},
                ],
            })

            count = idx.rebuild_reconciliations(rdir)
            self.assertEqual(count, 1)

            # Query by claimant
            ids, total = idx.query_reconciliations(claimant_peer="bob")
            self.assertEqual(total, 1)
            self.assertEqual(ids, ["recon_aaa"])

            ids, total = idx.query_reconciliations(claimant_peer="dave")
            self.assertEqual(total, 0)

    # -- upsert handoff ----------------------------------------------------

    def test_upsert_handoff_insert_and_update(self) -> None:
        """Upsert should insert a new entry and update an existing one."""
        with tempfile.TemporaryDirectory() as tmp:
            idx = self._make_index(Path(tmp))

            idx.upsert_handoff({
                "handoff_id": "handoff_new",
                "sender_peer": "alice",
                "recipient_peer": "bob",
                "recipient_status": "pending",
                "created_at": "2026-03-01T10:00:00Z",
                "task_id": None,
                "thread_id": None,
            })
            ids, total = idx.query_handoffs(sender_peer="alice")
            self.assertEqual(total, 1)
            self.assertEqual(ids, ["handoff_new"])

            # Update status via upsert
            idx.upsert_handoff({
                "handoff_id": "handoff_new",
                "sender_peer": "alice",
                "recipient_peer": "bob",
                "recipient_status": "accepted_advisory",
                "created_at": "2026-03-01T10:00:00Z",
                "task_id": None,
                "thread_id": None,
            })
            ids, total = idx.query_handoffs(status="accepted_advisory")
            self.assertEqual(total, 1)
            ids, total = idx.query_handoffs(status="pending")
            self.assertEqual(total, 0)

    # -- upsert shared -----------------------------------------------------

    def test_upsert_shared_replaces_participants(self) -> None:
        """Upsert should replace participant junction rows on update."""
        with tempfile.TemporaryDirectory() as tmp:
            idx = self._make_index(Path(tmp))

            idx.upsert_shared({
                "shared_id": "shared_x",
                "owner_peer": "alice",
                "participant_peers": ["bob"],
                "updated_at": "2026-03-01T12:00:00Z",
            })
            ids, _ = idx.query_shared(participant_peer="bob")
            self.assertEqual(ids, ["shared_x"])

            # Update: remove bob, add charlie
            idx.upsert_shared({
                "shared_id": "shared_x",
                "owner_peer": "alice",
                "participant_peers": ["charlie"],
                "updated_at": "2026-03-01T13:00:00Z",
            })
            ids, _ = idx.query_shared(participant_peer="bob")
            self.assertEqual(ids, [])
            ids, _ = idx.query_shared(participant_peer="charlie")
            self.assertEqual(ids, ["shared_x"])

    # -- upsert reconciliation ---------------------------------------------

    def test_upsert_reconciliation_replaces_claimants(self) -> None:
        """Upsert should replace claimant junction rows on update."""
        with tempfile.TemporaryDirectory() as tmp:
            idx = self._make_index(Path(tmp))

            idx.upsert_reconciliation({
                "reconciliation_id": "recon_x",
                "owner_peer": "alice",
                "status": "open",
                "classification": "stale_observation",
                "updated_at": "2026-03-01T12:00:00Z",
                "claims": [{"claimant_peer": "bob"}],
            })
            ids, _ = idx.query_reconciliations(claimant_peer="bob")
            self.assertEqual(ids, ["recon_x"])

            # Resolve: change status
            idx.upsert_reconciliation({
                "reconciliation_id": "recon_x",
                "owner_peer": "alice",
                "status": "resolved",
                "classification": "stale_observation",
                "updated_at": "2026-03-01T14:00:00Z",
                "claims": [{"claimant_peer": "bob"}],
            })
            ids, _ = idx.query_reconciliations(status="open")
            self.assertEqual(ids, [])
            ids, _ = idx.query_reconciliations(status="resolved")
            self.assertEqual(ids, ["recon_x"])

    # -- query filtering ---------------------------------------------------

    def test_query_handoffs_conjunctive_filters(self) -> None:
        """Multiple filters should be ANDed together."""
        with tempfile.TemporaryDirectory() as tmp:
            idx = self._make_index(Path(tmp))

            idx.upsert_handoff({
                "handoff_id": "h1", "sender_peer": "alice",
                "recipient_peer": "bob", "recipient_status": "pending",
                "created_at": "2026-03-01T10:00:00Z",
            })
            idx.upsert_handoff({
                "handoff_id": "h2", "sender_peer": "alice",
                "recipient_peer": "charlie", "recipient_status": "pending",
                "created_at": "2026-03-01T11:00:00Z",
            })
            idx.upsert_handoff({
                "handoff_id": "h3", "sender_peer": "bob",
                "recipient_peer": "alice", "recipient_status": "deferred",
                "created_at": "2026-03-01T12:00:00Z",
            })

            # sender_peer=alice AND recipient_peer=bob
            ids, total = idx.query_handoffs(sender_peer="alice", recipient_peer="bob")
            self.assertEqual(total, 1)
            self.assertEqual(ids, ["h1"])

            # sender_peer=alice AND status=pending
            ids, total = idx.query_handoffs(sender_peer="alice", status="pending")
            self.assertEqual(total, 2)

    def test_query_shared_conjunctive_filters(self) -> None:
        """Shared query filters should be ANDed — owner + participant + task_id."""
        with tempfile.TemporaryDirectory() as tmp:
            idx = self._make_index(Path(tmp))

            idx.upsert_shared({
                "shared_id": "s1", "owner_peer": "alice",
                "participant_peers": ["bob"], "task_id": "t1",
                "thread_id": None, "updated_at": "2026-03-01T10:00:00Z",
            })
            idx.upsert_shared({
                "shared_id": "s2", "owner_peer": "alice",
                "participant_peers": ["charlie"], "task_id": "t2",
                "thread_id": None, "updated_at": "2026-03-01T11:00:00Z",
            })

            # owner + participant
            ids, total = idx.query_shared(owner_peer="alice", participant_peer="bob")
            self.assertEqual(total, 1)
            self.assertEqual(ids, ["s1"])

            # owner + task_id
            ids, total = idx.query_shared(owner_peer="alice", task_id="t2")
            self.assertEqual(total, 1)
            self.assertEqual(ids, ["s2"])

    def test_query_reconciliations_all_filters(self) -> None:
        """Reconciliation query should support all filter combinations."""
        with tempfile.TemporaryDirectory() as tmp:
            idx = self._make_index(Path(tmp))

            idx.upsert_reconciliation({
                "reconciliation_id": "r1", "owner_peer": "alice",
                "status": "open", "classification": "contradictory",
                "task_id": "t1", "thread_id": "th1",
                "updated_at": "2026-03-01T10:00:00Z",
                "claims": [{"claimant_peer": "bob"}],
            })
            idx.upsert_reconciliation({
                "reconciliation_id": "r2", "owner_peer": "alice",
                "status": "resolved", "classification": "stale_observation",
                "task_id": "t1", "thread_id": "th2",
                "updated_at": "2026-03-01T11:00:00Z",
                "claims": [{"claimant_peer": "charlie"}],
            })

            # status filter
            ids, _ = idx.query_reconciliations(status="open")
            self.assertEqual(ids, ["r1"])

            # classification filter
            ids, _ = idx.query_reconciliations(classification="stale_observation")
            self.assertEqual(ids, ["r2"])

            # Combined: owner + task_id + status
            ids, total = idx.query_reconciliations(
                owner_peer="alice", task_id="t1", status="open",
            )
            self.assertEqual(total, 1)
            self.assertEqual(ids, ["r1"])

            # thread_id filter
            ids, _ = idx.query_reconciliations(thread_id="th2")
            self.assertEqual(ids, ["r2"])

    # -- sort order --------------------------------------------------------

    def test_handoff_sort_order_created_at_desc_id_asc(self) -> None:
        """Handoffs sort by created_at descending, handoff_id ascending on tie."""
        with tempfile.TemporaryDirectory() as tmp:
            idx = self._make_index(Path(tmp))

            # Same timestamp — should tie-break on ID ascending
            for hid in ["handoff_ccc", "handoff_aaa", "handoff_bbb"]:
                idx.upsert_handoff({
                    "handoff_id": hid, "sender_peer": "alice",
                    "recipient_peer": "bob", "recipient_status": "pending",
                    "created_at": "2026-03-01T10:00:00Z",
                })

            ids, _ = idx.query_handoffs()
            self.assertEqual(ids, ["handoff_aaa", "handoff_bbb", "handoff_ccc"])

    def test_shared_sort_order_updated_at_desc_id_asc(self) -> None:
        """Shared artifacts sort by updated_at descending, shared_id ascending on tie."""
        with tempfile.TemporaryDirectory() as tmp:
            idx = self._make_index(Path(tmp))

            idx.upsert_shared({
                "shared_id": "s_old", "owner_peer": "a",
                "participant_peers": ["b"], "updated_at": "2026-03-01T10:00:00Z",
            })
            idx.upsert_shared({
                "shared_id": "s_new", "owner_peer": "a",
                "participant_peers": ["b"], "updated_at": "2026-03-02T10:00:00Z",
            })

            ids, _ = idx.query_shared()
            self.assertEqual(ids, ["s_new", "s_old"])

    def test_reconciliation_sort_order(self) -> None:
        """Reconciliations sort by updated_at descending, id ascending on tie."""
        with tempfile.TemporaryDirectory() as tmp:
            idx = self._make_index(Path(tmp))

            idx.upsert_reconciliation({
                "reconciliation_id": "r_old", "owner_peer": "a",
                "status": "open", "classification": "contradictory",
                "updated_at": "2026-03-01T10:00:00Z", "claims": [],
            })
            idx.upsert_reconciliation({
                "reconciliation_id": "r_new", "owner_peer": "a",
                "status": "open", "classification": "contradictory",
                "updated_at": "2026-03-02T10:00:00Z", "claims": [],
            })

            ids, _ = idx.query_reconciliations()
            self.assertEqual(ids, ["r_new", "r_old"])

    # -- pagination --------------------------------------------------------

    def test_handoff_pagination_offset_limit(self) -> None:
        """Query should respect offset and limit for pagination."""
        with tempfile.TemporaryDirectory() as tmp:
            idx = self._make_index(Path(tmp))

            for i in range(5):
                idx.upsert_handoff({
                    "handoff_id": f"h{i:02d}", "sender_peer": "a",
                    "recipient_peer": "b", "recipient_status": "pending",
                    "created_at": f"2026-03-{i + 1:02d}T10:00:00Z",
                })

            # Total should always be 5 regardless of offset/limit
            ids, total = idx.query_handoffs(offset=0, limit=2)
            self.assertEqual(total, 5)
            self.assertEqual(len(ids), 2)
            # Newest first: h04, h03
            self.assertEqual(ids, ["h04", "h03"])

            # Second page
            ids, total = idx.query_handoffs(offset=2, limit=2)
            self.assertEqual(total, 5)
            self.assertEqual(ids, ["h02", "h01"])

            # Last page (partial)
            ids, total = idx.query_handoffs(offset=4, limit=2)
            self.assertEqual(total, 5)
            self.assertEqual(ids, ["h00"])

    def test_shared_pagination(self) -> None:
        """Shared query pagination returns correct page with total count."""
        with tempfile.TemporaryDirectory() as tmp:
            idx = self._make_index(Path(tmp))

            for i in range(3):
                idx.upsert_shared({
                    "shared_id": f"s{i:02d}", "owner_peer": "a",
                    "participant_peers": ["b"],
                    "updated_at": f"2026-03-{i + 1:02d}T10:00:00Z",
                })

            ids, total = idx.query_shared(offset=1, limit=1)
            self.assertEqual(total, 3)
            self.assertEqual(len(ids), 1)
            self.assertEqual(ids, ["s01"])  # middle entry

    # -- rebuild clears stale data -----------------------------------------

    def test_rebuild_clears_stale_entries(self) -> None:
        """Rebuild should replace all existing entries, not accumulate."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            idx = self._make_index(tmp_path)
            hdir = tmp_path / "handoffs"
            hdir.mkdir()

            _write_json(hdir, "handoff_a.json", {
                "handoff_id": "handoff_a", "sender_peer": "alice",
                "recipient_peer": "bob", "created_at": "2026-03-01T10:00:00Z",
            })
            idx.rebuild_handoffs(hdir)
            _, total = idx.query_handoffs()
            self.assertEqual(total, 1)

            # Remove the file and rebuild — should now be empty
            (hdir / "handoff_a.json").unlink()
            idx.rebuild_handoffs(hdir)
            _, total = idx.query_handoffs()
            self.assertEqual(total, 0)

    # -- unavailable index returns empty -----------------------------------

    def test_query_on_unavailable_index_returns_none(self) -> None:
        """An unavailable index should return None to signal failure."""
        idx = CoordinationQueryIndex(Path("/nonexistent/dir/.query_index.db"))
        self.assertFalse(idx.is_available)

        self.assertIsNone(idx.query_handoffs(sender_peer="alice"))
        self.assertIsNone(idx.query_shared(owner_peer="alice"))
        self.assertIsNone(idx.query_reconciliations(owner_peer="alice"))

    # -- upsert on unavailable index is silent -----------------------------

    def test_upsert_on_unavailable_index_is_noop(self) -> None:
        """Upsert on an unavailable index should not raise."""
        idx = CoordinationQueryIndex(Path("/nonexistent/dir/.query_index.db"))

        # These should all silently return without error
        idx.upsert_handoff({"handoff_id": "h1", "sender_peer": "a",
                            "recipient_peer": "b", "created_at": "2026-01-01T00:00:00Z"})
        idx.upsert_shared({"shared_id": "s1", "owner_peer": "a",
                           "updated_at": "2026-01-01T00:00:00Z"})
        idx.upsert_reconciliation({"reconciliation_id": "r1", "owner_peer": "a",
                                   "status": "open", "classification": "contradictory",
                                   "updated_at": "2026-01-01T00:00:00Z", "claims": []})

    # -- duplicate claimant dedup ------------------------------------------

    def test_reconciliation_deduplicates_claimant_peers(self) -> None:
        """Multiple claims by the same peer produce only one junction row."""
        with tempfile.TemporaryDirectory() as tmp:
            idx = self._make_index(Path(tmp))

            idx.upsert_reconciliation({
                "reconciliation_id": "r1", "owner_peer": "alice",
                "status": "open", "classification": "contradictory",
                "updated_at": "2026-03-01T12:00:00Z",
                "claims": [
                    {"claimant_peer": "bob", "claim_summary": "x"},
                    {"claimant_peer": "bob", "claim_summary": "y"},
                ],
            })

            ids, total = idx.query_reconciliations(claimant_peer="bob")
            self.assertEqual(total, 1)
            self.assertEqual(ids, ["r1"])


if __name__ == "__main__":
    unittest.main()
