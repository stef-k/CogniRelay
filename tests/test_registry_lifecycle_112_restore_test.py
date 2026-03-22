"""Tests for registry-history restore-test validation (issue #112)."""

from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from pathlib import Path

from app.maintenance.service import _validate_restored_registry_history


def _make_shard(shard_id: str, schema_type: str, summary: dict | None = None) -> dict:
    """Build a minimal valid registry-history shard payload."""
    return {
        "schema_type": schema_type,
        "schema_version": "1.0",
        "shard_id": shard_id,
        "summary": summary or {"record_count": 1},
    }


def _make_stub(
    shard_id: str,
    family: str,
    payload_path: str,
    summary: dict | None = None,
) -> dict:
    """Build a minimal valid registry-history stub."""
    return {
        "schema_type": "registry_history_stub",
        "schema_version": "1.0",
        "family": family,
        "shard_id": shard_id,
        "payload_path": payload_path,
        "created_at": "2026-03-20T12:00:00+00:00",
        "source_head_path": "messages/state/delivery_index.json",
        "summary": summary or {"record_count": 1},
    }


class TestValidateRestoredRegistryHistory(unittest.TestCase):
    # ------------------------------------------------------------------ #
    # 1. Empty restore root
    # ------------------------------------------------------------------ #
    def test_empty_restore_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            result = _validate_restored_registry_history(Path(td))
            self.assertTrue(result["ok"])
            self.assertEqual(result["payloads"], 0)
            self.assertEqual(result["cold_payloads"], 0)
            self.assertEqual(result["stubs"], 0)

    # ------------------------------------------------------------------ #
    # 2. Valid hot payload + stub
    # ------------------------------------------------------------------ #
    def test_valid_hot_payload_and_stub(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            history = r / "messages" / "state" / "history" / "delivery"
            history.mkdir(parents=True)
            idx = history / "index"
            idx.mkdir()

            sid = "delivery__20260320T120000Z__0001"
            shard = _make_shard(sid, "delivery_history_shard")
            (history / f"{sid}.json").write_text(json.dumps(shard), encoding="utf-8")

            stub = _make_stub(sid, "delivery", f"messages/state/history/delivery/{sid}.json")
            (idx / f"{sid}.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertTrue(result["ok"])
            self.assertEqual(result["payloads"], 1)
            self.assertEqual(result["stubs"], 1)

    # ------------------------------------------------------------------ #
    # 3. Valid cold payload + stub
    # ------------------------------------------------------------------ #
    def test_valid_cold_payload_and_stub(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            history = r / "messages" / "state" / "history" / "delivery"
            cold = history / "cold"
            cold.mkdir(parents=True)
            idx = history / "index"
            idx.mkdir()

            sid = "delivery__20260320T120000Z__0001"
            shard = _make_shard(sid, "delivery_history_shard")
            (cold / f"{sid}.json.gz").write_bytes(gzip.compress(json.dumps(shard).encode()))

            stub = _make_stub(sid, "delivery", f"messages/state/history/delivery/cold/{sid}.json.gz")
            (idx / f"{sid}.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertTrue(result["ok"])
            self.assertEqual(result["cold_payloads"], 1)
            self.assertEqual(result["stubs"], 1)

    # ------------------------------------------------------------------ #
    # 4. Invalid hot payload JSON
    # ------------------------------------------------------------------ #
    def test_invalid_payload_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            history = r / "messages" / "state" / "history" / "delivery"
            history.mkdir(parents=True)
            (history / "bad.json").write_text("NOT JSON", encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["invalid_payloads"]), 1)

    # ------------------------------------------------------------------ #
    # 5. Invalid cold payload (corrupt gzip)
    # ------------------------------------------------------------------ #
    def test_invalid_cold_payload_corrupt_gzip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            cold = r / "messages" / "state" / "history" / "delivery" / "cold"
            cold.mkdir(parents=True)
            (cold / "bad.json.gz").write_bytes(b"NOT GZIP")

            result = _validate_restored_registry_history(r)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["invalid_cold_payloads"]), 1)

    # ------------------------------------------------------------------ #
    # 6. Invalid stub JSON
    # ------------------------------------------------------------------ #
    def test_invalid_stub_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            idx = r / "messages" / "state" / "history" / "delivery" / "index"
            idx.mkdir(parents=True)
            (idx / "bad.json").write_text("{broken", encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["invalid_stubs"]), 1)

    # ------------------------------------------------------------------ #
    # 7. Stub wrong schema_type
    # ------------------------------------------------------------------ #
    def test_stub_wrong_schema_type(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            idx = r / "messages" / "state" / "history" / "delivery" / "index"
            idx.mkdir(parents=True)
            sid = "delivery__20260320T120000Z__0001"
            stub = _make_stub(sid, "delivery", f"messages/state/history/delivery/{sid}.json")
            stub["schema_type"] = "wrong_type"
            (idx / f"{sid}.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["invalid_stubs"]), 1)

    # ------------------------------------------------------------------ #
    # 8. Stub missing shard_id
    # ------------------------------------------------------------------ #
    def test_stub_missing_shard_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            idx = r / "messages" / "state" / "history" / "delivery" / "index"
            idx.mkdir(parents=True)
            sid = "delivery__20260320T120000Z__0001"
            stub = _make_stub(sid, "delivery", f"messages/state/history/delivery/{sid}.json")
            del stub["shard_id"]
            (idx / f"{sid}.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["invalid_stubs"]), 1)

    # ------------------------------------------------------------------ #
    # 9. Stub shard_id != filename
    # ------------------------------------------------------------------ #
    def test_stub_shard_id_mismatch_filename(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            idx = r / "messages" / "state" / "history" / "delivery" / "index"
            idx.mkdir(parents=True)
            sid = "delivery__20260320T120000Z__0001"
            stub = _make_stub(sid, "delivery", f"messages/state/history/delivery/{sid}.json")
            stub["shard_id"] = "different_id"
            (idx / f"{sid}.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["invalid_stubs"]), 1)

    # ------------------------------------------------------------------ #
    # 10. Stub points to missing payload
    # ------------------------------------------------------------------ #
    def test_stub_missing_payload(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            history = r / "messages" / "state" / "history" / "delivery"
            idx = history / "index"
            idx.mkdir(parents=True)

            sid = "delivery__20260320T120000Z__0001"
            stub = _make_stub(sid, "delivery", f"messages/state/history/delivery/{sid}.json")
            (idx / f"{sid}.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["missing_payloads"]), 1)

    # ------------------------------------------------------------------ #
    # 11. Hot payload without matching stub
    # ------------------------------------------------------------------ #
    def test_payload_without_stub(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            history = r / "messages" / "state" / "history" / "delivery"
            history.mkdir(parents=True)

            sid = "delivery__20260320T120000Z__0001"
            shard = _make_shard(sid, "delivery_history_shard")
            (history / f"{sid}.json").write_text(json.dumps(shard), encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["unmatched_payloads"]), 1)

    # ------------------------------------------------------------------ #
    # 12. Cold payload without matching stub
    # ------------------------------------------------------------------ #
    def test_cold_payload_without_stub(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            cold = r / "messages" / "state" / "history" / "delivery" / "cold"
            cold.mkdir(parents=True)

            sid = "delivery__20260320T120000Z__0001"
            shard = _make_shard(sid, "delivery_history_shard")
            (cold / f"{sid}.json.gz").write_bytes(gzip.compress(json.dumps(shard).encode()))

            result = _validate_restored_registry_history(r)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["unmatched_payloads"]), 1)

    # ------------------------------------------------------------------ #
    # 13. Stub/payload summary mismatch
    # ------------------------------------------------------------------ #
    def test_stub_payload_summary_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            history = r / "messages" / "state" / "history" / "delivery"
            history.mkdir(parents=True)
            idx = history / "index"
            idx.mkdir()

            sid = "delivery__20260320T120000Z__0001"
            shard = _make_shard(sid, "delivery_history_shard", summary={"record_count": 5})
            (history / f"{sid}.json").write_text(json.dumps(shard), encoding="utf-8")

            stub = _make_stub(sid, "delivery", f"messages/state/history/delivery/{sid}.json", summary={"record_count": 99})
            (idx / f"{sid}.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["mismatched_stubs"]), 1)

    # ------------------------------------------------------------------ #
    # 14. Stub/payload shard_id mismatch (stub points to valid payload
    #     but its own shard_id differs from the payload's shard_id)
    # ------------------------------------------------------------------ #
    def test_stub_payload_shard_id_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            history = r / "messages" / "state" / "history" / "delivery"
            history.mkdir(parents=True)
            idx = history / "index"
            idx.mkdir()

            # The shard on disk has shard_id matching its filename
            sid_a = "delivery__20260320T120000Z__0001"
            shard = _make_shard(sid_a, "delivery_history_shard")
            (history / f"{sid_a}.json").write_text(json.dumps(shard), encoding="utf-8")

            # The stub file also has filename matching sid_a, but its
            # internal shard_id matches its own filename (so it passes
            # individual validation). The mismatch is detected at cross-match.
            # To trigger this: stub shard_id == sid_a (matches filename),
            # but we make the payload have a *different* shard_id in its body
            # while keeping the same filename. That fails payload validation.
            # Instead: create two payloads, stub points to one but has the
            # other's shard_id. Simplest approach: stub with shard_id sid_a
            # pointing to a payload whose shard_id is sid_b.
            sid_b = "delivery__20260320T120000Z__0002"
            shard_b = _make_shard(sid_b, "delivery_history_shard")
            (history / f"{sid_b}.json").write_text(json.dumps(shard_b), encoding="utf-8")

            # Stub for sid_a points to payload sid_b
            stub = _make_stub(sid_a, "delivery", f"messages/state/history/delivery/{sid_b}.json")
            (idx / f"{sid_a}.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertFalse(result["ok"])
            # shard_id mismatch triggers mismatched_stubs
            self.assertTrue(len(result["mismatched_stubs"]) >= 1)

    # ------------------------------------------------------------------ #
    # 15. All 4 families valid
    # ------------------------------------------------------------------ #
    def test_all_four_families_valid(self) -> None:
        families = [
            ("delivery", "messages/state/history/delivery", "delivery_history_shard"),
            ("peer_trust", "peers/history/registry", "peer_trust_history_shard"),
            ("replication_state", "peers/history/replication_state", "replication_state_history_shard"),
            ("replication_tombstones", "peers/history/replication_tombstones", "replication_tombstone_shard"),
        ]
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            for family, hist_rel, schema in families:
                history = r / hist_rel
                history.mkdir(parents=True)
                idx = history / "index"
                idx.mkdir()

                sid = f"{family}__20260320T120000Z__0001"
                shard = _make_shard(sid, schema)
                (history / f"{sid}.json").write_text(json.dumps(shard), encoding="utf-8")

                stub = _make_stub(sid, family, f"{hist_rel}/{sid}.json")
                (idx / f"{sid}.json").write_text(json.dumps(stub), encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertTrue(result["ok"])
            self.assertEqual(result["payloads"], 4)
            self.assertEqual(result["stubs"], 4)

    # ------------------------------------------------------------------ #
    # 16. Mixed valid + invalid across families
    # ------------------------------------------------------------------ #
    def test_mixed_valid_and_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)

            # Valid delivery pair
            hist_d = r / "messages" / "state" / "history" / "delivery"
            hist_d.mkdir(parents=True)
            idx_d = hist_d / "index"
            idx_d.mkdir()
            sid_d = "delivery__20260320T120000Z__0001"
            (hist_d / f"{sid_d}.json").write_text(json.dumps(_make_shard(sid_d, "delivery_history_shard")), encoding="utf-8")
            (idx_d / f"{sid_d}.json").write_text(json.dumps(_make_stub(sid_d, "delivery", f"messages/state/history/delivery/{sid_d}.json")), encoding="utf-8")

            # Corrupt peer_trust payload
            hist_p = r / "peers" / "history" / "registry"
            hist_p.mkdir(parents=True)
            (hist_p / "bad.json").write_text("CORRUPT", encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["invalid_payloads"]), 1)
            self.assertIn("peers/history/registry/bad.json", result["invalid_payloads"][0])

    # ------------------------------------------------------------------ #
    # 17. Wrong schema_type for family
    # ------------------------------------------------------------------ #
    def test_wrong_schema_type_for_family(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            r = Path(td)
            history = r / "messages" / "state" / "history" / "delivery"
            history.mkdir(parents=True)

            sid = "delivery__20260320T120000Z__0001"
            # Use peer_trust schema type in delivery directory
            shard = _make_shard(sid, "peer_trust_history_shard")
            (history / f"{sid}.json").write_text(json.dumps(shard), encoding="utf-8")

            result = _validate_restored_registry_history(r)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["invalid_payloads"]), 1)


if __name__ == "__main__":
    unittest.main()
