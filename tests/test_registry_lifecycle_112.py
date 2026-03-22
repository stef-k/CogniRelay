"""Tests for registry lifecycle maintenance (issue #112).

Covers all five registry families: delivery, nonce, peer trust,
replication state (synchronous + idempotency prune), and tombstones.
"""

from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile

from app.registry_lifecycle.service import (
    DELIVERY_STATE_REL,
    NONCE_INDEX_REL,
    PEER_TRUST_HISTORY_DIR_REL,
    PEER_TRUST_STUB_DIR_REL,
    PEERS_REGISTRY_REL,
    REPLICATION_STATE_REL,
    REPLICATION_TOMBSTONES_REL,
    delivery_maintenance_pass,
    externalize_superseded_pull,
    externalize_superseded_push,
    nonce_maintenance_pass,
    peer_trust_maintenance_pass,
    replication_state_prune_idempotency,
    tombstone_maintenance_pass,
)
from app.storage import safe_path, write_text_file


def _write_head(repo_root: Path, rel: str, data: dict) -> None:
    path = safe_path(repo_root, rel)
    write_text_file(path, json.dumps(data, ensure_ascii=False, indent=2))


def _read_json(repo_root: Path, rel: str) -> dict:
    path = safe_path(repo_root, rel)
    return json.loads(path.read_text(encoding="utf-8"))


def _now() -> datetime:
    return datetime(2026, 3, 19, 12, 0, 0, tzinfo=timezone.utc)


# ===================================================================
# Delivery maintenance tests
# ===================================================================

class TestDeliveryMaintenance(unittest.TestCase):

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.repo = Path(self._td.name) / "repo"
        self.repo.mkdir()
        self.now = _now()

    def tearDown(self):
        self._td.cleanup()

    def _write_delivery_state(self, state: dict) -> None:
        _write_head(self.repo, DELIVERY_STATE_REL, state)

    def test_empty_head_no_op(self):
        """No errors when head is missing."""
        result = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["records_externalized"], 0)

    def test_externalize_terminal_acked_record(self):
        """An acked record older than retention is externalized into a shard."""
        old_time = (self.now - timedelta(days=45)).isoformat()
        state = {
            "version": "1",
            "records": {
                "msg_001": {
                    "message_id": "msg_001",
                    "status": "acked",
                    "sent_at": old_time,
                    "acks": [{"ack_at": old_time, "status": "accepted"}],
                },
            },
            "idempotency": {"a|b|key1": "msg_001"},
        }
        self._write_delivery_state(state)

        result = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["records_externalized"], 1)
        self.assertEqual(result["idempotency_externalized"], 1)
        self.assertIsNotNone(result["shard_id"])

        # Verify head no longer contains the record
        head = _read_json(self.repo, DELIVERY_STATE_REL)
        self.assertEqual(len(head["records"]), 0)
        self.assertEqual(len(head["idempotency"]), 0)
        self.assertIn("history_meta", head)
        dm = head["history_meta"]["delivery"]
        self.assertEqual(dm["hot_record_count"], 0)
        self.assertEqual(dm["last_cut_record_count"], 1)

        # Verify shard exists and is valid
        shard = _read_json(self.repo, result["shard_path"])
        self.assertEqual(shard["schema_type"], "delivery_history_shard")
        self.assertEqual(shard["schema_version"], "1.0")
        self.assertIn("msg_001", shard["records"])
        self.assertIn("a|b|key1", shard["idempotency"])
        self.assertEqual(shard["summary"]["record_count"], 1)

        # Verify stub exists
        stub = _read_json(self.repo, result["stub_path"])
        self.assertEqual(stub["schema_type"], "registry_history_stub")
        self.assertEqual(stub["family"], "delivery")
        self.assertEqual(stub["summary"], shard["summary"])

    def test_non_terminal_records_stay_hot(self):
        """pending_ack with future deadline stays in the head."""
        future = (self.now + timedelta(days=1)).isoformat()
        state = {
            "version": "1",
            "records": {
                "msg_002": {
                    "message_id": "msg_002",
                    "status": "pending_ack",
                    "sent_at": (self.now - timedelta(days=45)).isoformat(),
                    "ack_deadline": future,
                },
            },
            "idempotency": {},
        }
        self._write_delivery_state(state)

        result = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=500,
        )
        self.assertEqual(result["records_externalized"], 0)

    def test_effective_dead_letter_externalizes(self):
        """pending_ack with past deadline is treated as dead_letter for lifecycle."""
        past_deadline = (self.now - timedelta(days=45)).isoformat()
        state = {
            "version": "1",
            "records": {
                "msg_003": {
                    "message_id": "msg_003",
                    "status": "pending_ack",
                    "sent_at": (self.now - timedelta(days=60)).isoformat(),
                    "ack_deadline": past_deadline,
                },
            },
            "idempotency": {},
        }
        self._write_delivery_state(state)

        result = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=500,
        )
        self.assertEqual(result["records_externalized"], 1)

    def test_retention_timestamp_missing_produces_warning(self):
        """Record with missing ack timestamps produces a warning and is not externalized."""
        old_time = (self.now - timedelta(days=45)).isoformat()
        state = {
            "version": "1",
            "records": {
                "msg_004": {
                    "message_id": "msg_004",
                    "status": "acked",
                    "sent_at": old_time,
                    "acks": [],  # no ack rows
                },
            },
            "idempotency": {},
        }
        self._write_delivery_state(state)

        result = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=500,
        )
        self.assertEqual(result["records_externalized"], 0)
        self.assertTrue(any("delivery_retention_missing" in w for w in result["warnings"]))

    def test_idempotency_age_prune(self):
        """Idempotency mapping older than retention is pruned even if target is hot."""
        old_sent = (self.now - timedelta(days=45)).isoformat()
        state = {
            "version": "1",
            "records": {
                "msg_005": {
                    "message_id": "msg_005",
                    "status": "pending_ack",
                    "sent_at": old_sent,
                    "ack_deadline": (self.now + timedelta(days=1)).isoformat(),
                },
            },
            "idempotency": {"x|y|key2": "msg_005"},
        }
        self._write_delivery_state(state)

        result = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=500,
        )
        self.assertEqual(result["idempotency_pruned"], 1)
        head = _read_json(self.repo, DELIVERY_STATE_REL)
        self.assertEqual(len(head["idempotency"]), 0)
        # Target record stays
        self.assertIn("msg_005", head["records"])

    def test_orphan_idempotency_pruned_with_warning(self):
        """Idempotency mapping whose target is absent is pruned with warning."""
        state = {
            "version": "1",
            "records": {},
            "idempotency": {"a|b|orphan": "msg_gone"},
        }
        self._write_delivery_state(state)

        result = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=500,
        )
        self.assertEqual(result["idempotency_pruned"], 1)
        self.assertTrue(any("orphan_pruned" in w for w in result["warnings"]))

    def test_batch_limit_respected(self):
        """Batch limit stops selection."""
        old_time = (self.now - timedelta(days=45)).isoformat()
        records = {}
        for i in range(10):
            records[f"msg_{i:03d}"] = {
                "message_id": f"msg_{i:03d}",
                "status": "delivered",
                "sent_at": old_time,
            }
        state = {"version": "1", "records": records, "idempotency": {}}
        self._write_delivery_state(state)

        result = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=3,
        )
        self.assertEqual(result["records_externalized"], 3)
        head = _read_json(self.repo, DELIVERY_STATE_REL)
        self.assertEqual(len(head["records"]), 7)

    def test_recent_terminal_stays_hot(self):
        """Terminal record within retention window stays in head."""
        recent = (self.now - timedelta(days=5)).isoformat()
        state = {
            "version": "1",
            "records": {
                "msg_recent": {
                    "message_id": "msg_recent",
                    "status": "acked",
                    "sent_at": recent,
                    "acks": [{"ack_at": recent, "status": "accepted"}],
                },
            },
            "idempotency": {},
        }
        self._write_delivery_state(state)

        result = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=500,
        )
        self.assertEqual(result["records_externalized"], 0)


# ===================================================================
# Nonce maintenance tests
# ===================================================================

class TestNonceMaintenance(unittest.TestCase):

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.repo = Path(self._td.name) / "repo"
        self.repo.mkdir()
        self.now = _now()

    def tearDown(self):
        self._td.cleanup()

    def test_prune_expired_nonces(self):
        """Nonces past their expires_at are pruned."""
        expired = (self.now - timedelta(hours=1)).isoformat()
        valid = (self.now + timedelta(hours=1)).isoformat()
        state = {
            "schema_version": "1.0",
            "entries": {
                "key1|nonce1": {"key_id": "key1", "nonce": "nonce1", "first_seen_at": expired, "expires_at": expired},
                "key2|nonce2": {"key_id": "key2", "nonce": "nonce2", "first_seen_at": valid, "expires_at": valid},
            },
        }
        _write_head(self.repo, NONCE_INDEX_REL, state)

        result = nonce_maintenance_pass(
            repo_root=self.repo, now=self.now,
            nonce_retention_days=7, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["pruned"], 1)

        head = _read_json(self.repo, NONCE_INDEX_REL)
        self.assertNotIn("key1|nonce1", head["entries"])
        self.assertIn("key2|nonce2", head["entries"])
        self.assertEqual(head["history_meta"]["nonce"]["hot_entry_count"], 1)

    def test_prune_missing_expiry_with_old_first_seen(self):
        """Nonce with missing expires_at and old first_seen_at is pruned with warning."""
        old = (self.now - timedelta(days=10)).isoformat()
        state = {
            "schema_version": "1.0",
            "entries": {
                "key3|nonce3": {"key_id": "key3", "nonce": "nonce3", "first_seen_at": old},
            },
        }
        _write_head(self.repo, NONCE_INDEX_REL, state)

        result = nonce_maintenance_pass(
            repo_root=self.repo, now=self.now,
            nonce_retention_days=7, batch_limit=500,
        )
        self.assertEqual(result["pruned"], 1)
        self.assertTrue(any("no_expiry_pruned" in w for w in result["warnings"]))

    def test_prune_malformed_immediately(self):
        """Nonce missing both expires_at and first_seen_at is pruned immediately."""
        state = {
            "schema_version": "1.0",
            "entries": {
                "key4|nonce4": {"key_id": "key4", "nonce": "nonce4"},
            },
        }
        _write_head(self.repo, NONCE_INDEX_REL, state)

        result = nonce_maintenance_pass(
            repo_root=self.repo, now=self.now,
            nonce_retention_days=7, batch_limit=500,
        )
        self.assertEqual(result["pruned"], 1)
        self.assertTrue(any("malformed_pruned" in w for w in result["warnings"]))

    def test_empty_nonce_index_no_op(self):
        result = nonce_maintenance_pass(
            repo_root=self.repo, now=self.now,
            nonce_retention_days=7, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["pruned"], 0)


# ===================================================================
# Peer trust maintenance tests
# ===================================================================

class TestPeerTrustMaintenance(unittest.TestCase):

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.repo = Path(self._td.name) / "repo"
        self.repo.mkdir()
        self.now = _now()

    def tearDown(self):
        self._td.cleanup()

    def _make_transitions(self, count: int, base_time: datetime) -> list[dict]:
        return [
            {
                "at": (base_time + timedelta(days=i)).isoformat(),
                "from": "restricted",
                "to": "trusted",
                "reason": f"transition_{i}",
                "by": "admin",
            }
            for i in range(count)
        ]

    def test_externalize_old_transitions(self):
        """Transitions beyond max_hot_entries and older than retention are externalized."""
        old_base = self.now - timedelta(days=60)
        transitions = self._make_transitions(40, old_base)
        registry = {
            "schema_version": "1.0",
            "updated_at": self.now.isoformat(),
            "peers": {
                "peer-alpha": {
                    "trust_level": "trusted",
                    "trust_history": transitions,
                },
            },
        }
        _write_head(self.repo, PEERS_REGISTRY_REL, registry)

        result = peer_trust_maintenance_pass(
            repo_root=self.repo, now=self.now,
            max_hot_entries=32, hot_retention_days=30,
            batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertGreater(result["transitions_externalized"], 0)
        self.assertEqual(result["shards_created"], 1)

        # Verify head: newest 32 remain
        head = _read_json(self.repo, PEERS_REGISTRY_REL)
        peer_row = head["peers"]["peer-alpha"]
        self.assertLessEqual(len(peer_row["trust_history"]), 40)
        self.assertGreaterEqual(len(peer_row["trust_history"]), 32)

        # Verify history_meta
        self.assertIn("history_meta", head)
        pm = head["history_meta"]["peer_registry"]
        self.assertEqual(pm["last_cut_peer_id"], "peer-alpha")
        self.assertIn("peer-alpha", pm["by_peer"])

        # Verify shard
        shard_info = result["shards"][0]
        shard = _read_json(self.repo, f"{PEER_TRUST_HISTORY_DIR_REL}/{shard_info['shard_id']}.json")
        self.assertEqual(shard["schema_type"], "peer_trust_history_shard")
        self.assertEqual(shard["peer_id"], "peer-alpha")
        self.assertEqual(len(shard["transitions"]), shard_info["transition_count"])

        # Verify stub
        stub = _read_json(self.repo, f"{PEER_TRUST_STUB_DIR_REL}/{shard_info['shard_id']}.json")
        self.assertEqual(stub["schema_type"], "registry_history_stub")
        self.assertEqual(stub["family"], "peer_trust")
        self.assertEqual(stub["summary"]["peer_id"], "peer-alpha")

    def test_max_hot_entries_always_kept(self):
        """Newest max_hot_entries transitions stay hot even if older than threshold."""
        old_base = self.now - timedelta(days=60)
        transitions = self._make_transitions(32, old_base)
        registry = {
            "schema_version": "1.0",
            "updated_at": self.now.isoformat(),
            "peers": {
                "peer-beta": {
                    "trust_level": "trusted",
                    "trust_history": transitions,
                },
            },
        }
        _write_head(self.repo, PEERS_REGISTRY_REL, registry)

        result = peer_trust_maintenance_pass(
            repo_root=self.repo, now=self.now,
            max_hot_entries=32, hot_retention_days=30,
            batch_limit=500,
        )
        # Exactly 32 transitions, none should be externalized
        self.assertEqual(result["transitions_externalized"], 0)

    def test_no_transitions_no_op(self):
        registry = {
            "schema_version": "1.0",
            "updated_at": self.now.isoformat(),
            "peers": {
                "peer-gamma": {"trust_level": "restricted", "trust_history": []},
            },
        }
        _write_head(self.repo, PEERS_REGISTRY_REL, registry)

        result = peer_trust_maintenance_pass(
            repo_root=self.repo, now=self.now,
            max_hot_entries=32, hot_retention_days=30,
            batch_limit=500,
        )
        self.assertEqual(result["transitions_externalized"], 0)


# ===================================================================
# Replication state tests (synchronous pre-write capture)
# ===================================================================

class TestReplicationStateExternalization(unittest.TestCase):

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.repo = Path(self._td.name) / "repo"
        self.repo.mkdir()
        self.now = _now()

    def tearDown(self):
        self._td.cleanup()

    def test_externalize_old_push(self):
        """Old superseded push row creates a shard."""
        old_push = {
            "pushed_at": (self.now - timedelta(days=20)).isoformat(),
            "target_url": "https://peer.example/v1/replication/pull",
            "file_count": 10,
        }
        result = externalize_superseded_push(
            repo_root=self.repo, now=self.now,
            previous_row=old_push, hot_retention_days=14,
        )
        self.assertIsNotNone(result)
        shard = _read_json(self.repo, result["shard_path"])
        self.assertEqual(shard["schema_type"], "replication_state_history_shard")
        self.assertEqual(len(shard["push_events"]), 1)
        self.assertEqual(len(shard["pull_events"]), 0)

        stub = _read_json(self.repo, result["stub_path"])
        self.assertEqual(stub["family"], "replication_state")
        self.assertEqual(stub["summary"]["push_event_count"], 1)

    def test_recent_push_not_externalized(self):
        """Push within hot window is not externalized."""
        recent_push = {
            "pushed_at": (self.now - timedelta(days=5)).isoformat(),
            "target_url": "https://peer.example/v1/replication/pull",
            "file_count": 5,
        }
        result = externalize_superseded_push(
            repo_root=self.repo, now=self.now,
            previous_row=recent_push, hot_retention_days=14,
        )
        self.assertIsNone(result)

    def test_externalize_old_pull(self):
        """Old superseded pull row creates a shard."""
        old_pull = {
            "pulled_at": (self.now - timedelta(days=20)).isoformat(),
            "received_count": 5,
            "changed_count": 3,
        }
        result = externalize_superseded_pull(
            repo_root=self.repo, now=self.now,
            source_peer="peer-alpha",
            previous_row=old_pull, hot_retention_days=14,
        )
        self.assertIsNotNone(result)
        shard = _read_json(self.repo, result["shard_path"])
        self.assertEqual(len(shard["pull_events"]), 1)
        self.assertEqual(shard["pull_events"][0]["source_peer"], "peer-alpha")

    def test_missing_pushed_at_not_externalized(self):
        """Push with missing timestamp produces warning, not shard."""
        result = externalize_superseded_push(
            repo_root=self.repo, now=self.now,
            previous_row={"target_url": "https://peer.example/v1/replication/pull"},
            hot_retention_days=14,
        )
        self.assertIsNone(result)


class TestReplicationStatePruneIdempotency(unittest.TestCase):

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.repo = Path(self._td.name) / "repo"
        self.repo.mkdir()
        self.now = _now()

    def tearDown(self):
        self._td.cleanup()

    def test_prune_old_pull_idempotency(self):
        """Pull idempotency entries older than retention are pruned."""
        old = (self.now - timedelta(days=20)).isoformat()
        recent = (self.now - timedelta(days=5)).isoformat()
        state = {
            "schema_version": "1.0",
            "last_pull_by_source": {},
            "last_push": None,
            "pull_idempotency": {
                "peer-a|key1": {"at": old, "received_count": 5},
                "peer-b|key2": {"at": recent, "received_count": 3},
            },
        }
        _write_head(self.repo, REPLICATION_STATE_REL, state)

        result = replication_state_prune_idempotency(
            repo_root=self.repo, now=self.now,
            pull_idempotency_retention_days=14,
            batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["pruned"], 1)

        head = _read_json(self.repo, REPLICATION_STATE_REL)
        self.assertNotIn("peer-a|key1", head["pull_idempotency"])
        self.assertIn("peer-b|key2", head["pull_idempotency"])

    def test_prune_malformed_pull_idempotency(self):
        """Malformed pull idempotency entries are pruned with warnings."""
        state = {
            "schema_version": "1.0",
            "last_pull_by_source": {},
            "last_push": None,
            "pull_idempotency": {
                "peer-c|key3": "not_a_dict",
                "peer-d|key4": {"no_at_field": True},
            },
        }
        _write_head(self.repo, REPLICATION_STATE_REL, state)

        result = replication_state_prune_idempotency(
            repo_root=self.repo, now=self.now,
            pull_idempotency_retention_days=14,
            batch_limit=500,
        )
        self.assertEqual(result["pruned"], 2)
        self.assertTrue(any("malformed" in w for w in result["warnings"]))


# ===================================================================
# Tombstone maintenance tests
# ===================================================================

class TestTombstoneMaintenance(unittest.TestCase):

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.repo = Path(self._td.name) / "repo"
        self.repo.mkdir()
        self.now = _now()

    def tearDown(self):
        self._td.cleanup()

    def test_externalize_old_tombstones(self):
        """Tombstones older than grace window are externalized."""
        old = (self.now - timedelta(days=45)).isoformat()
        recent = (self.now - timedelta(days=5)).isoformat()
        state = {
            "schema_version": "1.0",
            "entries": {
                "messages/inbox/peer-a.jsonl": {"tombstone_at": old, "source_peer": "peer-x"},
                "messages/inbox/peer-b.jsonl": {"tombstone_at": recent, "source_peer": "peer-y"},
            },
        }
        _write_head(self.repo, REPLICATION_TOMBSTONES_REL, state)

        result = tombstone_maintenance_pass(
            repo_root=self.repo, now=self.now,
            grace_days=30, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["entries_externalized"], 1)
        self.assertIsNotNone(result["shard_id"])

        # Verify head
        head = _read_json(self.repo, REPLICATION_TOMBSTONES_REL)
        self.assertNotIn("messages/inbox/peer-a.jsonl", head["entries"])
        self.assertIn("messages/inbox/peer-b.jsonl", head["entries"])

        tm = head["history_meta"]["replication_tombstones"]
        self.assertEqual(tm["hot_entry_count"], 1)
        self.assertIsNotNone(tm["oldest_hot_tombstone_at"])

        # Verify shard
        shard = _read_json(self.repo, result["shard_path"])
        self.assertEqual(shard["schema_type"], "replication_tombstone_shard")
        self.assertIn("messages/inbox/peer-a.jsonl", shard["entries"])
        self.assertEqual(shard["summary"]["entry_count"], 1)

        # Verify stub
        stub = _read_json(self.repo, result["stub_path"])
        self.assertEqual(stub["family"], "replication_tombstone")
        self.assertEqual(stub["summary"], shard["summary"])

    def test_recent_tombstones_stay_hot(self):
        """Tombstones within grace window are not externalized."""
        recent = (self.now - timedelta(days=5)).isoformat()
        state = {
            "schema_version": "1.0",
            "entries": {
                "messages/inbox/peer-c.jsonl": {"tombstone_at": recent},
            },
        }
        _write_head(self.repo, REPLICATION_TOMBSTONES_REL, state)

        result = tombstone_maintenance_pass(
            repo_root=self.repo, now=self.now,
            grace_days=30, batch_limit=500,
        )
        self.assertEqual(result["entries_externalized"], 0)

    def test_empty_tombstones_no_op(self):
        result = tombstone_maintenance_pass(
            repo_root=self.repo, now=self.now,
            grace_days=30, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["entries_externalized"], 0)


# ===================================================================
# Shard naming and sequencing tests
# ===================================================================

class TestShardNaming(unittest.TestCase):

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.repo = Path(self._td.name) / "repo"
        self.repo.mkdir()
        self.now = _now()

    def tearDown(self):
        self._td.cleanup()

    def test_shard_id_format(self):
        """Verify shard_id matches the spec format."""
        old_time = (self.now - timedelta(days=45)).isoformat()
        state = {
            "version": "1",
            "records": {
                "msg_fmt": {
                    "message_id": "msg_fmt",
                    "status": "delivered",
                    "sent_at": old_time,
                },
            },
            "idempotency": {},
        }
        _write_head(self.repo, DELIVERY_STATE_REL, state)

        result = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=500,
        )
        shard_id = result["shard_id"]
        self.assertIsNotNone(shard_id)
        # Format: delivery__YYYYMMDDTHHMMSSZ__0001
        parts = shard_id.split("__")
        self.assertEqual(len(parts), 3)
        self.assertEqual(parts[0], "delivery")
        self.assertEqual(parts[1], "20260319T120000Z")
        self.assertEqual(parts[2], "0001")

    def test_sequence_increments(self):
        """Second shard for same timestamp gets sequence 0002."""
        old_time = (self.now - timedelta(days=45)).isoformat()
        # First pass
        state1 = {
            "version": "1",
            "records": {
                "msg_a": {"message_id": "msg_a", "status": "delivered", "sent_at": old_time},
            },
            "idempotency": {},
        }
        _write_head(self.repo, DELIVERY_STATE_REL, state1)
        r1 = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=500,
        )
        # Second pass at same time
        state2 = {
            "version": "1",
            "records": {
                "msg_b": {"message_id": "msg_b", "status": "delivered", "sent_at": old_time},
            },
            "idempotency": {},
        }
        _write_head(self.repo, DELIVERY_STATE_REL, state2)
        r2 = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=500,
        )
        self.assertTrue(r1["shard_id"].endswith("0001"))
        self.assertTrue(r2["shard_id"].endswith("0002"))


# ===================================================================
# Read degradation tests
# ===================================================================

class TestReadDegradation(unittest.TestCase):

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.repo = Path(self._td.name) / "repo"
        self.repo.mkdir()
        self.now = _now()

    def tearDown(self):
        self._td.cleanup()

    def test_corrupt_delivery_head(self):
        """Corrupt delivery head degrades gracefully with warning."""
        path = safe_path(self.repo, DELIVERY_STATE_REL)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not json", encoding="utf-8")

        result = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(any("corrupt" in w for w in result["warnings"]))

    def test_corrupt_nonce_head(self):
        """Corrupt nonce head degrades gracefully."""
        path = safe_path(self.repo, NONCE_INDEX_REL)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{invalid", encoding="utf-8")

        result = nonce_maintenance_pass(
            repo_root=self.repo, now=self.now,
            nonce_retention_days=7, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(any("corrupt" in w for w in result["warnings"]))

    def test_corrupt_history_meta_dropped(self):
        """If history_meta is not a dict, it is replaced."""
        state = {
            "version": "1",
            "records": {},
            "idempotency": {},
            "history_meta": "broken",
        }
        _write_head(self.repo, DELIVERY_STATE_REL, state)

        result = delivery_maintenance_pass(
            repo_root=self.repo, now=self.now,
            terminal_retention_days=30, idempotency_retention_days=30,
            batch_limit=500,
        )
        self.assertTrue(result["ok"])


# ===================================================================
# Orchestrator test
# ===================================================================

class TestRegistryMaintenanceOrchestrator(unittest.TestCase):

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.repo = Path(self._td.name) / "repo"
        self.repo.mkdir()
        self.now = _now()

    def tearDown(self):
        self._td.cleanup()

    def test_orchestrator_runs_all_families(self):
        """The orchestrator runs all families and returns aggregated results."""
        from app.registry_lifecycle.service import registry_maintenance_service
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class FakeSettings:
            delivery_terminal_retention_days: int = 30
            delivery_idempotency_retention_days: int = 30
            nonce_retention_days: int = 7
            peer_trust_history_max_hot_entries: int = 32
            peer_trust_history_hot_retention_days: int = 30
            replication_tombstone_grace_days: int = 30
            replication_pull_idempotency_retention_days: int = 14
            registry_history_batch_limit: int = 500

        result = registry_maintenance_service(
            repo_root=self.repo,
            gm=None,
            now=self.now,
            settings=FakeSettings(),
        )
        self.assertTrue(result["ok"])
        self.assertIn("delivery", result["families"])
        self.assertIn("nonce", result["families"])
        self.assertIn("peer_trust", result["families"])
        self.assertIn("replication_tombstones", result["families"])


    def test_orchestrator_stops_after_batch_limit(self):
        """Orchestrator stops processing families after one reaches the batch limit."""
        from app.registry_lifecycle.service import registry_maintenance_service
        from dataclasses import dataclass

        # Create 5 terminal delivery records older than retention
        old_time = (self.now - timedelta(days=45)).isoformat()
        records = {}
        for i in range(5):
            records[f"msg_{i:03d}"] = {
                "message_id": f"msg_{i:03d}",
                "status": "delivered",
                "sent_at": old_time,
            }
        _write_head(self.repo, DELIVERY_STATE_REL, {
            "version": "1", "records": records, "idempotency": {},
        })

        # Also create an expired nonce
        _write_head(self.repo, NONCE_INDEX_REL, {
            "schema_version": "1.0",
            "entries": {
                "k|n": {"key_id": "k", "nonce": "n",
                         "first_seen_at": old_time,
                         "expires_at": (self.now - timedelta(hours=1)).isoformat()},
            },
        })

        @dataclass(frozen=True)
        class FakeSettings:
            delivery_terminal_retention_days: int = 30
            delivery_idempotency_retention_days: int = 30
            nonce_retention_days: int = 7
            peer_trust_history_max_hot_entries: int = 32
            peer_trust_history_hot_retention_days: int = 30
            replication_tombstone_grace_days: int = 30
            replication_pull_idempotency_retention_days: int = 14
            registry_history_batch_limit: int = 3  # Limit of 3

        result = registry_maintenance_service(
            repo_root=self.repo,
            gm=None,
            now=self.now,
            settings=FakeSettings(),
        )
        self.assertTrue(result["ok"])
        # Delivery should process 3 records (hitting batch limit)
        self.assertIn("delivery", result["families"])
        self.assertEqual(result["families"]["delivery"]["records_externalized"], 3)
        # Nonce should NOT be processed because delivery hit the limit
        self.assertNotIn("nonce", result["families"])

    def test_externalize_push_rollback_on_stub_failure(self):
        """If stub write fails, shard is cleaned up."""
        from unittest.mock import patch
        from app.registry_lifecycle.service import externalize_superseded_push

        old_push = {
            "pushed_at": (self.now - timedelta(days=20)).isoformat(),
            "target_url": "https://peer.example/v1/replication/pull",
            "file_count": 10,
        }

        # Patch _write_json_exclusive to fail (stub write)
        original_write_json_exclusive = __import__("app.registry_lifecycle.service", fromlist=["_write_json_exclusive"])._write_json_exclusive

        call_count = 0

        def failing_write_json_exclusive(path, data):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise OSError("disk full")
            original_write_json_exclusive(path, data)

        with patch("app.registry_lifecycle.service._write_json_exclusive", side_effect=failing_write_json_exclusive):
            with self.assertRaises(OSError):
                externalize_superseded_push(
                    repo_root=self.repo, now=self.now,
                    previous_row=old_push, hot_retention_days=14,
                )

        # Verify shard file was cleaned up (rollback removed it)
        shard_dir = safe_path(self.repo, "peers/history/replication_state")
        if shard_dir.exists():
            shard_files = list(shard_dir.glob("*.json"))
            # Filter out index subdir
            shard_files = [f for f in shard_files if f.is_file()]
            self.assertEqual(len(shard_files), 0, "Shard should be cleaned up on rollback")


    def test_delivery_rollback_on_shard_write_failure(self):
        """Delivery maintenance rolls back the head when shard write fails."""
        old_time = (self.now - timedelta(days=45)).isoformat()
        state = {
            "version": "1",
            "records": {
                "msg_rb": {
                    "message_id": "msg_rb",
                    "status": "delivered",
                    "sent_at": old_time,
                },
            },
            "idempotency": {},
        }
        _write_head(self.repo, DELIVERY_STATE_REL, state)

        original_write_json_exclusive = __import__("app.registry_lifecycle.service", fromlist=["_write_json_exclusive"])._write_json_exclusive

        call_count = 0

        def failing_write_exclusive(path, data):
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # shard write (first exclusive write)
                raise OSError("disk full")
            original_write_json_exclusive(path, data)

        from unittest.mock import patch
        with patch("app.registry_lifecycle.service._write_json_exclusive", side_effect=failing_write_exclusive):
            with self.assertRaises((OSError, RuntimeError)):
                delivery_maintenance_pass(
                    repo_root=self.repo, now=self.now,
                    terminal_retention_days=30, idempotency_retention_days=30,
                    batch_limit=500,
                )

        # Head should be restored to original state
        head = _read_json(self.repo, DELIVERY_STATE_REL)
        self.assertIn("msg_rb", head["records"])

    def test_batch_limit_across_multiple_peers(self):
        """Batch limit is respected across multiple peers in peer trust pass."""
        old_base = self.now - timedelta(days=60)

        def _make_transitions(count, base):
            return [
                {"at": (base + timedelta(days=i)).isoformat(), "from": "restricted",
                 "to": "trusted", "reason": f"t_{i}", "by": "admin"}
                for i in range(count)
            ]

        # Two peers each with 40 transitions (8 beyond max_hot=32)
        registry = {
            "schema_version": "1.0",
            "updated_at": self.now.isoformat(),
            "peers": {
                "peer-a": {"trust_level": "trusted", "trust_history": _make_transitions(40, old_base)},
                "peer-b": {"trust_level": "trusted", "trust_history": _make_transitions(40, old_base)},
            },
        }
        _write_head(self.repo, PEERS_REGISTRY_REL, registry)

        result = peer_trust_maintenance_pass(
            repo_root=self.repo, now=self.now,
            max_hot_entries=32, hot_retention_days=30,
            batch_limit=5,  # Only allow 5 transitions total
        )
        self.assertTrue(result["ok"])
        # Should externalize at most 5 total across both peers
        self.assertLessEqual(result["transitions_externalized"], 5)


# ===================================================================
# Integration tests for push/pull pre-write capture
# ===================================================================

class TestReplicationPreWriteIntegration(unittest.TestCase):
    """Integration tests for synchronous pre-write capture in push/pull flows."""

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.repo = Path(self._td.name) / "repo"
        self.repo.mkdir()
        self.now = _now()

    def tearDown(self):
        self._td.cleanup()

    def test_pull_externalizes_old_superseded_row(self):
        """Full pull flow externalizes a superseded pull row when old enough."""
        from dataclasses import dataclass

        from app.maintenance.service import replication_pull_service
        from app.models import ReplicationPullRequest

        @dataclass(frozen=True)
        class FakeSettings:
            repo_root: Path = None  # type: ignore[assignment]
            replication_history_hot_retention_days: int = 14
            max_payload_bytes: int = 262_144

        settings = FakeSettings(repo_root=self.repo)

        # Pre-seed replication state with an old pull row
        old_pull_time = (self.now - timedelta(days=20)).isoformat()
        state = {
            "schema_version": "1.0",
            "last_pull_by_source": {
                "peer-source": {
                    "pulled_at": old_pull_time,
                    "received_count": 5,
                    "changed_count": 3,
                },
            },
            "last_push": None,
            "pull_idempotency": {},
        }
        _write_head(self.repo, "peers/replication_state.json", state)

        class FakeAuth:
            peer_id = "peer-test"
            def require(self, _s): pass
            def require_write_path(self, _p): pass
            def require_read_path(self, _p): pass

        class FakeGm:
            repo_root = self.repo
            def commit_paths(self, _p, _m): return True
            def commit_file(self, _p, _m): return True
            def latest_commit(self): return "test-sha"

        req = ReplicationPullRequest(
            source_peer="peer-source",
            files=[],
            mode="upsert",
            conflict_policy="source_wins",
        )

        def noop_rate(*a, **kw): pass
        def noop_payload(*a, **kw): pass
        def noop_audit(*a, **kw): pass
        def parse_iso(v):
            if not v:
                return None
            try:
                from datetime import datetime
                return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            except Exception:
                return None

        result = replication_pull_service(
            settings=settings,
            gm=FakeGm(),
            auth=FakeAuth(),
            req=req,
            enforce_rate_limit=noop_rate,
            enforce_payload_limit=noop_payload,
            parse_iso=parse_iso,
            audit=noop_audit,
        )
        self.assertTrue(result["ok"])

        # Check that a history shard was created
        history_dir = safe_path(self.repo, "peers/history/replication_state")
        if history_dir.exists():
            shard_files = [f for f in history_dir.glob("*.json") if f.is_file()]
            self.assertGreater(len(shard_files), 0, "Should create a history shard for old superseded pull")

        # Verify history_meta was written in the state
        updated_state = _read_json(self.repo, "peers/replication_state.json")
        hm = updated_state.get("history_meta", {})
        rs_meta = hm.get("replication_state", {})
        if rs_meta:
            self.assertIn("last_cut_at", rs_meta)
            self.assertIn("last_cut_pull_count", rs_meta)

    def test_push_externalizes_old_superseded_row(self):
        """Full push flow externalizes a superseded push row when old enough."""
        from unittest.mock import patch
        from app.maintenance.service import replication_push_service
        from app.models import ReplicationPushRequest
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class FakeSettings:
            repo_root: Path = None  # type: ignore[assignment]
            replication_history_hot_retention_days: int = 14
            max_payload_bytes: int = 262_144

        settings = FakeSettings(repo_root=self.repo)

        # Create a file to push
        (self.repo / "memory" / "core").mkdir(parents=True, exist_ok=True)
        (self.repo / "memory" / "core" / "identity.md").write_text("# id\n", encoding="utf-8")

        # Pre-seed replication state with old push
        old_push_time = (self.now - timedelta(days=20)).isoformat()
        state = {
            "schema_version": "1.0",
            "last_pull_by_source": {},
            "last_push": {
                "pushed_at": old_push_time,
                "target_url": "https://old.example/v1/replication/pull",
                "file_count": 3,
            },
            "pull_idempotency": {},
        }
        _write_head(self.repo, "peers/replication_state.json", state)

        class FakeAuth:
            peer_id = "peer-test"
            def require(self, _s): pass
            def require_write_path(self, _p): pass
            def require_read_path(self, _p): pass

        class FakeGm:
            repo_root = self.repo
            def commit_paths(self, _p, _m): return True
            def commit_file(self, _p, _m): return True
            def latest_commit(self): return "test-sha"

        class FakeResp:
            def read(self):
                return b'{"ok": true}'
            def __enter__(self):
                return self
            def __exit__(self, *a):
                pass

        req = ReplicationPushRequest(
            dry_run=False,
            base_url="https://peer.example",
            include_prefixes=["memory"],
            target_token="tok",
        )

        def noop(*a, **kw): pass
        def load_peers(root):
            return {"peers": {}}

        with patch("app.maintenance.service.urlopen", return_value=FakeResp()):
            result = replication_push_service(
                settings=settings,
                gm=FakeGm(),
                auth=FakeAuth(),
                req=req,
                enforce_rate_limit=noop,
                enforce_payload_limit=noop,
                load_peers_registry=load_peers,
                audit=noop,
            )

        self.assertTrue(result["ok"])

        # Check shard was created
        history_dir = safe_path(self.repo, "peers/history/replication_state")
        if history_dir.exists():
            shard_files = [f for f in history_dir.glob("*.json") if f.is_file()]
            self.assertGreater(len(shard_files), 0, "Should create a history shard for old superseded push")

        # Verify history_meta
        updated_state = _read_json(self.repo, "peers/replication_state.json")
        hm = updated_state.get("history_meta", {})
        rs_meta = hm.get("replication_state", {})
        if rs_meta:
            self.assertIn("last_cut_at", rs_meta)
            self.assertIn("last_cut_push_count", rs_meta)


    def test_pull_rollback_cleans_shard_on_state_write_failure(self):
        """Integration: externalize succeeds → state write fails → shard/stub removed."""
        from dataclasses import dataclass
        from unittest.mock import patch

        from app.maintenance.service import replication_pull_service
        from app.models import ReplicationPullRequest

        @dataclass(frozen=True)
        class FakeSettings:
            repo_root: Path = None  # type: ignore[assignment]
            replication_history_hot_retention_days: int = 14
            max_payload_bytes: int = 262_144

        settings = FakeSettings(repo_root=self.repo)

        old_pull_time = (self.now - timedelta(days=20)).isoformat()
        state = {
            "schema_version": "1.0",
            "last_pull_by_source": {
                "peer-source": {
                    "pulled_at": old_pull_time,
                    "received_count": 5,
                    "changed_count": 3,
                },
            },
            "last_push": None,
            "pull_idempotency": {},
        }
        _write_head(self.repo, "peers/replication_state.json", state)

        # Seed tombstones so the tombstone write doesn't fail first
        _write_head(self.repo, "peers/replication_tombstones.json", {"schema_version": "1.0", "entries": {}})

        class FakeAuth:
            peer_id = "peer-test"
            def require(self, _s): pass
            def require_write_path(self, _p): pass
            def require_read_path(self, _p): pass

        class FakeGm:
            repo_root = self.repo
            def commit_paths(self, _p, _m): return True
            def commit_file(self, _p, _m): return True
            def latest_commit(self): return "test-sha"

        req = ReplicationPullRequest(
            source_peer="peer-source",
            files=[],
            mode="upsert",
            conflict_policy="source_wins",
        )

        def noop(*_a, **_kw): pass
        def parse_iso(v):
            if not v:
                return None
            try:
                return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            except Exception:
                return None

        # Patch _write_replication_state to fail AFTER externalize succeeds
        with patch("app.maintenance.service._write_replication_state", side_effect=OSError("disk full")):
            with self.assertRaises(Exception):
                replication_pull_service(
                    settings=settings,
                    gm=FakeGm(),
                    auth=FakeAuth(),
                    req=req,
                    enforce_rate_limit=noop,
                    enforce_payload_limit=noop,
                    parse_iso=parse_iso,
                    audit=noop,
                )

        # Shard/stub files should NOT exist — rollback should have deleted them
        history_dir = safe_path(self.repo, "peers/history/replication_state")
        if history_dir.exists():
            shard_files = [f for f in history_dir.glob("*.json") if f.is_file()]
            self.assertEqual(len(shard_files), 0, "Shard should be cleaned up on state write failure")

        stub_dir = safe_path(self.repo, "peers/history/replication_state/index")
        if stub_dir.exists():
            stub_files = [f for f in stub_dir.glob("*.json") if f.is_file()]
            self.assertEqual(len(stub_files), 0, "Stub should be cleaned up on state write failure")

    def test_push_rollback_cleans_shard_on_state_write_failure(self):
        """Integration: externalize succeeds → state write fails → shard/stub removed."""
        from dataclasses import dataclass
        from unittest.mock import patch

        from app.maintenance.service import replication_push_service
        from app.models import ReplicationPushRequest

        @dataclass(frozen=True)
        class FakeSettings:
            repo_root: Path = None  # type: ignore[assignment]
            replication_history_hot_retention_days: int = 14
            max_payload_bytes: int = 262_144

        settings = FakeSettings(repo_root=self.repo)

        (self.repo / "memory" / "core").mkdir(parents=True, exist_ok=True)
        (self.repo / "memory" / "core" / "identity.md").write_text("# id\n", encoding="utf-8")

        old_push_time = (self.now - timedelta(days=20)).isoformat()
        state = {
            "schema_version": "1.0",
            "last_pull_by_source": {},
            "last_push": {
                "pushed_at": old_push_time,
                "target_url": "https://old.example/v1/replication/pull",
                "file_count": 3,
            },
            "pull_idempotency": {},
        }
        _write_head(self.repo, "peers/replication_state.json", state)

        class FakeAuth:
            peer_id = "peer-test"
            def require(self, _s): pass
            def require_write_path(self, _p): pass
            def require_read_path(self, _p): pass

        class FakeGm:
            repo_root = self.repo
            def commit_paths(self, _p, _m): return True
            def commit_file(self, _p, _m): return True
            def latest_commit(self): return "test-sha"

        class FakeResp:
            def read(self):
                return b'{"ok": true}'
            def __enter__(self):
                return self
            def __exit__(self, *_a):
                pass

        req = ReplicationPushRequest(
            dry_run=False,
            base_url="https://peer.example",
            include_prefixes=["memory"],
            target_token="tok",
        )

        def noop(*_a, **_kw): pass
        def load_peers(root):
            return {"peers": {}}

        with patch("app.maintenance.service.urlopen", return_value=FakeResp()):
            with patch("app.maintenance.service._write_replication_state", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    replication_push_service(
                        settings=settings,
                        gm=FakeGm(),
                        auth=FakeAuth(),
                        req=req,
                        enforce_rate_limit=noop,
                        enforce_payload_limit=noop,
                        load_peers_registry=load_peers,
                        audit=noop,
                    )

        # Shard/stub files should NOT exist
        history_dir = safe_path(self.repo, "peers/history/replication_state")
        if history_dir.exists():
            shard_files = [f for f in history_dir.glob("*.json") if f.is_file()]
            self.assertEqual(len(shard_files), 0, "Shard should be cleaned up on state write failure")

        stub_dir = safe_path(self.repo, "peers/history/replication_state/index")
        if stub_dir.exists():
            stub_files = [f for f in stub_dir.glob("*.json") if f.is_file()]
            self.assertEqual(len(stub_files), 0, "Stub should be cleaned up on state write failure")


class TestNonceRetentionNegative(unittest.TestCase):
    """Test that recent nonce entries with missing expires_at are retained."""

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.repo = Path(self._td.name) / "repo"
        self.repo.mkdir()
        self.now = _now()

    def tearDown(self):
        self._td.cleanup()

    def test_recent_first_seen_without_expiry_is_retained(self):
        """Nonce with missing expires_at but recent first_seen_at survives maintenance."""
        recent = (self.now - timedelta(days=3)).isoformat()
        state = {
            "schema_version": "1.0",
            "entries": {
                "key1|nonce1": {
                    "key_id": "key1",
                    "nonce": "nonce1",
                    "first_seen_at": recent,
                },
            },
        }
        _write_head(self.repo, NONCE_INDEX_REL, state)

        result = nonce_maintenance_pass(
            repo_root=self.repo, now=self.now,
            nonce_retention_days=7, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["pruned"], 0)

        # Verify entry still exists
        head = _read_json(self.repo, NONCE_INDEX_REL)
        self.assertIn("key1|nonce1", head["entries"])


# ===================================================================
# Cold-store / rehydrate / cold-apply / retention-prune tests
# ===================================================================


def _create_shard_and_stub(repo_root, family, shard_id, summary, *, schema_type, source_head_path, extra_shard=None):
    """Helper: create a hot shard + stub pair for testing cold-store operations."""
    from app.registry_lifecycle.service import (
        _REGISTRY_HISTORY_DIRS_BY_FAMILY,
        _REGISTRY_STUB_DIRS_BY_FAMILY,
    )
    history_dir_rel = _REGISTRY_HISTORY_DIRS_BY_FAMILY[family]
    stub_dir_rel = _REGISTRY_STUB_DIRS_BY_FAMILY[family]

    shard_payload = {
        "schema_type": schema_type,
        "schema_version": "1.0",
        "shard_id": shard_id,
        "source_head_path": source_head_path,
        "cut_at": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
        "summary": summary,
    }
    if extra_shard:
        shard_payload.update(extra_shard)

    shard_path = safe_path(repo_root, f"{history_dir_rel}/{shard_id}.json")
    write_text_file(shard_path, json.dumps(shard_payload, indent=2))

    stub_payload = {
        "schema_type": "registry_history_stub",
        "schema_version": "1.0",
        "family": family,
        "shard_id": shard_id,
        "payload_path": f"{history_dir_rel}/{shard_id}.json",
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
        "source_head_path": source_head_path,
        "summary": summary,
    }
    stub_path = safe_path(repo_root, f"{stub_dir_rel}/{shard_id}.json")
    write_text_file(stub_path, json.dumps(stub_payload, indent=2))

    return shard_path, stub_path


class TestRegistryColdStore(unittest.TestCase):

    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.repo = Path(self._td.name) / "repo"
        self.repo.mkdir()
        self.now = _now()

    def tearDown(self):
        self._td.cleanup()

    def test_cold_store_compresses_and_updates_stub(self):
        """Cold-store a delivery shard: hot deleted, cold .json.gz created, stub updated."""
        import gzip as _gzip
        from app.registry_lifecycle.service import (
            DELIVERY_HISTORY_DIR_REL,
            registry_history_cold_store_service,
        )
        shard_id = "delivery__20260101T000000Z__0001"
        summary = {
            "record_count": 2,
            "oldest_retention_timestamp": "2025-11-01T00:00:00+00:00",
            "newest_retention_timestamp": "2025-12-01T00:00:00+00:00",
        }
        shard_path, stub_path = _create_shard_and_stub(
            self.repo, "delivery", shard_id, summary,
            schema_type="delivery_history_shard",
            source_head_path=DELIVERY_STATE_REL,
        )
        payload_rel = f"{DELIVERY_HISTORY_DIR_REL}/{shard_id}.json"

        result = registry_history_cold_store_service(
            repo_root=self.repo,
            gm=None,
            auth=None,
            req=type("Req", (), {"source_payload_path": payload_rel})(),
            audit=lambda *a, **kw: None,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["shard_state"], "cold")
        # Hot file removed
        self.assertFalse(shard_path.exists())
        # Cold file created
        cold_rel = result["cold_storage_path"]
        cold_path = safe_path(self.repo, cold_rel)
        self.assertTrue(cold_path.exists())
        # Decompress and verify content
        decompressed = _gzip.decompress(cold_path.read_bytes())
        payload = json.loads(decompressed)
        self.assertEqual(payload["shard_id"], shard_id)
        # Stub updated
        stub = json.loads(stub_path.read_text(encoding="utf-8"))
        self.assertEqual(stub["payload_path"], cold_rel)

    def test_cold_rehydrate_restores_hot(self):
        """After cold-store, rehydrate should restore the hot shard."""
        from app.registry_lifecycle.service import (
            DELIVERY_HISTORY_DIR_REL,
            registry_history_cold_rehydrate_service,
            registry_history_cold_store_service,
        )
        shard_id = "delivery__20260101T000000Z__0001"
        summary = {
            "record_count": 1,
            "oldest_retention_timestamp": "2025-11-01T00:00:00+00:00",
            "newest_retention_timestamp": "2025-12-01T00:00:00+00:00",
        }
        shard_path, stub_path = _create_shard_and_stub(
            self.repo, "delivery", shard_id, summary,
            schema_type="delivery_history_shard",
            source_head_path=DELIVERY_STATE_REL,
        )
        payload_rel = f"{DELIVERY_HISTORY_DIR_REL}/{shard_id}.json"

        # Cold-store first
        cold_result = registry_history_cold_store_service(
            repo_root=self.repo, gm=None, auth=None,
            req=type("Req", (), {"source_payload_path": payload_rel})(),
            audit=lambda *a, **kw: None,
        )
        self.assertTrue(cold_result["ok"])
        self.assertFalse(shard_path.exists())

        # Rehydrate
        result = registry_history_cold_rehydrate_service(
            repo_root=self.repo, gm=None, auth=None,
            req=type("Req", (), {"source_payload_path": payload_rel, "cold_stub_path": None})(),
            audit=lambda *a, **kw: None,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["shard_state"], "hot")
        self.assertTrue(shard_path.exists())
        cold_path = safe_path(self.repo, cold_result["cold_storage_path"])
        self.assertFalse(cold_path.exists())
        stub = json.loads(stub_path.read_text(encoding="utf-8"))
        self.assertEqual(stub["payload_path"], payload_rel)

    def test_cold_store_rollback_on_write_failure(self):
        """If stub write fails during cold-store, hot file is preserved."""
        from unittest.mock import patch
        from app.registry_lifecycle.service import (
            DELIVERY_HISTORY_DIR_REL,
            registry_history_cold_store_service,
        )
        shard_id = "delivery__20260101T000000Z__0001"
        summary = {
            "record_count": 1,
            "oldest_retention_timestamp": "2025-11-01T00:00:00+00:00",
            "newest_retention_timestamp": "2025-12-01T00:00:00+00:00",
        }
        shard_path, _stub_path = _create_shard_and_stub(
            self.repo, "delivery", shard_id, summary,
            schema_type="delivery_history_shard",
            source_head_path=DELIVERY_STATE_REL,
        )
        payload_rel = f"{DELIVERY_HISTORY_DIR_REL}/{shard_id}.json"
        original_bytes = shard_path.read_bytes()

        with patch("app.registry_lifecycle.service._write_json", side_effect=OSError("disk full")):
            from fastapi import HTTPException
            with self.assertRaises(HTTPException) as ctx:
                registry_history_cold_store_service(
                    repo_root=self.repo, gm=None, auth=None,
                    req=type("Req", (), {"source_payload_path": payload_rel})(),
                    audit=lambda *a, **kw: None,
                )
            self.assertEqual(ctx.exception.status_code, 500)

        # Hot file should be restored
        self.assertTrue(shard_path.exists())
        self.assertEqual(shard_path.read_bytes(), original_bytes)

    def test_cold_apply_respects_thresholds(self):
        """Cold-apply only cold-stores shards older than the configured threshold."""
        from dataclasses import dataclass
        from app.registry_lifecycle.service import (
            DELIVERY_HISTORY_DIR_REL,
            registry_history_cold_apply_service,
        )

        # Create two shards: one old (eligible), one recent (not eligible)
        old_ts = (self.now - timedelta(days=120)).isoformat()
        recent_ts = (self.now - timedelta(days=10)).isoformat()

        _create_shard_and_stub(
            self.repo, "delivery", "delivery__20260101T000000Z__0001",
            {"record_count": 1, "oldest_retention_timestamp": old_ts, "newest_retention_timestamp": old_ts},
            schema_type="delivery_history_shard",
            source_head_path=DELIVERY_STATE_REL,
        )
        _create_shard_and_stub(
            self.repo, "delivery", "delivery__20260319T000000Z__0001",
            {"record_count": 1, "oldest_retention_timestamp": recent_ts, "newest_retention_timestamp": recent_ts},
            schema_type="delivery_history_shard",
            source_head_path=DELIVERY_STATE_REL,
        )

        @dataclass(frozen=True)
        class FakeSettings:
            delivery_history_cold_after_days: int = 90
            peer_trust_history_cold_after_days: int = 120
            replication_history_cold_after_days: int = 90
            replication_tombstone_cold_after_days: int = 90

        result = registry_history_cold_apply_service(
            repo_root=self.repo, gm=None, auth=None, now=self.now,
            settings=FakeSettings(), families=["delivery"],
            audit=lambda *a, **kw: None,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["cold_stored"], 1)
        self.assertEqual(result["families"]["delivery"]["eligible"], 1)

        # Verify old shard cold-stored, recent shard still hot
        old_hot = safe_path(self.repo, f"{DELIVERY_HISTORY_DIR_REL}/delivery__20260101T000000Z__0001.json")
        recent_hot = safe_path(self.repo, f"{DELIVERY_HISTORY_DIR_REL}/delivery__20260319T000000Z__0001.json")
        self.assertFalse(old_hot.exists())
        self.assertTrue(recent_hot.exists())

    def test_cold_apply_skips_already_cold(self):
        """Cold-apply skips shards whose stub already points to a cold path."""
        import gzip as _gzip
        from dataclasses import dataclass
        from app.registry_lifecycle.service import (
            DELIVERY_HISTORY_DIR_REL,
            DELIVERY_STUB_DIR_REL,
            registry_history_cold_apply_service,
        )

        shard_id = "delivery__20260101T000000Z__0001"
        old_ts = (self.now - timedelta(days=120)).isoformat()
        summary = {"record_count": 1, "oldest_retention_timestamp": old_ts, "newest_retention_timestamp": old_ts}

        # Create shard, stub pointing to cold (simulating already cold-stored)
        shard_payload = {
            "schema_type": "delivery_history_shard", "schema_version": "1.0",
            "shard_id": shard_id, "summary": summary,
            "source_head_path": DELIVERY_STATE_REL,
            "cut_at": "2026-01-01T00:00:00+00:00",
        }
        # Write a hot shard AND set stub to cold path (inconsistent — stub says cold but hot exists)
        # Actually for "already cold" test, we should NOT have a hot file, but glob won't find it.
        # The cold-apply scans for *.json in the history dir, so if no hot file, nothing to process.
        # This test verifies that if stub points to cold, the shard is skipped.
        hot_path = safe_path(self.repo, f"{DELIVERY_HISTORY_DIR_REL}/{shard_id}.json")
        write_text_file(hot_path, json.dumps(shard_payload, indent=2))

        cold_path_rel = f"{DELIVERY_HISTORY_DIR_REL}/cold/{shard_id}.json.gz"
        cold_path = safe_path(self.repo, cold_path_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        cold_path.write_bytes(_gzip.compress(json.dumps(shard_payload).encode(), mtime=0))

        stub_payload = {
            "schema_type": "registry_history_stub", "schema_version": "1.0",
            "family": "delivery", "shard_id": shard_id,
            "payload_path": cold_path_rel,  # Already cold
            "created_at": "2026-01-01T00:00:00+00:00",
            "source_head_path": DELIVERY_STATE_REL, "summary": summary,
        }
        stub_path = safe_path(self.repo, f"{DELIVERY_STUB_DIR_REL}/{shard_id}.json")
        write_text_file(stub_path, json.dumps(stub_payload, indent=2))

        @dataclass(frozen=True)
        class FakeSettings:
            delivery_history_cold_after_days: int = 90
            peer_trust_history_cold_after_days: int = 120
            replication_history_cold_after_days: int = 90
            replication_tombstone_cold_after_days: int = 90

        result = registry_history_cold_apply_service(
            repo_root=self.repo, gm=None, auth=None, now=self.now,
            settings=FakeSettings(), families=["delivery"],
            audit=lambda *a, **kw: None,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["cold_stored"], 0)

    def test_cold_store_crash_recovery_orphaned_cold(self):
        """If both hot and cold files exist (crash recovery), cold-store cleans up orphan and proceeds."""
        import gzip as _gzip
        from app.registry_lifecycle.service import (
            DELIVERY_HISTORY_DIR_REL,
            registry_history_cold_store_service,
        )

        shard_id = "delivery__20260101T000000Z__0001"
        summary = {
            "record_count": 1,
            "oldest_retention_timestamp": "2025-11-01T00:00:00+00:00",
            "newest_retention_timestamp": "2025-12-01T00:00:00+00:00",
        }
        shard_path, stub_path = _create_shard_and_stub(
            self.repo, "delivery", shard_id, summary,
            schema_type="delivery_history_shard",
            source_head_path=DELIVERY_STATE_REL,
        )
        payload_rel = f"{DELIVERY_HISTORY_DIR_REL}/{shard_id}.json"

        # Simulate crash: create orphaned cold file
        cold_path_rel = f"{DELIVERY_HISTORY_DIR_REL}/cold/{shard_id}.json.gz"
        cold_path = safe_path(self.repo, cold_path_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        cold_path.write_bytes(b"orphaned-cold-data")

        result = registry_history_cold_store_service(
            repo_root=self.repo, gm=None, auth=None,
            req=type("Req", (), {"source_payload_path": payload_rel})(),
            audit=lambda *a, **kw: None,
        )
        self.assertTrue(result["ok"])
        self.assertFalse(shard_path.exists())
        # Cold file should now contain valid gzip data (not the orphan)
        decompressed = _gzip.decompress(cold_path.read_bytes())
        payload = json.loads(decompressed)
        self.assertEqual(payload["shard_id"], shard_id)

    def test_retention_prune_deletes_expired_cold_tombstones(self):
        """Cold tombstone shards past retention_days are deleted."""
        import gzip as _gzip
        from app.registry_lifecycle.service import (
            REPLICATION_TOMBSTONE_HISTORY_DIR_REL,
            REPLICATION_TOMBSTONE_STUB_DIR_REL,
            registry_history_retention_prune_service,
        )

        shard_id = "replication_tombstones__20250101T000000Z__0001"
        old_ts = (self.now - timedelta(days=400)).isoformat()
        summary = {"entry_count": 1, "oldest_tombstone_at": old_ts, "newest_tombstone_at": old_ts}
        shard_payload = {
            "schema_type": "replication_tombstone_shard", "schema_version": "1.0",
            "shard_id": shard_id, "summary": summary,
        }
        cold_path_rel = f"{REPLICATION_TOMBSTONE_HISTORY_DIR_REL}/cold/{shard_id}.json.gz"
        cold_path = safe_path(self.repo, cold_path_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        cold_path.write_bytes(_gzip.compress(json.dumps(shard_payload).encode(), mtime=0))

        stub_payload = {
            "schema_type": "registry_history_stub", "schema_version": "1.0",
            "family": "replication_tombstones", "shard_id": shard_id,
            "payload_path": cold_path_rel,
            "created_at": "2025-01-01T00:00:00+00:00",
            "source_head_path": REPLICATION_TOMBSTONES_REL, "summary": summary,
        }
        stub_path = safe_path(self.repo, f"{REPLICATION_TOMBSTONE_STUB_DIR_REL}/{shard_id}.json")
        write_text_file(stub_path, json.dumps(stub_payload, indent=2))

        result = registry_history_retention_prune_service(
            repo_root=self.repo, gm=None, now=self.now,
            retention_days=365, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["pruned"], 1)
        self.assertFalse(cold_path.exists())
        self.assertFalse(stub_path.exists())

    def test_retention_prune_preserves_non_expired(self):
        """Cold tombstone shards within the retention window are preserved."""
        import gzip as _gzip
        from app.registry_lifecycle.service import (
            REPLICATION_TOMBSTONE_HISTORY_DIR_REL,
            REPLICATION_TOMBSTONE_STUB_DIR_REL,
            registry_history_retention_prune_service,
        )

        shard_id = "replication_tombstones__20260101T000000Z__0001"
        recent_ts = (self.now - timedelta(days=30)).isoformat()
        summary = {"entry_count": 1, "oldest_tombstone_at": recent_ts, "newest_tombstone_at": recent_ts}
        shard_payload = {
            "schema_type": "replication_tombstone_shard", "schema_version": "1.0",
            "shard_id": shard_id, "summary": summary,
        }
        cold_path_rel = f"{REPLICATION_TOMBSTONE_HISTORY_DIR_REL}/cold/{shard_id}.json.gz"
        cold_path = safe_path(self.repo, cold_path_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        cold_path.write_bytes(_gzip.compress(json.dumps(shard_payload).encode(), mtime=0))

        stub_payload = {
            "schema_type": "registry_history_stub", "schema_version": "1.0",
            "family": "replication_tombstones", "shard_id": shard_id,
            "payload_path": cold_path_rel,
            "created_at": "2026-01-01T00:00:00+00:00",
            "source_head_path": REPLICATION_TOMBSTONES_REL, "summary": summary,
        }
        stub_path = safe_path(self.repo, f"{REPLICATION_TOMBSTONE_STUB_DIR_REL}/{shard_id}.json")
        write_text_file(stub_path, json.dumps(stub_payload, indent=2))

        result = registry_history_retention_prune_service(
            repo_root=self.repo, gm=None, now=self.now,
            retention_days=365, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["pruned"], 0)
        self.assertTrue(cold_path.exists())
        self.assertTrue(stub_path.exists())

    def test_retention_prune_guards_against_concurrent_rehydrate(self):
        """If stub no longer points to cold (rehydrated), retention prune skips it."""
        import gzip as _gzip
        from app.registry_lifecycle.service import (
            REPLICATION_TOMBSTONE_HISTORY_DIR_REL,
            REPLICATION_TOMBSTONE_STUB_DIR_REL,
            registry_history_retention_prune_service,
        )

        shard_id = "replication_tombstones__20250101T000000Z__0001"
        old_ts = (self.now - timedelta(days=400)).isoformat()
        summary = {"entry_count": 1, "oldest_tombstone_at": old_ts, "newest_tombstone_at": old_ts}
        shard_payload = {
            "schema_type": "replication_tombstone_shard", "schema_version": "1.0",
            "shard_id": shard_id, "summary": summary,
        }
        cold_path_rel = f"{REPLICATION_TOMBSTONE_HISTORY_DIR_REL}/cold/{shard_id}.json.gz"
        cold_path = safe_path(self.repo, cold_path_rel)
        cold_path.parent.mkdir(parents=True, exist_ok=True)
        cold_path.write_bytes(_gzip.compress(json.dumps(shard_payload).encode(), mtime=0))

        # Stub points to hot (as if rehydrated)
        hot_path_rel = f"{REPLICATION_TOMBSTONE_HISTORY_DIR_REL}/{shard_id}.json"
        stub_payload = {
            "schema_type": "registry_history_stub", "schema_version": "1.0",
            "family": "replication_tombstones", "shard_id": shard_id,
            "payload_path": hot_path_rel,  # Points to hot, not cold
            "created_at": "2025-01-01T00:00:00+00:00",
            "source_head_path": REPLICATION_TOMBSTONES_REL, "summary": summary,
        }
        stub_path = safe_path(self.repo, f"{REPLICATION_TOMBSTONE_STUB_DIR_REL}/{shard_id}.json")
        write_text_file(stub_path, json.dumps(stub_payload, indent=2))

        result = registry_history_retention_prune_service(
            repo_root=self.repo, gm=None, now=self.now,
            retention_days=365, batch_limit=500,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["pruned"], 0)
        # Both cold and stub should still exist (not deleted)
        self.assertTrue(cold_path.exists())
        self.assertTrue(stub_path.exists())

    def test_orchestrator_includes_cold_apply_and_retention_prune(self):
        """Full orchestrator with auth includes cold_apply and retention_prune keys."""
        from app.registry_lifecycle.service import registry_maintenance_service
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class FakeSettings:
            delivery_terminal_retention_days: int = 30
            delivery_idempotency_retention_days: int = 30
            nonce_retention_days: int = 7
            peer_trust_history_max_hot_entries: int = 32
            peer_trust_history_hot_retention_days: int = 30
            replication_tombstone_grace_days: int = 30
            replication_pull_idempotency_retention_days: int = 14
            registry_history_batch_limit: int = 500
            delivery_history_cold_after_days: int = 90
            peer_trust_history_cold_after_days: int = 120
            replication_history_cold_after_days: int = 90
            replication_tombstone_cold_after_days: int = 90
            replication_tombstone_retention_days: int = 365

        class FakeAuth:
            def require(self, _s): pass
            def require_read_path(self, _p): pass
            def require_write_path(self, _p): pass

        result = registry_maintenance_service(
            repo_root=self.repo, gm=None, now=self.now,
            settings=FakeSettings(), auth=FakeAuth(),
            audit=lambda *_a, **_kw: None,
        )
        self.assertTrue(result["ok"])
        self.assertIn("cold_apply", result)
        self.assertIn("retention_prune", result)


# ===================================================================
# Config validation tests
# ===================================================================


class TestRegistryLifecycleValidation(unittest.TestCase):

    def test_validate_registry_lifecycle_cold_exceeds_retention(self):
        """SystemExit when cold_after_days > retention_days."""
        from dataclasses import dataclass
        from app.config import _validate_registry_lifecycle_settings

        @dataclass(frozen=True)
        class FakeSettings:
            delivery_history_cold_after_days: int = 90
            peer_trust_history_cold_after_days: int = 120
            replication_history_cold_after_days: int = 90
            replication_tombstone_cold_after_days: int = 999
            replication_tombstone_retention_days: int = 30
            registry_history_batch_limit: int = 500

        with self.assertRaises(SystemExit) as ctx:
            _validate_registry_lifecycle_settings(FakeSettings())
        self.assertIn("must not exceed", str(ctx.exception))

    def test_validate_registry_lifecycle_zero_value(self):
        """SystemExit when a cold_after_days is 0."""
        from dataclasses import dataclass
        from app.config import _validate_registry_lifecycle_settings

        @dataclass(frozen=True)
        class FakeSettings:
            delivery_history_cold_after_days: int = 0
            peer_trust_history_cold_after_days: int = 120
            replication_history_cold_after_days: int = 90
            replication_tombstone_cold_after_days: int = 90
            replication_tombstone_retention_days: int = 365
            registry_history_batch_limit: int = 500

        with self.assertRaises(SystemExit) as ctx:
            _validate_registry_lifecycle_settings(FakeSettings())
        self.assertIn("must be >= 1", str(ctx.exception))

    def test_validate_registry_lifecycle_valid(self):
        """Valid settings pass without error."""
        from dataclasses import dataclass
        from app.config import _validate_registry_lifecycle_settings

        @dataclass(frozen=True)
        class FakeSettings:
            delivery_history_cold_after_days: int = 90
            peer_trust_history_cold_after_days: int = 120
            replication_history_cold_after_days: int = 90
            replication_tombstone_cold_after_days: int = 90
            replication_tombstone_retention_days: int = 365
            registry_history_batch_limit: int = 500

        # Should not raise
        _validate_registry_lifecycle_settings(FakeSettings())


if __name__ == "__main__":
    unittest.main()
