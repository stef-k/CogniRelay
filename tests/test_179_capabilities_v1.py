"""Tests for the versioned GET /v1/capabilities endpoint (#179)."""

import json
import unittest

from app.discovery.service import capabilities_v1_payload
from app.main import capabilities, capabilities_v1

# All 12 v1 feature keys from the spec (§4).
V1_FEATURE_KEYS = frozenset(
    {
        "continuity.read.startup_view",
        "continuity.read.trust_signals",
        "continuity.upsert.session_end_snapshot",
        "continuity.read.salience_ranking",
        "continuity.read.thread_identity",
        "continuity.stable_preferences",
        "context.retrieve.continuity_state",
        "coordination.handoffs",
        "coordination.shared_state",
        "messaging.direct",
        "peers.registry",
        "discovery.tools",
    }
)


class TestCapabilitiesV1Unit(unittest.TestCase):
    """Unit tests: capabilities_v1_payload() returns the frozen shape."""

    def test_top_level_keys(self) -> None:
        """Response has exactly two top-level keys: version and features."""
        payload = capabilities_v1_payload()
        self.assertEqual(set(payload.keys()), {"version", "features"})

    def test_version_is_string_one(self) -> None:
        """version is the string '1'."""
        payload = capabilities_v1_payload()
        self.assertEqual(payload["version"], "1")

    def test_all_v1_keys_present(self) -> None:
        """Every key from the v1 registry (§4) is present."""
        payload = capabilities_v1_payload()
        self.assertEqual(set(payload["features"].keys()), V1_FEATURE_KEYS)

    def test_feature_entries_have_only_summary(self) -> None:
        """Each feature entry has exactly one field: summary (non-empty, ≤120 chars)."""
        payload = capabilities_v1_payload()
        for key, entry in payload["features"].items():
            self.assertEqual(
                set(entry.keys()),
                {"summary"},
                f"unexpected fields in {key}: {set(entry.keys())}",
            )
            self.assertIsInstance(entry["summary"], str, f"{key} summary is not a string")
            self.assertTrue(len(entry["summary"]) > 0, f"{key} summary is empty")
            self.assertLessEqual(
                len(entry["summary"]),
                120,
                f"{key} summary exceeds 120 chars ({len(entry['summary'])})",
            )

    def test_determinism_byte_identical(self) -> None:
        """Two sequential calls return byte-identical JSON."""
        a = json.dumps(capabilities_v1_payload(), sort_keys=True)
        b = json.dumps(capabilities_v1_payload(), sort_keys=True)
        self.assertEqual(a, b)


class TestCapabilitiesV1Integration(unittest.TestCase):
    """Integration tests: the route function returns the expected shape."""

    def test_route_returns_200_shape(self) -> None:
        """GET /v1/capabilities returns the expected top-level shape and all v1 keys."""
        payload = capabilities_v1()
        self.assertIn("version", payload)
        self.assertIn("features", payload)
        self.assertEqual(payload["version"], "1")
        self.assertEqual(set(payload["features"].keys()), V1_FEATURE_KEYS)


class TestLegacyCapabilitiesUnchanged(unittest.TestCase):
    """Legacy GET /capabilities continues to return its flat-list payload."""

    def test_legacy_returns_flat_list(self) -> None:
        """Legacy endpoint still returns a dict with a 'features' list of strings."""
        payload = capabilities()
        self.assertIn("features", payload)
        self.assertIsInstance(payload["features"], list)
        # Spot-check a few known legacy entries.
        self.assertIn("write", payload["features"])
        self.assertIn("search", payload["features"])
        # Legacy should NOT have a 'version' key.
        self.assertNotIn("version", payload)


if __name__ == "__main__":
    unittest.main()
