"""Guard tests for the continuity service extraction (issue #174).

1. Import surface test -- all 13 public functions importable from both
   ``app.continuity`` and ``app.continuity.service``.
2. Re-export isolation test -- ``app.continuity.service`` no longer
   re-exports ``_``-prefixed names that were previously shimmed purely
   for external consumers.
"""

from __future__ import annotations

import importlib
import unittest

# The 13 public service functions exported by app.continuity.__init__
_PUBLIC_FUNCTIONS = [
    "continuity_archive_service",
    "continuity_cold_rehydrate_service",
    "continuity_cold_store_service",
    "continuity_compare_service",
    "continuity_delete_service",
    "continuity_retention_apply_service",
    "continuity_retention_plan_service",
    "continuity_refresh_plan_service",
    "build_continuity_state",
    "continuity_list_service",
    "continuity_read_service",
    "continuity_revalidate_service",
    "continuity_upsert_service",
]

# Names that were previously re-exported from service.py purely for
# external consumers but are NOT used by service.py's own orchestration
# code.  After the extraction cleanup these must no longer appear in
# service.__dict__.
_REMOVED_REEXPORT_NAMES = {
    # constants that service.py does not reference internally
    "CONTINUITY_COLD_DIR_REL",
    "CONTINUITY_COLD_STUB_FRONTMATTER_ORDER",
    "CONTINUITY_COLD_STUB_SCHEMA_TYPE",
    "CONTINUITY_COLD_STUB_SECTION_ORDER",
    "CONTINUITY_FALLBACK_SCHEMA_TYPE",
    "CONTINUITY_FALLBACK_SCHEMA_VERSION",
    # paths helper not used by service.py
    "continuity_archive_rel_path_from_cold_artifact",
    # cold helper not used by service.py
    "_render_cold_rationale_entries",
}


class TestImportSurface(unittest.TestCase):
    """All 13 public functions are importable and callable."""

    def test_importable_from_package(self) -> None:
        pkg = importlib.import_module("app.continuity")
        for name in _PUBLIC_FUNCTIONS:
            obj = getattr(pkg, name, None)
            self.assertIsNotNone(obj, f"{name} missing from app.continuity")
            self.assertTrue(callable(obj), f"{name} is not callable")

    def test_importable_from_service(self) -> None:
        svc = importlib.import_module("app.continuity.service")
        for name in _PUBLIC_FUNCTIONS:
            obj = getattr(svc, name, None)
            self.assertIsNotNone(obj, f"{name} missing from app.continuity.service")
            self.assertTrue(callable(obj), f"{name} is not callable")


class TestReExportIsolation(unittest.TestCase):
    """service.py must not re-export names that were removed in slice 13."""

    def test_removed_reexports_absent(self) -> None:
        svc = importlib.import_module("app.continuity.service")
        svc_names = set(dir(svc))
        leaked = _REMOVED_REEXPORT_NAMES & svc_names
        self.assertEqual(
            leaked,
            set(),
            f"service.py still re-exports removed names: {sorted(leaked)}",
        )


if __name__ == "__main__":
    unittest.main()
