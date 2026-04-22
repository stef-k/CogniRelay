"""Tests for #217 slice 2 persistence-path related_documents behavior."""

from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from pathlib import Path

from app.continuity.service import (
    continuity_cold_rehydrate_service,
    continuity_cold_store_service,
)
from app.models import ContinuityColdRehydrateRequest, ContinuityColdStoreRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


def _archive_rel(subject_id: str) -> str:
    return f"memory/continuity/archive/user-{subject_id}-20260320T120000Z.json"


def _archive_payload(*, subject_id: str, related_documents: object) -> dict[str, object]:
    now_iso = "2026-03-20T12:00:00Z"
    return {
        "schema_type": "continuity_archive_envelope",
        "schema_version": "1.0",
        "archived_at": now_iso,
        "archived_by": "peer-test",
        "reason": "retention",
        "active_path": f"memory/continuity/user-{subject_id}.json",
        "capsule": {
            "schema_version": "1.0",
            "subject_kind": "user",
            "subject_id": subject_id,
            "updated_at": now_iso,
            "verified_at": now_iso,
            "verification_kind": "system_check",
            "source": {
                "producer": "test",
                "update_reason": "manual",
                "inputs": ["memory/core/identity.md"],
            },
            "continuity": {
                "top_priorities": ["test priority"],
                "active_constraints": ["test constraint"],
                "active_concerns": ["test concern"],
                "open_loops": ["test loop"],
                "stance_summary": "Test stance.",
                "drift_signals": ["test drift"],
                "related_documents": related_documents,
            },
            "confidence": {"continuity": 0.9, "relationship_model": 0.0},
            "freshness": {"freshness_class": "durable"},
            "verification_state": {
                "status": "system_confirmed",
                "last_revalidated_at": now_iso,
                "strongest_signal": "system_check",
                "evidence_refs": ["memory/core/identity.md"],
            },
            "capsule_health": {
                "status": "healthy",
                "reasons": [],
                "last_checked_at": now_iso,
            },
        },
    }


def _write_archive(repo_root: Path, *, subject_id: str, related_documents: object) -> str:
    archive_rel = _archive_rel(subject_id)
    archive_path = repo_root / archive_rel
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    archive_path.write_text(
        json.dumps(_archive_payload(subject_id=subject_id, related_documents=related_documents)),
        encoding="utf-8",
    )
    return archive_rel


class TestRelatedDocuments217Slice2(unittest.TestCase):
    """Exercise archive/cold persistence degradation for malformed related_documents."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.repo_root = Path(self.tempdir.name)
        (self.repo_root / ".locks").mkdir()
        self.auth = AllowAllAuthStub()
        self.gm = SimpleGitManagerStub(self.repo_root)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_cold_store_surfaces_non_metadata_warning_for_malformed_archive_capsule(self) -> None:
        archive_rel = _write_archive(
            self.repo_root,
            subject_id="alpha",
            related_documents=[
                {
                    "path": "docs/spec.md",
                    "kind": "spec",
                    "label": "Spec",
                    "payload": {"nested": {"body": "forbidden"}},
                }
            ],
        )

        result = continuity_cold_store_service(
            repo_root=self.repo_root,
            gm=self.gm,
            auth=self.auth,
            req=ContinuityColdStoreRequest(source_archive_path=archive_rel),
            audit=lambda *_args, **_kwargs: None,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["recovery_warnings"], ["related_documents_omitted_non_metadata"])

    def test_cold_rehydrate_omits_invalid_related_documents_and_warns_once(self) -> None:
        archive_rel = _write_archive(
            self.repo_root,
            subject_id="beta",
            related_documents=[
                {
                    "path": "docs/spec.md",
                    "kind": "Spec",
                    "label": "Spec",
                }
            ],
        )

        store_result = continuity_cold_store_service(
            repo_root=self.repo_root,
            gm=self.gm,
            auth=self.auth,
            req=ContinuityColdStoreRequest(source_archive_path=archive_rel),
            audit=lambda *_args, **_kwargs: None,
        )
        cold_payload_path = self.repo_root / store_result["cold_storage_path"]
        cold_payload = json.loads(gzip.decompress(cold_payload_path.read_bytes()).decode("utf-8"))

        result = continuity_cold_rehydrate_service(
            repo_root=self.repo_root,
            gm=self.gm,
            auth=self.auth,
            req=ContinuityColdRehydrateRequest(source_archive_path=archive_rel),
            audit=lambda *_args, **_kwargs: None,
        )

        archive_path = self.repo_root / archive_rel
        restored = json.loads(archive_path.read_text(encoding="utf-8"))

        self.assertTrue(result["ok"])
        self.assertEqual(result["recovery_warnings"], ["related_documents_omitted_invalid"])
        self.assertNotIn("related_documents", restored["capsule"]["continuity"])
        self.assertIn("related_documents", cold_payload["capsule"]["continuity"])


if __name__ == "__main__":
    unittest.main()
