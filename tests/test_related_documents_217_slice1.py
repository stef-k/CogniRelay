"""Tests for #217 slice 1: related_documents runtime validation and degradation."""

from __future__ import annotations

import json
import tempfile
import unittest
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException

from app.main import discovery_tools
from app.config import Settings
from app.context import context_retrieve_service
from app.continuity.service import continuity_read_service, continuity_upsert_service
from app.models import ContinuityReadRequest, ContinuityUpsertRequest, ContextRetrieveRequest
from tests.helpers import AllowAllAuthStub, SimpleGitManagerStub


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _settings(repo_root: Path) -> Settings:
    return Settings(
        repo_root=repo_root,
        auto_init_git=False,
        git_author_name="n/a",
        git_author_email="n/a",
        tokens={},
        audit_log_enabled=False,
    )


def _base_capsule_payload(*, related_documents: object | None = None) -> dict:
    now = _now_iso()
    continuity: dict[str, object] = {
        "top_priorities": ["ship #217 slice"],
        "active_concerns": ["keep runtime deterministic"],
        "active_constraints": ["avoid broad schema churn"],
        "open_loops": ["land related_documents support"],
        "stance_summary": "Implement the bounded runtime slice without changing unrelated semantics.",
        "drift_signals": [],
    }
    if related_documents is not None:
        continuity["related_documents"] = related_documents
    return {
        "schema_version": "1.0",
        "subject_kind": "user",
        "subject_id": "stef",
        "updated_at": now,
        "verified_at": now,
        "verification_kind": "self_review",
        "source": {
            "producer": "test-suite",
            "update_reason": "manual",
            "inputs": [],
        },
        "continuity": continuity,
        "confidence": {"continuity": 0.9, "relationship_model": 0.0},
    }


def _related_document(
    *,
    path: str = "docs/payload-reference.md",
    kind: str = "spec",
    label: str = "Payload reference",
    relevance: str | None = "primary",
) -> dict[str, str]:
    entry = {
        "path": path,
        "kind": kind,
        "label": label,
    }
    if relevance is not None:
        entry["relevance"] = relevance
    return entry


def _write_active_capsule(repo_root: Path, payload: dict) -> None:
    target = repo_root / "memory" / "continuity"
    target.mkdir(parents=True, exist_ok=True)
    (target / "user-stef.json").write_text(json.dumps(payload), encoding="utf-8")


class TestRelatedDocuments217Slice1(unittest.TestCase):
    """Exercise the bounded related_documents runtime slice."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.repo_root = Path(self.tempdir.name)
        self.settings = _settings(self.repo_root)
        self.auth = AllowAllAuthStub()
        self.gm = SimpleGitManagerStub(self.repo_root)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _upsert(self, payload: dict) -> dict:
        req = ContinuityUpsertRequest.model_validate(
            {
                "subject_kind": payload["subject_kind"],
                "subject_id": payload["subject_id"],
                "capsule": payload,
            }
        )
        return continuity_upsert_service(
            repo_root=self.repo_root,
            gm=self.gm,
            auth=self.auth,
            req=req,
            audit=lambda *_args, **_kwargs: None,
        )

    def _assert_upsert_detail(self, related_documents: object, expected_detail: str) -> None:
        payload = _base_capsule_payload(related_documents=related_documents)

        with self.assertRaises(HTTPException) as ctx:
            self._upsert(payload)

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail, expected_detail)

    def test_upsert_persists_valid_related_documents(self) -> None:
        payload = _base_capsule_payload(
            related_documents=[
                _related_document(),
                _related_document(
                    path="memory/notes/continuity-audit.md",
                    kind="analysis_note",
                    label="Continuity audit notes",
                    relevance="supporting",
                ),
            ]
        )

        result = self._upsert(payload)
        stored = continuity_read_service(
            repo_root=self.repo_root,
            auth=self.auth,
            req=ContinuityReadRequest(subject_kind="user", subject_id="stef", allow_fallback=True),
            now=datetime.now(timezone.utc),
            audit=lambda *_args, **_kwargs: None,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(stored["recovery_warnings"], [])
        self.assertEqual(stored["capsule"]["continuity"]["related_documents"], payload["continuity"]["related_documents"])

    def test_upsert_rejects_reserved_embedded_content_key_with_highest_precedence(self) -> None:
        payload = _base_capsule_payload(
            related_documents=[
                {
                    "path": "docs/../payload-reference.md",
                    "kind": "spec",
                    "label": "Bad path still loses to embedded content precedence",
                    "content": "forbidden",
                }
            ]
        )

        with self.assertRaises(HTTPException) as ctx:
            self._upsert(payload)

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail, "Embedded content is not allowed in continuity.related_documents[]")

    def test_upsert_rejects_invalid_path_before_over_length_label(self) -> None:
        payload = _base_capsule_payload(
            related_documents=[
                _related_document(
                    path="/docs/spec.md",
                    label="x" * 121,
                )
            ]
        )

        with self.assertRaises(HTTPException) as ctx:
            self._upsert(payload)

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertEqual(ctx.exception.detail, "Invalid path in continuity.related_documents[].path")

    def test_upsert_rejects_named_invalid_related_documents_classes(self) -> None:
        over_count = [
            _related_document(path=f"docs/spec-{index}.md", label=f"Spec {index}")
            for index in range(9)
        ]
        duplicate = _related_document(relevance=None)
        cases: Sequence[tuple[str, object, str]] = (
            (
                "missing required key",
                [{"kind": "spec", "label": "Spec"}],
                "Missing required key in continuity.related_documents[]",
            ),
            (
                "unknown key",
                [{"path": "docs/spec.md", "kind": "spec", "label": "Spec", "note": "unexpected"}],
                "Unknown key in continuity.related_documents[]",
            ),
            (
                "whitespace-only string",
                [_related_document(label="   ")],
                "Leading or trailing whitespace is not allowed in continuity.related_documents[]",
            ),
            (
                "leading/trailing whitespace",
                [_related_document(label=" Spec ")],
                "Leading or trailing whitespace is not allowed in continuity.related_documents[]",
            ),
            (
                "empty string",
                [_related_document(label="")],
                "Value too short in continuity.related_documents[]",
            ),
            (
                "invalid relevance alone",
                [_related_document(relevance="secondary")],
                "Invalid relevance in continuity.related_documents[]",
            ),
            (
                "invalid kind format alone",
                [_related_document(kind="Spec")],
                "Invalid kind format in continuity.related_documents[]",
            ),
            (
                "wrong top-level type",
                {"path": "docs/payload-reference.md"},
                "Invalid value type in continuity.related_documents[]",
            ),
            (
                "wrong entry type",
                ["docs/spec.md"],
                "Invalid value type in continuity.related_documents[]",
            ),
            (
                "over-count",
                over_count,
                "Too many entries in continuity.related_documents",
            ),
            (
                "over-length path",
                [_related_document(path=f"docs/{'a' * 236}")],
                "Value too long in continuity.related_documents[].path",
            ),
            (
                "over-length kind",
                [_related_document(kind="a" * 33)],
                "Value too long in continuity.related_documents[].kind",
            ),
            (
                "over-length label",
                [_related_document(label="x" * 121)],
                "Value too long in continuity.related_documents[].label",
            ),
            (
                "over-length relevance",
                [_related_document(relevance="a" * 33)],
                "Value too long in continuity.related_documents[].relevance",
            ),
            (
                "duplicate with omitted relevance equality",
                [duplicate, dict(duplicate)],
                "Duplicate related_documents entry",
            ),
            (
                "invalid path with ..",
                [_related_document(path="docs/../spec.md")],
                "Invalid path in continuity.related_documents[].path",
            ),
            (
                "invalid path with backslash",
                [_related_document(path=r"docs\\spec.md")],
                "Invalid path in continuity.related_documents[].path",
            ),
        )

        for name, related_documents, expected_detail in cases:
            with self.subTest(name=name):
                self._assert_upsert_detail(related_documents, expected_detail)

    def test_upsert_rejects_reserved_embedded_content_key_alone(self) -> None:
        self._assert_upsert_detail(
            [
                {
                    "path": "docs/spec.md",
                    "kind": "spec",
                    "label": "Spec",
                    "content": "forbidden",
                }
            ],
            "Embedded content is not allowed in continuity.related_documents[]",
        )

    def test_read_omits_invalid_related_documents_with_non_metadata_warning(self) -> None:
        payload = _base_capsule_payload(
            related_documents=[
                {
                    "path": "docs/spec.md",
                    "kind": "spec",
                    "label": "Spec",
                    "payload": {"body": "forbidden"},
                }
            ]
        )
        _write_active_capsule(self.repo_root, payload)

        out = continuity_read_service(
            repo_root=self.repo_root,
            auth=self.auth,
            req=ContinuityReadRequest(subject_kind="user", subject_id="stef", view="startup"),
            now=datetime.now(timezone.utc),
            audit=lambda *_args, **_kwargs: None,
        )

        self.assertEqual(out["recovery_warnings"], ["related_documents_omitted_non_metadata"])
        self.assertNotIn("related_documents", out["capsule"]["continuity"])
        self.assertIn("startup_summary", out)

    def test_context_retrieve_omits_invalid_related_documents_with_invalid_warning(self) -> None:
        payload = _base_capsule_payload(
            related_documents=[
                {
                    "path": "docs/spec.md",
                    "kind": "Spec",
                    "label": "Spec",
                }
            ]
        )
        _write_active_capsule(self.repo_root, payload)

        out = context_retrieve_service(
            repo_root=self.repo_root,
            auth=self.auth,
            req=ContextRetrieveRequest(
                task="Load continuity",
                subject_kind="user",
                subject_id="stef",
            ),
            now=datetime.now(timezone.utc),
            audit=lambda *_args, **_kwargs: None,
        )

        continuity_state = out["bundle"]["continuity_state"]
        self.assertIn("related_documents_omitted_invalid", continuity_state["recovery_warnings"])
        capsule = continuity_state["capsules"][0]
        self.assertNotIn("related_documents", capsule["continuity"])

    def test_read_omits_nested_reserved_key_with_non_metadata_warning(self) -> None:
        payload = _base_capsule_payload(
            related_documents=[
                {
                    "path": "docs/spec.md",
                    "kind": "spec",
                    "label": "Spec",
                    "payload": {"meta": {"nested": {"body": "forbidden"}}},
                }
            ]
        )
        _write_active_capsule(self.repo_root, payload)

        out = continuity_read_service(
            repo_root=self.repo_root,
            auth=self.auth,
            req=ContinuityReadRequest(subject_kind="user", subject_id="stef", allow_fallback=True),
            now=datetime.now(timezone.utc),
            audit=lambda *_args, **_kwargs: None,
        )

        self.assertEqual(out["recovery_warnings"], ["related_documents_omitted_non_metadata"])
        self.assertNotIn("related_documents", out["capsule"]["continuity"])

    def test_read_emits_exactly_one_warning_for_multiple_invalid_classes(self) -> None:
        payload = _base_capsule_payload(
            related_documents=[
                {
                    "path": "docs/../spec.md",
                    "kind": "Spec",
                    "label": "Spec",
                    "note": "unexpected",
                }
            ]
        )
        _write_active_capsule(self.repo_root, payload)

        out = continuity_read_service(
            repo_root=self.repo_root,
            auth=self.auth,
            req=ContinuityReadRequest(subject_kind="user", subject_id="stef", allow_fallback=True),
            now=datetime.now(timezone.utc),
            audit=lambda *_args, **_kwargs: None,
        )

        self.assertEqual(out["recovery_warnings"], ["related_documents_omitted_invalid"])
        self.assertNotIn("related_documents", out["capsule"]["continuity"])

    def test_read_appends_related_documents_warning_to_existing_recovery_warnings(self) -> None:
        fallback_payload = _base_capsule_payload(
            related_documents=[
                {
                    "path": "docs/spec.md",
                    "kind": "Spec",
                    "label": "Spec",
                }
            ]
        )
        fallback_target = self.repo_root / "memory" / "continuity" / "fallback"
        fallback_target.mkdir(parents=True, exist_ok=True)
        (fallback_target / "user-stef.json").write_text(
            json.dumps(
                {
                    "schema_type": "continuity_fallback_snapshot",
                    "schema_version": "1.1",
                    "captured_at": fallback_payload["updated_at"],
                    "source_path": "memory/continuity/user-stef.json",
                    "verification_status": "unverified",
                    "health_status": "healthy",
                    "capsule": fallback_payload,
                }
            ),
            encoding="utf-8",
        )

        out = continuity_read_service(
            repo_root=self.repo_root,
            auth=self.auth,
            req=ContinuityReadRequest(
                subject_kind="user",
                subject_id="stef",
                allow_fallback=True,
            ),
            now=datetime.now(timezone.utc),
            audit=lambda *_args, **_kwargs: None,
        )

        self.assertEqual(
            out["recovery_warnings"],
            [
                "continuity_active_missing",
                "continuity_fallback_used",
                "related_documents_omitted_invalid",
            ],
        )
        self.assertEqual(out["source_state"], "fallback")
        self.assertNotIn("related_documents", out["capsule"]["continuity"])

    def test_read_prefers_reserved_key_warning_over_generic_invalidity(self) -> None:
        payload = _base_capsule_payload(
            related_documents=[
                {
                    "path": "docs/../spec.md",
                    "kind": "Spec",
                    "label": "Spec",
                    "payload": {"nested": {"body": "forbidden"}},
                }
            ]
        )
        _write_active_capsule(self.repo_root, payload)

        out = continuity_read_service(
            repo_root=self.repo_root,
            auth=self.auth,
            req=ContinuityReadRequest(subject_kind="user", subject_id="stef"),
            now=datetime.now(timezone.utc),
            audit=lambda *_args, **_kwargs: None,
        )

        self.assertEqual(out["recovery_warnings"], ["related_documents_omitted_non_metadata"])
        self.assertNotIn("related_documents", out["capsule"]["continuity"])

    def test_public_schema_exposes_bounded_related_documents_contract(self) -> None:
        payload = discovery_tools()
        by_name = {tool["name"]: tool for tool in payload["tools"]}
        schema = by_name["continuity.upsert"]["input_schema"]
        continuity_ref = schema["$defs"]["ContinuityCapsule"]["properties"]["continuity"]["$ref"]
        continuity_schema = schema["$defs"][continuity_ref.split("/")[-1]]
        related_documents = continuity_schema["properties"]["related_documents"]

        self.assertNotIn("related_documents", continuity_schema.get("required", []))
        self.assertEqual(related_documents["type"], "array")
        self.assertEqual(related_documents["maxItems"], 8)
        self.assertNotIn("anyOf", related_documents)

        items = related_documents["items"]
        self.assertEqual(items["type"], "object")
        self.assertEqual(items["required"], ["path", "kind", "label"])
        self.assertFalse(items["additionalProperties"])
        self.assertEqual(sorted(items["properties"]), ["kind", "label", "path", "relevance"])
        self.assertEqual(
            items["properties"],
            {
                "path": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 240,
                    "pattern": r"^[A-Za-z0-9._/-]+$",
                },
                "kind": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 32,
                    "pattern": r"^[a-z][a-z0-9_]*$",
                },
                "label": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 120,
                },
                "relevance": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 32,
                    "enum": ["primary", "supporting", "background"],
                },
            },
        )
