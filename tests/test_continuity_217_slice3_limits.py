"""Tests for #217 slice 3: exact bounded continuity limit rebalance."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from pydantic import ValidationError

from app.config import Settings
from app.continuity.service import continuity_read_service
from app.continuity.validation import _validate_capsule
from app.main import discovery_tools
from app.models import ContinuityCapsule, ContinuityReadRequest, SessionEndSnapshot
from tests.helpers import AllowAllAuthStub


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


def _related_document(index: int) -> dict[str, str]:
    return {
        "path": f"docs/spec-{index}.md",
        "kind": "spec",
        "label": f"Spec {index}",
        "relevance": "supporting",
    }


def _base_capsule_payload(
    *,
    top_count: int = 1,
    loop_count: int = 1,
    constraint_count: int = 1,
    rationale_summary: str = "Short rationale summary.",
    rationale_reasoning: str = "Short rationale reasoning.",
    related_documents: object | None = None,
) -> dict:
    now = _now_iso()
    continuity: dict[str, object] = {
        "top_priorities": [f"priority {index}" for index in range(top_count)],
        "active_concerns": ["concern 0"],
        "active_constraints": [f"constraint {index}" for index in range(constraint_count)],
        "open_loops": [f"loop {index}" for index in range(loop_count)],
        "stance_summary": "Keep the continuity contract deterministic while widening the targeted bounds.",
        "drift_signals": [],
        "rationale_entries": [
            {
                "tag": "limit_rebalance",
                "kind": "decision",
                "status": "active",
                "summary": rationale_summary,
                "reasoning": rationale_reasoning,
                "last_confirmed_at": now,
            }
        ],
    }
    if related_documents is not None:
        continuity["related_documents"] = related_documents
    return {
        "schema_version": "1.1",
        "subject_kind": "user",
        "subject_id": "slice3-agent",
        "updated_at": now,
        "verified_at": now,
        "verification_kind": "self_review",
        "source": {
            "producer": "slice3-test",
            "update_reason": "manual",
            "inputs": [],
        },
        "continuity": continuity,
        "confidence": {"continuity": 0.9, "relationship_model": 0.0},
    }


def _write_active_capsule(repo_root: Path, payload: dict) -> None:
    target = repo_root / "memory" / "continuity"
    target.mkdir(parents=True, exist_ok=True)
    (target / "user-slice3-agent.json").write_text(json.dumps(payload), encoding="utf-8")


class TestContinuity217Slice3Limits(unittest.TestCase):
    """Lock the exact slice-3 continuity bounds from #217."""

    def test_continuity_state_accepts_rebalanced_maxima(self) -> None:
        capsule = ContinuityCapsule.model_validate(
            _base_capsule_payload(
                top_count=8,
                loop_count=8,
                constraint_count=8,
                related_documents=[_related_document(index) for index in range(8)],
            )
        )

        self.assertEqual(len(capsule.continuity.top_priorities), 8)
        self.assertEqual(len(capsule.continuity.open_loops), 8)
        self.assertEqual(len(capsule.continuity.active_constraints), 8)
        self.assertEqual(len(capsule.continuity.related_documents), 8)

    def test_continuity_state_rejects_nine_item_core_lists(self) -> None:
        for field_name in ("top_priorities", "open_loops", "active_constraints"):
            payload = _base_capsule_payload(top_count=8, loop_count=8, constraint_count=8)
            payload["continuity"][field_name] = [f"{field_name} {index}" for index in range(9)]
            with self.assertRaises(ValidationError, msg=field_name):
                ContinuityCapsule.model_validate(payload)

    def test_session_end_snapshot_accepts_eight_items(self) -> None:
        snapshot = SessionEndSnapshot.model_validate(
            {
                "open_loops": [f"loop {index}" for index in range(8)],
                "top_priorities": [f"priority {index}" for index in range(8)],
                "active_constraints": [f"constraint {index}" for index in range(8)],
                "stance_summary": "Capture enough bounded state to resume real work without losing determinism.",
            }
        )

        self.assertEqual(len(snapshot.open_loops), 8)
        self.assertEqual(len(snapshot.top_priorities), 8)
        self.assertEqual(len(snapshot.active_constraints), 8)

    def test_session_end_snapshot_rejects_nine_items(self) -> None:
        with self.assertRaises(ValidationError):
            SessionEndSnapshot.model_validate(
                {
                    "open_loops": [f"loop {index}" for index in range(9)],
                    "top_priorities": ["priority 0"],
                    "active_constraints": ["constraint 0"],
                    "stance_summary": "Valid stance summary.",
                }
            )

    def test_validate_capsule_accepts_rebalanced_rationale_lengths(self) -> None:
        capsule = ContinuityCapsule.model_validate(
            _base_capsule_payload(
                rationale_summary="s" * 320,
                rationale_reasoning="r" * 560,
            )
        )

        with tempfile.TemporaryDirectory() as td:
            result, _ = _validate_capsule(Path(td), capsule)

        entry = result["continuity"]["rationale_entries"][0]
        self.assertEqual(len(entry["summary"]), 320)
        self.assertEqual(len(entry["reasoning"]), 560)

    def test_model_rejects_rationale_lengths_past_new_maxima(self) -> None:
        cases = (
            ("summary", "s" * 321, "reasoning ok"),
            ("reasoning", "summary ok", "r" * 561),
        )

        for field_name, summary, reasoning in cases:
            with self.assertRaises(ValidationError, msg=field_name):
                ContinuityCapsule.model_validate(
                    _base_capsule_payload(
                        rationale_summary=summary,
                        rationale_reasoning=reasoning,
                    )
                )

    def test_read_degrades_invalid_related_documents_without_failing_rebalanced_capsule(self) -> None:
        repo_root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: __import__("shutil").rmtree(repo_root, ignore_errors=True))
        _settings(repo_root)
        payload = _base_capsule_payload(
            top_count=8,
            loop_count=8,
            constraint_count=8,
            rationale_summary="s" * 320,
            rationale_reasoning="r" * 560,
            related_documents=[
                {
                    "path": "docs/spec.md",
                    "kind": "Spec",
                    "label": "Invalid kind should degrade on read",
                }
            ],
        )
        _write_active_capsule(repo_root, payload)

        out = continuity_read_service(
            repo_root=repo_root,
            auth=AllowAllAuthStub(),
            req=ContinuityReadRequest(subject_kind="user", subject_id="slice3-agent", allow_fallback=True),
            now=datetime.now(timezone.utc),
            audit=lambda *_args, **_kwargs: None,
        )

        self.assertEqual(out["recovery_warnings"], ["related_documents_omitted_invalid"])
        continuity = out["capsule"]["continuity"]
        self.assertEqual(len(continuity["top_priorities"]), 8)
        self.assertEqual(len(continuity["open_loops"]), 8)
        self.assertEqual(len(continuity["active_constraints"]), 8)
        self.assertEqual(len(continuity["rationale_entries"][0]["summary"]), 320)
        self.assertEqual(len(continuity["rationale_entries"][0]["reasoning"]), 560)
        self.assertNotIn("related_documents", continuity)

    def test_public_schema_exposes_slice3_limits(self) -> None:
        payload = discovery_tools()
        by_name = {tool["name"]: tool for tool in payload["tools"]}
        schema = by_name["continuity.upsert"]["input_schema"]
        continuity_ref = schema["$defs"]["ContinuityCapsule"]["properties"]["continuity"]["$ref"].split("/")[-1]
        continuity_schema = schema["$defs"][continuity_ref]
        rationale_ref = continuity_schema["properties"]["rationale_entries"]["items"]["$ref"].split("/")[-1]
        rationale_schema = schema["$defs"][rationale_ref]
        snapshot_ref = schema["properties"]["session_end_snapshot"]["anyOf"][0]["$ref"].split("/")[-1]
        snapshot_schema = schema["$defs"][snapshot_ref]

        self.assertEqual(continuity_schema["properties"]["top_priorities"]["maxItems"], 8)
        self.assertEqual(continuity_schema["properties"]["open_loops"]["maxItems"], 8)
        self.assertEqual(continuity_schema["properties"]["active_constraints"]["maxItems"], 8)
        self.assertEqual(continuity_schema["properties"]["related_documents"]["maxItems"], 8)
        self.assertEqual(rationale_schema["properties"]["summary"]["maxLength"], 320)
        self.assertEqual(rationale_schema["properties"]["reasoning"]["maxLength"], 560)
        self.assertEqual(snapshot_schema["properties"]["open_loops"]["maxItems"], 8)
        self.assertEqual(snapshot_schema["properties"]["top_priorities"]["maxItems"], 8)
        self.assertEqual(snapshot_schema["properties"]["active_constraints"]["maxItems"], 8)

    def test_payload_reference_documents_runtime_core_item_limits(self) -> None:
        text = (Path(__file__).resolve().parents[1] / "docs" / "payload-reference.md").read_text(encoding="utf-8")

        self.assertNotIn("no per-item limit", text)
        self.assertIn("| `top_priorities[]` | 160 chars |", text)
        self.assertIn("| `open_loops[]` | 160 chars |", text)
        self.assertIn("| `active_constraints[]` | 160 chars |", text)
        self.assertIn("| `session_trajectory[]` | 80 chars |", text)
        self.assertIn("| `top_priorities` | list of strings | yes | max 8, each ≤ 160 chars |", text)
        self.assertIn("| `session_trajectory` | list of strings | no | max 5, each ≤ 80 chars, default `[]` |", text)


if __name__ == "__main__":
    unittest.main()
