"""Last-mile adapter kit coverage for issue #289."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app
from app.mcp.service import reset_bootstrap_state

ROOT = Path(__file__).resolve().parents[1]
RETRIEVAL_HOOK = ROOT / "agent-assets" / "hooks" / "cognirelay_retrieval_hook.py"
SAVE_HOOK = ROOT / "agent-assets" / "hooks" / "cognirelay_continuity_save_hook.py"
SKILL = ROOT / "agent-assets" / "skills" / "cognirelay-continuity-authoring" / "SKILL.md"

REQUIRED_HELP_LINKS = {
    "onboarding": "/v1/help/onboarding",
    "last_mile_topic": "/v1/help/topics/last-mile-adapter",
    "continuity_read_tool": "/v1/help/tools/continuity.read",
    "continuity_upsert_tool": "/v1/help/tools/continuity.upsert",
    "context_retrieve_tool": "/v1/help/tools/context.retrieve",
    "limits_index": "/v1/help/limits",
}


def run_hook(path: Path, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    for key in list(merged_env):
        if key.startswith("COGNIRELAY_"):
            merged_env.pop(key)
    if env:
        merged_env.update(env)
    return subprocess.run(
        [sys.executable, str(path), *args],
        cwd=ROOT,
        env=merged_env,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )


def valid_payload() -> dict[str, object]:
    return {
        "subject_kind": "thread",
        "subject_id": "issue-289",
        "merge_mode": "preserve",
        "capsule": {
            "schema_version": "1.1",
            "subject_kind": "thread",
            "subject_id": "issue-289",
            "updated_at": "2026-04-26T12:00:00Z",
            "verified_at": "2026-04-26T12:00:00Z",
            "continuity": {
                "top_priorities": ["Ship the last-mile adapter kit."],
                "active_concerns": [],
                "active_constraints": ["Keep hooks generic and stdlib-only."],
                "open_loops": [],
                "drift_signals": [],
                "stance_summary": "Implement the generic adapter assets without semantic inference.",
                "negative_decisions": [],
                "rationale_entries": [],
                "retrieval_hints": {"must_include": ["agent-assets/README.md"], "load_next": [], "avoid": []},
            },
            "source": {"producer": "agent-authored-test", "update_reason": "manual", "inputs": []},
            "confidence": {"continuity": 0.8, "relationship_model": 0.5},
        },
    }


class TestAgentAssets289(unittest.TestCase):
    """Verify shipped assets and offline hook behavior."""

    def test_agent_asset_files_exist(self) -> None:
        for path in (
            ROOT / "agent-assets" / "README.md",
            SKILL,
            ROOT / "agent-assets" / "hooks" / "README.md",
            RETRIEVAL_HOOK,
            SAVE_HOOK,
        ):
            with self.subTest(path=path):
                self.assertTrue(path.exists())

    def test_skill_preserves_responsibility_split_and_bans_semantic_inference(self) -> None:
        text = SKILL.read_text(encoding="utf-8")
        for token in (
            "CogniRelay is the substrate",
            "not the semantic author",
            "The running agent authors semantic fields",
            "must not infer semantic continuity",
            "Graph and schedule sections are read-only orientation adjuncts",
        ):
            self.assertIn(token, text)

    def test_retrieval_hook_help_and_no_write_modes(self) -> None:
        completed = run_hook(RETRIEVAL_HOOK, "--help")
        self.assertEqual(completed.returncode, 0)
        self.assertIn("--context-retrieve", completed.stdout)
        self.assertNotIn("write", completed.stdout)
        self.assertNotIn("dry-run", completed.stdout)

    def test_retrieval_hook_missing_subject_exits_2_without_http(self) -> None:
        completed = run_hook(
            RETRIEVAL_HOOK,
            "--base-url",
            "http://127.0.0.1:9",
            "--token",
            "secret-token",
        )
        self.assertEqual(completed.returncode, 2)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["mode"], "retrieval")
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["errors"][0]["code"], "missing_subject")
        self.assertEqual(completed.stderr, "")
        self.assertNotIn("secret-token", completed.stdout)

    def test_save_hook_help_exposes_required_modes(self) -> None:
        completed = run_hook(SAVE_HOOK, "--help")
        self.assertEqual(completed.returncode, 0)
        for token in ("facts", "template", "dry-run", "write", "readback", "doctor"):
            self.assertIn(token, completed.stdout)

    def test_facts_output_contains_exact_required_help_links(self) -> None:
        completed = run_hook(SAVE_HOOK, "facts", "--subject-kind", "thread", "--subject-id", "issue-289")
        self.assertEqual(completed.returncode, 0)
        payload = json.loads(completed.stdout)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["result"]["help_links"], REQUIRED_HELP_LINKS)

    def test_template_emits_schema_aligned_skeleton(self) -> None:
        completed = run_hook(SAVE_HOOK, "template", "--subject-kind", "thread", "--subject-id", "issue-289")
        self.assertEqual(completed.returncode, 0)
        payload = json.loads(completed.stdout)["result"]["payload"]
        self.assertEqual(payload["capsule"]["schema_version"], "1.1")
        self.assertIn("updated_at", payload["capsule"])
        self.assertIn("verified_at", payload["capsule"])
        self.assertIn("retrieval_hints", payload["capsule"]["continuity"])

    def test_dry_run_rejects_placeholders_before_write(self) -> None:
        completed = run_hook(SAVE_HOOK, "template")
        template_payload = json.loads(completed.stdout)["result"]["payload"]
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json") as handle:
            json.dump(template_payload, handle)
            handle.flush()
            dry_run = run_hook(SAVE_HOOK, "dry-run", "--input", handle.name)
        self.assertEqual(dry_run.returncode, 2)
        payload = json.loads(dry_run.stdout)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["result"]["valid"], False)
        self.assertTrue(payload["result"]["placeholder_errors"])

    def test_dry_run_valid_payload_returns_deterministic_diff_without_server(self) -> None:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json") as handle:
            json.dump(valid_payload(), handle)
            handle.flush()
            completed = run_hook(SAVE_HOOK, "dry-run", "--input", handle.name)
        self.assertEqual(completed.returncode, 0)
        payload = json.loads(completed.stdout)
        diff = payload["result"]["diff"]
        self.assertEqual(set(diff), {"current_available", "added", "removed", "changed"})
        self.assertFalse(diff["current_available"])
        self.assertEqual(diff["removed"], [])
        self.assertEqual(diff["changed"], [])
        self.assertEqual([item["path"] for item in diff["added"]], sorted(item["path"] for item in diff["added"]))
        self.assertIn("/capsule/continuity/stance_summary", {item["path"] for item in diff["added"]})
        self.assertNotIn("/subject_kind", {item["path"] for item in diff["added"]})

    def test_server_compare_not_implemented_without_contacting_server(self) -> None:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json") as handle:
            json.dump(valid_payload(), handle)
            handle.flush()
            completed = run_hook(
                SAVE_HOOK,
                "dry-run",
                "--input",
                handle.name,
                "--server-compare",
                "--base-url",
                "http://127.0.0.1:9",
                "--token",
                "secret-token",
            )
        self.assertEqual(completed.returncode, 2)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["errors"][0]["code"], "server_compare_not_implemented")
        self.assertEqual(completed.stderr, "")

    def test_docs_link_to_shipped_paths(self) -> None:
        onboarding = (ROOT / "docs" / "agent-onboarding.md").read_text(encoding="utf-8")
        index = (ROOT / "docs" / "index.md").read_text(encoding="utf-8")
        for token in (
            "agent-assets/README.md",
            "agent-assets/skills/cognirelay-continuity-authoring/SKILL.md",
            "agent-assets/hooks/cognirelay_retrieval_hook.py",
            "agent-assets/hooks/cognirelay_continuity_save_hook.py",
        ):
            self.assertIn(token, onboarding)
        self.assertIn("../agent-assets/README.md", index)


class TestLastMileRuntimeHelp289(unittest.TestCase):
    """Verify HTTP and MCP discovery for the last-mile adapter topic."""

    _HEADERS = {"authorization": "Bearer last-mile-topic"}

    @classmethod
    def setUpClass(cls) -> None:
        cls._client_context = TestClient(app)
        cls.client = cls._client_context.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._client_context.__exit__(None, None, None)

    def setUp(self) -> None:
        reset_bootstrap_state()

    def _bootstrap(self) -> None:
        response = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-11-25"}},
            headers=self._HEADERS,
        )
        self.assertEqual(response.status_code, 200)

    def test_help_topic_returns_required_guidance(self) -> None:
        response = self.client.get("/v1/help/topics/last-mile-adapter")
        self.assertEqual(response.status_code, 200)
        text = json.dumps(response.json(), sort_keys=True)
        for token in (
            "CogniRelay is the substrate",
            "agent authors semantic fields",
            "agent-assets/skills/cognirelay-continuity-authoring/SKILL.md",
            "agent-assets/hooks/cognirelay_retrieval_hook.py",
            "agent-assets/hooks/cognirelay_continuity_save_hook.py",
            "facts",
            "template",
            "dry-run",
            "write",
            "readback",
            "semantic inference",
        ):
            self.assertIn(token, text)

    def test_root_and_unsupported_topic_hints_include_last_mile_adapter(self) -> None:
        root = self.client.get("/v1/help").json()
        self.assertIn("last-mile-adapter", root["non_tool_topics"])
        response = self.client.get("/v1/help/topics/not-a-topic")
        self.assertEqual(response.status_code, 400)
        hint = response.json()["error"]["validation_hints"][0]
        self.assertIn("last-mile-adapter", hint["allowed_values"])
        self.assertIn("last-mile-adapter", hint["correction_hint"])

    def test_system_topic_help_preserves_compact_shape(self) -> None:
        self._bootstrap()
        response = self.client.post(
            "/v1/mcp",
            json={"jsonrpc": "2.0", "id": 2, "method": "system.topic_help", "params": {"id": "last-mile-adapter"}},
            headers=self._HEADERS,
        )
        self.assertEqual(response.status_code, 200)
        structured = response.json()["result"]["structuredContent"]
        self.assertEqual(set(structured), {"surface", "httpEquivalent", "id", "title", "summary"})
        self.assertEqual(structured["httpEquivalent"], "/v1/help/topics/last-mile-adapter")
        summary = structured["summary"]
        for token in ("CogniRelay is the substrate", "agent authors semantic fields", "semantic inference"):
            self.assertIn(token, summary)
