"""Tests for Issue #38 Phase 3 reconciliation discovery, manifest, and MCP exposure."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from app.auth import AuthContext
from app.config import Settings
from app.main import (
    discovery_tools,
    manifest,
    mcp_rpc,
)
from app.storage import canonical_json
from tests.helpers import SimpleGitManagerStub


class _GitManagerStub(SimpleGitManagerStub):
    """Git stub that records commit requests."""

    def __init__(self) -> None:
        """Initialize commit tracking."""
        self.commits: list[tuple[str, str]] = []

    def commit_file(self, path: Path, message: str) -> bool:
        """Record one commit."""
        self.commits.append((str(path), message))
        return True


class TestCoordination38Phase3Discovery(unittest.TestCase):
    """Validate reconciliation endpoints appear in discovery, manifest, and MCP surfaces."""

    def test_tool_catalog_includes_reconciliation_tools(self) -> None:
        """Discovery tool catalog should list all four reconciliation tools."""
        payload = discovery_tools()
        by_name = {tool["name"]: tool for tool in payload["tools"]}
        self.assertIn("coordination.reconciliation_open", by_name)
        self.assertIn("coordination.reconciliation_read", by_name)
        self.assertIn("coordination.reconciliations_query", by_name)
        self.assertIn("coordination.reconciliation_resolve", by_name)

    def test_reconciliation_open_tool_schema(self) -> None:
        """Reconciliation open tool should expose the open request input schema."""
        payload = discovery_tools()
        by_name = {tool["name"]: tool for tool in payload["tools"]}
        tool = by_name["coordination.reconciliation_open"]
        self.assertEqual(tool["method"], "POST")
        self.assertEqual(tool["path"], "/v1/coordination/reconciliation/open")
        self.assertFalse(tool["idempotent"])
        props = tool["input_schema"].get("properties", {})
        self.assertIn("title", props)
        self.assertIn("claims", props)
        self.assertIn("classification", props)
        self.assertIn("trigger", props)

    def test_reconciliation_read_tool_schema(self) -> None:
        """Reconciliation read tool should expose a reconciliation_id input schema."""
        payload = discovery_tools()
        by_name = {tool["name"]: tool for tool in payload["tools"]}
        tool = by_name["coordination.reconciliation_read"]
        self.assertEqual(tool["method"], "GET")
        self.assertTrue(tool["idempotent"])
        props = tool["input_schema"].get("properties", {})
        self.assertIn("reconciliation_id", props)
        self.assertIn("reconciliation_id", tool["input_schema"].get("required", []))

    def test_reconciliation_query_tool_schema(self) -> None:
        """Reconciliation query tool should expose filter parameters."""
        payload = discovery_tools()
        by_name = {tool["name"]: tool for tool in payload["tools"]}
        tool = by_name["coordination.reconciliations_query"]
        self.assertEqual(tool["method"], "GET")
        self.assertTrue(tool["idempotent"])
        props = tool["input_schema"].get("properties", {})
        self.assertIn("owner_peer", props)
        self.assertIn("claimant_peer", props)
        self.assertIn("status", props)

    def test_reconciliation_resolve_tool_schema(self) -> None:
        """Reconciliation resolve tool should include reconciliation_id plus resolve fields."""
        payload = discovery_tools()
        by_name = {tool["name"]: tool for tool in payload["tools"]}
        tool = by_name["coordination.reconciliation_resolve"]
        self.assertEqual(tool["method"], "POST")
        self.assertFalse(tool["idempotent"])
        props = tool["input_schema"].get("properties", {})
        self.assertIn("reconciliation_id", props)
        self.assertIn("expected_version", props)
        self.assertIn("outcome", props)
        required = tool["input_schema"].get("required", [])
        self.assertIn("reconciliation_id", required)
        self.assertIn("expected_version", required)
        self.assertIn("outcome", required)

    def test_manifest_includes_reconciliation_endpoints(self) -> None:
        """Manifest should list all four reconciliation HTTP endpoints."""
        m = manifest()
        endpoints = m["endpoints"]
        self.assertIn("POST /v1/coordination/reconciliation/open", endpoints)
        self.assertIn("GET /v1/coordination/reconciliation/{reconciliation_id}", endpoints)
        self.assertIn("GET /v1/coordination/reconciliations/query", endpoints)
        self.assertIn("POST /v1/coordination/reconciliation/{reconciliation_id}/resolve", endpoints)

    def test_mcp_tools_list_includes_reconciliation_tools(self) -> None:
        """MCP tools/list should expose all four reconciliation tools."""
        req = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
        res = mcp_rpc(req)
        tools = res["result"]["tools"]
        by_name = {t["name"]: t for t in tools}
        self.assertIn("coordination.reconciliation_open", by_name)
        self.assertIn("coordination.reconciliation_read", by_name)
        self.assertIn("coordination.reconciliations_query", by_name)
        self.assertIn("coordination.reconciliation_resolve", by_name)


class TestCoordination38Phase3MCPDispatch(unittest.TestCase):
    """Validate MCP tool dispatch for reconciliation operations."""

    def _settings(self, repo_root: Path) -> Settings:
        """Return repo-rooted settings for MCP dispatch tests."""
        return Settings(
            repo_root=repo_root,
            auto_init_git=False,
            git_author_name="n/a",
            git_author_email="n/a",
            tokens={},
            audit_log_enabled=False,
        )

    def _now(self) -> str:
        """Return a stable UTC timestamp string."""
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _auth(self, *, scopes: set[str] | None = None) -> AuthContext:
        """Return an AuthContext for MCP dispatch tests."""
        return AuthContext(
            token="token",
            peer_id="peer-alpha",
            scopes=scopes or {"write:projects", "read:files"},
            read_namespaces={"*"},
            write_namespaces={"*"},
            client_ip="127.0.0.1",
        )

    def _write_peer_registry(self, repo_root: Path, peer_id: str, *, trust_level: str = "restricted") -> None:
        """Persist one peer registry row."""
        path = repo_root / "peers" / "registry.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        now = self._now()
        payload: dict = {"schema_version": "1.0", "updated_at": now, "peers": {}}
        payload["peers"][peer_id] = {
            "base_url": f"https://{peer_id}.example.net",
            "public_key": None,
            "public_key_fingerprint": None,
            "capabilities_url": "/v1/manifest",
            "trust_level": trust_level,
            "allowed_scopes": [],
            "created_at": now,
            "updated_at": now,
            "trust_history": [],
        }
        if path.exists():
            current = json.loads(path.read_text(encoding="utf-8"))
            current.setdefault("peers", {}).update(payload["peers"])
            payload = current
        path.write_text(json.dumps(payload), encoding="utf-8")

    def _write_shared_artifact(self, repo_root: Path, payload: dict) -> None:
        """Persist one raw shared artifact fixture."""
        path = repo_root / "memory" / "coordination" / "shared" / f"{payload['shared_id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(canonical_json(payload), encoding="utf-8")

    def _shared_payload(self, **overrides: object) -> dict:
        """Return one valid stored shared artifact payload."""
        now = self._now()
        payload = {
            "schema_type": "coordination_shared_state",
            "schema_version": "1.0",
            "shared_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "created_at": now,
            "updated_at": now,
            "created_by": "peer-alpha",
            "owner_peer": "peer-alpha",
            "participant_peers": ["peer-beta"],
            "task_id": "task-123",
            "thread_id": "thread-abc",
            "title": "Release timing",
            "summary": "Bounded shared coordination state.",
            "shared_state": {
                "constraints": ["Do not lift the freeze without review."],
                "drift_signals": ["One participant sees stale context."],
                "coordination_alerts": ["Missing context remains possible."],
            },
            "version": 3,
            "last_updated_by": "peer-alpha",
        }
        payload.update(overrides)
        return payload

    def _write_reconciliation_artifact(self, repo_root: Path, payload: dict) -> None:
        """Persist one raw reconciliation artifact fixture."""
        path = repo_root / "memory" / "coordination" / "reconciliations" / f"{payload['reconciliation_id']}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(canonical_json(payload), encoding="utf-8")

    def _reconciliation_payload(self, **overrides: object) -> dict:
        """Return one valid stored open reconciliation artifact payload."""
        now = self._now()
        payload = {
            "schema_type": "coordination_reconciliation_record",
            "schema_version": "1.0",
            "reconciliation_id": "recon_cccccccccccccccccccccccccccccccc",
            "created_at": now,
            "updated_at": now,
            "opened_by": "peer-alpha",
            "owner_peer": "peer-alpha",
            "participant_peers": ["peer-beta"],
            "task_id": "task-123",
            "thread_id": "thread-abc",
            "title": "Constraint disagreement",
            "summary": "Two claims disagree.",
            "classification": "contradictory",
            "trigger": "shared_vs_shared",
            "claims": [
                {
                    "source_kind": "shared",
                    "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "claimant_peer": "peer-alpha",
                    "claim_summary": "The freeze still applies.",
                    "epistemic_status": "frame_present",
                    "evidence_refs": ["msg_123"],
                    "observed_version": 2,
                },
                {
                    "source_kind": "shared",
                    "source_id": "shared_dddddddddddddddddddddddddddddddd",
                    "claimant_peer": "peer-beta",
                    "claim_summary": "The freeze was lifted.",
                    "epistemic_status": "frame_status_unknown",
                    "evidence_refs": ["msg_456"],
                    "observed_version": 3,
                },
            ],
            "status": "open",
            "resolution_outcome": None,
            "resolution_summary": None,
            "resolved_at": None,
            "resolved_by": None,
            "version": 1,
            "last_updated_by": "peer-alpha",
        }
        payload.update(overrides)
        return payload

    def test_mcp_dispatch_can_open_reconciliation(self) -> None:
        """MCP tools/call for coordination.reconciliation_open should create an artifact."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_peer_registry(repo_root, "peer-alpha", trust_level="trusted")
            self._write_peer_registry(repo_root, "peer-beta", trust_level="restricted")
            self._write_shared_artifact(repo_root, self._shared_payload(shared_id="shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", version=2))
            self._write_shared_artifact(
                repo_root,
                self._shared_payload(
                    shared_id="shared_dddddddddddddddddddddddddddddddd",
                    owner_peer="peer-beta",
                    participant_peers=["peer-alpha"],
                    version=3,
                ),
            )
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            auth = self._auth()

            with patch("app.main._services", return_value=(settings, gm)), patch(
                "app.main.require_auth", return_value=auth
            ):
                req = {
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "tools/call",
                    "params": {
                        "name": "coordination.reconciliation_open",
                        "arguments": {
                            "task_id": "task-123",
                            "title": "MCP open test",
                            "classification": "contradictory",
                            "trigger": "shared_vs_shared",
                            "claims": [
                                {
                                    "source_kind": "shared",
                                    "source_id": "shared_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                    "claimant_peer": "peer-alpha",
                                    "claim_summary": "Freeze applies.",
                                    "epistemic_status": "frame_present",
                                    "observed_version": 2,
                                },
                                {
                                    "source_kind": "shared",
                                    "source_id": "shared_dddddddddddddddddddddddddddddddd",
                                    "claimant_peer": "peer-beta",
                                    "claim_summary": "Freeze lifted.",
                                    "epistemic_status": "frame_status_unknown",
                                    "observed_version": 3,
                                },
                            ],
                        },
                    },
                }
                res = mcp_rpc(req, authorization="Bearer token")

            self.assertEqual(res["id"], 10)
            result_data = res["result"]["structuredContent"]
            self.assertTrue(result_data["ok"])
            self.assertTrue(result_data["created"])

    def test_mcp_dispatch_can_read_reconciliation(self) -> None:
        """MCP tools/call for coordination.reconciliation_read should return the artifact."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            auth = self._auth()

            with patch("app.main._services", return_value=(settings, _GitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                req = {
                    "jsonrpc": "2.0",
                    "id": 11,
                    "method": "tools/call",
                    "params": {
                        "name": "coordination.reconciliation_read",
                        "arguments": {"reconciliation_id": "recon_cccccccccccccccccccccccccccccccc"},
                    },
                }
                res = mcp_rpc(req, authorization="Bearer token")

            result_data = res["result"]["structuredContent"]
            self.assertTrue(result_data["ok"])
            self.assertEqual(result_data["reconciliation"]["reconciliation_id"], "recon_cccccccccccccccccccccccccccccccc")

    def test_mcp_dispatch_can_query_reconciliations(self) -> None:
        """MCP tools/call for coordination.reconciliations_query should return matching results."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            auth = self._auth(scopes={"read:files", "write:projects"})

            with patch("app.main._services", return_value=(settings, _GitManagerStub())), patch(
                "app.main.require_auth", return_value=auth
            ):
                req = {
                    "jsonrpc": "2.0",
                    "id": 12,
                    "method": "tools/call",
                    "params": {
                        "name": "coordination.reconciliations_query",
                        "arguments": {"owner_peer": "peer-alpha"},
                    },
                }
                res = mcp_rpc(req, authorization="Bearer token")

            result_data = res["result"]["structuredContent"]
            self.assertTrue(result_data["ok"])
            self.assertEqual(result_data["count"], 1)

    def test_mcp_dispatch_can_resolve_reconciliation(self) -> None:
        """MCP tools/call for coordination.reconciliation_resolve should resolve the artifact."""
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            self._write_reconciliation_artifact(repo_root, self._reconciliation_payload())
            settings = self._settings(repo_root)
            gm = _GitManagerStub()
            auth = self._auth()

            with patch("app.main._services", return_value=(settings, gm)), patch(
                "app.main.require_auth", return_value=auth
            ):
                req = {
                    "jsonrpc": "2.0",
                    "id": 13,
                    "method": "tools/call",
                    "params": {
                        "name": "coordination.reconciliation_resolve",
                        "arguments": {
                            "reconciliation_id": "recon_cccccccccccccccccccccccccccccccc",
                            "expected_version": 1,
                            "outcome": "conflicted",
                            "resolution_summary": "Evidence insufficient.",
                        },
                    },
                }
                res = mcp_rpc(req, authorization="Bearer token")

            result_data = res["result"]["structuredContent"]
            self.assertTrue(result_data["ok"])
            self.assertTrue(result_data["updated"])
            self.assertEqual(result_data["reconciliation"]["status"], "resolved")


if __name__ == "__main__":
    unittest.main()
