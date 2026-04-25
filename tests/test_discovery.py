"""Tests for discovery, manifest, and workflow catalog endpoints."""

import unittest

from app.main import capabilities, capabilities_v1, discovery, discovery_tools, discovery_workflows, manifest


class TestDiscoveryEndpoints(unittest.TestCase):
    """Validate the public discovery surface exposed by the API."""

    def test_discovery_has_bounded_mcp_metadata(self) -> None:
        """Discovery should advertise the bounded MCP 2025-11-25 metadata fields."""
        payload = discovery()
        self.assertTrue(payload["ok"])
        self.assertEqual(
            payload["protocol"],
            {
                "name": "cognirelay-http",
                "style": "mcp-2025-11-25",
                "version": payload["protocol"]["version"],
                "transport": "streamable-http",
                "mcp_protocol_version": "2025-11-25",
                "post_endpoint": {"path": "/v1/mcp", "method": "POST", "posture": "active"},
                "get_endpoint": {"path": "/v1/mcp", "status": 405, "allow": "POST", "posture": "deferred"},
            },
        )
        self.assertEqual(
            payload["entrypoints"],
            {
                "manifest": "/v1/manifest",
                "contracts": "/v1/contracts",
                "governance_policy": "/v1/governance/policy",
                "ops_catalog": "/v1/ops/catalog",
                "ops_status": "/v1/ops/status",
                "tools": "/v1/discovery/tools",
                "workflows": "/v1/discovery/workflows",
                "mcp_rpc": "/v1/mcp",
                "mcp_well_known": "/.well-known/mcp.json",
            },
        )
        self.assertEqual(
            payload["agent_guidance"]["mcp_first_calls"],
            [
                "GET /.well-known/mcp.json",
                (
                    "POST /v1/mcp "
                    "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
                    "\"params\":{\"protocolVersion\":\"2025-11-25\"}}"
                ),
                "POST /v1/mcp {\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\",\"params\":{}}",
            ],
        )

    def test_tool_catalog_includes_core_tools(self) -> None:
        """Tool catalog should list the core public tool entries."""
        payload = discovery_tools()
        self.assertTrue(payload["ok"])
        self.assertGreater(payload["count"], 0)

        by_name = {tool["name"]: tool for tool in payload["tools"]}
        self.assertIn("memory.write", by_name)
        self.assertIn("recent.list", by_name)
        self.assertIn("context.retrieve", by_name)
        self.assertIn("continuity.upsert", by_name)
        self.assertIn("continuity.read", by_name)
        self.assertIn("continuity.compare", by_name)
        self.assertIn("continuity.revalidate", by_name)
        self.assertIn("continuity.refresh_plan", by_name)
        self.assertIn("continuity.list", by_name)
        self.assertIn("continuity.archive", by_name)
        self.assertIn("continuity.delete", by_name)
        self.assertIn("messages.send", by_name)
        self.assertIn("messages.ack", by_name)
        self.assertIn("messages.pending", by_name)
        self.assertIn("peers.list", by_name)
        self.assertIn("context.snapshot_create", by_name)
        self.assertIn("tasks.create", by_name)
        self.assertIn("tasks.update", by_name)
        self.assertIn("tasks.query", by_name)
        self.assertIn("docs.patch_propose", by_name)
        self.assertIn("docs.patch_apply", by_name)
        self.assertIn("code.patch_propose", by_name)
        self.assertIn("code.checks_run", by_name)
        self.assertIn("code.merge", by_name)
        self.assertIn("security.tokens_list", by_name)
        self.assertIn("security.tokens_issue", by_name)
        self.assertIn("security.tokens_revoke", by_name)
        self.assertIn("security.tokens_rotate", by_name)
        self.assertIn("security.keys_rotate", by_name)
        self.assertIn("messages.verify", by_name)
        self.assertIn("metrics.get", by_name)
        self.assertIn("messages.replay", by_name)
        self.assertIn("replication.pull", by_name)
        self.assertIn("replication.push", by_name)
        self.assertIn("system.capabilities_v1", by_name)
        self.assertIn("system.contracts", by_name)
        self.assertIn("system.governance_policy", by_name)
        self.assertIn("peers.trust_transition", by_name)
        self.assertIn("backup.create", by_name)
        self.assertIn("backup.restore_test", by_name)
        self.assertIn("ops.catalog", by_name)
        self.assertIn("ops.status", by_name)
        self.assertIn("ops.run", by_name)
        self.assertIn("ops.schedule_export", by_name)

        write_schema = by_name["memory.write"]["input_schema"]
        self.assertIn("path", write_schema.get("properties", {}))
        self.assertIn("content", write_schema.get("properties", {}))
        self.assertEqual(
            by_name["continuity.refresh_plan"]["scopes"],
            ["read:files", "write:projects", "read_namespaces", "write_namespaces"],
        )
        self.assertFalse(by_name["continuity.refresh_plan"]["idempotent"])

        context_retrieve_descriptor = f"{by_name['context.retrieve']['name']} {by_name['context.retrieve']['description']}"
        for token in ("context.retrieve", "bundle.graph_context", "bundle.schedule_context", "graph_context", "schedule_context"):
            with self.subTest(tool="context.retrieve", token=token):
                self.assertIn(token, context_retrieve_descriptor)

        continuity_read_descriptor = f"{by_name['continuity.read']['name']} {by_name['continuity.read']['description']}"
        for token in ("continuity.read", "startup", "graph_summary", "schedule_context"):
            with self.subTest(tool="continuity.read", token=token):
                self.assertIn(token, continuity_read_descriptor)

    def test_capabilities_advertise_graph_and_schedule_runtime_surfaces(self) -> None:
        """Capability descriptors should stay coherent with shipped graph and schedule orientation."""
        legacy_features = set(capabilities()["features"])
        self.assertIn("derived_graph_runtime_context", legacy_features)
        self.assertIn("schedule.one_shot_reminders", legacy_features)

        feature_map = capabilities_v1()["features"]
        for feature_key in (
            "context.retrieve.graph_context",
            "continuity.read.startup_graph_summary",
            "schedule.one_shot_reminders",
        ):
            with self.subTest(feature_key=feature_key):
                self.assertIn(feature_key, feature_map)

        graph_context_summary = feature_map["context.retrieve.graph_context"]["summary"]
        for token in ("graph", "context", "default", "context retrieval"):
            with self.subTest(feature_key="context.retrieve.graph_context", token=token):
                self.assertIn(token, graph_context_summary)

        startup_graph_summary = feature_map["continuity.read.startup_graph_summary"]["summary"]
        for token in ("graph", "summary", "startup", "continuity reads"):
            with self.subTest(feature_key="continuity.read.startup_graph_summary", token=token):
                self.assertIn(token, startup_graph_summary)

        schedule_summary = feature_map["schedule.one_shot_reminders"]["summary"]
        for token in ("reminders", "task nudges", "orientation responses"):
            with self.subTest(feature_key="schedule.one_shot_reminders", token=token):
                self.assertIn(token, schedule_summary)

    def test_workflow_catalog_has_bootstrap(self) -> None:
        """Workflow catalog should expose the bootstrap workflow."""
        payload = discovery_workflows()
        self.assertTrue(payload["ok"])
        by_name = {wf["name"]: wf for wf in payload["workflows"]}
        self.assertIn("bootstrap_cycle", by_name)
        self.assertIn("collaborative_tasks_and_code", by_name)
        self.assertIn("federation_hardening", by_name)
        self.assertIn("maintenance_compaction", by_name)
        self.assertIn("host_ops_automation", by_name)
        first_tool = by_name["bootstrap_cycle"]["steps"][0]["tool"]
        self.assertEqual(first_tool, "system.discovery")

        federation_tools = [step["tool"] for step in by_name["federation_hardening"]["steps"]]
        self.assertIn("peers.trust_transition", federation_tools)
        maintenance_tools = [step["tool"] for step in by_name["maintenance_compaction"]["steps"]]
        self.assertIn("continuity.refresh_plan", maintenance_tools)
        self.assertIn("backup.create", maintenance_tools)
        self.assertIn("backup.restore_test", maintenance_tools)

    def test_manifest_exposes_discovery_endpoints(self) -> None:
        """Manifest should link back to the discovery endpoints."""
        m = manifest()
        endpoints = m["endpoints"]
        self.assertIn("GET /v1/capabilities", endpoints)
        self.assertIn("GET /v1/discovery", endpoints)
        self.assertIn("GET /v1/discovery/tools", endpoints)
        self.assertIn("GET /v1/discovery/workflows", endpoints)
        self.assertIn("GET /.well-known/cognirelay.json", endpoints)
        self.assertEqual(
            endpoints["GET /v1/mcp"],
            {"scope": None, "status": 405, "allow": "POST", "posture": "deferred"},
        )
        self.assertEqual(
            endpoints["POST /v1/mcp"],
            {
                "scope": "mixed (depends on tool)",
                "transport": "streamable-http",
                "mcp_protocol_version": "2025-11-25",
                "posture": "active",
            },
        )
        self.assertIn("POST /v1/continuity/upsert", endpoints)
        self.assertIn("POST /v1/continuity/read", endpoints)
        self.assertIn("POST /v1/continuity/compare", endpoints)
        self.assertIn("POST /v1/continuity/revalidate", endpoints)
        self.assertIn("POST /v1/continuity/refresh/plan", endpoints)
        self.assertIn("POST /v1/continuity/list", endpoints)
        self.assertIn("POST /v1/continuity/archive", endpoints)
        self.assertIn("POST /v1/continuity/delete", endpoints)
        self.assertIn("GET /v1/peers", endpoints)
        self.assertIn("POST /v1/peers/register", endpoints)
        self.assertIn("POST /v1/recent", endpoints)
        self.assertIn("POST /v1/context/snapshot", endpoints)
        self.assertIn("GET /v1/context/snapshot/{snapshot_id}", endpoints)
        self.assertIn("POST /v1/tasks", endpoints)
        self.assertIn("PATCH /v1/tasks/{task_id}", endpoints)
        self.assertIn("GET /v1/tasks/query", endpoints)
        self.assertIn("POST /v1/docs/patch/propose", endpoints)
        self.assertIn("POST /v1/docs/patch/apply", endpoints)
        self.assertIn("POST /v1/code/patch/propose", endpoints)
        self.assertIn("POST /v1/code/checks/run", endpoints)
        self.assertIn("POST /v1/code/merge", endpoints)
        self.assertIn("GET /v1/security/tokens", endpoints)
        self.assertIn("POST /v1/security/tokens/issue", endpoints)
        self.assertIn("POST /v1/security/tokens/revoke", endpoints)
        self.assertIn("POST /v1/security/tokens/rotate", endpoints)
        self.assertIn("POST /v1/security/keys/rotate", endpoints)
        self.assertIn("POST /v1/messages/verify", endpoints)
        self.assertIn("GET /v1/metrics", endpoints)
        self.assertIn("POST /v1/replay/messages", endpoints)
        self.assertIn("POST /v1/replication/pull", endpoints)
        self.assertIn("POST /v1/replication/push", endpoints)
        self.assertIn("GET /v1/contracts", endpoints)
        self.assertIn("GET /v1/governance/policy", endpoints)
        self.assertIn("POST /v1/peers/{peer_id}/trust", endpoints)
        self.assertIn("POST /v1/backup/create", endpoints)
        self.assertIn("POST /v1/backup/restore-test", endpoints)
        self.assertIn("GET /v1/ops/catalog", endpoints)
        self.assertIn("GET /v1/ops/status", endpoints)
        self.assertIn("POST /v1/ops/run", endpoints)
        self.assertIn("GET /v1/ops/schedule/export", endpoints)


if __name__ == "__main__":
    unittest.main()
