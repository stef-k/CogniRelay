"""Runtime onboarding and validation-limit help tests for issue #243."""

from __future__ import annotations

import re
import unittest

from fastapi.testclient import TestClient

from app.constants import (
    CONTEXT_RETRIEVE_DEFAULT_MAX_TOKENS,
    CONTEXT_RETRIEVE_MAX_MAX_TOKENS,
    CONTEXT_RETRIEVE_MIN_MAX_TOKENS,
)
from app.continuity.constants import CAPSULE_SIZE_LIMIT_BYTES, CAPSULE_SIZE_LIMIT_LABEL, PATCH_ALL_TARGETS, PATCH_MAX_OPERATIONS
from app.help.service import onboarding_section_ids, validation_limit_field_paths
from app.main import app


SECTION_IDS = [
    "bootstrap",
    "hooks",
    "help_lookup",
    "limits_and_routing",
    "workflow_rules",
    "retrieval",
    "trust_and_degradation",
    "examples",
    "anti_patterns",
    "references",
]

PRIORITY_FIELD_PATHS = [
    "continuity.top_priorities",
    "continuity.open_loops",
    "continuity.active_constraints",
    "continuity.session_trajectory",
    "continuity.negative_decisions",
    "continuity.rationale_entries",
    "continuity.related_documents",
    "continuity.stance_summary",
    "session_end_snapshot.open_loops",
    "session_end_snapshot.top_priorities",
    "session_end_snapshot.active_constraints",
    "session_end_snapshot.stance_summary",
    "session_end_snapshot.negative_decisions",
    "session_end_snapshot.session_trajectory",
    "session_end_snapshot.rationale_entries",
    "patch.operations",
    "patch.target.continuity.open_loops",
    "patch.target.continuity.top_priorities",
    "patch.target.continuity.active_constraints",
    "patch.target.continuity.active_concerns",
    "patch.target.continuity.drift_signals",
    "patch.target.continuity.working_hypotheses",
    "patch.target.continuity.long_horizon_commitments",
    "patch.target.continuity.session_trajectory",
    "patch.target.continuity.trailing_notes",
    "patch.target.continuity.curiosity_queue",
    "patch.target.continuity.negative_decisions",
    "patch.target.continuity.rationale_entries",
    "patch.target.stable_preferences",
    "patch.target.thread_descriptor.keywords",
    "patch.target.thread_descriptor.scope_anchors",
    "patch.target.thread_descriptor.identity_anchors",
    "context.retrieve.max_tokens_estimate",
    "context.retrieve.continuity_max_capsules",
    "continuity.capsule_serialized_utf8",
]


class TestHelp243RuntimeOnboardingLimits(unittest.TestCase):
    """Validate bounded HTTP onboarding and limits help surfaces."""

    @classmethod
    def setUpClass(cls) -> None:
        cls._client_context = TestClient(app)
        cls.client = cls._client_context.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._client_context.__exit__(None, None, None)

    def test_onboarding_index_is_bounded_and_ordered(self) -> None:
        response = self.client.get("/v1/help/onboarding")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(list(payload), ["kind", "recommended_first_section", "sections"])
        self.assertEqual(payload["kind"], "onboarding_index")
        self.assertEqual(payload["recommended_first_section"], "bootstrap")
        self.assertEqual([section["id"] for section in payload["sections"]], SECTION_IDS)
        for section in payload["sections"]:
            self.assertEqual(list(section), ["id", "title", "purpose", "when_to_use", "http_path", "mcp_method"])
            self.assertNotIn("body_md", section)
            self.assertEqual(section["mcp_method"], "system.onboarding_section")

    def test_onboarding_bootstrap_is_compact(self) -> None:
        response = self.client.get("/v1/help/onboarding/bootstrap")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(
            list(payload),
            [
                "kind",
                "recommended_first_section",
                "startup_route",
                "retrieval_route",
                "help_routes",
                "discover_more",
                "next_sections",
                "warnings",
            ],
        )
        self.assertEqual(payload["kind"], "onboarding_bootstrap")
        self.assertEqual(payload["startup_route"]["http"], "POST /v1/continuity/read")
        self.assertEqual(payload["startup_route"]["mcp_tool"], "continuity.read")
        self.assertEqual(payload["startup_route"]["params"], {"view": "startup", "allow_fallback": True})
        self.assertEqual(payload["next_sections"], ["hooks", "help_lookup", "limits_and_routing"])
        self.assertNotIn("body_md", payload)
        self.assertNotIn("field_paths", payload)

    def test_each_onboarding_section_returns_only_one_bounded_section(self) -> None:
        for section_id in SECTION_IDS:
            with self.subTest(section_id=section_id):
                response = self.client.get(f"/v1/help/onboarding/sections/{section_id}")
                self.assertEqual(response.status_code, 200)
                payload = response.json()
                self.assertEqual(
                    list(payload),
                    ["kind", "id", "title", "format", "body_md", "bullets", "related_http", "related_mcp", "references"],
                )
                self.assertEqual(payload["kind"], "onboarding_section")
                self.assertEqual(payload["id"], section_id)
                self.assertEqual(payload["format"], "markdown_and_bullets")
                self.assertTrue(payload["body_md"].startswith(f"## {payload['title']}"))
                self.assertLess(len(payload["body_md"]), 2000)
                self.assertNotIn("# Agent Onboarding", payload["body_md"])
                self.assertTrue(payload["bullets"])

    def test_invalid_onboarding_section_uses_exact_validation_body(self) -> None:
        response = self.client.get("/v1/help/onboarding/sections/bad-id", follow_redirects=False)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {
                "error": {
                    "code": "validation",
                    "detail": "Unsupported onboarding section id.",
                    "validation_hints": [
                        {
                            "field": "id",
                            "area": "request.path",
                            "reason": "unsupported_value",
                            "limit": None,
                            "allowed_values": SECTION_IDS,
                            "correction_hint": "Use one of the onboarding section ids returned by GET /v1/help/onboarding.",
                        }
                    ],
                }
            },
        )

    def test_limits_index_groups_priority_first_and_no_full_details(self) -> None:
        response = self.client.get("/v1/help/limits")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(list(payload), ["kind", "groups", "field_paths"])
        self.assertEqual(payload["kind"], "validation_limits_index")
        self.assertEqual([group["id"] for group in payload["groups"]], [
            "continuity_orientation",
            "session_end_snapshot",
            "patch_targets",
            "retrieval_budget",
            "capsule_write_cap",
            "continuity_payload",
        ])
        self.assertEqual(payload["field_paths"][: len(PRIORITY_FIELD_PATHS)], PRIORITY_FIELD_PATHS)
        self.assertEqual(payload["field_paths"][len(PRIORITY_FIELD_PATHS) :], sorted(payload["field_paths"][len(PRIORITY_FIELD_PATHS) :]))
        self.assertNotIn("subfield_limits", str(payload["groups"]))
        self.assertNotIn("correction_guidance", str(payload))

    def test_representative_limit_items_and_runtime_constants(self) -> None:
        cases = {
            "continuity.stance_summary": ("string", 240, None, None),
            "continuity.top_priorities": ("string_list", None, 8, 160),
            "continuity.negative_decisions": ("object_list", None, 4, None),
            "continuity.source.update_reason": ("enum", None, None, None),
            "continuity.confidence.continuity": ("number", None, None, None),
            "patch.operations": ("operation_list", None, PATCH_MAX_OPERATIONS, None),
            "context.retrieve.max_tokens_estimate": ("integer_budget", None, None, None),
            "continuity.capsule_serialized_utf8": ("serialized_bytes", CAPSULE_SIZE_LIMIT_BYTES, None, None),
            "continuity.metadata": ("object", None, 12, None),
            "patch.target.thread_descriptor.scope_anchors": ("string_list", None, 4, None),
        }
        for field_path, (value_type, max_length, max_items, per_item_max_length) in cases.items():
            with self.subTest(field_path=field_path):
                response = self.client.get(f"/v1/help/limits/{field_path}")
                self.assertEqual(response.status_code, 200)
                payload = response.json()
                self.assertEqual(list(payload), ["kind", "limit"])
                item = payload["limit"]
                self.assertEqual(
                    list(item),
                    [
                        "field_path",
                        "category",
                        "value_type",
                        "max_items",
                        "max_length",
                        "per_item_max_length",
                        "subfield_limits",
                        "applies_to",
                        "correction_guidance",
                        "reference",
                    ],
                )
                self.assertEqual(item["field_path"], field_path)
                self.assertEqual(item["value_type"], value_type)
                self.assertEqual(item["max_length"], max_length)
                self.assertEqual(item["max_items"], max_items)
                self.assertEqual(item["per_item_max_length"], per_item_max_length)

        budget = self.client.get("/v1/help/limits/context.retrieve.max_tokens_estimate").json()["limit"]
        self.assertEqual(
            budget["subfield_limits"],
            {
                "default": CONTEXT_RETRIEVE_DEFAULT_MAX_TOKENS,
                "minimum": CONTEXT_RETRIEVE_MIN_MAX_TOKENS,
                "maximum": CONTEXT_RETRIEVE_MAX_MAX_TOKENS,
            },
        )
        capsule = self.client.get("/v1/help/limits/continuity.capsule_serialized_utf8").json()["limit"]
        self.assertEqual(capsule["subfield_limits"]["label"], CAPSULE_SIZE_LIMIT_LABEL)
        self.assertEqual(capsule["reference"], "app.continuity.constants.CAPSULE_SIZE_LIMIT_BYTES")
        patch_targets = {path.removeprefix("patch.target.") for path in validation_limit_field_paths() if path.startswith("patch.target.")}
        self.assertEqual(patch_targets, PATCH_ALL_TARGETS)

    def test_invalid_limit_lookup_and_aliases_are_rejected(self) -> None:
        response = self.client.get("/v1/help/limits/bad.path", follow_redirects=False)
        self.assertEqual(response.status_code, 400)
        body = response.json()
        self.assertEqual(body["error"]["detail"], "Unsupported validation limit field path.")
        self.assertEqual(body["error"]["validation_hints"][0]["allowed_values"], validation_limit_field_paths())
        for alias in ("top_priorities", "capsule.continuity.top_priorities", "continuity.patch.open_loops", "continuity/top_priorities"):
            with self.subTest(alias=alias):
                alias_response = self.client.get(f"/v1/help/limits/{alias}", follow_redirects=False)
                self.assertEqual(alias_response.status_code, 400)

    def test_new_help_slash_aliases_return_direct_404(self) -> None:
        for path in (
            "/v1/help/onboarding/",
            "/v1/help/onboarding/bootstrap/",
            "/v1/help/onboarding/sections/bootstrap/",
            "/v1/help/limits/",
            "/v1/help/limits/continuity.top_priorities/",
        ):
            with self.subTest(path=path):
                response = self.client.get(path, follow_redirects=False)
                self.assertEqual(response.status_code, 404)
                self.assertNotIn("location", response.headers)

    def test_docs_agent_onboarding_mentions_runtime_lookup_without_duplication(self) -> None:
        text = open("docs/agent-onboarding.md", encoding="utf-8").read()
        self.assertIn("GET /v1/help/onboarding", text)
        self.assertIn("system.onboarding_index", text)
        self.assertIn("GET /v1/help/limits/{field_path}", text)
        self.assertNotIn("future machine-facing onboarding/help surface is out of scope", text)
        for block in re.findall(r"```json(.*?)```", text, flags=re.DOTALL):
            self.assertNotRegex(block, r"onboarding_index|onboarding_bootstrap|onboarding_section|validation_limits_index|validation_limit")
        self.assertNotRegex(text, r"\|[^\n]*field_path[^\n]*max_items")
        field_path_mentions = set(re.findall(r"`((?:continuity|session_end_snapshot|patch|context\.retrieve)\.[a-zA-Z0-9_.]+)`", text))
        self.assertLessEqual(len(field_path_mentions), 20)

    def test_onboarding_runtime_constants_align_with_doc_anchors(self) -> None:
        text = open("docs/agent-onboarding.md", encoding="utf-8").read()
        self.assertEqual(onboarding_section_ids(), SECTION_IDS)
        anchors = {
            "bootstrap": ["POST /v1/continuity/read", 'view="startup"', "allow_fallback=true"],
            "hooks": ["startup", "pre_prompt", "post_prompt", "pre_compaction_or_handoff"],
            "help_lookup": ["GET /v1/help", "GET /v1/help/tools/{name}", "GET /v1/help/topics/{id}", "GET /v1/help/hooks", "GET /v1/help/errors/{code}", "system.tool_usage", "system.topic_help", "system.hook_guide", "system.error_guide"],
            "limits_and_routing": ["continuity.top_priorities", "continuity.open_loops", "continuity.active_constraints", "GET /v1/help/limits/{field_path}"],
            "retrieval": ["POST /v1/context/retrieve", "max_tokens_estimate", "continuity_max_capsules"],
            "trust_and_degradation": ["warnings", "allow_fallback", "degraded"],
            "anti_patterns": ["Do not", "full onboarding document", "full payload schema"],
            "references": ["docs/api-surface.md", "docs/mcp.md", "docs/payload-reference.md"],
        }
        for section_id, phrases in anchors.items():
            with self.subTest(section_id=section_id):
                section = self.client.get(f"/v1/help/onboarding/sections/{section_id}").json()
                self.assertTrue(any(ref.startswith("docs/agent-onboarding.md") for ref in section["references"]))
                for phrase in phrases:
                    self.assertIn(phrase, text)


if __name__ == "__main__":
    unittest.main()
