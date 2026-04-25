"""Runtime onboarding and validation-limit help tests for issue #243."""

from __future__ import annotations

import re
import unittest
from pathlib import Path
from typing import Any, Literal, get_args, get_origin

from fastapi.testclient import TestClient

from app.constants import (
    CONTEXT_RETRIEVE_DEFAULT_MAX_TOKENS,
    CONTEXT_RETRIEVE_MAX_MAX_TOKENS,
    CONTEXT_RETRIEVE_MIN_MAX_TOKENS,
)
from app.continuity.constants import (
    CAPSULE_SIZE_LIMIT_BYTES,
    CAPSULE_SIZE_LIMIT_LABEL,
    CONTINUITY_INTERACTION_BOUNDARY_KINDS,
    PATCH_ALL_TARGETS,
    PATCH_MAX_OPERATIONS,
    PATCH_STRUCTURED_MATCH_KEYS,
    PATCH_TARGET_MAX_LENGTH,
)
from app.continuity.validation import related_documents_limit_fixture
from app.help.service import onboarding_section_ids, validation_limit_field_paths
from app.main import app
from app.models import (
    ContextRetrieveRequest,
    ContinuityAttentionPolicy,
    ContinuityCapsule,
    ContinuityConfidence,
    ContinuityFreshness,
    ContinuityPatchRequest,
    ContinuityRelationshipModel,
    ContinuityRetrievalHints,
    ContinuitySelector,
    ContinuitySource,
    ContinuityState,
    ContinuityUpsertRequest,
    SessionEndSnapshot,
    StablePreference,
    ThreadDescriptor,
)


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
    "context.retrieve.graph_context.nodes",
    "context.retrieve.graph_context.edges",
    "context.retrieve.graph_context.related_documents",
    "context.retrieve.graph_context.blockers",
    "continuity.read.startup.graph_summary.nodes",
    "continuity.read.startup.graph_summary.edges",
    "continuity.read.startup.graph_summary.related_documents",
    "continuity.read.startup.graph_summary.blockers",
    "continuity.capsule_serialized_utf8",
]


MODEL_DERIVED_FIELD_LIMITS = {
    "context.retrieve.continuity_max_capsules": (ContextRetrieveRequest, "continuity_max_capsules", "integer_budget", None),
    "context.retrieve.continuity_mode": (ContextRetrieveRequest, "continuity_mode", "enum", None),
    "context.retrieve.continuity_resilience_policy": (ContextRetrieveRequest, "continuity_resilience_policy", "enum", None),
    "context.retrieve.continuity_selectors": (ContextRetrieveRequest, "continuity_selectors", "object_list", None),
    "context.retrieve.continuity_selectors.subject_id": (ContinuitySelector, "subject_id", "string", None),
    "context.retrieve.continuity_selectors.subject_kind": (ContinuitySelector, "subject_kind", "enum", None),
    "context.retrieve.continuity_verification_policy": (ContextRetrieveRequest, "continuity_verification_policy", "enum", None),
    "context.retrieve.limit": (ContextRetrieveRequest, "limit", "number", None),
    "context.retrieve.max_tokens_estimate": (ContextRetrieveRequest, "max_tokens_estimate", "integer_budget", None),
    "context.retrieve.subject_id": (ContextRetrieveRequest, "subject_id", "string", None),
    "context.retrieve.subject_kind": (ContextRetrieveRequest, "subject_kind", "enum", None),
    "context.retrieve.time_window_days": (ContextRetrieveRequest, "time_window_days", "number", None),
    "continuity.active_concerns": (ContinuityState, "active_concerns", "string_list", 160),
    "continuity.active_constraints": (ContinuityState, "active_constraints", "string_list", 160),
    "continuity.attention_policy.early_load": (ContinuityAttentionPolicy, "early_load", "string_list", None),
    "continuity.attention_policy.presence_bias_overrides": (ContinuityAttentionPolicy, "presence_bias_overrides", "string_list", 160),
    "continuity.canonical_sources": (ContinuityCapsule, "canonical_sources", "string_list", None),
    "continuity.confidence.continuity": (ContinuityConfidence, "continuity", "number", None),
    "continuity.confidence.relationship_model": (ContinuityConfidence, "relationship_model", "number", None),
    "continuity.curiosity_queue": (ContinuityState, "curiosity_queue", "string_list", 120),
    "continuity.drift_signals": (ContinuityState, "drift_signals", "string_list", 160),
    "continuity.freshness.freshness_class": (ContinuityFreshness, "freshness_class", "enum", None),
    "continuity.freshness.stale_after_seconds": (ContinuityFreshness, "stale_after_seconds", "number", None),
    "continuity.long_horizon_commitments": (ContinuityState, "long_horizon_commitments", "string_list", 160),
    "continuity.negative_decisions": (ContinuityState, "negative_decisions", "object_list", None),
    "continuity.open_loops": (ContinuityState, "open_loops", "string_list", 160),
    "continuity.rationale_entries": (ContinuityState, "rationale_entries", "object_list", None),
    "continuity.relationship_model.preferred_style": (ContinuityRelationshipModel, "preferred_style", "string_list", 80),
    "continuity.relationship_model.sensitivity_notes": (ContinuityRelationshipModel, "sensitivity_notes", "string_list", 120),
    "continuity.relationship_model.trust_level": (ContinuityRelationshipModel, "trust_level", "enum", None),
    "continuity.retrieval_hints.avoid": (ContinuityRetrievalHints, "avoid", "string_list", 160),
    "continuity.retrieval_hints.load_next": (ContinuityRetrievalHints, "load_next", "string_list", None),
    "continuity.retrieval_hints.must_include": (ContinuityRetrievalHints, "must_include", "string_list", 160),
    "continuity.schema_version": (ContinuityCapsule, "schema_version", "enum", None),
    "continuity.session_trajectory": (ContinuityState, "session_trajectory", "string_list", 80),
    "continuity.source.inputs": (ContinuitySource, "inputs", "string_list", 200),
    "continuity.source.producer": (ContinuitySource, "producer", "string", None),
    "continuity.source.update_reason": (ContinuitySource, "update_reason", "enum", None),
    "continuity.stance_summary": (ContinuityState, "stance_summary", "string", None),
    "continuity.subject_id": (ContinuityCapsule, "subject_id", "string", None),
    "continuity.subject_kind": (ContinuityCapsule, "subject_kind", "enum", None),
    "continuity.thread_descriptor.identity_anchors": (ThreadDescriptor, "identity_anchors", "object_list", None),
    "continuity.thread_descriptor.keywords": (ThreadDescriptor, "keywords", "string_list", 40),
    "continuity.thread_descriptor.label": (ThreadDescriptor, "label", "string", None),
    "continuity.thread_descriptor.scope_anchors": (ThreadDescriptor, "scope_anchors", "string_list", None),
    "continuity.top_priorities": (ContinuityState, "top_priorities", "string_list", 160),
    "continuity.trailing_notes": (ContinuityState, "trailing_notes", "string_list", 160),
    "continuity.upsert.commit_message": (ContinuityUpsertRequest, "commit_message", "string", None),
    "continuity.upsert.idempotency_key": (ContinuityUpsertRequest, "idempotency_key", "string", None),
    "continuity.upsert.lifecycle_transition": (ContinuityUpsertRequest, "lifecycle_transition", "enum", None),
    "continuity.upsert.merge_mode": (ContinuityUpsertRequest, "merge_mode", "enum", None),
    "continuity.upsert.subject_id": (ContinuityUpsertRequest, "subject_id", "string", None),
    "continuity.upsert.subject_kind": (ContinuityUpsertRequest, "subject_kind", "enum", None),
    "continuity.upsert.superseded_by": (ContinuityUpsertRequest, "superseded_by", "string", None),
    "continuity.verification_kind": (ContinuityCapsule, "verification_kind", "enum", None),
    "continuity.working_hypotheses": (ContinuityState, "working_hypotheses", "string_list", 160),
    "continuity.patch.commit_message": (ContinuityPatchRequest, "commit_message", "string", None),
    "continuity.patch.subject_id": (ContinuityPatchRequest, "subject_id", "string", None),
    "continuity.patch.subject_kind": (ContinuityPatchRequest, "subject_kind", "enum", None),
    "session_end_snapshot.active_constraints": (SessionEndSnapshot, "active_constraints", "string_list", 160),
    "session_end_snapshot.negative_decisions": (SessionEndSnapshot, "negative_decisions", "object_list", None),
    "session_end_snapshot.open_loops": (SessionEndSnapshot, "open_loops", "string_list", 160),
    "session_end_snapshot.rationale_entries": (SessionEndSnapshot, "rationale_entries", "object_list", None),
    "session_end_snapshot.session_trajectory": (SessionEndSnapshot, "session_trajectory", "string_list", 80),
    "session_end_snapshot.stance_summary": (SessionEndSnapshot, "stance_summary", "string", None),
    "session_end_snapshot.top_priorities": (SessionEndSnapshot, "top_priorities", "string_list", 160),
}


def _literal_values(model: type, field_name: str) -> list[str]:
    annotation = model.model_fields[field_name].annotation
    if get_origin(annotation) is Literal:
        return list(get_args(annotation))
    for arg in get_args(annotation):
        if get_origin(arg) is Literal:
            return list(get_args(arg))
    raise AssertionError(f"{model.__name__}.{field_name} is not a Literal field")


def _field_constraint(model: type, field_name: str, attr: str) -> object:
    for metadata in model.model_fields[field_name].metadata:
        if hasattr(metadata, attr):
            return getattr(metadata, attr)
    raise AssertionError(f"{model.__name__}.{field_name} has no {attr} constraint")


def _optional_field_constraint(model: type, field_name: str, attr: str) -> object:
    for metadata in model.model_fields[field_name].metadata:
        if hasattr(metadata, attr):
            return getattr(metadata, attr)
    return None


def _expected_model_metadata(model: type, field_name: str, value_type: str, per_item_max_length: int | None) -> dict[str, Any]:
    expected: dict[str, Any] = {
        "max_items": _optional_field_constraint(model, field_name, "max_length") if value_type in {"string_list", "object_list"} else None,
        "max_length": _optional_field_constraint(model, field_name, "max_length") if value_type in {"string", "serialized_bytes"} else None,
        "per_item_max_length": per_item_max_length,
        "subfield_limits": {},
    }
    if value_type == "string":
        min_length = _optional_field_constraint(model, field_name, "min_length")
        if min_length is not None:
            expected["subfield_limits"]["min_length"] = min_length
    if value_type in {"number", "integer_budget"}:
        minimum = _optional_field_constraint(model, field_name, "ge")
        maximum = _optional_field_constraint(model, field_name, "le")
        if value_type == "integer_budget":
            expected["subfield_limits"]["default"] = model.model_fields[field_name].default
        if minimum is not None:
            expected["subfield_limits"]["minimum"] = minimum
        if maximum is not None:
            expected["subfield_limits"]["maximum"] = maximum
    if value_type == "enum":
        expected["subfield_limits"]["allowed_values"] = _literal_values(model, field_name)
    return expected


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
            "response_orientation_caps",
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
            "continuity.patch.updated_at": ("string", None, None, None),
            "continuity.source.update_reason": ("enum", None, None, None),
            "continuity.verification_kind": ("enum", None, None, None),
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
        self.assertEqual(budget["subfield_limits"], _expected_model_metadata(ContextRetrieveRequest, "max_tokens_estimate", "integer_budget", None)["subfield_limits"])
        continuity_capsules = self.client.get("/v1/help/limits/context.retrieve.continuity_max_capsules").json()["limit"]
        self.assertEqual(
            continuity_capsules["subfield_limits"],
            _expected_model_metadata(ContextRetrieveRequest, "continuity_max_capsules", "integer_budget", None)["subfield_limits"],
        )
        capsule = self.client.get("/v1/help/limits/continuity.capsule_serialized_utf8").json()["limit"]
        self.assertEqual(capsule["subfield_limits"]["label"], CAPSULE_SIZE_LIMIT_LABEL)
        self.assertEqual(capsule["reference"], "app.continuity.constants.CAPSULE_SIZE_LIMIT_BYTES")
        patch_targets = {path.removeprefix("patch.target.") for path in validation_limit_field_paths() if path.startswith("patch.target.")}
        self.assertEqual(patch_targets, PATCH_ALL_TARGETS)

    def test_model_derived_limit_values_match_pydantic_metadata(self) -> None:
        self.assertLessEqual(set(MODEL_DERIVED_FIELD_LIMITS), set(validation_limit_field_paths()))
        for field_path, (model, field_name, value_type, per_item_max_length) in MODEL_DERIVED_FIELD_LIMITS.items():
            with self.subTest(field_path=field_path):
                emitted = self.client.get(f"/v1/help/limits/{field_path}").json()["limit"]
                expected = _expected_model_metadata(model, field_name, value_type, per_item_max_length)
                self.assertEqual(emitted["max_items"], expected["max_items"])
                self.assertEqual(emitted["max_length"], expected["max_length"])
                self.assertEqual(emitted["per_item_max_length"], expected["per_item_max_length"])
                for key, value in expected["subfield_limits"].items():
                    self.assertEqual(emitted["subfield_limits"].get(key), value)

        for field_path, model, field_name in (
            ("continuity.source.producer", ContinuitySource, "producer"),
            ("continuity.thread_descriptor.label", ThreadDescriptor, "label"),
            ("continuity.patch.subject_id", ContinuityPatchRequest, "subject_id"),
        ):
            with self.subTest(min_length_path=field_path):
                emitted = self.client.get(f"/v1/help/limits/{field_path}").json()["limit"]
                self.assertEqual(emitted["subfield_limits"]["min_length"], _field_constraint(model, field_name, "min_length"))

    def test_related_documents_limits_match_validation_fixture(self) -> None:
        emitted = self.client.get("/v1/help/limits/continuity.related_documents").json()["limit"]
        fixture = related_documents_limit_fixture()
        self.assertEqual(emitted["max_items"], fixture["max_items"])
        self.assertEqual(emitted["subfield_limits"], fixture["subfield_limits"])
        self.assertEqual(fixture["max_items"], 8)
        self.assertEqual(fixture["subfield_limits"]["path"]["max_length"], 240)
        self.assertEqual(fixture["subfield_limits"]["kind"]["max_length"], 32)
        self.assertEqual(fixture["subfield_limits"]["label"]["max_length"], 120)
        self.assertEqual(fixture["subfield_limits"]["relevance"]["max_length"], 32)
        self.assertEqual(set(fixture["subfield_limits"]["relevance"]["allowed_values"]), {"primary", "supporting", "background"})
        self.assertEqual(fixture["subfield_limits"]["required"], ["path", "kind", "label"])
        self.assertIs(fixture["subfield_limits"]["additional_properties"], False)
        self.assertEqual(
            set(fixture["subfield_limits"]["reserved_embedded_content_keys"]),
            {"content", "body", "text", "excerpt", "markdown", "html", "payload"},
        )

    def test_validation_limit_coverage_matches_runtime_model_and_service_truth(self) -> None:
        for model, fields in {
            ContinuityUpsertRequest: {
                "subject_id",
                "subject_kind",
                "commit_message",
                "idempotency_key",
                "lifecycle_transition",
                "merge_mode",
                "superseded_by",
            },
            ContinuityPatchRequest: {"subject_id", "subject_kind", "updated_at", "commit_message"},
            ContextRetrieveRequest: {
                "subject_id",
                "subject_kind",
                "continuity_mode",
                "continuity_verification_policy",
                "continuity_resilience_policy",
                "continuity_selectors",
                "continuity_max_capsules",
                "max_tokens_estimate",
                "time_window_days",
                "limit",
            },
            ContinuityCapsule: {
                "schema_version",
                "subject_id",
                "subject_kind",
                "verification_kind",
                "source",
                "continuity",
                "confidence",
                "attention_policy",
                "freshness",
                "canonical_sources",
                "metadata",
                "stable_preferences",
                "thread_descriptor",
            },
            SessionEndSnapshot: {
                "open_loops",
                "top_priorities",
                "active_constraints",
                "stance_summary",
                "negative_decisions",
                "session_trajectory",
                "rationale_entries",
            },
        }.items():
            with self.subTest(model=model.__name__):
                self.assertLessEqual(fields, set(model.model_fields))

        request_field_paths = {
            f"continuity.upsert.{field}"
            for field in (
                "subject_id",
                "subject_kind",
                "commit_message",
                "idempotency_key",
                "lifecycle_transition",
                "merge_mode",
                "superseded_by",
            )
        } | {
            f"continuity.patch.{field}"
            for field in ("subject_id", "subject_kind", "updated_at", "commit_message")
        } | {
            f"context.retrieve.{field}"
            for field in (
                "subject_id",
                "subject_kind",
                "continuity_mode",
                "continuity_verification_policy",
                "continuity_resilience_policy",
                "continuity_selectors",
                "time_window_days",
                "limit",
            )
        } | {
            f"context.retrieve.continuity_selectors.{field}" for field in ContinuitySelector.model_fields
        }
        priority_capsule_fields = {
            "top_priorities",
            "open_loops",
            "active_constraints",
            "session_trajectory",
            "negative_decisions",
            "rationale_entries",
            "related_documents",
            "stance_summary",
        }
        continuity_state_field_paths = {
            f"continuity.{field}"
            for field in ContinuityState.model_fields
            if field not in priority_capsule_fields | {"relationship_model", "retrieval_hints"}
        }
        capsule_model_field_paths = {
            f"continuity.{field}"
            for field in ("schema_version", "subject_id", "subject_kind", "updated_at", "verified_at", "canonical_sources", "verification_kind")
        }
        nested_model_field_paths = (
            {f"continuity.source.{field}" for field in ContinuitySource.model_fields}
            | {f"continuity.confidence.{field}" for field in ContinuityConfidence.model_fields}
            | {f"continuity.attention_policy.{field}" for field in ContinuityAttentionPolicy.model_fields}
            | {f"continuity.freshness.{field}" for field in ContinuityFreshness.model_fields}
            | {f"continuity.relationship_model.{field}" for field in ContinuityRelationshipModel.model_fields}
            | {f"continuity.retrieval_hints.{field}" for field in ContinuityRetrievalHints.model_fields}
            | {f"continuity.thread_descriptor.{field}" for field in ThreadDescriptor.model_fields if field not in {"lifecycle", "superseded_by"}}
        )
        service_fixture_paths = {"continuity.metadata", "continuity.stable_preferences"}
        self.assertIn("tag", StablePreference.model_fields)
        expected_additional = request_field_paths | continuity_state_field_paths | capsule_model_field_paths | nested_model_field_paths | service_fixture_paths
        paths = validation_limit_field_paths()
        self.assertEqual(paths[: len(PRIORITY_FIELD_PATHS)], PRIORITY_FIELD_PATHS)
        self.assertEqual(paths[len(PRIORITY_FIELD_PATHS) :], sorted(expected_additional))
        self.assertEqual(set(paths), set(PRIORITY_FIELD_PATHS) | expected_additional)

        for stripped_or_managed_path in (
            "continuity.thread_descriptor.lifecycle",
            "continuity.thread_descriptor.superseded_by",
            "continuity.verification_state",
            "continuity.capsule_health",
        ):
            self.assertNotIn(stripped_or_managed_path, paths)

        metadata = self.client.get("/v1/help/limits/continuity.metadata").json()["limit"]
        expected_boundary_kinds = [
            value
            for value in ("person_switch", "thread_switch", "task_switch", "public_reply", "manual_checkpoint")
            if value in CONTINUITY_INTERACTION_BOUNDARY_KINDS
        ]
        self.assertEqual(metadata["subfield_limits"]["interaction_boundary_kind"]["allowed_values"], expected_boundary_kinds)
        self.assertEqual(set(expected_boundary_kinds), CONTINUITY_INTERACTION_BOUNDARY_KINDS)

        early_load = self.client.get("/v1/help/limits/continuity.attention_policy.early_load").json()["limit"]
        self.assertEqual(early_load["max_items"], _field_constraint(ContinuityAttentionPolicy, "early_load", "max_length"))
        self.assertIsNone(early_load["per_item_max_length"])

        selector_subject_kind = self.client.get("/v1/help/limits/context.retrieve.continuity_selectors.subject_kind").json()["limit"]
        self.assertEqual(selector_subject_kind["subfield_limits"]["allowed_values"], _literal_values(ContinuitySelector, "subject_kind"))

        for timestamp_path in ("continuity.updated_at", "continuity.verified_at", "continuity.freshness.expires_at"):
            with self.subTest(timestamp_path=timestamp_path):
                timestamp_limit = self.client.get(f"/v1/help/limits/{timestamp_path}").json()["limit"]
                self.assertEqual(
                    timestamp_limit["subfield_limits"],
                    {"require_utc_timestamp": True, "deterministic": True, "timezone": "UTC"},
                )

        top_priorities = self.client.get("/v1/help/limits/continuity.top_priorities").json()["limit"]
        self.assertEqual(top_priorities["max_items"], _field_constraint(ContinuityState, "top_priorities", "max_length"))
        patch_top_priorities = self.client.get("/v1/help/limits/patch.target.continuity.top_priorities").json()["limit"]
        self.assertEqual(patch_top_priorities["max_items"], PATCH_TARGET_MAX_LENGTH["continuity.top_priorities"])

        patch_updated_at = self.client.get("/v1/help/limits/continuity.patch.updated_at").json()["limit"]
        self.assertIn("updated_at", ContinuityPatchRequest.model_fields)
        self.assertEqual(patch_updated_at["subfield_limits"], {"require_utc_timestamp": True, "deterministic": True, "timezone": "UTC"})
        self.assertEqual(
            patch_updated_at["correction_guidance"],
            'Use an explicit deterministic UTC timestamp and retry with field_path "continuity.patch.updated_at".',
        )
        self.assertEqual(patch_updated_at["applies_to"], ["POST /v1/continuity/patch", "continuity.patch"])

        verification_kind = self.client.get("/v1/help/limits/continuity.verification_kind").json()["limit"]
        self.assertEqual(verification_kind["subfield_limits"]["allowed_values"], _literal_values(ContinuityCapsule, "verification_kind"))
        stable_preferences = self.client.get("/v1/help/limits/patch.target.stable_preferences").json()["limit"]
        self.assertEqual(stable_preferences["subfield_limits"]["match_key"], PATCH_STRUCTURED_MATCH_KEYS["stable_preferences"])

    def test_correction_guidance_matches_exact_templates_by_value_type(self) -> None:
        cases = {
            "continuity.stance_summary": (
                'Shorten this value to at most 240 characters and retry with field_path "continuity.stance_summary".'
            ),
            "continuity.top_priorities": (
                "Keep at most 8 items, shorten each item to at most 160 characters, "
                'and retry with field_path "continuity.top_priorities".'
            ),
            "continuity.attention_policy.early_load": (
                'Keep at most 8 items and retry with field_path "continuity.attention_policy.early_load".'
            ),
            "patch.target.thread_descriptor.scope_anchors": (
                "Keep at most 4 items, make each item match the documented pattern and subfield metadata, "
                'and retry with field_path "patch.target.thread_descriptor.scope_anchors".'
            ),
            "continuity.related_documents": (
                "Keep at most 8 items, apply the documented subfield limits, "
                'and retry with field_path "continuity.related_documents".'
            ),
            "patch.operations": (
                'Send between 1 and 10 patch operations and retry with field_path "patch.operations".'
            ),
            "context.retrieve.max_tokens_estimate": (
                'Use a value between 256 and 100000 and retry with field_path "context.retrieve.max_tokens_estimate".'
            ),
            "continuity.capsule_serialized_utf8": (
                "Reduce the serialized capsule below 20 KB (20480 bytes) "
                'and retry with field_path "continuity.capsule_serialized_utf8".'
            ),
            "continuity.verification_kind": (
                'Use one of the allowed values in subfield_limits and retry with field_path "continuity.verification_kind".'
            ),
            "continuity.confidence.continuity": (
                'Use a value within the documented numeric bounds and retry with field_path "continuity.confidence.continuity".'
            ),
            "continuity.metadata": (
                'Apply the documented nested field limits and retry with field_path "continuity.metadata".'
            ),
        }
        for field_path, expected_guidance in cases.items():
            with self.subTest(field_path=field_path):
                item = self.client.get(f"/v1/help/limits/{field_path}").json()["limit"]
                self.assertEqual(item["correction_guidance"], expected_guidance)

    def test_timestamp_correction_guidance_matches_exact_template_for_every_timestamp_field(self) -> None:
        for field_path in (
            "continuity.updated_at",
            "continuity.verified_at",
            "continuity.freshness.expires_at",
            "continuity.patch.updated_at",
        ):
            with self.subTest(field_path=field_path):
                item = self.client.get(f"/v1/help/limits/{field_path}").json()["limit"]
                self.assertEqual(
                    item["correction_guidance"],
                    f'Use an explicit deterministic UTC timestamp and retry with field_path "{field_path}".',
                )

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
        text = Path("docs/agent-onboarding.md").read_text(encoding="utf-8")
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
        text = Path("docs/agent-onboarding.md").read_text(encoding="utf-8")
        self.assertEqual(onboarding_section_ids(), SECTION_IDS)
        anchors = {
            "bootstrap": ["POST /v1/continuity/read", 'view="startup"', "allow_fallback=true", "schedule_context.due.items", "graph_summary"],
            "hooks": ["startup", "pre_prompt", "post_prompt", "pre_compaction_or_handoff"],
            "help_lookup": [
                "GET /v1/help",
                "GET /v1/help/tools/{name}",
                "GET /v1/help/topics/{id}",
                "GET /v1/help/hooks",
                "GET /v1/help/errors/{code}",
                "system.tool_usage",
                "system.topic_help",
                "system.hook_guide",
                "system.error_guide",
            ],
            "limits_and_routing": ["continuity.top_priorities", "continuity.open_loops", "continuity.active_constraints", "GET /v1/help/limits/{field_path}"],
            "retrieval": ["POST /v1/context/retrieve", "max_tokens_estimate", "continuity_max_capsules", "bundle.graph_context", "schedule_context"],
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

    def test_onboarding_runtime_help_mentions_shipped_graph_and_schedule_orientation(self) -> None:
        bootstrap = self.client.get("/v1/help/onboarding/sections/bootstrap").json()
        bootstrap_text = " ".join([bootstrap["body_md"], *bootstrap["bullets"]])
        self.assertIn("graph_summary", bootstrap_text)
        self.assertIn("schedule_context.due.items", bootstrap_text)

        retrieval = self.client.get("/v1/help/onboarding/sections/retrieval").json()
        retrieval_text = " ".join([retrieval["body_md"], *retrieval["bullets"]])
        self.assertIn("bundle.graph_context", retrieval_text)
        self.assertIn("schedule_context", retrieval_text)

        anti_patterns = self.client.get("/v1/help/onboarding/sections/anti_patterns").json()
        anti_pattern_text = " ".join([anti_patterns["body_md"], *anti_patterns["bullets"]])
        self.assertIn("Do not persist derived graph or schedule orientation into continuity capsules.", anti_pattern_text)


if __name__ == "__main__":
    unittest.main()
