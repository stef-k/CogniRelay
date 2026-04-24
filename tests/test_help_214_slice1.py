"""HTTP help baseline contract tests for issue #214 slice 1."""

import unittest

from fastapi.testclient import TestClient

from app.main import app
from app.models import ContinuityUpsertRequest


EXPECTED_ROOT = {
    "http_endpoints": [
        "GET /v1/help",
        "GET /v1/help/tools/{name}",
        "GET /v1/help/topics/{id}",
        "GET /v1/help/hooks",
        "GET /v1/help/errors/{code}",
        "GET /v1/help/onboarding",
        "GET /v1/help/onboarding/bootstrap",
        "GET /v1/help/onboarding/sections/{id}",
        "GET /v1/help/limits",
        "GET /v1/help/limits/{field_path}",
    ],
    "mcp_methods": [
        "system.help",
        "system.tool_usage",
        "system.topic_help",
        "system.hook_guide",
        "system.error_guide",
        "system.onboarding_index",
        "system.onboarding_bootstrap",
        "system.onboarding_section",
        "system.validation_limits",
        "system.validation_limit",
    ],
    "tool_topics": [
        "continuity.read",
        "continuity.upsert",
        "context.retrieve",
    ],
    "non_tool_topics": [
        "continuity.read.startup_view",
        "continuity.read.trust_signals",
        "continuity.upsert.session_end_snapshot",
    ],
    "hook_ids": [
        "startup",
        "pre_prompt",
        "post_prompt",
        "pre_compaction_or_handoff",
    ],
    "errors": [
        "validation",
        "tool_not_found",
        "unknown_help_topic",
    ],
}

EXPECTED_TOOLS = {
    "continuity.read": {
        "kind": "tool",
        "id": "continuity.read",
        "purpose": "Read continuity state for a subject.",
        "when_to_use": [
            "Use when the runtime needs persisted orientation for a subject.",
            "Use at session start when continuity is needed before prompting.",
        ],
        "read_operations": [
            "POST /v1/continuity/read",
            "continuity.read",
        ],
        "write_operations": [],
        "minimal_payload": {
            "subject_kind": "thread",
            "subject_id": "issue-214",
            "view": "startup",
            "allow_fallback": True,
        },
        "common_mistakes": [
            "Using a view value that is not defined by the continuity.read contract.",
            "Omitting subject_kind or subject_id.",
        ],
        "correction_hints": [
            "Use view: startup and allow_fallback: true for startup continuity guidance.",
            "Provide both subject_kind and subject_id.",
        ],
    },
    "continuity.upsert": {
        "kind": "tool",
        "id": "continuity.upsert",
        "purpose": "Create or update continuity state for a subject.",
        "when_to_use": [
            "Use when the runtime needs to persist an updated continuity capsule.",
            "Use at session end when storing a bounded snapshot for the next startup read.",
        ],
        "read_operations": [
            "POST /v1/continuity/read",
            "continuity.read",
        ],
        "write_operations": [
            "POST /v1/continuity/upsert",
            "continuity.upsert",
        ],
        "minimal_payload": {
            "subject_kind": "thread",
            "subject_id": "issue-214",
            "capsule": {
                "subject_kind": "thread",
                "subject_id": "issue-214",
                "updated_at": "2026-04-21T12:00:00Z",
                "verified_at": "2026-04-21T12:00:00Z",
                "source": {
                    "producer": "runtime-help",
                    "update_reason": "interaction_boundary",
                    "inputs": [],
                },
                "continuity": {
                    "top_priorities": [],
                    "active_concerns": [],
                    "active_constraints": [],
                    "open_loops": [],
                    "stance_summary": "Ready to continue issue 214 work.",
                    "drift_signals": [],
                },
                "confidence": {"continuity": 0.9, "relationship_model": 0.0},
            },
        },
        "common_mistakes": [
            "Sending a capsule without updated_at.",
            "Sending session_end_snapshot with fields outside the closed field set in this issue.",
        ],
        "correction_hints": [
            "Include updated_at in the capsule using an explicit UTC timestamp.",
            "Use only the session_end_snapshot fields closed by this issue.",
        ],
    },
    "context.retrieve": {
        "kind": "tool",
        "id": "context.retrieve",
        "purpose": "Retrieve a bounded context package for a task, thread, or subject.",
        "when_to_use": [
            "Use when the runtime needs a compact context package instead of a raw continuity capsule.",
            "Use before prompting when context retrieval is the contract-defined entrypoint.",
        ],
        "read_operations": [
            "POST /v1/context/retrieve",
            "context.retrieve",
        ],
        "write_operations": [],
        "minimal_payload": {
            "task": "Address determinism findings on issue #214 only.",
            "subject_kind": "thread",
            "subject_id": "issue-214",
            "continuity_mode": "required",
        },
        "common_mistakes": [
            "Using continuity.read fields as if they were context.retrieve fields.",
            "Persisting prompt text, retrieved snippets, or transcript material through context.retrieve.",
        ],
        "correction_hints": [
            "Use exactly task, subject_kind, subject_id, and continuity_mode in the minimal payload shape defined by this issue.",
            "Keep context.retrieve read-only and do not persist prompt or retrieval transcript material.",
        ],
    },
}

EXPECTED_TOPICS = {
    "continuity.read.startup_view": {
        "kind": "topic",
        "id": "continuity.read.startup_view",
        "purpose": "Explain the startup continuity view used to re-establish orientation.",
        "when_to_use": [
            "Use when selecting the startup view for continuity.read.",
            "Use when a runtime needs startup-oriented continuity guidance rather than a full raw capsule.",
        ],
        "read_operations": [
            "POST /v1/continuity/read",
            "continuity.read",
        ],
        "write_operations": [],
        "minimal_payload": {
            "subject_kind": "thread",
            "subject_id": "issue-214",
            "view": "startup",
            "allow_fallback": True,
        },
        "common_mistakes": [
            "Using startup_view as if it were a literal request value.",
            "Disabling allow_fallback when degraded startup continuity is acceptable.",
        ],
        "correction_hints": [
            "Use view: startup in the request payload.",
            "Set allow_fallback to true when fallback continuity is acceptable.",
        ],
    },
    "continuity.read.trust_signals": {
        "kind": "topic",
        "id": "continuity.read.trust_signals",
        "purpose": "Explain trust-oriented continuity signals surfaced by continuity.read.",
        "when_to_use": [
            "Use when interpreting trust signals returned with a continuity read.",
            "Use when a caller needs to distinguish healthy continuity from degraded continuity.",
        ],
        "read_operations": [
            "POST /v1/continuity/read",
            "continuity.read",
        ],
        "write_operations": [],
        "minimal_payload": {
            "subject_kind": "thread",
            "subject_id": "issue-214",
            "view": "startup",
            "allow_fallback": True,
        },
        "common_mistakes": [
            "Treating trust signals as a separate request field.",
            "Ignoring degraded trust signals when choosing the next runtime action.",
        ],
        "correction_hints": [
            "Read trust signals from the continuity.read response rather than inventing a request field.",
            "Use degraded trust signals to trigger a cautious or recovery-oriented next step.",
        ],
    },
    "continuity.upsert.session_end_snapshot": {
        "kind": "topic",
        "id": "continuity.upsert.session_end_snapshot",
        "purpose": "Explain the bounded session_end_snapshot helper for continuity.upsert.",
        "when_to_use": [
            "Use when persisting a startup-focused summary at session end.",
            "Use when the runtime needs to update startup-critical continuity fields without rebuilding the full capsule.",
        ],
        "read_operations": [
            "POST /v1/continuity/read",
            "continuity.read",
        ],
        "write_operations": [
            "POST /v1/continuity/upsert",
            "continuity.upsert",
        ],
        "minimal_payload": {
            "subject_kind": "thread",
            "subject_id": "issue-214",
            "capsule": {
                "subject_kind": "thread",
                "subject_id": "issue-214",
                "updated_at": "2026-04-21T12:00:00Z",
                "verified_at": "2026-04-21T12:00:00Z",
                "source": {
                    "producer": "runtime-help",
                    "update_reason": "interaction_boundary",
                    "inputs": [],
                },
                "continuity": {
                    "top_priorities": [],
                    "active_concerns": [],
                    "active_constraints": [],
                    "open_loops": [],
                    "stance_summary": "Ready to continue issue 214 work.",
                    "drift_signals": [],
                },
                "confidence": {"continuity": 0.9, "relationship_model": 0.0},
            },
            "session_end_snapshot": {
                "open_loops": [],
                "top_priorities": [],
                "active_constraints": [],
                "stance_summary": "Ready to continue issue 214 work.",
                "negative_decisions": [],
                "session_trajectory": [],
                "rationale_entries": [],
            },
        },
        "common_mistakes": [
            "Sending session_end_snapshot without a base capsule.",
            "Sending fields in session_end_snapshot that are outside the closed field set in this issue.",
        ],
        "correction_hints": [
            "Include the base capsule and then provide session_end_snapshot as a bounded helper.",
            "Use only open_loops, top_priorities, active_constraints, stance_summary, negative_decisions, session_trajectory, and rationale_entries in session_end_snapshot.",
        ],
    },
}

EXPECTED_HOOKS = {
    "hooks": [
        {
            "id": "startup",
            "purpose": "Re-establish orientation at session start or agent re-entry.",
            "when_to_use": [
                "Use when a runtime is about to begin work and needs startup continuity guidance.",
            ],
            "read_operations": [
                "POST /v1/continuity/read",
                "continuity.read",
            ],
            "write_operations": [],
            "minimal_payload": {
                "subject_kind": "thread",
                "subject_id": "issue-214",
                "view": "startup",
                "allow_fallback": True,
            },
            "common_mistakes": [
                "Using a hook ID that is not one of the four canonical hook IDs in this issue.",
            ],
            "correction_hints": [
                "Use startup exactly for the startup hook.",
            ],
        },
        {
            "id": "pre_prompt",
            "purpose": "Retrieve bounded working context before a major work step.",
            "when_to_use": [
                "Use when the runtime is about to start a major work step and needs bounded retrieval.",
            ],
            "read_operations": [
                "POST /v1/context/retrieve",
                "context.retrieve",
            ],
            "write_operations": [],
            "minimal_payload": {
                "task": "Address determinism findings on issue #214 only.",
                "subject_kind": "thread",
                "subject_id": "issue-214",
                "continuity_mode": "required",
            },
            "common_mistakes": [
                "Using continuity.read fields as if pre_prompt were bound to continuity.read.",
                "Persisting prompt text, retrieved snippets, or transcript material through pre_prompt.",
            ],
            "correction_hints": [
                "Use exactly task, subject_kind, subject_id, and continuity_mode in the minimal payload shape defined by this issue.",
                "Keep pre_prompt read-only and do not persist prompt or retrieval transcript material.",
            ],
        },
        {
            "id": "post_prompt",
            "purpose": "Persist durable orientation changes caused by the completed work step.",
            "when_to_use": [
                "Use when a completed work step changed durable continuity state that should persist.",
            ],
            "read_operations": [],
            "write_operations": [
                "POST /v1/continuity/upsert",
                "continuity.upsert",
            ],
            "minimal_payload": {
                "subject_kind": "thread",
                "subject_id": "issue-214",
                "capsule": {
                    "subject_kind": "thread",
                    "subject_id": "issue-214",
                    "updated_at": "2026-04-21T12:00:00Z",
                    "verified_at": "2026-04-21T12:00:00Z",
                    "source": {
                        "producer": "runtime-help",
                        "update_reason": "interaction_boundary",
                        "inputs": [],
                    },
                    "continuity": {
                        "top_priorities": [],
                        "active_concerns": [],
                        "active_constraints": [],
                        "open_loops": [],
                        "stance_summary": "Ready to continue issue 214 work.",
                        "drift_signals": [],
                    },
                    "confidence": {"continuity": 0.9, "relationship_model": 0.0},
                },
            },
            "common_mistakes": [
                "Treating post_prompt as read-oriented guidance.",
                "Using post_prompt as an interaction log or prompt/response summary sink.",
            ],
            "correction_hints": [
                "Use continuity.upsert only when a completed work step produced durable orientation state that should persist.",
                "Keep post_prompt focused on durable continuity rather than transcript material.",
            ],
        },
        {
            "id": "pre_compaction_or_handoff",
            "purpose": "Persist a bounded savepoint immediately before context loss, compaction, or a real inter-agent handoff boundary.",
            "when_to_use": [
                "Use when a runtime is about to compact local context or cross a real inter-agent handoff boundary.",
            ],
            "read_operations": [],
            "write_operations": [
                "POST /v1/continuity/upsert",
                "continuity.upsert",
            ],
            "additional_operations_for_real_handoff": [
                "POST /v1/coordination/handoff/create",
                "coordination.handoff_create",
            ],
            "minimal_payload": {
                "subject_kind": "thread",
                "subject_id": "issue-214",
                "capsule": {
                    "subject_kind": "thread",
                    "subject_id": "issue-214",
                    "updated_at": "2026-04-21T12:00:00Z",
                    "verified_at": "2026-04-21T12:00:00Z",
                    "source": {
                        "producer": "runtime-help",
                        "update_reason": "interaction_boundary",
                        "inputs": [],
                    },
                    "continuity": {
                        "top_priorities": [],
                        "active_concerns": [],
                        "active_constraints": [],
                        "open_loops": [],
                        "stance_summary": "Ready to continue issue 214 work.",
                        "drift_signals": [],
                    },
                    "confidence": {"continuity": 0.9, "relationship_model": 0.0},
                },
                "session_end_snapshot": {
                    "open_loops": [],
                    "top_priorities": [],
                    "active_constraints": [],
                    "stance_summary": "Ready to continue issue 214 work.",
                    "negative_decisions": [],
                    "session_trajectory": [],
                    "rationale_entries": [],
                },
            },
            "common_mistakes": [
                "Using the deprecated hook spelling pre_compaction_handoff.",
                "Sending session_end_snapshot with fields outside the closed field set in this issue.",
                "Calling handoff creation before the local continuity step completes.",
            ],
            "correction_hints": [
                "Use pre_compaction_or_handoff exactly.",
                "Use only open_loops, top_priorities, active_constraints, stance_summary, negative_decisions, session_trajectory, and rationale_entries in session_end_snapshot.",
                "For a real inter-agent handoff, call coordination.handoff_create only after the local continuity step completes.",
            ],
        },
    ],
}

EXPECTED_ERRORS = {
    "validation": {
        "kind": "error",
        "id": "validation",
        "purpose": "Explain how to correct a contract-validation failure.",
        "when_to_use": [
            "Use when a request failed contract validation.",
        ],
        "common_mistakes": [
            "Guessing field names or allowed values from the error detail string alone.",
        ],
        "correction_hints": [
            "Inspect validation_hints and correct the named field directly.",
        ],
    },
    "tool_not_found": {
        "kind": "error",
        "id": "tool_not_found",
        "purpose": "Explain the meaning of the tool_not_found error-guide target.",
        "when_to_use": [
            "Use when reading help about the tool_not_found error class itself.",
        ],
        "common_mistakes": [
            "Treating tool_not_found as the rejection contract for unsupported tool names on the help surface.",
        ],
        "correction_hints": [
            "For unsupported tool names on the help surface, use the validation rejection contract defined in this issue.",
        ],
    },
    "unknown_help_topic": {
        "kind": "error",
        "id": "unknown_help_topic",
        "purpose": "Explain the meaning of the unknown_help_topic error-guide target.",
        "when_to_use": [
            "Use when reading help about the unknown_help_topic error class itself.",
        ],
        "common_mistakes": [
            "Treating unknown_help_topic as the rejection contract for unsupported topic IDs on the help surface.",
        ],
        "correction_hints": [
            "For unsupported topic IDs on the help surface, use the validation rejection contract defined in this issue.",
        ],
    },
}

class HelpHttpTestCase(unittest.TestCase):
    """Share a real HTTP client for the slice-1 help contract tests."""

    @classmethod
    def setUpClass(cls) -> None:
        """Start a real client so routing assertions exercise FastAPI itself."""
        cls._client_context = TestClient(app)
        cls.client = cls._client_context.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        """Close the shared client after the HTTP contract checks finish."""
        cls._client_context.__exit__(None, None, None)


class TestHelp214Slice1Routes(HelpHttpTestCase):
    """The HTTP help surface is closed to the five slice-1 routes."""

    def test_help_routes_are_exact(self) -> None:
        """Only the exact issue-defined /v1/help routes are registered."""
        help_routes = {
            (frozenset(route.methods), route.path)
            for route in app.routes
            if route.path.startswith("/v1/help")
        }
        self.assertEqual(
            help_routes,
            {
                (frozenset({"GET"}), "/v1/help"),
                (frozenset({"GET"}), "/v1/help/tools/{name}"),
                (frozenset({"GET"}), "/v1/help/topics/{id}"),
                (frozenset({"GET"}), "/v1/help/hooks"),
                (frozenset({"GET"}), "/v1/help/errors/{code}"),
                (frozenset({"GET"}), "/v1/help/onboarding"),
                (frozenset({"GET"}), "/v1/help/onboarding/bootstrap"),
                (frozenset({"GET"}), "/v1/help/onboarding/sections/{id}"),
                (frozenset({"GET"}), "/v1/help/limits"),
                (frozenset({"GET"}), "/v1/help/limits/{field_path:path}"),
            },
        )

    def test_alternate_topic_routing_path_is_not_exposed(self) -> None:
        """Only the canonical single-segment topic path is live over HTTP."""
        response = self.client.get("/v1/help/topics/continuity.read/startup_view", follow_redirects=False)
        self.assertEqual(response.status_code, 404)
        self.assertNotIn("location", response.headers)
        self.assertEqual(response.json(), {"detail": "Not Found"})

    def test_non_help_trailing_slash_paths_keep_existing_redirects(self) -> None:
        """The help-alias guard must not intercept unrelated slash redirects."""
        response = self.client.get("/v1/capabilities/", follow_redirects=False)
        self.assertEqual(response.status_code, 307)
        self.assertEqual(response.headers["location"], "http://testserver/v1/capabilities")


class TestHelp214Slice1SuccessBodies(HelpHttpTestCase):
    """Successful slice-1 help responses must match the hardened issue body exactly."""

    def test_root_body(self) -> None:
        """GET /v1/help returns the exact closed root body."""
        response = self.client.get("/v1/help")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), EXPECTED_ROOT)

    def test_tool_bodies(self) -> None:
        """Each supported tool target returns its exact closed body."""
        for name, expected in EXPECTED_TOOLS.items():
            with self.subTest(name=name):
                response = self.client.get(f"/v1/help/tools/{name}")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), expected)

    def test_topic_bodies(self) -> None:
        """Each supported topic target returns its exact closed body."""
        for topic_id, expected in EXPECTED_TOPICS.items():
            with self.subTest(topic_id=topic_id):
                response = self.client.get(f"/v1/help/topics/{topic_id}")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), expected)

    def test_hooks_body(self) -> None:
        """GET /v1/help/hooks returns the exact closed hook map."""
        response = self.client.get("/v1/help/hooks")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), EXPECTED_HOOKS)

    def test_upsert_help_examples_match_runtime_request_shape(self) -> None:
        """Machine-facing upsert examples must be valid ContinuityUpsertRequest payloads."""
        payloads = [
            EXPECTED_TOOLS["continuity.upsert"]["minimal_payload"],
            EXPECTED_TOPICS["continuity.upsert.session_end_snapshot"]["minimal_payload"],
            EXPECTED_HOOKS["hooks"][2]["minimal_payload"],
            EXPECTED_HOOKS["hooks"][3]["minimal_payload"],
        ]
        for payload in payloads:
            with self.subTest(subject_id=payload["subject_id"]):
                ContinuityUpsertRequest.model_validate(payload)

    def test_error_bodies(self) -> None:
        """Each supported error target returns its exact closed body."""
        for code, expected in EXPECTED_ERRORS.items():
            with self.subTest(code=code):
                response = self.client.get(f"/v1/help/errors/{code}")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json(), expected)

    def test_slash_suffixed_variants_do_not_redirect_or_succeed(self) -> None:
        """Slash aliases must fail directly so the help surface stays exact."""
        for path in (
            "/v1/help/",
            "/v1/help/tools/continuity.read/",
            "/v1/help/topics/continuity.read.startup_view/",
            "/v1/help/hooks/",
            "/v1/help/errors/validation/",
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
                self.assertEqual(response.json(), {"detail": "Not Found"})


class TestHelp214Slice1Validation(HelpHttpTestCase):
    """Unsupported help targets are validation failures, not not-found lookups."""

    def test_unsupported_tool_name_returns_400_validation_body(self) -> None:
        """Unsupported tool names use the exact validation contract."""
        response = self.client.get("/v1/help/tools/memory.write", follow_redirects=False)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {
                "error": {
                    "code": "validation",
                    "detail": "Unsupported tool name.",
                    "validation_hints": [
                        {
                            "field": "name",
                            "area": "request.path",
                            "reason": "unsupported_value",
                            "limit": None,
                            "allowed_values": [
                                "continuity.read",
                                "continuity.upsert",
                                "context.retrieve",
                            ],
                            "correction_hint": "Use one of: continuity.read, continuity.upsert, context.retrieve.",
                        }
                    ],
                }
            },
        )

    def test_unsupported_topic_id_returns_400_validation_body(self) -> None:
        """Unsupported topic ids use the exact validation contract."""
        response = self.client.get("/v1/help/topics/continuity.read.startup", follow_redirects=False)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {
                "error": {
                    "code": "validation",
                    "detail": "Unsupported topic id.",
                    "validation_hints": [
                        {
                            "field": "id",
                            "area": "request.path",
                            "reason": "unsupported_value",
                            "limit": None,
                            "allowed_values": [
                                "continuity.read.startup_view",
                                "continuity.read.trust_signals",
                                "continuity.upsert.session_end_snapshot",
                            ],
                            "correction_hint": "Use one of: continuity.read.startup_view, continuity.read.trust_signals, continuity.upsert.session_end_snapshot.",
                        }
                    ],
                }
            },
        )

    def test_unsupported_error_code_returns_400_validation_body(self) -> None:
        """Unsupported error codes use the exact validation contract."""
        response = self.client.get("/v1/help/errors/not_found", follow_redirects=False)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {
                "error": {
                    "code": "validation",
                    "detail": "Unsupported error code.",
                    "validation_hints": [
                        {
                            "field": "code",
                            "area": "request.path",
                            "reason": "unsupported_value",
                            "limit": None,
                            "allowed_values": [
                                "validation",
                                "tool_not_found",
                                "unknown_help_topic",
                            ],
                            "correction_hint": "Use one of: validation, tool_not_found, unknown_help_topic.",
                        }
                    ],
                }
            },
        )


if __name__ == "__main__":
    unittest.main()
