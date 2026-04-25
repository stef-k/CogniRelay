"""Deterministic machine-facing help payloads for issue #214 slice 1."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Literal, get_args, get_origin

from fastapi.responses import JSONResponse

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

_TOOL_IDS = [
    "continuity.read",
    "continuity.upsert",
    "context.retrieve",
    "schedule.create",
    "schedule.get",
    "schedule.list",
    "schedule.update",
    "schedule.acknowledge",
    "schedule.retire",
]

_TOPIC_IDS = [
    "continuity.read.startup_view",
    "continuity.read.trust_signals",
    "continuity.upsert.session_end_snapshot",
]

_ERROR_CODES = [
    "validation",
    "tool_not_found",
    "unknown_help_topic",
]

_EXACT_HELP_PATHS = frozenset(
    {
        "/v1/help",
        "/v1/help/hooks",
        "/v1/help/onboarding",
        "/v1/help/onboarding/bootstrap",
        "/v1/help/limits",
    }
)

_PARAMETERIZED_HELP_PREFIXES = (
    "/v1/help/tools/",
    "/v1/help/topics/",
    "/v1/help/errors/",
    "/v1/help/onboarding/sections/",
    "/v1/help/limits/",
)

_ROOT_BODY = {
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
    "tool_topics": _TOOL_IDS,
    "non_tool_topics": _TOPIC_IDS,
    "hook_ids": [
        "startup",
        "pre_prompt",
        "post_prompt",
        "pre_compaction_or_handoff",
    ],
    "errors": _ERROR_CODES,
}

_TOOLS = {
    "continuity.read": {
        "kind": "tool",
        "id": "continuity.read",
        "purpose": "Read continuity state for a subject.",
        "when_to_use": [
            "Use when the runtime needs persisted orientation for a subject.",
            "Use at session start when continuity is needed before prompting.",
            "Use view=\"startup\" when the agent also needs the bounded top-level graph_summary.",
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
            "Expecting graph_summary on non-startup continuity.read responses.",
        ],
        "correction_hints": [
            "Use view: startup and allow_fallback: true for startup continuity guidance.",
            "Provide both subject_kind and subject_id.",
            "Read graph warnings from graph_summary.warnings; non-startup reads are intentionally graph-free.",
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
            "Use when the agent needs the default bounded bundle.graph_context alongside continuity_state.",
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
            "Looking for graph warnings in continuity_state.warnings instead of bundle.graph_context.warnings.",
        ],
        "correction_hints": [
            "Use exactly task, subject_kind, subject_id, and continuity_mode in the minimal payload shape defined by this issue.",
            "Keep context.retrieve read-only and do not persist prompt or retrieval transcript material.",
            "When continuity_mode is off, expect an empty bundle.graph_context with graph_suppressed_by_continuity_mode.",
        ],
    },
    "schedule.create": {
        "kind": "tool",
        "id": "schedule.create",
        "purpose": "Create a one-shot reminder or task nudge.",
        "when_to_use": [
            "Use when an agent needs a durable reminder surfaced later through startup/context orientation or schedule.list.",
            "Use task_nudge only when the item is linked to a task, thread, or continuity subject.",
        ],
        "read_operations": ["GET /v1/schedule/items", "schedule.list"],
        "write_operations": ["POST /v1/schedule/items", "schedule.create"],
        "minimal_payload": {"kind": "reminder", "title": "Check build status", "due_at": "2026-05-01T12:00:00Z"},
        "common_mistakes": [
            "Using offsets, local times, or subseconds instead of exact UTC YYYY-MM-DDTHH:MM:SSZ.",
            "Expecting CogniRelay to execute commands, send callbacks, or mutate tasks when a reminder is due.",
        ],
        "correction_hints": [
            "Use UTC Z timestamps at seconds precision.",
            "Due reminders are data records surfaced by pull/list and orientation responses until acknowledged, done, or retired.",
        ],
    },
    "schedule.get": {
        "kind": "tool",
        "id": "schedule.get",
        "purpose": "Read one scheduled item by schedule_id.",
        "when_to_use": ["Use when inspecting a known reminder or task nudge."],
        "read_operations": ["GET /v1/schedule/items/{schedule_id}", "schedule.get"],
        "write_operations": [],
        "minimal_payload": {"schedule_id": "sched_example1"},
        "common_mistakes": ["Treating due_state as persisted; derived_state is computed at read time."],
        "correction_hints": ["Use schedule.list with due=true for due-item polling."],
    },
    "schedule.list": {
        "kind": "tool",
        "id": "schedule.list",
        "purpose": "List scheduled reminders and task nudges with deterministic filters.",
        "when_to_use": [
            "Use for manual inspection of all reminders or for explicit due polling.",
            "Use due=true to return pending items whose due_at is at or before the operation clock.",
        ],
        "read_operations": ["GET /v1/schedule/items", "schedule.list"],
        "write_operations": [],
        "minimal_payload": {"due": True, "limit": 10},
        "common_mistakes": ["Expecting schedule.list to mark reminders delivered or acknowledged."],
        "correction_hints": ["Call schedule.acknowledge after the agent has handled a due reminder."],
    },
    "schedule.update": {
        "kind": "tool",
        "id": "schedule.update",
        "purpose": "Patch mutable fields on a pending scheduled item.",
        "when_to_use": ["Use to change title, due_at, links, kind, note, or metadata before terminal transition."],
        "read_operations": ["GET /v1/schedule/items/{schedule_id}", "schedule.get"],
        "write_operations": ["PATCH /v1/schedule/items/{schedule_id}", "schedule.update"],
        "minimal_payload": {"schedule_id": "sched_example1", "expected_version": 1, "title": "Updated title"},
        "common_mistakes": ["Omitting expected_version.", "Trying to patch an acknowledged, done, or retired item."],
        "correction_hints": ["Read the item first and supply the current version; terminal rows cannot be updated."],
    },
    "schedule.acknowledge": {
        "kind": "tool",
        "id": "schedule.acknowledge",
        "purpose": "Mark a pending scheduled item acknowledged or done.",
        "when_to_use": ["Use after a due reminder has been seen or the associated task nudge is complete."],
        "read_operations": ["GET /v1/schedule/items/{schedule_id}", "schedule.get"],
        "write_operations": ["POST /v1/schedule/items/{schedule_id}/acknowledge", "schedule.acknowledge"],
        "minimal_payload": {"schedule_id": "sched_example1", "expected_version": 1, "status": "acknowledged"},
        "common_mistakes": ["Looking for a separate schedule.done tool."],
        "correction_hints": ["Use schedule.acknowledge with status=\"done\" to mark completion."],
    },
    "schedule.retire": {
        "kind": "tool",
        "id": "schedule.retire",
        "purpose": "Retire a scheduled item without deleting it.",
        "when_to_use": ["Use when a reminder is no longer relevant but should remain auditable."],
        "read_operations": ["GET /v1/schedule/items/{schedule_id}", "schedule.get"],
        "write_operations": ["POST /v1/schedule/items/{schedule_id}/retire", "schedule.retire"],
        "minimal_payload": {"schedule_id": "sched_example1", "expected_version": 1, "reason": "No longer relevant"},
        "common_mistakes": ["Looking for a hard-delete route."],
        "correction_hints": ["Hard delete, recurrence, SSE, UI mutation, callbacks, and automation are deferred."],
    },
}

_TOPICS = {
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

_HOOKS = {
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

_ERRORS = {
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

_ONBOARDING_SECTION_ORDER = [
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

_ONBOARDING_SECTION_TITLES = {
    "bootstrap": "Minimum Startup Path",
    "hooks": "Canonical Hooks",
    "help_lookup": "Runtime Help Lookup",
    "limits_and_routing": "Bootstrap-Critical Limits and Routing Rules",
    "workflow_rules": "Operational Workflow Rules",
    "retrieval": "Retrieval Mental Model",
    "trust_and_degradation": "Trust and Degradation Rules",
    "examples": "Minimal Examples",
    "anti_patterns": "Anti-Patterns",
    "references": "References / Where To Look Next",
}

_ONBOARDING_INDEX_PURPOSES = {
    "bootstrap": ("Find the minimum startup route.", "Use at process start or after a reset."),
    "hooks": ("Map runtime hooks to CogniRelay calls.", "Use when wiring startup, prompt, persistence, or handoff behavior."),
    "help_lookup": ("Find bounded help lookup routes.", "Use when a tool, topic, hook, error, onboarding section, or limit is unclear."),
    "limits_and_routing": ("Recover from validation limits and route writes correctly.", "Use after continuity write, patch, snapshot, or retrieval validation errors."),
    "workflow_rules": ("Apply durable continuity workflow boundaries.", "Use before deciding whether to write a preference, thread, task, document reference, or rationale."),
    "retrieval": ("Choose bounded context retrieval instead of raw document loading.", "Use before the first work step that needs context beyond startup orientation."),
    "trust_and_degradation": ("Interpret degraded continuity and warnings.", "Use when responses include warnings, fallback, or degraded trust signals."),
    "examples": ("Review compact workflow examples.", "Use when forming the first valid request shape for common workflows."),
    "anti_patterns": ("Avoid unsafe or out-of-scope runtime usage.", "Use before adding broad reads, schema dumps, or full-manual preload behavior."),
    "references": ("Locate deeper docs and runtime references.", "Use when bounded runtime help is not enough for implementation work."),
}

_ONBOARDING_RELATED = {
    "bootstrap": (
        ["POST /v1/continuity/read", "POST /v1/context/retrieve", "GET /v1/help/onboarding/bootstrap"],
        ["continuity.read", "context.retrieve", "system.onboarding_bootstrap"],
        ["docs/agent-onboarding.md#minimum-startup-path"],
    ),
    "hooks": (["GET /v1/help/hooks"], ["system.hook_guide"], ["docs/agent-onboarding.md#canonical-hooks"]),
    "help_lookup": (
        [
            "GET /v1/help",
            "GET /v1/help/tools/{name}",
            "GET /v1/help/topics/{id}",
            "GET /v1/help/hooks",
            "GET /v1/help/errors/{code}",
            "GET /v1/help/onboarding",
            "GET /v1/help/onboarding/sections/{id}",
            "GET /v1/help/limits",
            "GET /v1/help/limits/{field_path}",
        ],
        [
            "system.help",
            "system.tool_usage",
            "system.topic_help",
            "system.hook_guide",
            "system.error_guide",
            "system.onboarding_index",
            "system.onboarding_section",
            "system.validation_limits",
            "system.validation_limit",
        ],
        ["docs/agent-onboarding.md#runtime-help-lookup", "docs/api-surface.md", "docs/mcp.md"],
    ),
    "limits_and_routing": (
        ["GET /v1/help/limits", "GET /v1/help/limits/{field_path}", "POST /v1/continuity/upsert", "POST /v1/continuity/patch", "POST /v1/context/retrieve"],
        ["system.validation_limits", "system.validation_limit", "continuity.upsert", "continuity.patch", "context.retrieve"],
        ["docs/agent-onboarding.md#bootstrap-critical-limits-and-routing-rules", "docs/payload-reference.md"],
    ),
    "workflow_rules": (
        ["POST /v1/continuity/read", "POST /v1/continuity/upsert", "POST /v1/continuity/patch"],
        ["continuity.read", "continuity.upsert", "continuity.patch"],
        ["docs/agent-onboarding.md#operational-workflow-rules"],
    ),
    "retrieval": (
        ["POST /v1/context/retrieve", "POST /v1/continuity/read"],
        ["context.retrieve", "continuity.read"],
        ["docs/agent-onboarding.md#retrieval-mental-model"],
    ),
    "trust_and_degradation": (
        ["POST /v1/continuity/read", "POST /v1/context/retrieve"],
        ["continuity.read", "context.retrieve"],
        ["docs/agent-onboarding.md#trust-and-degradation-rules"],
    ),
    "examples": (
        ["POST /v1/continuity/read", "POST /v1/context/retrieve", "POST /v1/continuity/upsert", "POST /v1/continuity/patch"],
        ["continuity.read", "context.retrieve", "continuity.upsert", "continuity.patch"],
        ["docs/agent-onboarding.md#minimal-examples", "docs/payload-reference.md"],
    ),
    "anti_patterns": (
        ["GET /v1/help/onboarding/bootstrap", "POST /v1/continuity/read", "POST /v1/context/retrieve"],
        ["system.onboarding_bootstrap", "continuity.read", "context.retrieve"],
        ["docs/agent-onboarding.md#anti-patterns"],
    ),
    "references": (
        ["GET /v1/help", "GET /v1/help/onboarding", "GET /v1/help/limits"],
        ["system.help", "system.onboarding_index", "system.validation_limits"],
        ["docs/agent-onboarding.md#references--where-to-look-next", "docs/api-surface.md", "docs/mcp.md", "docs/payload-reference.md"],
    ),
}

_ONBOARDING_BODIES = {
    "bootstrap": (
        "## Minimum Startup Path\n"
        "Start with POST /v1/continuity/read using view=\"startup\" and allow_fallback=true. "
        "Check schedule_context.due.items when present; due reminders arrive through startup/context orientation and remain read-only until schedule.acknowledge or schedule.retire. "
        "Use POST /v1/context/retrieve only when the first work step needs bounded context beyond startup orientation."
    ),
    "hooks": (
        "## Canonical Hooks\n"
        "Use startup for orientation, pre_prompt for bounded retrieval, post_prompt for durable continuity updates, "
        "and pre_compaction_or_handoff before context loss or a real handoff."
    ),
    "help_lookup": (
        "## Runtime Help Lookup\n"
        "Use GET /v1/help, GET /v1/help/tools/{name}, GET /v1/help/topics/{id}, GET /v1/help/hooks, "
        "GET /v1/help/errors/{code}, system.tool_usage, system.topic_help, system.hook_guide, and system.error_guide. "
        "Use onboarding and limits routes for bounded startup and validation recovery."
    ),
    "limits_and_routing": (
        "## Bootstrap-Critical Limits and Routing Rules\n"
        "For validation recovery, query continuity.top_priorities, continuity.open_loops, continuity.active_constraints, "
        "or any exact field path with GET /v1/help/limits/{field_path}. Keep continuity writes routed through upsert or patch."
    ),
    "workflow_rules": (
        "## Operational Workflow Rules\n"
        "`stable_preferences`: use only for durable standing instructions or preferences that should survive across sessions and across work threads. "
        "Threads: use a thread when the work is one bounded stream of ongoing context. Tasks: create a task when there is a bounded deliverable. "
        "`related_documents`: attach these when a bounded set of repo-relative documents matters. `blocked_by[]`: use this on tasks when the task cannot proceed. "
        "Supersede vs mutate: mutate existing thread/task continuity unless lineage should explicitly move. "
        "`negative_decisions` vs `rationale_entries`: use `negative_decisions` for compact records of deliberate non-actions."
    ),
    "retrieval": (
        "## Retrieval Mental Model\n"
        "Use POST /v1/context/retrieve for bounded context packages. It includes scoped schedule_context when the request carries "
        "a primary subject or continuity selectors. Tune max_tokens_estimate and continuity_max_capsules within runtime limits."
    ),
    "trust_and_degradation": (
        "## Trust and Degradation Rules\n"
        "Read warnings before acting. allow_fallback can return degraded continuity so the agent still has a working path forward."
    ),
    "examples": (
        "## Minimal Examples\n"
        "### Thread-Only Workflow\nRead startup continuity, retrieve bounded context, then persist durable changes.\n"
        "### Thread + Task Workflow\nUse thread continuity for stream orientation and task continuity for the bounded deliverable.\n"
        "### Task + `related_documents` Workflow\nAttach repo-relative references instead of embedded document text.\n"
        "### Resume After Reset\nRead startup continuity again and inspect warnings before continuing."
    ),
    "anti_patterns": (
        "## Anti-Patterns\n"
        "Do not request or preload the full onboarding document by default. Do not ask for a full payload schema when a field-specific limit lookup is enough."
    ),
    "references": (
        "## References / Where To Look Next\n"
        "Use docs/api-surface.md, docs/mcp.md, and docs/payload-reference.md when implementation details exceed bounded runtime help."
    ),
}

_ONBOARDING_BULLETS = {
    "bootstrap": [
        "Call continuity.read with view=\"startup\" and allow_fallback=true.",
        "Use context.retrieve only when bounded working context is needed.",
        "Use /v1/help/onboarding for section discovery.",
    ],
    "hooks": [
        "startup reads orientation.",
        "pre_prompt retrieves bounded context.",
        "post_prompt and pre_compaction_or_handoff write durable continuity.",
    ],
    "help_lookup": [
        "Use exact HTTP help routes for tools, topics, hooks, errors, onboarding, and limits.",
        "Use MCP request methods for the same help surfaces.",
        "Do not treat help request methods as MCP tools.",
    ],
    "limits_and_routing": [
        "Use GET /v1/help/limits/{field_path} after validation failures.",
        "Use exact field_path strings from the limits index.",
        "Route writes through continuity.upsert or continuity.patch.",
    ],
    "workflow_rules": [
        "Store stable preferences only for durable standing instructions.",
        "Use threads for ongoing context streams and tasks for bounded deliverables.",
        "Record compact non-actions in negative_decisions and richer reasoning in rationale_entries.",
    ],
    "retrieval": [
        "Retrieve bounded context through context.retrieve.",
        "Keep max_tokens_estimate and continuity_max_capsules inside runtime bounds.",
        "Avoid changing continuity read semantics for retrieval.",
    ],
    "trust_and_degradation": [
        "Inspect warnings before trusting a response.",
        "Treat degraded data as usable but cautionary.",
        "Prefer recovery over crashing when fallback is allowed.",
    ],
    "examples": [
        "Use the examples as minimal request-shape reminders.",
        "Attach related_documents as repo-relative metadata.",
        "Resume after reset by reading startup continuity.",
    ],
    "anti_patterns": [
        "Do not add a full onboarding fallback.",
        "Do not return a full payload schema by default.",
        "Do not broaden this surface into UI, scheduling, graph retrieval, or continuity semantics.",
    ],
    "references": [
        "Start with runtime help when available.",
        "Use docs only for deeper implementation detail.",
        "Keep payload limit recovery field-specific.",
    ],
}

_PRIORITY_LIMIT_FIELD_PATHS = [
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

_NEGATIVE_DECISION_LIMITS = {"decision": {"max_length": 160}, "rationale": {"max_length": 240}, "timestamps": {"require_utc_when_present": True}}
_RATIONALE_ENTRY_LIMITS = {
    "tag": {"max_length": 80},
    "kind": {"allowed_values": ["decision", "assumption", "tension"]},
    "status": {"allowed_values": ["active", "superseded", "retired"]},
    "summary": {"max_length": 320},
    "reasoning": {"max_length": 560},
    "alternatives_considered": {"max_items": 3, "per_item_max_length": 160},
    "depends_on": {"max_items": 3, "per_item_max_length": 120},
    "supersedes": {"max_length": 80},
    "timestamps": {"require_utc_when_present": True},
}
_SCOPE_ANCHOR_LIMITS = {
    "pattern": "^(user|peer|thread|task):[a-z0-9._-]{1,120}$",
    "prefix": {"allowed_values": ["user", "peer", "thread", "task"]},
    "anchor_value": {"min_length": 1, "max_length": 120, "pattern": "^[a-z0-9._-]{1,120}$"},
}
_IDENTITY_ANCHOR_LIMITS = {"kind": {"max_length": 40, "pattern": "^[a-z][a-z0-9_-]{0,39}$"}, "value": {"max_length": 200}, "match_key": "kind:value"}
_INTERACTION_BOUNDARY_KIND_ORDER = ["person_switch", "thread_switch", "task_switch", "public_reply", "manual_checkpoint"]
_INTERACTION_BOUNDARY_KIND_VALUES = [kind for kind in _INTERACTION_BOUNDARY_KIND_ORDER if kind in CONTINUITY_INTERACTION_BOUNDARY_KINDS]


def _field_constraint(model: type, field_name: str, attr: str) -> Any:
    for metadata in model.model_fields[field_name].metadata:
        if hasattr(metadata, attr):
            return getattr(metadata, attr)
    return None


def _literal_values(model: type, field_name: str) -> list[Any]:
    def collect(annotation: Any) -> list[Any]:
        if get_origin(annotation) is None and not get_args(annotation):
            return []
        if get_origin(annotation) is Literal:
            return list(get_args(annotation))
        values: list[Any] = []
        for arg in get_args(annotation):
            values.extend(collect(arg))
        return values

    return collect(model.model_fields[field_name].annotation)


def _field_limit(
    field_path: str,
    category: str,
    value_type: str,
    model: type,
    field_name: str,
    *,
    per_item_max_length: int | None = None,
    subfield_limits: dict[str, Any] | None = None,
    applies_to: list[str] | None = None,
    reference: str | None = None,
) -> dict[str, Any]:
    subfields = dict(subfield_limits or {})
    max_items = _field_constraint(model, field_name, "max_length") if value_type in {"string_list", "object_list"} else None
    max_length = _field_constraint(model, field_name, "max_length") if value_type in {"string", "serialized_bytes"} else None
    if value_type == "string":
        min_length = _field_constraint(model, field_name, "min_length")
        if min_length is not None:
            subfields["min_length"] = min_length
    if value_type in {"number", "integer_budget"}:
        minimum = _field_constraint(model, field_name, "ge")
        maximum = _field_constraint(model, field_name, "le")
        default = model.model_fields[field_name].default
        if value_type == "integer_budget" and default is not None:
            subfields["default"] = default
        if minimum is not None:
            subfields["minimum"] = minimum
        if maximum is not None:
            subfields["maximum"] = maximum
    if value_type == "enum":
        subfields["allowed_values"] = _literal_values(model, field_name)
    return _limit(
        field_path,
        category,
        value_type,
        max_items=max_items,
        max_length=max_length,
        per_item_max_length=per_item_max_length,
        subfield_limits=subfields,
        applies_to=applies_to,
        reference=reference,
    )


def _utc_timestamp_limit(field_path: str, *, applies_to: list[str] | None = None) -> dict[str, Any]:
    return _limit(
        field_path,
        "continuity_payload",
        "string",
        subfield_limits={"require_utc_timestamp": True, "deterministic": True, "timezone": "UTC"},
        applies_to=applies_to,
    )


def _limit(
    field_path: str,
    category: str,
    value_type: str,
    *,
    max_items: int | None = None,
    max_length: int | None = None,
    per_item_max_length: int | None = None,
    subfield_limits: dict[str, Any] | None = None,
    applies_to: list[str] | None = None,
    reference: str | None = None,
) -> dict[str, Any]:
    if applies_to is None:
        if field_path.startswith("session_end_snapshot."):
            applies_to = ["POST /v1/continuity/upsert", "continuity.upsert"]
        elif field_path.startswith("patch.") or field_path.startswith("continuity.patch."):
            applies_to = ["POST /v1/continuity/patch", "continuity.patch"]
        elif field_path.startswith("context.retrieve."):
            applies_to = ["POST /v1/context/retrieve", "context.retrieve"]
        elif field_path == "continuity.capsule_serialized_utf8":
            applies_to = ["POST /v1/continuity/upsert", "continuity.upsert", "POST /v1/continuity/patch", "continuity.patch"]
        else:
            applies_to = ["POST /v1/continuity/upsert", "continuity.upsert"]
    if reference is None:
        if field_path.startswith("session_end_snapshot."):
            reference = "docs/payload-reference.md#session-end-snapshot-helper"
        elif field_path.startswith("patch.") or field_path.startswith("continuity.patch."):
            reference = "docs/payload-reference.md#patch--post-v1continuitypatch"
        elif field_path.startswith("context.retrieve."):
            reference = "docs/payload-reference.md#retrieve--post-v1contextretrieve"
        elif field_path == "continuity.capsule_serialized_utf8":
            reference = "app.continuity.constants.CAPSULE_SIZE_LIMIT_BYTES"
        else:
            reference = "docs/payload-reference.md"
    subfield_limits = subfield_limits or {}
    guidance = _correction_guidance(field_path, value_type, max_items, max_length, per_item_max_length, subfield_limits)
    if field_path.startswith("session_end_snapshot.") and field_path in {
        "session_end_snapshot.negative_decisions",
        "session_end_snapshot.session_trajectory",
        "session_end_snapshot.rationale_entries",
    }:
        guidance += " Use null only when preserving the existing capsule value is intended."
    return {
        "field_path": field_path,
        "category": category,
        "value_type": value_type,
        "max_items": max_items,
        "max_length": max_length,
        "per_item_max_length": per_item_max_length,
        "subfield_limits": subfield_limits,
        "applies_to": applies_to,
        "correction_guidance": guidance,
        "reference": reference,
    }


def _correction_guidance(
    field_path: str,
    value_type: str,
    max_items: int | None,
    max_length: int | None,
    per_item_max_length: int | None,
    subfield_limits: dict[str, Any],
) -> str:
    if value_type == "string":
        if subfield_limits.get("require_utc_timestamp"):
            return f"Use an explicit deterministic UTC timestamp and retry with field_path \"{field_path}\"."
        return f"Shorten this value to at most {max_length} characters and retry with field_path \"{field_path}\"."
    if value_type == "string_list" and per_item_max_length is not None:
        return f"Keep at most {max_items} items, shorten each item to at most {per_item_max_length} characters, and retry with field_path \"{field_path}\"."
    if value_type == "string_list":
        if _has_pattern_or_suffix_metadata(subfield_limits):
            return f"Keep at most {max_items} items, make each item match the documented pattern and subfield metadata, and retry with field_path \"{field_path}\"."
        return f"Keep at most {max_items} items and retry with field_path \"{field_path}\"."
    if value_type == "object_list":
        return f"Keep at most {max_items} items, apply the documented subfield limits, and retry with field_path \"{field_path}\"."
    if value_type == "operation_list":
        return f"Send between {subfield_limits['min_items']} and {max_items} patch operations and retry with field_path \"{field_path}\"."
    if value_type == "integer_budget":
        return f"Use a value between {subfield_limits['minimum']} and {subfield_limits['maximum']} and retry with field_path \"{field_path}\"."
    if value_type == "serialized_bytes":
        return f"Reduce the serialized capsule below {subfield_limits['label']} ({max_length} bytes) and retry with field_path \"{field_path}\"."
    if value_type == "enum":
        return f"Use one of the allowed values in subfield_limits and retry with field_path \"{field_path}\"."
    if value_type == "number":
        return f"Use a value within the documented numeric bounds and retry with field_path \"{field_path}\"."
    return f"Apply the documented nested field limits and retry with field_path \"{field_path}\"."


def _has_pattern_or_suffix_metadata(subfield_limits: dict[str, Any]) -> bool:
    if any(key in subfield_limits for key in ("pattern", "suffix", "suffix_pattern", "anchor_value")):
        return True
    return any(isinstance(value, dict) and "pattern" in value for value in subfield_limits.values())


def _validation_limits_table() -> dict[str, dict[str, Any]]:
    related_documents_limits = related_documents_limit_fixture()
    limits: list[dict[str, Any]] = [
        _field_limit("continuity.top_priorities", "continuity_orientation", "string_list", ContinuityState, "top_priorities", per_item_max_length=160),
        _field_limit("continuity.open_loops", "continuity_orientation", "string_list", ContinuityState, "open_loops", per_item_max_length=160),
        _field_limit("continuity.active_constraints", "continuity_orientation", "string_list", ContinuityState, "active_constraints", per_item_max_length=160),
        _field_limit("continuity.session_trajectory", "continuity_orientation", "string_list", ContinuityState, "session_trajectory", per_item_max_length=80),
        _field_limit("continuity.negative_decisions", "continuity_orientation", "object_list", ContinuityState, "negative_decisions", subfield_limits=_NEGATIVE_DECISION_LIMITS),
        _field_limit("continuity.rationale_entries", "continuity_orientation", "object_list", ContinuityState, "rationale_entries", subfield_limits=_RATIONALE_ENTRY_LIMITS),
        _limit(
            "continuity.related_documents",
            "continuity_orientation",
            "object_list",
            max_items=related_documents_limits["max_items"],
            subfield_limits=related_documents_limits["subfield_limits"],
            reference="docs/payload-reference.md#continuityrelated_documents",
        ),
        _field_limit("continuity.stance_summary", "continuity_orientation", "string", ContinuityState, "stance_summary"),
        _field_limit("session_end_snapshot.open_loops", "session_end_snapshot", "string_list", SessionEndSnapshot, "open_loops", per_item_max_length=160),
        _field_limit("session_end_snapshot.top_priorities", "session_end_snapshot", "string_list", SessionEndSnapshot, "top_priorities", per_item_max_length=160),
        _field_limit("session_end_snapshot.active_constraints", "session_end_snapshot", "string_list", SessionEndSnapshot, "active_constraints", per_item_max_length=160),
        _field_limit("session_end_snapshot.stance_summary", "session_end_snapshot", "string", SessionEndSnapshot, "stance_summary"),
        _field_limit("session_end_snapshot.negative_decisions", "session_end_snapshot", "object_list", SessionEndSnapshot, "negative_decisions", subfield_limits=_NEGATIVE_DECISION_LIMITS),
        _field_limit("session_end_snapshot.session_trajectory", "session_end_snapshot", "string_list", SessionEndSnapshot, "session_trajectory", per_item_max_length=80),
        _field_limit("session_end_snapshot.rationale_entries", "session_end_snapshot", "object_list", SessionEndSnapshot, "rationale_entries", subfield_limits=_RATIONALE_ENTRY_LIMITS),
        _limit(
            "patch.operations",
            "patch_targets",
            "operation_list",
            max_items=_field_constraint(ContinuityPatchRequest, "operations", "max_length"),
            subfield_limits={
                "min_items": _field_constraint(ContinuityPatchRequest, "operations", "min_length"),
                "max_items": PATCH_MAX_OPERATIONS,
                "actions": ["append", "remove", "replace_at"],
            },
        ),
        _limit("patch.target.continuity.open_loops", "patch_targets", "string_list", max_items=PATCH_TARGET_MAX_LENGTH["continuity.open_loops"], per_item_max_length=160),
        _limit("patch.target.continuity.top_priorities", "patch_targets", "string_list", max_items=PATCH_TARGET_MAX_LENGTH["continuity.top_priorities"], per_item_max_length=160),
        _limit("patch.target.continuity.active_constraints", "patch_targets", "string_list", max_items=PATCH_TARGET_MAX_LENGTH["continuity.active_constraints"], per_item_max_length=160),
        _limit("patch.target.continuity.active_concerns", "patch_targets", "string_list", max_items=PATCH_TARGET_MAX_LENGTH["continuity.active_concerns"], per_item_max_length=160),
        _limit("patch.target.continuity.drift_signals", "patch_targets", "string_list", max_items=PATCH_TARGET_MAX_LENGTH["continuity.drift_signals"], per_item_max_length=160),
        _limit("patch.target.continuity.working_hypotheses", "patch_targets", "string_list", max_items=PATCH_TARGET_MAX_LENGTH["continuity.working_hypotheses"], per_item_max_length=160),
        _limit("patch.target.continuity.long_horizon_commitments", "patch_targets", "string_list", max_items=PATCH_TARGET_MAX_LENGTH["continuity.long_horizon_commitments"], per_item_max_length=160),
        _limit("patch.target.continuity.session_trajectory", "patch_targets", "string_list", max_items=PATCH_TARGET_MAX_LENGTH["continuity.session_trajectory"], per_item_max_length=80),
        _limit("patch.target.continuity.trailing_notes", "patch_targets", "string_list", max_items=PATCH_TARGET_MAX_LENGTH["continuity.trailing_notes"], per_item_max_length=160),
        _limit("patch.target.continuity.curiosity_queue", "patch_targets", "string_list", max_items=PATCH_TARGET_MAX_LENGTH["continuity.curiosity_queue"], per_item_max_length=120),
        _limit(
            "patch.target.continuity.negative_decisions",
            "patch_targets",
            "object_list",
            max_items=PATCH_TARGET_MAX_LENGTH["continuity.negative_decisions"],
            subfield_limits=_NEGATIVE_DECISION_LIMITS | {"match_key": PATCH_STRUCTURED_MATCH_KEYS["continuity.negative_decisions"]},
        ),
        _limit(
            "patch.target.continuity.rationale_entries",
            "patch_targets",
            "object_list",
            max_items=PATCH_TARGET_MAX_LENGTH["continuity.rationale_entries"],
            subfield_limits=_RATIONALE_ENTRY_LIMITS | {"match_key": PATCH_STRUCTURED_MATCH_KEYS["continuity.rationale_entries"]},
        ),
        _limit(
            "patch.target.stable_preferences",
            "patch_targets",
            "object_list",
            max_items=PATCH_TARGET_MAX_LENGTH["stable_preferences"],
            subfield_limits={
                "tag": {"max_length": _field_constraint(StablePreference, "tag", "max_length")},
                "content": {"max_length": _field_constraint(StablePreference, "content", "max_length")},
                "match_key": PATCH_STRUCTURED_MATCH_KEYS["stable_preferences"],
                "timestamps": {"require_utc_when_present": True},
                "subject_kind": {"allowed_values": ["user", "peer"]},
            },
        ),
        _limit("patch.target.thread_descriptor.keywords", "patch_targets", "string_list", max_items=PATCH_TARGET_MAX_LENGTH["thread_descriptor.keywords"], per_item_max_length=40),
        _limit(
            "patch.target.thread_descriptor.scope_anchors",
            "patch_targets",
            "string_list",
            max_items=PATCH_TARGET_MAX_LENGTH["thread_descriptor.scope_anchors"],
            subfield_limits=_SCOPE_ANCHOR_LIMITS,
        ),
        _limit(
            "patch.target.thread_descriptor.identity_anchors",
            "patch_targets",
            "object_list",
            max_items=PATCH_TARGET_MAX_LENGTH["thread_descriptor.identity_anchors"],
            subfield_limits=_IDENTITY_ANCHOR_LIMITS,
        ),
        _limit(
            "context.retrieve.max_tokens_estimate",
            "retrieval_budget",
            "integer_budget",
            subfield_limits={
                "default": CONTEXT_RETRIEVE_DEFAULT_MAX_TOKENS,
                "minimum": CONTEXT_RETRIEVE_MIN_MAX_TOKENS,
                "maximum": CONTEXT_RETRIEVE_MAX_MAX_TOKENS,
            },
        ),
        _field_limit("context.retrieve.continuity_max_capsules", "retrieval_budget", "integer_budget", ContextRetrieveRequest, "continuity_max_capsules"),
        _limit(
            "context.retrieve.graph_context.nodes",
            "response_orientation_caps",
            "response_cap",
            max_items=24,
            subfield_limits={"default": 24},
            applies_to=["POST /v1/context/retrieve", "context.retrieve"],
            reference="docs/payload-reference.md#graph-runtime-sections",
        ),
        _limit(
            "context.retrieve.graph_context.edges",
            "response_orientation_caps",
            "response_cap",
            max_items=32,
            subfield_limits={"default": 32},
            applies_to=["POST /v1/context/retrieve", "context.retrieve"],
            reference="docs/payload-reference.md#graph-runtime-sections",
        ),
        _limit(
            "context.retrieve.graph_context.related_documents",
            "response_orientation_caps",
            "response_cap",
            max_items=8,
            subfield_limits={"default": 8},
            applies_to=["POST /v1/context/retrieve", "context.retrieve"],
            reference="docs/payload-reference.md#graph-runtime-sections",
        ),
        _limit(
            "context.retrieve.graph_context.blockers",
            "response_orientation_caps",
            "response_cap",
            max_items=8,
            subfield_limits={"default": 8},
            applies_to=["POST /v1/context/retrieve", "context.retrieve"],
            reference="docs/payload-reference.md#graph-runtime-sections",
        ),
        _limit(
            "continuity.read.startup.graph_summary.nodes",
            "response_orientation_caps",
            "response_cap",
            max_items=12,
            subfield_limits={"default": 12},
            applies_to=["POST /v1/continuity/read", "continuity.read"],
            reference="docs/payload-reference.md#graph-runtime-sections",
        ),
        _limit(
            "continuity.read.startup.graph_summary.edges",
            "response_orientation_caps",
            "response_cap",
            max_items=16,
            subfield_limits={"default": 16},
            applies_to=["POST /v1/continuity/read", "continuity.read"],
            reference="docs/payload-reference.md#graph-runtime-sections",
        ),
        _limit(
            "continuity.read.startup.graph_summary.related_documents",
            "response_orientation_caps",
            "response_cap",
            max_items=4,
            subfield_limits={"default": 4},
            applies_to=["POST /v1/continuity/read", "continuity.read"],
            reference="docs/payload-reference.md#graph-runtime-sections",
        ),
        _limit(
            "continuity.read.startup.graph_summary.blockers",
            "response_orientation_caps",
            "response_cap",
            max_items=4,
            subfield_limits={"default": 4},
            applies_to=["POST /v1/continuity/read", "continuity.read"],
            reference="docs/payload-reference.md#graph-runtime-sections",
        ),
        _limit(
            "continuity.capsule_serialized_utf8",
            "capsule_write_cap",
            "serialized_bytes",
            max_length=CAPSULE_SIZE_LIMIT_BYTES,
            subfield_limits={"label": CAPSULE_SIZE_LIMIT_LABEL, "serialization": "canonical_json_utf8"},
        ),
    ]
    additional = [
        _field_limit("context.retrieve.continuity_mode", "continuity_payload", "enum", ContextRetrieveRequest, "continuity_mode"),
        _field_limit("context.retrieve.continuity_resilience_policy", "continuity_payload", "enum", ContextRetrieveRequest, "continuity_resilience_policy"),
        _limit(
            "context.retrieve.continuity_selectors",
            "continuity_payload",
            "object_list",
            max_items=_field_constraint(ContextRetrieveRequest, "continuity_selectors", "max_length"),
            subfield_limits={
                "subject_kind": {"allowed_values": _literal_values(ContinuitySelector, "subject_kind")},
                "subject_id": {
                    "min_length": _field_constraint(ContinuitySelector, "subject_id", "min_length"),
                    "max_length": _field_constraint(ContinuitySelector, "subject_id", "max_length"),
                },
            },
        ),
        _field_limit("context.retrieve.continuity_selectors.subject_id", "continuity_payload", "string", ContinuitySelector, "subject_id"),
        _field_limit("context.retrieve.continuity_selectors.subject_kind", "continuity_payload", "enum", ContinuitySelector, "subject_kind"),
        _field_limit("context.retrieve.continuity_verification_policy", "continuity_payload", "enum", ContextRetrieveRequest, "continuity_verification_policy"),
        _field_limit("context.retrieve.limit", "continuity_payload", "number", ContextRetrieveRequest, "limit"),
        _field_limit("context.retrieve.subject_id", "continuity_payload", "string", ContextRetrieveRequest, "subject_id"),
        _field_limit("context.retrieve.subject_kind", "continuity_payload", "enum", ContextRetrieveRequest, "subject_kind"),
        _field_limit("context.retrieve.time_window_days", "continuity_payload", "number", ContextRetrieveRequest, "time_window_days"),
        _field_limit("continuity.active_concerns", "continuity_payload", "string_list", ContinuityState, "active_concerns", per_item_max_length=160),
        _field_limit("continuity.attention_policy.early_load", "continuity_payload", "string_list", ContinuityAttentionPolicy, "early_load"),
        _field_limit("continuity.attention_policy.presence_bias_overrides", "continuity_payload", "string_list", ContinuityAttentionPolicy, "presence_bias_overrides", per_item_max_length=160),
        _field_limit("continuity.canonical_sources", "continuity_payload", "string_list", ContinuityCapsule, "canonical_sources", subfield_limits={"pattern": "repo_relative_path"}),
        _field_limit("continuity.confidence.continuity", "continuity_payload", "number", ContinuityConfidence, "continuity"),
        _field_limit("continuity.confidence.relationship_model", "continuity_payload", "number", ContinuityConfidence, "relationship_model"),
        _field_limit("continuity.curiosity_queue", "continuity_payload", "string_list", ContinuityState, "curiosity_queue", per_item_max_length=120),
        _field_limit("continuity.drift_signals", "continuity_payload", "string_list", ContinuityState, "drift_signals", per_item_max_length=160),
        _utc_timestamp_limit("continuity.freshness.expires_at"),
        _field_limit("continuity.freshness.freshness_class", "continuity_payload", "enum", ContinuityFreshness, "freshness_class"),
        _field_limit("continuity.freshness.stale_after_seconds", "continuity_payload", "number", ContinuityFreshness, "stale_after_seconds"),
        _field_limit("continuity.long_horizon_commitments", "continuity_payload", "string_list", ContinuityState, "long_horizon_commitments", per_item_max_length=160),
        _limit(
            "continuity.metadata",
            "continuity_payload",
            "object",
            max_items=12,
            subfield_limits={
                "max_items": 12,
                "values": {"scalar_only": True},
                "interaction_boundary_kind": {"allowed_values": _INTERACTION_BOUNDARY_KIND_VALUES},
            },
        ),
        _field_limit("continuity.relationship_model.preferred_style", "continuity_payload", "string_list", ContinuityRelationshipModel, "preferred_style", per_item_max_length=80),
        _field_limit("continuity.relationship_model.sensitivity_notes", "continuity_payload", "string_list", ContinuityRelationshipModel, "sensitivity_notes", per_item_max_length=120),
        _field_limit("continuity.relationship_model.trust_level", "continuity_payload", "enum", ContinuityRelationshipModel, "trust_level"),
        _field_limit("continuity.retrieval_hints.avoid", "continuity_payload", "string_list", ContinuityRetrievalHints, "avoid", per_item_max_length=160),
        _field_limit("continuity.retrieval_hints.load_next", "continuity_payload", "string_list", ContinuityRetrievalHints, "load_next", subfield_limits={"pattern": "repo_relative_path"}),
        _field_limit("continuity.retrieval_hints.must_include", "continuity_payload", "string_list", ContinuityRetrievalHints, "must_include", per_item_max_length=160),
        _field_limit("continuity.schema_version", "continuity_payload", "enum", ContinuityCapsule, "schema_version"),
        _field_limit("continuity.source.inputs", "continuity_payload", "string_list", ContinuitySource, "inputs", per_item_max_length=200),
        _field_limit("continuity.source.producer", "continuity_payload", "string", ContinuitySource, "producer"),
        _field_limit("continuity.source.update_reason", "continuity_payload", "enum", ContinuitySource, "update_reason"),
        _limit(
            "continuity.stable_preferences",
            "continuity_payload",
            "object_list",
            max_items=_field_constraint(ContinuityCapsule, "stable_preferences", "max_length"),
            subfield_limits={
                "tag": {
                    "min_length": _field_constraint(StablePreference, "tag", "min_length"),
                    "max_length": _field_constraint(StablePreference, "tag", "max_length"),
                },
                "content": {
                    "min_length": _field_constraint(StablePreference, "content", "min_length"),
                    "max_length": _field_constraint(StablePreference, "content", "max_length"),
                },
                "timestamps": {"require_utc_when_present": True},
                "subject_kind": {"allowed_values": ["user", "peer"]},
            },
        ),
        _field_limit("continuity.subject_id", "continuity_payload", "string", ContinuityCapsule, "subject_id"),
        _field_limit("continuity.subject_kind", "continuity_payload", "enum", ContinuityCapsule, "subject_kind"),
        _field_limit("continuity.thread_descriptor.identity_anchors", "continuity_payload", "object_list", ThreadDescriptor, "identity_anchors", subfield_limits=_IDENTITY_ANCHOR_LIMITS),
        _field_limit("continuity.thread_descriptor.keywords", "continuity_payload", "string_list", ThreadDescriptor, "keywords", per_item_max_length=40),
        _field_limit("continuity.thread_descriptor.label", "continuity_payload", "string", ThreadDescriptor, "label"),
        _field_limit("continuity.thread_descriptor.scope_anchors", "continuity_payload", "string_list", ThreadDescriptor, "scope_anchors", subfield_limits=_SCOPE_ANCHOR_LIMITS),
        _field_limit("continuity.trailing_notes", "continuity_payload", "string_list", ContinuityState, "trailing_notes", per_item_max_length=160),
        _utc_timestamp_limit("continuity.updated_at"),
        _field_limit("continuity.upsert.commit_message", "continuity_payload", "string", ContinuityUpsertRequest, "commit_message"),
        _field_limit("continuity.upsert.idempotency_key", "continuity_payload", "string", ContinuityUpsertRequest, "idempotency_key"),
        _field_limit("continuity.upsert.lifecycle_transition", "continuity_payload", "enum", ContinuityUpsertRequest, "lifecycle_transition"),
        _field_limit("continuity.upsert.merge_mode", "continuity_payload", "enum", ContinuityUpsertRequest, "merge_mode"),
        _field_limit("continuity.upsert.subject_id", "continuity_payload", "string", ContinuityUpsertRequest, "subject_id"),
        _field_limit("continuity.upsert.subject_kind", "continuity_payload", "enum", ContinuityUpsertRequest, "subject_kind"),
        _field_limit("continuity.upsert.superseded_by", "continuity_payload", "string", ContinuityUpsertRequest, "superseded_by"),
        _utc_timestamp_limit("continuity.verified_at"),
        _field_limit("continuity.verification_kind", "continuity_payload", "enum", ContinuityCapsule, "verification_kind"),
        _field_limit("continuity.working_hypotheses", "continuity_payload", "string_list", ContinuityState, "working_hypotheses", per_item_max_length=160),
        _field_limit("continuity.patch.commit_message", "continuity_payload", "string", ContinuityPatchRequest, "commit_message"),
        _field_limit("continuity.patch.subject_id", "continuity_payload", "string", ContinuityPatchRequest, "subject_id"),
        _field_limit("continuity.patch.subject_kind", "continuity_payload", "enum", ContinuityPatchRequest, "subject_kind"),
        _utc_timestamp_limit("continuity.patch.updated_at", applies_to=["POST /v1/continuity/patch", "continuity.patch"]),
    ]
    table = {item["field_path"]: item for item in limits}
    for item in sorted(additional, key=lambda limit: limit["field_path"]):
        table[item["field_path"]] = item
    emitted_patch_targets = {path.removeprefix("patch.target.") for path in table if path.startswith("patch.target.")}
    if emitted_patch_targets != PATCH_ALL_TARGETS:
        raise RuntimeError("validation limit patch target coverage drifted from PATCH_ALL_TARGETS")
    return table


def _copy(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a defensive deep copy of a frozen help payload."""
    return deepcopy(payload)


def _validation_error(
    *,
    field: str,
    detail: str,
    allowed_values: list[str],
    correction_hint: str,
) -> JSONResponse:
    """Build the exact slice-1 HTTP validation error body."""
    return JSONResponse(
        status_code=400,
        content={
            "error": {
                "code": "validation",
                "detail": detail,
                "validation_hints": [
                    {
                        "field": field,
                        "area": "request.path",
                        "reason": "unsupported_value",
                        "limit": None,
                        "allowed_values": allowed_values,
                        "correction_hint": correction_hint,
                    }
                ],
            }
        },
    )


def help_root_payload() -> dict[str, Any]:
    """Return the exact slice-1 HTTP help root body."""
    return _copy(_ROOT_BODY)


def onboarding_section_ids() -> list[str]:
    """Return supported onboarding section ids in runtime order."""
    return list(_ONBOARDING_SECTION_ORDER)


def validation_limit_field_paths() -> list[str]:
    """Return supported validation-limit field paths in runtime order."""
    return list(_validation_limits_table())


def help_onboarding_index_payload() -> dict[str, Any]:
    """Return the bounded onboarding section index."""
    return {
        "kind": "onboarding_index",
        "recommended_first_section": "bootstrap",
        "sections": [
            {
                "id": section_id,
                "title": _ONBOARDING_SECTION_TITLES[section_id],
                "purpose": _ONBOARDING_INDEX_PURPOSES[section_id][0],
                "when_to_use": _ONBOARDING_INDEX_PURPOSES[section_id][1],
                "http_path": f"/v1/help/onboarding/sections/{section_id}",
                "mcp_method": "system.onboarding_section",
            }
            for section_id in _ONBOARDING_SECTION_ORDER
        ],
    }


def help_onboarding_bootstrap_payload() -> dict[str, Any]:
    """Return the compact onboarding bootstrap payload."""
    return {
        "kind": "onboarding_bootstrap",
        "recommended_first_section": "bootstrap",
        "startup_route": {
            "http": "POST /v1/continuity/read",
            "mcp_tool": "continuity.read",
            "params": {"view": "startup", "allow_fallback": True},
        },
        "retrieval_route": {
            "http": "POST /v1/context/retrieve",
            "mcp_tool": "context.retrieve",
            "when_to_use": "when the first work step needs bounded context beyond startup orientation",
        },
        "help_routes": {
            "tools": {"http": "GET /v1/help/tools/{name}", "mcp_method": "system.tool_usage"},
            "topics": {"http": "GET /v1/help/topics/{id}", "mcp_method": "system.topic_help"},
            "hooks": {"http": "GET /v1/help/hooks", "mcp_method": "system.hook_guide"},
            "errors": {"http": "GET /v1/help/errors/{code}", "mcp_method": "system.error_guide"},
        },
        "discover_more": {"http": "GET /v1/help/onboarding", "mcp_method": "system.onboarding_index"},
        "next_sections": ["hooks", "help_lookup", "limits_and_routing"],
        "warnings": [
            "Do not preload the full onboarding manual by default.",
            "Use field-specific validation-limit lookup after ordinary continuity validation failures.",
            "Do not expect schedule recurrence, SSE, callbacks, UI mutation, background execution, or automatic task/continuity mutation.",
            "Treat warnings, fallback, and degraded responses as caution signals, not crashes.",
        ],
    }


def help_onboarding_section_payload(section_id: str) -> dict[str, Any] | JSONResponse:
    """Return one bounded onboarding section or the exact validation error."""
    if section_id not in _ONBOARDING_SECTION_ORDER:
        return _validation_error(
            field="id",
            detail="Unsupported onboarding section id.",
            allowed_values=list(_ONBOARDING_SECTION_ORDER),
            correction_hint="Use one of the onboarding section ids returned by GET /v1/help/onboarding.",
        )
    related_http, related_mcp, references = _ONBOARDING_RELATED[section_id]
    return {
        "kind": "onboarding_section",
        "id": section_id,
        "title": _ONBOARDING_SECTION_TITLES[section_id],
        "format": "markdown_and_bullets",
        "body_md": _ONBOARDING_BODIES[section_id],
        "bullets": list(_ONBOARDING_BULLETS[section_id]),
        "related_http": list(related_http),
        "related_mcp": list(related_mcp),
        "references": list(references),
    }


def help_limits_index_payload() -> dict[str, Any]:
    """Return the bounded validation-limits index."""
    table = _validation_limits_table()
    groups = [
        ("continuity_orientation", "Continuity Orientation", "Startup-critical continuity orientation fields."),
        ("session_end_snapshot", "Session-End Snapshot", "Bounded session-end snapshot helper fields."),
        ("patch_targets", "Patch Targets", "Continuity patch operation and target limits."),
        ("retrieval_budget", "Retrieval Budget", "Context retrieval budget and capsule-count limits."),
        ("response_orientation_caps", "Response Orientation Caps", "Derived graph response caps separate from validation limits and the 20 KB capsule write cap."),
        ("capsule_write_cap", "Capsule Write Cap", "Serialized continuity capsule write cap."),
        ("continuity_payload", "Continuity Payload", "Additional public continuity/context payload limits."),
    ]
    return {
        "kind": "validation_limits_index",
        "groups": [
            {
                "id": group_id,
                "title": title,
                "purpose": purpose,
                "field_paths": [field_path for field_path, item in table.items() if item["category"] == group_id],
            }
            for group_id, title, purpose in groups
        ],
        "field_paths": list(table),
    }


def help_limit_payload(field_path: str) -> dict[str, Any] | JSONResponse:
    """Return one validation-limit item or the exact validation error."""
    table = _validation_limits_table()
    item = table.get(field_path)
    if item is None:
        return _validation_error(
            field="field_path",
            detail="Unsupported validation limit field path.",
            allowed_values=list(table),
            correction_hint="Use one of the field_path values returned by GET /v1/help/limits.",
        )
    return {"kind": "validation_limit", "limit": _copy(item)}


def is_forbidden_help_alias_path(path: str) -> bool:
    """Return ``True`` when a slash-suffixed alias targets the closed help surface."""
    if not path.startswith("/v1/help") or not path.endswith("/"):
        return False

    canonical_path = path[:-1]
    if canonical_path in _EXACT_HELP_PATHS:
        return True

    return any(
        canonical_path.startswith(prefix) and len(canonical_path) > len(prefix)
        for prefix in _PARAMETERIZED_HELP_PREFIXES
    )


def help_tool_payload(name: str) -> dict[str, Any] | JSONResponse:
    """Return the exact tool help body or the exact unsupported-tool validation error."""
    payload = _TOOLS.get(name)
    if payload is not None:
        return _copy(payload)
    return _validation_error(
        field="name",
        detail="Unsupported tool name.",
        allowed_values=list(_TOOL_IDS),
        correction_hint="Use one of the tool names returned by GET /v1/help.",
    )


def help_topic_payload(topic_id: str) -> dict[str, Any] | JSONResponse:
    """Return the exact topic help body or the exact unsupported-topic validation error."""
    payload = _TOPICS.get(topic_id)
    if payload is not None:
        return _copy(payload)
    return _validation_error(
        field="id",
        detail="Unsupported topic id.",
        allowed_values=list(_TOPIC_IDS),
        correction_hint="Use one of: continuity.read.startup_view, continuity.read.trust_signals, continuity.upsert.session_end_snapshot.",
    )


def help_hooks_payload() -> dict[str, Any]:
    """Return the exact slice-1 HTTP hook guidance body."""
    return _copy(_HOOKS)


def help_error_payload(code: str) -> dict[str, Any] | JSONResponse:
    """Return the exact error help body or the exact unsupported-error validation error."""
    payload = _ERRORS.get(code)
    if payload is not None:
        return _copy(payload)
    return _validation_error(
        field="code",
        detail="Unsupported error code.",
        allowed_values=list(_ERROR_CODES),
        correction_hint="Use one of: validation, tool_not_found, unknown_help_topic.",
    )


_MCP_HELP_METHODS = (
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
)

_MCP_ERROR_GUIDES = {
    -32700: {
        "title": "Parse error",
        "summary": "The request body must contain valid JSON before the server can apply JSON-RPC or MCP validation.",
    },
    -32600: {
        "title": "Invalid Request",
        "summary": "The JSON-RPC envelope itself is malformed, so fix the top-level request shape before retrying.",
    },
    -32601: {
        "title": "Method not found",
        "summary": "The method name is outside the recognized `#216` surface; correct the method string instead of retrying bootstrap.",
    },
    -32602: {
        "title": "Invalid params",
        "summary": "The method name is recognized, but the params shape or target value is invalid for that specific request.",
    },
    -32000: {
        "title": "Server not initialized",
        "summary": "Bootstrap is incomplete; complete `initialize` and then `notifications/initialized` before calling normal-operation methods.",
    },
    -32001: {
        "title": "Unauthorized",
        "summary": "The request needs valid bearer authentication before the addressed operation can proceed.",
    },
    -32002: {
        "title": "Forbidden",
        "summary": "The caller is authenticated or identified, but local policy, scope, or origin rules still block the requested operation.",
    },
    -32003: {
        "title": "Tool execution failed",
        "summary": "Request validation passed, but the tool failed during execution and should be retried only after correcting the underlying runtime issue.",
    },
    -32004: {
        "title": "Not Found",
        "summary": "The addressed object was missing only after request validation, auth, and target resolution had already succeeded.",
    },
}


def mcp_help_method_names() -> list[str]:
    """Return the exact slice-3 MCP help/reference request methods."""
    return list(_MCP_HELP_METHODS)


def is_mcp_help_method(name: str) -> bool:
    """Return whether *name* is one of the five slice-3 MCP help methods."""
    return name in _MCP_HELP_METHODS


def _mcp_result(structured_content: dict[str, Any]) -> dict[str, Any]:
    summary = str(structured_content["summary"])
    return {
        "content": [{"type": "text", "text": summary}],
        "structuredContent": structured_content,
    }


def _mcp_invalid_params(reason: str, *, field: str | None = None, **extra: Any) -> dict[str, Any]:
    data: dict[str, Any] = {"reason": reason}
    if field is not None:
        data["field"] = field
    data.update(extra)
    return data


def _ascii_whitespace_only(value: str) -> bool:
    return bool(value) and all(ch in {" ", "\t", "\n", "\r"} for ch in value)


def _validate_zero_param_method(name: str, params_present: bool, params: Any) -> dict[str, Any] | None:
    if not params_present:
        return None
    if not isinstance(params, dict):
        return _mcp_invalid_params("params must be an object")
    for key in params:
        return _mcp_invalid_params(f"unexpected {name} param", field=key)
    return None


def _validate_targeted_string_param(
    method: str,
    field_name: str,
    params_present: bool,
    params: Any,
) -> tuple[str | None, dict[str, Any] | None]:
    if not params_present or not isinstance(params, dict):
        return None, _mcp_invalid_params("params must be an object")
    for key in params:
        if key != field_name:
            return None, _mcp_invalid_params(f"unexpected {method} param", field=key)
    if field_name not in params:
        return None, _mcp_invalid_params(f"{field_name} is required")
    value = params[field_name]
    if not isinstance(value, str):
        return None, _mcp_invalid_params(f"{field_name} must be a non-empty string")
    if value == "" or _ascii_whitespace_only(value):
        return None, _mcp_invalid_params(f"{field_name} is required")
    return value, None


def _validate_error_code_param(params_present: bool, params: Any) -> tuple[int | None, dict[str, Any] | None]:
    if not params_present or not isinstance(params, dict):
        return None, _mcp_invalid_params("params must be an object")
    for key in params:
        if key != "code":
            return None, _mcp_invalid_params("unexpected system.error_guide param", field=key)
    if "code" not in params:
        return None, _mcp_invalid_params("code is required")
    code = params["code"]
    if isinstance(code, bool) or not isinstance(code, int):
        return None, _mcp_invalid_params("code must be an integer")
    return code, None


def resolve_mcp_help_method(
    name: str,
    *,
    params_present: bool,
    params: Any,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Resolve one slice-3 MCP help method into a result or an exact invalid-params body."""
    if name == "system.help":
        error = _validate_zero_param_method(name, params_present, params)
        if error is not None:
            return None, error
        return _mcp_result(
            {
                "surface": "help_index",
                "httpEquivalent": "/v1/help",
                "title": "CogniRelay Help Index",
                "summary": "Browse the canonical machine-facing help surfaces for tools, topics, hooks, and MCP error guidance.",
            }
        ), None

    if name == "system.hook_guide":
        error = _validate_zero_param_method(name, params_present, params)
        if error is not None:
            return None, error
        return _mcp_result(
            {
                "surface": "hook_guide",
                "httpEquivalent": "/v1/help/hooks",
                "title": "CogniRelay Hook Guide",
                "summary": "Review the canonical startup, prompt, persistence, and handoff hook guidance exposed by the HTTP help surface.",
            }
        ), None

    if name == "system.onboarding_index":
        error = _validate_zero_param_method(name, params_present, params)
        if error is not None:
            return None, error
        return _mcp_result(
            {
                "summary": "Browse the bounded CogniRelay onboarding index.",
                "httpEquivalent": "/v1/help/onboarding",
                **help_onboarding_index_payload(),
            }
        ), None

    if name == "system.onboarding_bootstrap":
        error = _validate_zero_param_method(name, params_present, params)
        if error is not None:
            return None, error
        return _mcp_result(
            {
                "summary": "Read the compact CogniRelay onboarding bootstrap.",
                "httpEquivalent": "/v1/help/onboarding/bootstrap",
                **help_onboarding_bootstrap_payload(),
            }
        ), None

    if name == "system.tool_usage":
        tool_name, error = _validate_targeted_string_param(name, "name", params_present, params)
        if error is not None:
            return None, error
        payload = help_tool_payload(tool_name or "")
        if isinstance(payload, JSONResponse):
            return None, _mcp_invalid_params("unknown tool", name=tool_name)
        return _mcp_result(
            {
                "surface": "tool_help",
                "httpEquivalent": f"/v1/help/tools/{tool_name}",
                "name": tool_name,
                "summary": str(payload["purpose"]),
            }
        ), None

    if name == "system.onboarding_section":
        section_id, error = _validate_targeted_string_param(name, "id", params_present, params)
        if error is not None:
            return None, error
        payload = help_onboarding_section_payload(section_id or "")
        if isinstance(payload, JSONResponse):
            return None, _mcp_invalid_params("unknown onboarding section", id=section_id)
        return _mcp_result(
            {
                "summary": f"Read CogniRelay onboarding section: {payload['title']}.",
                "httpEquivalent": f"/v1/help/onboarding/sections/{section_id}",
                **payload,
            }
        ), None

    if name == "system.validation_limits":
        error = _validate_zero_param_method(name, params_present, params)
        if error is not None:
            return None, error
        return _mcp_result(
            {
                "summary": "Browse bounded validation limits for agent-authored fields.",
                "httpEquivalent": "/v1/help/limits",
                **help_limits_index_payload(),
            }
        ), None

    if name == "system.validation_limit":
        field_path, error = _validate_targeted_string_param(name, "field_path", params_present, params)
        if error is not None:
            return None, error
        payload = help_limit_payload(field_path or "")
        if isinstance(payload, JSONResponse):
            return None, _mcp_invalid_params("unknown validation limit", field_path=field_path)
        return _mcp_result(
            {
                "summary": f"Read validation limits for {field_path}.",
                "httpEquivalent": f"/v1/help/limits/{field_path}",
                **payload,
            }
        ), None

    if name == "system.topic_help":
        topic_id, error = _validate_targeted_string_param(name, "id", params_present, params)
        if error is not None:
            return None, error
        payload = help_topic_payload(topic_id or "")
        if isinstance(payload, JSONResponse):
            return None, _mcp_invalid_params("unknown topic", id=topic_id)
        return _mcp_result(
            {
                "surface": "topic_help",
                "httpEquivalent": f"/v1/help/topics/{topic_id}",
                "id": topic_id,
                "title": topic_id,
                "summary": str(payload["purpose"]),
            }
        ), None

    code, error = _validate_error_code_param(params_present, params)
    if error is not None:
        return None, error
    guide = _MCP_ERROR_GUIDES.get(code)
    if guide is None:
        return None, _mcp_invalid_params("unknown error code", code=code)
    return _mcp_result(
        {
            "surface": "error_guide",
            "httpEquivalent": f"/v1/help/errors/{code}",
            "code": code,
            "title": guide["title"],
            "summary": guide["summary"],
        }
    ), None
