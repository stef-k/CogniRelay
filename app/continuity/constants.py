"""Continuity capsule constants, warning strings, schema identifiers, and ordering tables."""

from __future__ import annotations

import re
from pathlib import Path

CONTINUITY_DIR_REL = "memory/continuity"
CONTINUITY_SUBJECT_RE = re.compile(r"^(task|thread):(.+)$")
THREAD_DESCRIPTOR_SCOPE_ANCHOR_RE = re.compile(r"^(user|peer|thread|task):[a-z0-9._-]{1,120}$")
THREAD_DESCRIPTOR_ANCHOR_KIND_RE = re.compile(r"^[a-z][a-z0-9_-]{0,39}$")
THREAD_LIFECYCLE_TRANSITIONS: dict[str, set[str]] = {
    "active": {"suspend", "conclude", "supersede"},
    "suspended": {"resume", "conclude", "supersede"},
}
THREAD_LIFECYCLE_TRANSITION_TARGETS: dict[str, str] = {
    "suspend": "suspended",
    "resume": "active",
    "conclude": "concluded",
    "supersede": "superseded",
}
CONTINUITY_PATH_RE = re.compile(r"^[A-Za-z0-9._/\-]+$")
CONTINUITY_DEFAULT_STALE: dict[str, int | None] = {
    "persistent": None,
    "durable": 15552000,
    "situational": 2592000,
    "ephemeral": 259200,
}
CONTINUITY_WARNING_STALE_SOFT = "continuity_stale_soft"
CONTINUITY_WARNING_STALE_HARD = "continuity_stale_hard"
CONTINUITY_WARNING_EXPIRED = "continuity_expired"
CONTINUITY_WARNING_TRUNCATED = "continuity_truncated_to_zero"
CONTINUITY_WARNING_TRUNCATED_MULTI = "continuity_capsule_truncated_to_zero"
CONTINUITY_WARNING_DEGRADED = "continuity_degraded"
CONTINUITY_WARNING_CONFLICTED = "continuity_conflicted"
CONTINUITY_WARNING_INVALID = "continuity_invalid_capsule"
CONTINUITY_WARNING_ACTIVE_MISSING = "continuity_active_missing"
CONTINUITY_WARNING_ACTIVE_INVALID = "continuity_active_invalid"
CONTINUITY_WARNING_FALLBACK_WRITE_FAILED = "continuity_fallback_write_failed"
CONTINUITY_WARNING_FALLBACK_USED = "continuity_fallback_used"
CONTINUITY_WARNING_FALLBACK_MISSING = "continuity_fallback_missing"
CONTINUITY_WARNING_STARTUP_SUMMARY_BUILD_FAILED = "startup_summary_build_failed"
CONTINUITY_FALLBACK_SCHEMA_TYPE = "continuity_fallback_snapshot"
CONTINUITY_FALLBACK_SCHEMA_VERSION = "1.0"
CONTINUITY_ARCHIVE_SCHEMA_TYPE = "continuity_archive_envelope"
CONTINUITY_ARCHIVE_SCHEMA_VERSION = "1.0"
CONTINUITY_COLD_STUB_SCHEMA_TYPE = "continuity_cold_stub"
CONTINUITY_COLD_STUB_SCHEMA_VERSION = "1.0"
CONTINUITY_REFRESH_STATE_SCHEMA_VERSION = "1.0"
CONTINUITY_INTERACTION_BOUNDARY_KINDS = {
    "person_switch",
    "thread_switch",
    "task_switch",
    "public_reply",
    "manual_checkpoint",
}
CONTINUITY_SIGNAL_RANK = {
    "self_review": 0,
    "external_observation": 1,
    "peer_confirmation": 2,
    "user_confirmation": 3,
    "system_check": 4,
}
CONTINUITY_COMPARE_TOP_LEVEL_ORDER = [
    "subject_kind",
    "subject_id",
    "schema_version",
    "updated_at",
    "source",
    "continuity",
    "confidence",
    "attention_policy",
    "freshness",
    "canonical_sources",
    "metadata",
    "thread_descriptor",
]
CONTINUITY_COMPARE_NESTED_ORDERS: dict[str, list[str]] = {
    "source": ["producer", "update_reason", "inputs"],
    "confidence": ["continuity", "relationship_model"],
    "freshness": ["freshness_class", "expires_at", "stale_after_seconds"],
    "attention_policy": ["early_load", "presence_bias_overrides"],
    "continuity": [
        "top_priorities",
        "active_concerns",
        "active_constraints",
        "open_loops",
        "stance_summary",
        "drift_signals",
        "working_hypotheses",
        "long_horizon_commitments",
        "session_trajectory",
        "negative_decisions",
        "trailing_notes",
        "curiosity_queue",
        "rationale_entries",
        "relationship_model",
        "retrieval_hints",
    ],
    "relationship_model": ["trust_level", "preferred_style", "sensitivity_notes"],
    "retrieval_hints": ["must_include", "avoid", "load_next"],
    "thread_descriptor": ["label", "keywords", "scope_anchors", "identity_anchors", "lifecycle", "superseded_by"],
}
CONTINUITY_COMPARE_IGNORED_FIELDS = {"verified_at", "verification_kind", "verification_state", "capsule_health"}
CONTINUITY_SIGNAL_STATUS = {
    "self_review": "self_attested",
    "external_observation": "externally_supported",
    "peer_confirmation": "peer_confirmed",
    "user_confirmation": "user_confirmed",
    "system_check": "system_confirmed",
}
CONTINUITY_HEALTH_ORDER = {"healthy": 0, "degraded": 1, "conflicted": 2}
CONTINUITY_PHASE_SEVERITY: dict[str, int] = {
    "fresh": 0,
    "stale_soft": 1,
    "stale_hard": 2,
    "expired_by_age": 3,
    "expired": 4,
}
CONTINUITY_WARNING_TRUST_SIGNALS_FAILED = "trust_signals_build_failed"
CONTINUITY_WARNING_TRUST_SIGNALS_COMPACT = "trust_signals_compact"
CONTINUITY_WARNING_TRUST_SIGNALS_AGGREGATE_FAILED = "trust_signals_aggregate_failed"
# Token overhead for the "trust_signals": null key/value when embedded in a
# capsule dict.  Precomputed: ceil(len("trust_signals: null") / 4) == 5.
_TRUST_SIGNALS_NULL_OVERHEAD_TOKENS = 5
CONTINUITY_REFRESH_STATE_REL = f"{CONTINUITY_DIR_REL}/refresh_state.json"
CONTINUITY_RETENTION_ARCHIVE_DAYS = 90
CONTINUITY_RETENTION_STATE_REL = f"{CONTINUITY_DIR_REL}/retention_state.json"
CONTINUITY_RETENTION_PLAN_SCHEMA_TYPE = "continuity_retention_plan"
CONTINUITY_RETENTION_PLAN_SCHEMA_VERSION = "1.0"
CONTINUITY_STATE_METADATA_FILES = {
    Path(CONTINUITY_REFRESH_STATE_REL).name,
    Path(CONTINUITY_RETENTION_STATE_REL).name,
}
CONTINUITY_COLD_DIR_REL = f"{CONTINUITY_DIR_REL}/cold"
CONTINUITY_COLD_INDEX_DIR_REL = f"{CONTINUITY_COLD_DIR_REL}/index"
CONTINUITY_COLD_STUB_SECTION_ORDER = [
    "top_priorities",
    "active_constraints",
    "active_concerns",
    "open_loops",
    "stance_summary",
    "drift_signals",
    "session_trajectory",
    "trailing_notes",
    "curiosity_queue",
    "negative_decisions",
    "rationale_entries",
]
CONTINUITY_COLD_STUB_FRONTMATTER_ORDER = [
    "type",
    "schema_version",
    "artifact_state",
    "subject_kind",
    "subject_id",
    "source_archive_path",
    "cold_storage_path",
    "archived_at",
    "cold_stored_at",
    "verification_kind",
    "verification_status",
    "health_status",
    "freshness_class",
    "phase",
    "update_reason",
]
