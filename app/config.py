"""Settings and token configuration loading for CogniRelay."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Set

from dotenv import load_dotenv


load_dotenv()


ALL_SCOPES = {
    "read:files",
    "read:index",
    "write:journal",
    "write:messages",
    "write:projects",
    "search",
    "compact:trigger",
    "admin:peers",
}

# Default maximum JSONL file size (bytes) that will be fully loaded into
# memory. Used as the fallback when Settings.max_jsonl_read_bytes is not
# provided (e.g. in direct test calls). See issue #75.
#
# NOTE: There is a small TOCTOU window between stat() and read_text(): a file
# can grow past the threshold after the check (risking OOM), or shrink below
# it (e.g. log rotation), causing a false-positive degraded response. This is
# an accepted trade-off; the guard is best-effort, not a guarantee.
DEFAULT_MAX_JSONL_READ_BYTES: int = 10 * 1024 * 1024  # 10 MB


@dataclass(frozen=True)
class PeerToken:
    """Normalized peer token record loaded from env or file configuration."""
    peer_id: str
    scopes: Set[str]
    read_namespaces: Set[str]
    write_namespaces: Set[str]
    token_id: str | None = None
    status: str = "active"
    expires_at: str | None = None
    issued_at: str | None = None
    description: str | None = None


@dataclass(frozen=True)
class Settings:
    """Runtime settings derived from environment variables and repository files."""
    repo_root: Path
    auto_init_git: bool
    git_author_name: str
    git_author_email: str
    tokens: Dict[str, PeerToken]
    audit_log_enabled: bool
    require_signed_ingress: bool = False

    # Go-live hardening controls
    use_external_key_store: bool = True
    key_store_path: Path = Path("~/.cognirelay/security_keys.json")
    max_payload_bytes: int = 262_144
    token_rate_limit_per_minute: int = 240
    ip_rate_limit_per_minute: int = 480
    verify_failure_limit: int = 20
    verify_failure_window_seconds: int = 600
    backlog_alarm_threshold: int = 100
    verification_alarm_threshold: int = 20
    replication_drift_max_age_seconds: int = 3_600
    contract_version: str = "2026-02-25"
    coordination_query_scan_threshold: int = 5000
    max_jsonl_read_bytes: int = DEFAULT_MAX_JSONL_READ_BYTES  # env override: COGNIRELAY_MAX_JSONL_READ_BYTES
    continuity_retention_archive_days: int = 90

    # Registry lifecycle settings (issue #112)
    delivery_terminal_retention_days: int = 30
    delivery_history_cold_after_days: int = 90
    delivery_idempotency_retention_days: int = 30
    nonce_retention_days: int = 7
    peer_trust_history_max_hot_entries: int = 32
    peer_trust_history_hot_retention_days: int = 30
    peer_trust_history_cold_after_days: int = 120
    replication_history_hot_retention_days: int = 14
    replication_history_cold_after_days: int = 90
    replication_pull_idempotency_retention_days: int = 14
    replication_tombstone_grace_days: int = 30
    replication_tombstone_cold_after_days: int = 90
    replication_tombstone_retention_days: int = 365
    registry_history_batch_limit: int = 500

    # Artifact lifecycle settings (issue #113)
    handoff_terminal_retention_days: int = 30
    handoff_cold_after_days: int = 90
    shared_history_hot_retention_days: int = 30
    shared_history_cold_after_days: int = 90
    reconciliation_resolved_retention_days: int = 30
    reconciliation_cold_after_days: int = 90
    task_done_hot_retention_days: int = 30
    task_done_cold_after_days: int = 90
    patch_applied_hot_retention_days: int = 30
    patch_applied_cold_after_days: int = 90
    artifact_history_batch_limit: int = 500

    # Segment-history lifecycle settings (issue #114)
    journal_cold_after_days: int = 30
    journal_retention_days: int = 365
    audit_log_rollover_bytes: int = 1_048_576
    audit_log_cold_after_days: int = 30
    audit_log_retention_days: int = 365
    ops_run_rollover_bytes: int = 1_048_576
    ops_run_cold_after_days: int = 30
    ops_run_retention_days: int = 365
    message_stream_rollover_bytes: int = 1_048_576
    message_stream_max_hot_days: int = 14
    message_stream_cold_after_days: int = 30
    message_stream_retention_days: int = 180
    message_thread_rollover_bytes: int = 2_097_152
    message_thread_inactivity_days: int = 30
    message_thread_cold_after_days: int = 60
    message_thread_retention_days: int = 365
    episodic_rollover_bytes: int = 1_048_576
    episodic_cold_after_days: int = 30
    episodic_retention_days: int = 180
    segment_history_batch_limit: int = 500


_cached: Settings | None = None


def _parse_bool(value: str | None, default: bool) -> bool:
    """Parse a boolean-like environment value with a default fallback."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(value: str | None, default: int, minimum: int | None = None) -> int:
    """Parse an integer-like environment value with default and minimum guards."""
    if value is None:
        return default
    try:
        out = int(value.strip())
    except Exception:
        return default
    if minimum is not None and out < minimum:
        return minimum
    return out


def _env_first(*names: str, default: str | None = None) -> str | None:
    """Return the first populated environment variable from a candidate list."""
    for name in names:
        value = os.getenv(name)
        if value is not None:
            return value
    return default


def sha256_token(token: str) -> str:
    """Hash a raw token into the repo-stored SHA256 representation."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _parse_tokens_inline(raw: str | None) -> Dict[str, PeerToken]:
    """Parse inline token configuration from the environment."""
    if not raw:
        return {}

    result: Dict[str, PeerToken] = {}
    for idx, item in enumerate([x.strip() for x in raw.split(",") if x.strip()]):
        peer_id = f"peer-{idx+1}"
        if ":" in item and "|" in item:
            token, scopes_raw = item.split(":", 1)
            scopes = {s.strip() for s in scopes_raw.split("|") if s.strip()}
            result[token] = PeerToken(peer_id=peer_id, scopes=scopes or set(ALL_SCOPES), read_namespaces={"*"}, write_namespaces={"*"})
        else:
            result[item] = PeerToken(peer_id=peer_id, scopes=set(ALL_SCOPES), read_namespaces={"*"}, write_namespaces={"*"})
    return result


def _load_tokens_file(repo_root: Path) -> Dict[str, PeerToken]:
    """Load peer tokens from the repository config file if present."""
    cfg_path = repo_root / "config" / "peer_tokens.json"
    if not cfg_path.exists():
        return {}

    data = json.loads(cfg_path.read_text(encoding="utf-8"))
    out: Dict[str, PeerToken] = {}
    for item in data.get("tokens", []):
        if not isinstance(item, dict):
            continue
        peer_id = str(item.get("peer_id", "unknown"))
        scopes = {str(s) for s in item.get("scopes", []) if str(s)} or set(ALL_SCOPES)
        legacy_namespaces = {str(n) for n in item.get("namespaces", []) if str(n)}
        read_namespaces = {str(n) for n in item.get("read_namespaces", []) if str(n)}
        write_namespaces = {str(n) for n in item.get("write_namespaces", []) if str(n)}
        if not read_namespaces:
            read_namespaces = legacy_namespaces or {"*"}
        if not write_namespaces:
            write_namespaces = legacy_namespaces or {"*"}

        token_obj = PeerToken(
            peer_id=peer_id,
            scopes=scopes,
            read_namespaces=read_namespaces,
            write_namespaces=write_namespaces,
            token_id=(str(item.get("token_id")) if item.get("token_id") else None),
            status=str(item.get("status") or "active"),
            expires_at=(str(item.get("expires_at")) if item.get("expires_at") else None),
            issued_at=(str(item.get("issued_at")) if item.get("issued_at") else None),
            description=(str(item.get("description")) if item.get("description") else None),
        )

        if item.get("token"):
            out[str(item["token"])] = token_obj
        if item.get("token_sha256"):
            out[f"sha256:{item['token_sha256']}"] = token_obj
    return out


def _merge_tokens(repo_root: Path) -> Dict[str, PeerToken]:
    """Merge file-based tokens with environment-provided tokens."""
    file_tokens = _load_tokens_file(repo_root)
    env_tokens = _parse_tokens_inline(_env_first("COGNIRELAY_TOKENS", "AMR_TOKENS"))
    # Env tokens override same raw key names.
    merged = {**file_tokens, **env_tokens}
    return merged


def _validate_segment_history_settings(settings: Settings) -> None:
    """Validate cross-field invariants for segment-history lifecycle settings.

    Raises SystemExit if any cold_after_days exceeds its corresponding
    retention_days or if any value is less than 1.
    """
    checks: list[tuple[str, int, str, int]] = [
        ("journal_cold_after_days", settings.journal_cold_after_days,
         "journal_retention_days", settings.journal_retention_days),
        ("audit_log_cold_after_days", settings.audit_log_cold_after_days,
         "audit_log_retention_days", settings.audit_log_retention_days),
        ("ops_run_cold_after_days", settings.ops_run_cold_after_days,
         "ops_run_retention_days", settings.ops_run_retention_days),
        ("message_stream_cold_after_days", settings.message_stream_cold_after_days,
         "message_stream_retention_days", settings.message_stream_retention_days),
        ("message_thread_cold_after_days", settings.message_thread_cold_after_days,
         "message_thread_retention_days", settings.message_thread_retention_days),
        ("episodic_cold_after_days", settings.episodic_cold_after_days,
         "episodic_retention_days", settings.episodic_retention_days),
    ]
    errors: list[str] = []
    for cold_name, cold_val, ret_name, ret_val in checks:
        if cold_val < 1:
            errors.append(f"{cold_name} ({cold_val}) must be >= 1")
        if ret_val < 1:
            errors.append(f"{ret_name} ({ret_val}) must be >= 1")
        if cold_val > ret_val:
            errors.append(
                f"{cold_name} ({cold_val}) must not exceed {ret_name} ({ret_val})"
            )
    # Validate standalone *_DAYS settings (not part of cold/retention pairs)
    standalone_days: list[tuple[str, int]] = [
        ("message_stream_max_hot_days", settings.message_stream_max_hot_days),
        ("message_thread_inactivity_days", settings.message_thread_inactivity_days),
    ]
    for name, val in standalone_days:
        if val < 1:
            errors.append(f"{name} ({val}) must be >= 1")
    # Validate byte thresholds
    byte_thresholds: list[tuple[str, int]] = [
        ("audit_log_rollover_bytes", settings.audit_log_rollover_bytes),
        ("ops_run_rollover_bytes", settings.ops_run_rollover_bytes),
        ("message_stream_rollover_bytes", settings.message_stream_rollover_bytes),
        ("message_thread_rollover_bytes", settings.message_thread_rollover_bytes),
        ("episodic_rollover_bytes", settings.episodic_rollover_bytes),
    ]
    for name, val in byte_thresholds:
        if val < 1:
            errors.append(f"{name} ({val}) must be >= 1")
    # Validate batch limit
    if settings.segment_history_batch_limit < 1:
        errors.append(
            f"segment_history_batch_limit ({settings.segment_history_batch_limit}) must be >= 1"
        )
    if errors:
        raise SystemExit(
            "Invalid segment-history settings:\n  " + "\n  ".join(errors)
        )


def get_settings(force_reload: bool = False) -> Settings:
    """Load and cache runtime settings for the current process."""
    global _cached
    if _cached is not None and not force_reload:
        return _cached

    repo_root_raw = _env_first("COGNIRELAY_REPO_ROOT", "AMR_REPO_ROOT", default="./data_repo") or "./data_repo"
    repo_root = Path(repo_root_raw).expanduser().resolve()

    key_store_raw = _env_first(
        "COGNIRELAY_KEY_STORE_PATH",
        "AMR_KEY_STORE_PATH",
        default="~/.cognirelay/security_keys.json",
    ) or "~/.cognirelay/security_keys.json"

    _cached = Settings(
        repo_root=repo_root,
        auto_init_git=_parse_bool(_env_first("COGNIRELAY_AUTO_INIT_GIT", "AMR_AUTO_INIT_GIT"), True),
        git_author_name=_env_first("COGNIRELAY_GIT_AUTHOR_NAME", "AMR_GIT_AUTHOR_NAME", default="CogniRelay Bot") or "CogniRelay Bot",
        git_author_email=_env_first("COGNIRELAY_GIT_AUTHOR_EMAIL", "AMR_GIT_AUTHOR_EMAIL", default="bot@example.local") or "bot@example.local",
        tokens=_merge_tokens(repo_root),
        audit_log_enabled=_parse_bool(_env_first("COGNIRELAY_AUDIT_LOG_ENABLED", "AMR_AUDIT_LOG_ENABLED"), True),
        require_signed_ingress=_parse_bool(_env_first("COGNIRELAY_REQUIRE_SIGNED_INGRESS", "AMR_REQUIRE_SIGNED_INGRESS"), False),
        use_external_key_store=_parse_bool(_env_first("COGNIRELAY_USE_EXTERNAL_KEY_STORE", "AMR_USE_EXTERNAL_KEY_STORE"), True),
        key_store_path=Path(key_store_raw).expanduser().resolve(),
        max_payload_bytes=_parse_int(_env_first("COGNIRELAY_MAX_PAYLOAD_BYTES", "AMR_MAX_PAYLOAD_BYTES"), 262_144, minimum=1024),
        token_rate_limit_per_minute=_parse_int(_env_first("COGNIRELAY_TOKEN_RATE_LIMIT_PER_MIN", "AMR_TOKEN_RATE_LIMIT_PER_MIN"), 240, minimum=1),
        ip_rate_limit_per_minute=_parse_int(_env_first("COGNIRELAY_IP_RATE_LIMIT_PER_MIN", "AMR_IP_RATE_LIMIT_PER_MIN"), 480, minimum=1),
        verify_failure_limit=_parse_int(_env_first("COGNIRELAY_VERIFY_FAILURE_LIMIT", "AMR_VERIFY_FAILURE_LIMIT"), 20, minimum=1),
        verify_failure_window_seconds=_parse_int(_env_first("COGNIRELAY_VERIFY_FAILURE_WINDOW_SECONDS", "AMR_VERIFY_FAILURE_WINDOW_SECONDS"), 600, minimum=60),
        backlog_alarm_threshold=_parse_int(_env_first("COGNIRELAY_BACKLOG_ALARM_THRESHOLD", "AMR_BACKLOG_ALARM_THRESHOLD"), 100, minimum=1),
        verification_alarm_threshold=_parse_int(_env_first("COGNIRELAY_VERIFICATION_ALARM_THRESHOLD", "AMR_VERIFICATION_ALARM_THRESHOLD"), 20, minimum=1),
        replication_drift_max_age_seconds=_parse_int(_env_first("COGNIRELAY_REPLICATION_DRIFT_MAX_AGE_SECONDS", "AMR_REPLICATION_DRIFT_MAX_AGE_SECONDS"), 3600, minimum=60),
        contract_version=_env_first("COGNIRELAY_CONTRACT_VERSION", "AMR_CONTRACT_VERSION", default="2026-02-25") or "2026-02-25",
        coordination_query_scan_threshold=_parse_int(
            _env_first("COGNIRELAY_COORDINATION_QUERY_SCAN_THRESHOLD"), 5000, minimum=100,
        ),
        max_jsonl_read_bytes=_parse_int(
            _env_first("COGNIRELAY_MAX_JSONL_READ_BYTES"), DEFAULT_MAX_JSONL_READ_BYTES, minimum=1024,
        ),
        continuity_retention_archive_days=_parse_int(
            _env_first("COGNIRELAY_CONTINUITY_RETENTION_ARCHIVE_DAYS"),
            90,
            minimum=1,
        ),
        delivery_terminal_retention_days=_parse_int(
            _env_first("COGNIRELAY_DELIVERY_TERMINAL_RETENTION_DAYS"), 30, minimum=1,
        ),
        delivery_history_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_DELIVERY_HISTORY_COLD_AFTER_DAYS"), 90, minimum=1,
        ),
        delivery_idempotency_retention_days=_parse_int(
            _env_first("COGNIRELAY_DELIVERY_IDEMPOTENCY_RETENTION_DAYS"), 30, minimum=1,
        ),
        nonce_retention_days=_parse_int(
            _env_first("COGNIRELAY_NONCE_RETENTION_DAYS"), 7, minimum=1,
        ),
        peer_trust_history_max_hot_entries=_parse_int(
            _env_first("COGNIRELAY_PEER_TRUST_HISTORY_MAX_HOT_ENTRIES"), 32, minimum=1,
        ),
        peer_trust_history_hot_retention_days=_parse_int(
            _env_first("COGNIRELAY_PEER_TRUST_HISTORY_HOT_RETENTION_DAYS"), 30, minimum=1,
        ),
        peer_trust_history_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_PEER_TRUST_HISTORY_COLD_AFTER_DAYS"), 120, minimum=1,
        ),
        replication_history_hot_retention_days=_parse_int(
            _env_first("COGNIRELAY_REPLICATION_HISTORY_HOT_RETENTION_DAYS"), 14, minimum=1,
        ),
        replication_history_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_REPLICATION_HISTORY_COLD_AFTER_DAYS"), 90, minimum=1,
        ),
        replication_pull_idempotency_retention_days=_parse_int(
            _env_first("COGNIRELAY_REPLICATION_PULL_IDEMPOTENCY_RETENTION_DAYS"), 14, minimum=1,
        ),
        replication_tombstone_grace_days=_parse_int(
            _env_first("COGNIRELAY_REPLICATION_TOMBSTONE_GRACE_DAYS"), 30, minimum=1,
        ),
        replication_tombstone_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_REPLICATION_TOMBSTONE_COLD_AFTER_DAYS"), 90, minimum=1,
        ),
        replication_tombstone_retention_days=_parse_int(
            _env_first("COGNIRELAY_REPLICATION_TOMBSTONE_RETENTION_DAYS"), 365, minimum=1,
        ),
        registry_history_batch_limit=_parse_int(
            _env_first("COGNIRELAY_REGISTRY_HISTORY_BATCH_LIMIT"), 500, minimum=1,
        ),
        handoff_terminal_retention_days=_parse_int(
            _env_first("COGNIRELAY_HANDOFF_TERMINAL_RETENTION_DAYS"), 30, minimum=1,
        ),
        handoff_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_HANDOFF_COLD_AFTER_DAYS"), 90, minimum=1,
        ),
        shared_history_hot_retention_days=_parse_int(
            _env_first("COGNIRELAY_SHARED_HISTORY_HOT_RETENTION_DAYS"), 30, minimum=1,
        ),
        shared_history_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_SHARED_HISTORY_COLD_AFTER_DAYS"), 90, minimum=1,
        ),
        reconciliation_resolved_retention_days=_parse_int(
            _env_first("COGNIRELAY_RECONCILIATION_RESOLVED_RETENTION_DAYS"), 30, minimum=1,
        ),
        reconciliation_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_RECONCILIATION_COLD_AFTER_DAYS"), 90, minimum=1,
        ),
        task_done_hot_retention_days=_parse_int(
            _env_first("COGNIRELAY_TASK_DONE_HOT_RETENTION_DAYS"), 30, minimum=1,
        ),
        task_done_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_TASK_DONE_COLD_AFTER_DAYS"), 90, minimum=1,
        ),
        patch_applied_hot_retention_days=_parse_int(
            _env_first("COGNIRELAY_PATCH_APPLIED_HOT_RETENTION_DAYS"), 30, minimum=1,
        ),
        patch_applied_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_PATCH_APPLIED_COLD_AFTER_DAYS"), 90, minimum=1,
        ),
        artifact_history_batch_limit=_parse_int(
            _env_first("COGNIRELAY_ARTIFACT_HISTORY_BATCH_LIMIT"), 500, minimum=1,
        ),
        # Segment-history lifecycle (issue #114)
        journal_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_JOURNAL_COLD_AFTER_DAYS"), 30, minimum=1,
        ),
        journal_retention_days=_parse_int(
            _env_first("COGNIRELAY_JOURNAL_RETENTION_DAYS"), 365, minimum=1,
        ),
        audit_log_rollover_bytes=_parse_int(
            _env_first("COGNIRELAY_AUDIT_LOG_ROLLOVER_BYTES"), 1_048_576, minimum=1,
        ),
        audit_log_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_AUDIT_LOG_COLD_AFTER_DAYS"), 30, minimum=1,
        ),
        audit_log_retention_days=_parse_int(
            _env_first("COGNIRELAY_AUDIT_LOG_RETENTION_DAYS"), 365, minimum=1,
        ),
        ops_run_rollover_bytes=_parse_int(
            _env_first("COGNIRELAY_OPS_RUN_ROLLOVER_BYTES"), 1_048_576, minimum=1,
        ),
        ops_run_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_OPS_RUN_COLD_AFTER_DAYS"), 30, minimum=1,
        ),
        ops_run_retention_days=_parse_int(
            _env_first("COGNIRELAY_OPS_RUN_RETENTION_DAYS"), 365, minimum=1,
        ),
        message_stream_rollover_bytes=_parse_int(
            _env_first("COGNIRELAY_MESSAGE_STREAM_ROLLOVER_BYTES"), 1_048_576, minimum=1,
        ),
        message_stream_max_hot_days=_parse_int(
            _env_first("COGNIRELAY_MESSAGE_STREAM_MAX_HOT_DAYS"), 14, minimum=1,
        ),
        message_stream_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_MESSAGE_STREAM_COLD_AFTER_DAYS"), 30, minimum=1,
        ),
        message_stream_retention_days=_parse_int(
            _env_first("COGNIRELAY_MESSAGE_STREAM_RETENTION_DAYS"), 180, minimum=1,
        ),
        message_thread_rollover_bytes=_parse_int(
            _env_first("COGNIRELAY_MESSAGE_THREAD_ROLLOVER_BYTES"), 2_097_152, minimum=1,
        ),
        message_thread_inactivity_days=_parse_int(
            _env_first("COGNIRELAY_MESSAGE_THREAD_INACTIVITY_DAYS"), 30, minimum=1,
        ),
        message_thread_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_MESSAGE_THREAD_COLD_AFTER_DAYS"), 60, minimum=1,
        ),
        message_thread_retention_days=_parse_int(
            _env_first("COGNIRELAY_MESSAGE_THREAD_RETENTION_DAYS"), 365, minimum=1,
        ),
        episodic_rollover_bytes=_parse_int(
            _env_first("COGNIRELAY_EPISODIC_ROLLOVER_BYTES"), 1_048_576, minimum=1,
        ),
        episodic_cold_after_days=_parse_int(
            _env_first("COGNIRELAY_EPISODIC_COLD_AFTER_DAYS"), 30, minimum=1,
        ),
        episodic_retention_days=_parse_int(
            _env_first("COGNIRELAY_EPISODIC_RETENTION_DAYS"), 180, minimum=1,
        ),
        segment_history_batch_limit=_parse_int(
            _env_first("COGNIRELAY_SEGMENT_HISTORY_BATCH_LIMIT"), 500, minimum=1,
        ),
    )
    _validate_segment_history_settings(_cached)
    return _cached
