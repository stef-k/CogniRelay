"""Tests for #120: Thread identity and continuity scope boundaries."""

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

from app.config import Settings
from app.main import continuity_list, continuity_read, continuity_upsert
from app.models import (
    ContinuityCapsule,
    ContinuityListRequest,
    ContinuityReadRequest,
    ContinuityUpsertRequest,
)
from app.continuity.validation import (
    _strip_service_managed_descriptor_fields,
    _validate_lifecycle_transition_request,
    _validate_thread_descriptor,
)

from fastapi import HTTPException

_CALL_COUNTER = 0


# ---------------------------------------------------------------------------
# Stubs & helpers
# ---------------------------------------------------------------------------


class _AuthStub:
    peer_id = "peer-test"

    def require(self, _scope: str) -> None:
        return None

    def require_read_path(self, _path: str) -> None:
        return None

    def require_write_path(self, _path: str) -> None:
        return None


class _GitManagerStub:
    def __init__(self, repo_root: Path | None = None) -> None:
        self.repo_root = repo_root or Path(".")
        self.commits: list[tuple[str, str]] = []

    def latest_commit(self) -> str:
        return "test-sha"

    def commit_file(self, path: Path, message: str) -> bool:
        self.commits.append((str(path), message))
        return True


def _settings(repo_root: Path) -> Settings:
    return Settings(
        repo_root=repo_root,
        auto_init_git=False,
        git_author_name="n/a",
        git_author_email="n/a",
        tokens={},
        audit_log_enabled=False,
    )


def _now_iso() -> str:
    """Return a unique, monotonically increasing UTC timestamp per call."""
    global _CALL_COUNTER
    _CALL_COUNTER += 1
    dt = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=_CALL_COUNTER)
    return dt.isoformat().replace("+00:00", "Z")


def _base_capsule(
    *,
    subject_kind: str = "thread",
    subject_id: str = "test-thread",
    thread_descriptor: dict | None = None,
    updated_at: str | None = None,
) -> dict:
    """Return a valid baseline capsule dict."""
    now = updated_at or _now_iso()
    capsule: dict[str, Any] = {
        "schema_version": "1.0",
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "updated_at": now,
        "verified_at": now,
        "verification_kind": "self_review",
        "source": {
            "producer": "test-hook",
            "update_reason": "pre_compaction",
            "inputs": [],
        },
        "continuity": {
            "top_priorities": ["priority-a"],
            "active_concerns": ["concern-a"],
            "active_constraints": ["constraint-a"],
            "open_loops": ["loop-a"],
            "stance_summary": "A sufficiently long stance summary for testing adequacy checks.",
            "drift_signals": [],
        },
        "confidence": {"continuity": 0.80, "relationship_model": 0.50},
    }
    if thread_descriptor is not None:
        capsule["thread_descriptor"] = thread_descriptor
    return capsule


def _td(
    *,
    label: str = "Test Thread",
    keywords: list[str] | None = None,
    scope_anchors: list[str] | None = None,
    identity_anchors: list[dict] | None = None,
    lifecycle: str | None = None,
    superseded_by: str | None = None,
) -> dict:
    """Build a thread_descriptor dict."""
    d: dict[str, Any] = {"label": label}
    if keywords is not None:
        d["keywords"] = keywords
    if scope_anchors is not None:
        d["scope_anchors"] = scope_anchors
    if identity_anchors is not None:
        d["identity_anchors"] = identity_anchors
    if lifecycle is not None:
        d["lifecycle"] = lifecycle
    if superseded_by is not None:
        d["superseded_by"] = superseded_by
    return d


def _do_upsert(
    repo_root: Path,
    capsule: dict,
    *,
    lifecycle_transition: str | None = None,
    superseded_by: str | None = None,
) -> dict:
    """Execute a continuity upsert through the main endpoint."""
    gm = _GitManagerStub()
    s = _settings(repo_root)
    req_data: dict[str, Any] = {
        "subject_kind": capsule["subject_kind"],
        "subject_id": capsule["subject_id"],
        "capsule": capsule,
    }
    if lifecycle_transition is not None:
        req_data["lifecycle_transition"] = lifecycle_transition
    if superseded_by is not None:
        req_data["superseded_by"] = superseded_by
    req = ContinuityUpsertRequest(**req_data)  # type: ignore[arg-type]
    with patch("app.main._services", return_value=(s, gm)):
        return continuity_upsert(req=req, auth=_AuthStub())


def _do_read(
    repo_root: Path,
    *,
    subject_kind: str = "thread",
    subject_id: str = "test-thread",
    allow_fallback: bool = False,
    view: str | None = None,
) -> dict:
    """Execute a continuity read through the main endpoint."""
    s = _settings(repo_root)
    gm = _GitManagerStub()
    req_data: dict[str, Any] = {
        "subject_kind": subject_kind,
        "subject_id": subject_id,
        "allow_fallback": allow_fallback,
    }
    if view is not None:
        req_data["view"] = view
    req = ContinuityReadRequest(**req_data)  # type: ignore[arg-type]
    with patch("app.main._services", return_value=(s, gm)):
        return continuity_read(req=req, auth=_AuthStub())


def _do_list(
    repo_root: Path,
    **kwargs: Any,
) -> dict:
    """Execute a continuity list through the main endpoint."""
    s = _settings(repo_root)
    gm = _GitManagerStub()
    req = ContinuityListRequest(**kwargs)  # type: ignore[arg-type]
    with patch("app.main._services", return_value=(s, gm)):
        return continuity_list(req=req, auth=_AuthStub())


# ===========================================================================
# 1. Model validation
# ===========================================================================


class TestThreadDescriptorValidation(unittest.TestCase):
    """Validate ThreadDescriptor field constraints."""

    def test_identity_anchor_valid(self) -> None:
        capsule = ContinuityCapsule(
            **_base_capsule(
                thread_descriptor=_td(identity_anchors=[{"kind": "repo", "value": "my-repo"}]),
            )
        )
        _validate_thread_descriptor(capsule)

    def test_identity_anchor_invalid_kind(self) -> None:
        capsule = ContinuityCapsule(
            **_base_capsule(
                thread_descriptor=_td(identity_anchors=[{"kind": "123bad", "value": "x"}]),
            )
        )
        with self.assertRaises(HTTPException) as ctx:
            _validate_thread_descriptor(capsule)
        self.assertEqual(ctx.exception.status_code, 400)

    def test_thread_descriptor_rejected_on_user(self) -> None:
        capsule = ContinuityCapsule(
            **_base_capsule(
                subject_kind="user",
                subject_id="test-user",
                thread_descriptor=_td(),
            )
        )
        with self.assertRaises(HTTPException) as ctx:
            _validate_thread_descriptor(capsule)
        self.assertIn("only allowed for thread and task", str(ctx.exception.detail))

    def test_thread_descriptor_rejected_on_peer(self) -> None:
        capsule = ContinuityCapsule(
            **_base_capsule(
                subject_kind="peer",
                subject_id="test-peer",
                thread_descriptor=_td(),
            )
        )
        with self.assertRaises(HTTPException) as ctx:
            _validate_thread_descriptor(capsule)
        self.assertIn("only allowed for thread and task", str(ctx.exception.detail))

    def test_thread_descriptor_accepted_on_task(self) -> None:
        capsule = ContinuityCapsule(
            **_base_capsule(
                subject_kind="task",
                subject_id="test-task",
                thread_descriptor=_td(),
            )
        )
        _validate_thread_descriptor(capsule)  # No exception

    def test_keyword_normalization(self) -> None:
        capsule = ContinuityCapsule(
            **_base_capsule(
                thread_descriptor=_td(keywords=["  FOO ", "foo", "Bar"]),
            )
        )
        _validate_thread_descriptor(capsule)
        self.assertEqual(capsule.thread_descriptor.keywords, ["foo", "bar"])

    def test_scope_anchor_valid(self) -> None:
        capsule = ContinuityCapsule(
            **_base_capsule(
                thread_descriptor=_td(scope_anchors=["user:alice", "thread:main-123"]),
            )
        )
        _validate_thread_descriptor(capsule)

    def test_scope_anchor_invalid(self) -> None:
        capsule = ContinuityCapsule(
            **_base_capsule(
                thread_descriptor=_td(scope_anchors=["invalid-format"]),
            )
        )
        with self.assertRaises(HTTPException) as ctx:
            _validate_thread_descriptor(capsule)
        self.assertIn("Invalid scope_anchor", str(ctx.exception.detail))

    def test_anchor_kind_normalization(self) -> None:
        capsule = ContinuityCapsule(
            **_base_capsule(
                thread_descriptor=_td(identity_anchors=[{"kind": "  Repo  ", "value": " my-repo "}]),
            )
        )
        _validate_thread_descriptor(capsule)
        self.assertEqual(capsule.thread_descriptor.identity_anchors[0].kind, "repo")
        self.assertEqual(capsule.thread_descriptor.identity_anchors[0].value, "my-repo")

    def test_anchor_duplicate_rejected(self) -> None:
        capsule = ContinuityCapsule(
            **_base_capsule(
                thread_descriptor=_td(
                    identity_anchors=[
                        {"kind": "repo", "value": "my-repo"},
                        {"kind": "repo", "value": "my-repo"},
                    ]
                ),
            )
        )
        with self.assertRaises(HTTPException) as ctx:
            _validate_thread_descriptor(capsule)
        self.assertIn("Duplicate identity_anchor", str(ctx.exception.detail))


# ===========================================================================
# 2. Write-side stripping
# ===========================================================================


class TestServiceManagedFieldStripping(unittest.TestCase):
    """Verify that caller-supplied lifecycle/superseded_by are discarded."""

    def test_lifecycle_stripped(self) -> None:
        capsule = ContinuityCapsule(
            **_base_capsule(
                thread_descriptor=_td(lifecycle="suspended"),
            )
        )
        _strip_service_managed_descriptor_fields(capsule)
        self.assertIsNone(capsule.thread_descriptor.lifecycle)

    def test_superseded_by_stripped(self) -> None:
        capsule = ContinuityCapsule(
            **_base_capsule(
                thread_descriptor=_td(superseded_by="other-thread"),
            )
        )
        _strip_service_managed_descriptor_fields(capsule)
        self.assertIsNone(capsule.thread_descriptor.superseded_by)


# ===========================================================================
# 3. First creation
# ===========================================================================


class TestFirstCreation(unittest.TestCase):
    """Thread descriptor defaults to active on first creation."""

    def test_first_creation_defaults_to_active(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = _do_upsert(Path(td), _base_capsule(thread_descriptor=_td()))
        self.assertTrue(out["ok"])
        self.assertEqual(out["lifecycle"], "active")

    def test_first_creation_rejects_lifecycle_transition(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(HTTPException) as ctx:
                _do_upsert(
                    Path(td),
                    _base_capsule(thread_descriptor=_td()),
                    lifecycle_transition="suspend",
                )
            self.assertIn("no thread_descriptor to transition", str(ctx.exception.detail))

    def test_pre_feature_capsule_upgrade(self) -> None:
        """Capsule without descriptor gets one added → defaults to active."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            # First upsert without descriptor
            _do_upsert(root, _base_capsule())
            # Second upsert adds descriptor
            out = _do_upsert(root, _base_capsule(thread_descriptor=_td()))
        self.assertEqual(out["lifecycle"], "active")


# ===========================================================================
# 4. Ordinary update
# ===========================================================================


class TestOrdinaryUpdate(unittest.TestCase):
    """Ordinary updates preserve stored lifecycle state."""

    def test_preserves_stored_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _do_upsert(root, _base_capsule(thread_descriptor=_td()))
            # Suspend it
            _do_upsert(
                root,
                _base_capsule(thread_descriptor=_td()),
                lifecycle_transition="suspend",
            )
            # Ordinary update (no transition) should stay suspended
            out = _do_upsert(root, _base_capsule(thread_descriptor=_td()))
        self.assertEqual(out["lifecycle"], "suspended")

    def test_updates_caller_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _do_upsert(root, _base_capsule(thread_descriptor=_td(label="First")))
            out = _do_upsert(root, _base_capsule(thread_descriptor=_td(label="Second")))
            self.assertTrue(out["ok"])
            # Read back to verify label changed
            read = _do_read(root)
            self.assertEqual(read["capsule"]["thread_descriptor"]["label"], "Second")


# ===========================================================================
# 5. Lifecycle transitions
# ===========================================================================


class TestLifecycleTransitions(unittest.TestCase):
    """All 6 allowed transitions and terminal state rejections."""

    def _create_active(self, root: Path) -> dict:
        return _do_upsert(root, _base_capsule(thread_descriptor=_td()))

    def test_active_to_suspended(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._create_active(root)
            out = _do_upsert(root, _base_capsule(thread_descriptor=_td()), lifecycle_transition="suspend")
        self.assertEqual(out["lifecycle"], "suspended")

    def test_active_to_concluded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._create_active(root)
            out = _do_upsert(root, _base_capsule(thread_descriptor=_td()), lifecycle_transition="conclude")
        self.assertEqual(out["lifecycle"], "concluded")

    def test_active_to_superseded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._create_active(root)
            out = _do_upsert(
                root,
                _base_capsule(thread_descriptor=_td()),
                lifecycle_transition="supersede",
                superseded_by="thread:better-thread",
            )
        self.assertEqual(out["lifecycle"], "superseded")

    def test_suspended_to_active(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._create_active(root)
            _do_upsert(root, _base_capsule(thread_descriptor=_td()), lifecycle_transition="suspend")
            out = _do_upsert(root, _base_capsule(thread_descriptor=_td()), lifecycle_transition="resume")
        self.assertEqual(out["lifecycle"], "active")

    def test_suspended_to_concluded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._create_active(root)
            _do_upsert(root, _base_capsule(thread_descriptor=_td()), lifecycle_transition="suspend")
            out = _do_upsert(root, _base_capsule(thread_descriptor=_td()), lifecycle_transition="conclude")
        self.assertEqual(out["lifecycle"], "concluded")

    def test_suspended_to_superseded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._create_active(root)
            _do_upsert(root, _base_capsule(thread_descriptor=_td()), lifecycle_transition="suspend")
            out = _do_upsert(
                root,
                _base_capsule(thread_descriptor=_td()),
                lifecycle_transition="supersede",
                superseded_by="thread:replacement",
            )
        self.assertEqual(out["lifecycle"], "superseded")

    def test_concluded_rejects_transition(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._create_active(root)
            _do_upsert(root, _base_capsule(thread_descriptor=_td()), lifecycle_transition="conclude")
            with self.assertRaises(HTTPException) as ctx:
                _do_upsert(root, _base_capsule(thread_descriptor=_td()), lifecycle_transition="resume")
            self.assertIn("lifecycle transition not allowed", str(ctx.exception.detail))

    def test_superseded_rejects_transition(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._create_active(root)
            _do_upsert(
                root,
                _base_capsule(thread_descriptor=_td()),
                lifecycle_transition="supersede",
                superseded_by="thread:new",
            )
            with self.assertRaises(HTTPException) as ctx:
                _do_upsert(root, _base_capsule(thread_descriptor=_td()), lifecycle_transition="resume")
            self.assertIn("lifecycle transition not allowed", str(ctx.exception.detail))

    def test_supersede_requires_superseded_by(self) -> None:
        with self.assertRaises(HTTPException) as ctx:
            req = ContinuityUpsertRequest(
                subject_kind="thread",
                subject_id="t",
                capsule=ContinuityCapsule(**_base_capsule(thread_descriptor=_td())),
                lifecycle_transition="supersede",
            )
            _validate_lifecycle_transition_request(req)
        self.assertIn("superseded_by is required", str(ctx.exception.detail))

    def test_superseded_by_forbidden_without_supersede(self) -> None:
        with self.assertRaises(HTTPException) as ctx:
            req = ContinuityUpsertRequest(
                subject_kind="thread",
                subject_id="t",
                capsule=ContinuityCapsule(**_base_capsule(thread_descriptor=_td())),
                lifecycle_transition="suspend",
                superseded_by="thread:other",
            )
            _validate_lifecycle_transition_request(req)
        self.assertIn("superseded_by is only allowed", str(ctx.exception.detail))


# ===========================================================================
# 6. lifecycle_transition without descriptor
# ===========================================================================


class TestLifecycleTransitionWithoutDescriptor(unittest.TestCase):
    """Transition requests without a thread_descriptor fail cleanly."""

    def test_transition_no_descriptor_on_capsule(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(HTTPException) as ctx:
                _do_upsert(
                    Path(td),
                    _base_capsule(),  # no thread_descriptor
                    lifecycle_transition="suspend",
                )
            self.assertIn("no thread_descriptor to transition", str(ctx.exception.detail))

    def test_transition_no_prior_descriptor_on_disk(self) -> None:
        """Capsule exists on disk without descriptor, then transition requested with descriptor."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            # First upsert: no descriptor
            _do_upsert(root, _base_capsule())
            # Second upsert: has descriptor + transition → should fail (no prior descriptor)
            with self.assertRaises(HTTPException) as ctx:
                _do_upsert(
                    root,
                    _base_capsule(thread_descriptor=_td()),
                    lifecycle_transition="suspend",
                )
            self.assertIn("no thread_descriptor to transition", str(ctx.exception.detail))


# ===========================================================================
# 7. List filtering
# ===========================================================================


class TestListFiltering(unittest.TestCase):
    """Thread descriptor list filters."""

    def _setup_capsules(self, root: Path) -> None:
        """Create a few capsules with varying descriptors."""
        _do_upsert(
            root,
            _base_capsule(
                subject_id="thread-a",
                thread_descriptor=_td(
                    label="Alpha Thread",
                    keywords=["deploy", "infra"],
                    scope_anchors=["user:alice"],
                    identity_anchors=[{"kind": "repo", "value": "main-repo"}],
                ),
            ),
        )
        _do_upsert(
            root,
            _base_capsule(
                subject_id="thread-b",
                thread_descriptor=_td(
                    label="Beta Thread",
                    keywords=["deploy"],
                    scope_anchors=["user:bob"],
                    identity_anchors=[{"kind": "ticket", "value": "JIRA-123"}],
                ),
            ),
        )
        # Suspend thread-b
        _do_upsert(
            root,
            _base_capsule(
                subject_id="thread-b",
                thread_descriptor=_td(
                    label="Beta Thread",
                    keywords=["deploy"],
                    scope_anchors=["user:bob"],
                    identity_anchors=[{"kind": "ticket", "value": "JIRA-123"}],
                ),
            ),
            lifecycle_transition="suspend",
        )
        # A capsule without descriptor
        _do_upsert(
            root,
            _base_capsule(
                subject_kind="user",
                subject_id="test-user",
            ),
        )

    def test_filter_by_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._setup_capsules(root)
            out = _do_list(root, lifecycle="active")
        self.assertEqual(out["count"], 1)
        self.assertEqual(out["capsules"][0]["subject_id"], "thread-a")

    def test_filter_by_scope_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._setup_capsules(root)
            out = _do_list(root, scope_anchor="user:alice")
        self.assertEqual(out["count"], 1)
        self.assertEqual(out["capsules"][0]["subject_id"], "thread-a")

    def test_filter_by_keyword(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._setup_capsules(root)
            out = _do_list(root, keyword="infra")
        self.assertEqual(out["count"], 1)
        self.assertEqual(out["capsules"][0]["subject_id"], "thread-a")

    def test_filter_by_label_exact(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._setup_capsules(root)
            out = _do_list(root, label_exact="Beta Thread")
        self.assertEqual(out["count"], 1)
        self.assertEqual(out["capsules"][0]["subject_id"], "thread-b")

    def test_filter_by_anchor_kind_and_value(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._setup_capsules(root)
            out = _do_list(root, anchor_kind="ticket", anchor_value="JIRA-123")
        self.assertEqual(out["count"], 1)
        self.assertEqual(out["capsules"][0]["subject_id"], "thread-b")

    def test_conjunctive_filters(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._setup_capsules(root)
            out = _do_list(root, keyword="deploy", lifecycle="active")
        self.assertEqual(out["count"], 1)
        self.assertEqual(out["capsules"][0]["subject_id"], "thread-a")

    def test_unique_match_true(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._setup_capsules(root)
            out = _do_list(root, label_exact="Alpha Thread")
        self.assertTrue(out["unique_match"])

    def test_unique_match_false_multiple(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._setup_capsules(root)
            out = _do_list(root, keyword="deploy")
        self.assertEqual(out["count"], 2)
        self.assertFalse(out["unique_match"])

    def test_unique_match_false_no_filters(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._setup_capsules(root)
            out = _do_list(root)
        self.assertFalse(out["unique_match"])

    def test_summary_includes_thread_descriptor(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _do_upsert(root, _base_capsule(thread_descriptor=_td(label="My Thread")))
            out = _do_list(root)
        row = out["capsules"][0]
        self.assertIn("thread_descriptor", row)
        self.assertEqual(row["thread_descriptor"]["label"], "My Thread")

    def test_capsules_without_descriptor_excluded_from_filtered(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            # Create capsule without descriptor
            _do_upsert(root, _base_capsule())
            out = _do_list(root, lifecycle="active")
        self.assertEqual(out["count"], 0)


# ===========================================================================
# 8. Read path
# ===========================================================================


class TestReadPath(unittest.TestCase):
    """Read-path warnings for superseded and missing descriptors."""

    def test_superseded_capsule_warning(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _do_upsert(root, _base_capsule(thread_descriptor=_td()))
            _do_upsert(
                root,
                _base_capsule(thread_descriptor=_td()),
                lifecycle_transition="supersede",
                superseded_by="thread:replacement",
            )
            out = _do_read(root)
        warnings = out["recovery_warnings"]
        self.assertTrue(any("continuity_capsule_superseded" in w for w in warnings))

    def test_active_capsule_no_superseded_warning(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _do_upsert(root, _base_capsule(thread_descriptor=_td()))
            out = _do_read(root)
        warnings = out["recovery_warnings"]
        self.assertFalse(any("continuity_capsule_superseded" in w for w in warnings))

    def test_startup_view_missing_descriptor_warning(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _do_upsert(root, _base_capsule())  # no descriptor
            out = _do_read(root, view="startup")
        rw = out["startup_summary"]["recovery"]["recovery_warnings"]
        self.assertTrue(any("continuity_thread_descriptor_missing" in w for w in rw))

    def test_startup_view_with_descriptor_no_warning(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _do_upsert(root, _base_capsule(thread_descriptor=_td()))
            out = _do_read(root, view="startup")
        rw = out["startup_summary"]["recovery"]["recovery_warnings"]
        self.assertFalse(any("continuity_thread_descriptor_missing" in w for w in rw))


# ===========================================================================
# 9. Backward compatibility
# ===========================================================================


class TestBackwardCompatibility(unittest.TestCase):
    """Capsules without thread_descriptor continue to work."""

    def test_capsule_without_descriptor_upsert(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            out = _do_upsert(Path(td), _base_capsule())
        self.assertTrue(out["ok"])
        self.assertNotIn("lifecycle", out)

    def test_capsule_without_descriptor_reads(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _do_upsert(root, _base_capsule())
            out = _do_read(root)
        self.assertTrue(out["ok"])
        self.assertIsNone(out["capsule"].get("thread_descriptor"))

    def test_list_without_filters_returns_all(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _do_upsert(root, _base_capsule())
            _do_upsert(
                root,
                _base_capsule(
                    subject_id="thread-2",
                    thread_descriptor=_td(),
                ),
            )
            out = _do_list(root)
        self.assertEqual(out["count"], 2)
        self.assertFalse(out["unique_match"])


# ===========================================================================
# 10. Size limit
# ===========================================================================


class TestSizeLimit(unittest.TestCase):
    """Maximally-sized thread_descriptor stays under 12 KB."""

    def test_max_descriptor_under_12kb(self) -> None:
        td = _td(
            label="x" * 120,
            keywords=["k" * 40] * 6,
            scope_anchors=["user:" + "a" * 115] * 4,
            identity_anchors=[
                {"kind": "a" * 40, "value": "v" * 200},
                {"kind": "b" * 40, "value": "v" * 200},
                {"kind": "c" * 40, "value": "v" * 200},
                {"kind": "d" * 40, "value": "v" * 200},
            ],
        )
        with tempfile.TemporaryDirectory() as tmp:
            out = _do_upsert(Path(tmp), _base_capsule(thread_descriptor=td))
        self.assertTrue(out["ok"])


# ===========================================================================
# 11. Archive roundtrip
# ===========================================================================


class TestArchiveRoundtrip(unittest.TestCase):
    """Thread descriptor survives archive and retrieval."""

    def test_archive_preserves_descriptor(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _do_upsert(root, _base_capsule(thread_descriptor=_td(label="Persistent Thread")))
            # Read back to verify descriptor is persisted
            out = _do_read(root)
            self.assertEqual(out["capsule"]["thread_descriptor"]["label"], "Persistent Thread")
            self.assertEqual(out["capsule"]["thread_descriptor"]["lifecycle"], "active")


if __name__ == "__main__":
    unittest.main()
