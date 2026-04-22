"""Runtime hook orchestration tests for issue #215 slice 2."""

from __future__ import annotations

import copy
import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.continuity.service import continuity_read_service, continuity_upsert_service
from app.models import (
    ContextRetrieveRequest,
    ContinuityCapsule,
    ContinuityReadRequest,
    ContinuityUpsertRequest,
    CoordinationHandoffCreateRequest,
    SessionEndSnapshot,
)
from app.runtime.hooks import (
    HookExecutionDependencies,
    HookLocalStep,
    _changed_eligible_fields,
    execute_post_prompt_hook,
    execute_pre_compaction_or_handoff_hook,
    execute_pre_prompt_hook,
    execute_startup_hook,
)


class _AuthStub:
    peer_id = "agent-alpha"

    def require(self, _scope: str) -> None:
        return None

    def require_read_path(self, _path: str) -> None:
        return None

    def require_write_path(self, _path: str) -> None:
        return None


def _capsule_payload() -> dict:
    return {
        "schema_version": "1.1",
        "subject_kind": "thread",
        "subject_id": "issue-215",
        "updated_at": "2026-04-22T12:00:00Z",
        "verified_at": "2026-04-22T12:00:00Z",
        "source": {
            "producer": "agent-runtime",
            "update_reason": "interaction_boundary",
            "inputs": [],
        },
        "continuity": {
            "top_priorities": ["land slice 2"],
            "active_concerns": ["do not broaden scope"],
            "active_constraints": ["work only on #215 slice 2"],
            "open_loops": ["verify hook ordering"],
            "stance_summary": "Implement the hardened runtime hook contract only.",
            "drift_signals": [],
            "working_hypotheses": [],
            "long_horizon_commitments": [],
            "session_trajectory": [],
            "negative_decisions": [],
            "trailing_notes": [],
            "curiosity_queue": [],
            "rationale_entries": [],
        },
        "confidence": {
            "continuity": 0.9,
            "relationship_model": 0.0,
        },
        "stable_preferences": [],
        "thread_descriptor": {
            "label": "Issue 215",
            "keywords": ["hooks"],
            "scope_anchors": ["CogniRelay"],
            "identity_anchors": [],
            "lifecycle": "active",
        },
    }


def _capsule(**updates: object) -> ContinuityCapsule:
    payload = _capsule_payload()
    for key, value in updates.items():
        if key == "continuity":
            payload["continuity"].update(value)  # type: ignore[arg-type]
        elif key == "thread_descriptor":
            thread_descriptor = payload.setdefault("thread_descriptor", {})
            if value is None:
                payload["thread_descriptor"] = None
            else:
                thread_descriptor.update(value)  # type: ignore[union-attr]
        else:
            payload[key] = value
    return ContinuityCapsule.model_validate(payload)


def _capsule_from_payload(payload: dict) -> ContinuityCapsule:
    return ContinuityCapsule.model_validate(payload)


def _snapshot() -> SessionEndSnapshot:
    return SessionEndSnapshot(
        open_loops=["verify handoff ordering"],
        top_priorities=["land slice 2"],
        active_constraints=["work only on #215 slice 2"],
        stance_summary="Persist startup-critical state before compaction.",
        negative_decisions=[],
        session_trajectory=["implemented tests"],
        rationale_entries=[],
    )


@dataclass
class _Recorder:
    calls: list[tuple[str, object]]

    def __init__(self) -> None:
        self.calls = []

    def continuity_read(self, req: ContinuityReadRequest, auth: object) -> dict:
        self.calls.append(("continuity_read", req))
        return {
            "ok": True,
            "path": "memory/continuity/thread-issue-215.json",
            "capsule": copy.deepcopy(_capsule_payload()),
            "archived": False,
            "source_state": "active",
            "recovery_warnings": [],
            "trust_signals": {"status": "healthy"},
        }

    def context_retrieve(self, req: ContextRetrieveRequest, auth: object) -> dict:
        self.calls.append(("context_retrieve", req))
        return {"ok": True, "task": req.task, "items": ["context"]}

    def continuity_upsert(self, req: ContinuityUpsertRequest, auth: object) -> dict:
        self.calls.append(("continuity_upsert", req))
        return {"ok": True, "updated": True}

    def handoff_create(self, req: CoordinationHandoffCreateRequest, auth: object) -> dict:
        self.calls.append(("handoff_create", req))
        return {"ok": True, "handoff": {"recipient_peer": req.recipient_peer}}


class _GitManagerStub:
    def __init__(self, repo_root: Path | None = None) -> None:
        self.repo_root = repo_root or Path(".")
        self.commits: list[tuple[str, str]] = []

    def latest_commit(self) -> str:
        return "test-sha"

    def commit_file(self, path: Path, message: str) -> bool:
        self.commits.append((str(path), message))
        return True


def _noop_audit(*_args: object, **_kwargs: object) -> None:
    return None


def _make_dirs(root: Path) -> None:
    (root / "memory" / "continuity").mkdir(parents=True, exist_ok=True)
    (root / "memory" / "continuity" / "fallback").mkdir(parents=True, exist_ok=True)
    (root / ".locks").mkdir(parents=True, exist_ok=True)


class TestRuntime215Slice2Hooks(unittest.TestCase):
    def setUp(self) -> None:
        self.auth = _AuthStub()
        self.recorder = _Recorder()
        self.deps = HookExecutionDependencies(
            continuity_read=self.recorder.continuity_read,
            context_retrieve=self.recorder.context_retrieve,
            continuity_upsert=self.recorder.continuity_upsert,
            handoff_create=self.recorder.handoff_create,
        )

    def test_startup_uses_exact_startup_read_and_forwards_result_unchanged(self) -> None:
        result = execute_startup_hook(
            subject_kind="thread",
            subject_id="issue-215",
            auth=self.auth,
            deps=self.deps,
        )

        self.assertEqual(result["source_state"], "active")
        self.assertEqual(len(self.recorder.calls), 1)
        call_name, req = self.recorder.calls[0]
        self.assertEqual(call_name, "continuity_read")
        assert isinstance(req, ContinuityReadRequest)
        self.assertEqual(req.subject_kind, "thread")
        self.assertEqual(req.subject_id, "issue-215")
        self.assertTrue(req.allow_fallback)
        self.assertEqual(req.view, "startup")

    def test_pre_prompt_calls_context_retrieve_only(self) -> None:
        req = ContextRetrieveRequest(
            task="land issue 215 slice 2",
            subject_kind="thread",
            subject_id="issue-215",
            continuity_mode="required",
        )

        result = execute_pre_prompt_hook(req=req, auth=self.auth, deps=self.deps)

        self.assertEqual(result, {"ok": True, "task": "land issue 215 slice 2", "items": ["context"]})
        self.assertEqual([name for name, _ in self.recorder.calls], ["context_retrieve"])

    def test_post_prompt_skips_when_only_non_eligible_fields_changed(self) -> None:
        capsule = _capsule(continuity={"working_hypotheses": ["new hypothesis"]})

        result = execute_post_prompt_hook(capsule=capsule, auth=self.auth, deps=self.deps)

        self.assertEqual(result.local_step, HookLocalStep.SKIPPED)
        self.assertEqual(result.changed_fields, [])
        self.assertEqual([name for name, _ in self.recorder.calls], ["continuity_read"])

    def test_post_prompt_uses_first_write_baseline(self) -> None:
        def missing_read(req: ContinuityReadRequest, auth: object) -> dict:
            self.recorder.calls.append(("continuity_read", req))
            return {
                "ok": True,
                "path": "memory/continuity/thread-issue-215.json",
                "capsule": None,
                "archived": False,
                "source_state": "missing",
                "recovery_warnings": ["continuity_active_missing", "continuity_fallback_missing"],
                "trust_signals": None,
            }

        deps = HookExecutionDependencies(
            continuity_read=missing_read,
            context_retrieve=self.recorder.context_retrieve,
            continuity_upsert=self.recorder.continuity_upsert,
            handoff_create=self.recorder.handoff_create,
        )

        skipped = execute_post_prompt_hook(
            capsule=_capsule(
                continuity={
                    "top_priorities": [],
                    "active_concerns": [],
                    "active_constraints": [],
                    "open_loops": [],
                    "stance_summary": "",
                    "drift_signals": [],
                    "working_hypotheses": ["non-eligible only"],
                },
                thread_descriptor=None,
            ),
            auth=self.auth,
            deps=deps,
        )
        written = execute_post_prompt_hook(
            capsule=_capsule(
                continuity={
                    "top_priorities": ["land slice 2"],
                    "active_concerns": [],
                    "active_constraints": [],
                    "open_loops": [],
                    "stance_summary": "",
                    "drift_signals": [],
                },
                thread_descriptor=None,
            ),
            auth=self.auth,
            deps=deps,
        )

        self.assertEqual(skipped.local_step, HookLocalStep.SKIPPED)
        self.assertEqual(written.local_step, HookLocalStep.WROTE)
        self.assertEqual([name for name, _ in self.recorder.calls], ["continuity_read", "continuity_read", "continuity_upsert"])

    def test_post_prompt_treats_reordered_arrays_as_changed(self) -> None:
        capsule = _capsule(continuity={"top_priorities": ["verify hook ordering", "land slice 2"]})

        result = execute_post_prompt_hook(capsule=capsule, auth=self.auth, deps=self.deps)

        self.assertEqual(result.local_step, HookLocalStep.WROTE)
        self.assertEqual(result.changed_fields, ["top_priorities"])
        self.assertEqual([name for name, _ in self.recorder.calls], ["continuity_read", "continuity_upsert"])

    def test_pre_compaction_uses_snapshot_only_when_snapshot_fields_changed(self) -> None:
        capsule = _capsule(continuity={"open_loops": ["verify handoff ordering"]})

        result = execute_pre_compaction_or_handoff_hook(
            capsule=capsule,
            session_end_snapshot=_snapshot(),
            auth=self.auth,
            deps=self.deps,
        )

        self.assertEqual(result.local_step, HookLocalStep.WROTE)
        self.assertTrue(result.used_session_end_snapshot)
        _, upsert_req = self.recorder.calls[-1]
        assert isinstance(upsert_req, ContinuityUpsertRequest)
        self.assertIsNotNone(upsert_req.session_end_snapshot)

    def test_pre_compaction_uses_snapshot_when_only_snapshot_overlay_changed(self) -> None:
        capsule = _capsule()

        result = execute_pre_compaction_or_handoff_hook(
            capsule=capsule,
            session_end_snapshot=_snapshot(),
            auth=self.auth,
            deps=self.deps,
        )

        self.assertEqual(result.local_step, HookLocalStep.WROTE)
        self.assertEqual(
            result.changed_fields,
            ["open_loops", "stance_summary", "session_trajectory"],
        )
        self.assertTrue(result.used_session_end_snapshot)
        _, upsert_req = self.recorder.calls[-1]
        assert isinstance(upsert_req, ContinuityUpsertRequest)
        self.assertIsNotNone(upsert_req.session_end_snapshot)

    def test_pre_compaction_omits_snapshot_when_non_snapshot_eligible_field_changed(self) -> None:
        capsule = _capsule(
            continuity={
                "open_loops": ["verify handoff ordering"],
                "active_concerns": ["changed concern"],
            }
        )

        result = execute_pre_compaction_or_handoff_hook(
            capsule=capsule,
            session_end_snapshot=_snapshot(),
            auth=self.auth,
            deps=self.deps,
        )

        self.assertEqual(result.local_step, HookLocalStep.WROTE)
        self.assertFalse(result.used_session_end_snapshot)
        _, upsert_req = self.recorder.calls[-1]
        assert isinstance(upsert_req, ContinuityUpsertRequest)
        self.assertIsNone(upsert_req.session_end_snapshot)

    def test_post_prompt_exact_compare_distinguishes_omitted_from_null_after_first_write(self) -> None:
        def read_with_null(req: ContinuityReadRequest, auth: object) -> dict:
            self.recorder.calls.append(("continuity_read", req))
            payload = copy.deepcopy(_capsule_payload())
            payload["continuity"]["negative_decisions"] = None
            return {
                "ok": True,
                "path": "memory/continuity/thread-issue-215.json",
                "capsule": payload,
                "archived": False,
                "source_state": "active",
                "recovery_warnings": [],
                "trust_signals": {"status": "healthy"},
            }

        deps = HookExecutionDependencies(
            continuity_read=read_with_null,
            context_retrieve=self.recorder.context_retrieve,
            continuity_upsert=self.recorder.continuity_upsert,
            handoff_create=self.recorder.handoff_create,
        )
        payload = _capsule_payload()
        payload["continuity"].pop("negative_decisions", None)

        result = execute_post_prompt_hook(
            capsule=_capsule_from_payload(payload),
            auth=self.auth,
            deps=deps,
        )

        self.assertEqual(result.local_step, HookLocalStep.WROTE)
        self.assertEqual(result.changed_fields, ["negative_decisions"])

    def test_post_prompt_exact_compare_distinguishes_omitted_from_empty_list_after_first_write(self) -> None:
        payload = _capsule_payload()
        payload.pop("stable_preferences", None)

        result = execute_post_prompt_hook(
            capsule=_capsule_from_payload(payload),
            auth=self.auth,
            deps=self.deps,
        )

        self.assertEqual(result.local_step, HookLocalStep.WROTE)
        self.assertEqual(result.changed_fields, ["stable_preferences"])

    def test_exact_compare_distinguishes_empty_string_from_omitted_when_persisted_exists(self) -> None:
        payload = _capsule_payload()
        payload["continuity"]["stance_summary"] = ""
        persisted = copy.deepcopy(_capsule_payload())
        persisted["continuity"].pop("stance_summary", None)

        changed_fields = _changed_eligible_fields(
            _capsule_from_payload(payload),
            persisted,
        )

        self.assertEqual(changed_fields, ["stance_summary"])

    def test_post_prompt_treats_lifecycle_delta_as_write_eligible(self) -> None:
        capsule = _capsule(thread_descriptor={"lifecycle": "suspended"})

        result = execute_post_prompt_hook(
            capsule=capsule,
            auth=self.auth,
            deps=self.deps,
        )

        self.assertEqual(result.local_step, HookLocalStep.WROTE)
        self.assertEqual(result.changed_fields, ["thread_descriptor.lifecycle"])
        _, upsert_req = self.recorder.calls[-1]
        assert isinstance(upsert_req, ContinuityUpsertRequest)
        self.assertEqual(upsert_req.lifecycle_transition, "suspend")
        self.assertIsNone(upsert_req.superseded_by)

    def test_pre_compaction_treats_superseded_by_delta_as_write_eligible_and_rejects_snapshot(self) -> None:
        capsule = _capsule(thread_descriptor={"lifecycle": "superseded", "superseded_by": "thread-next"})

        result = execute_pre_compaction_or_handoff_hook(
            capsule=capsule,
            session_end_snapshot=_snapshot(),
            auth=self.auth,
            deps=self.deps,
        )

        self.assertEqual(result.local_step, HookLocalStep.WROTE)
        self.assertEqual(
            result.changed_fields,
            [
                "open_loops",
                "stance_summary",
                "session_trajectory",
                "thread_descriptor.lifecycle",
                "thread_descriptor.superseded_by",
            ],
        )
        self.assertFalse(result.used_session_end_snapshot)
        _, upsert_req = self.recorder.calls[-1]
        assert isinstance(upsert_req, ContinuityUpsertRequest)
        self.assertIsNone(upsert_req.session_end_snapshot)
        self.assertEqual(upsert_req.lifecycle_transition, "supersede")
        self.assertEqual(upsert_req.superseded_by, "thread-next")

    def test_first_write_baseline_includes_lifecycle_and_superseded_by_null_semantics(self) -> None:
        changed_fields = _changed_eligible_fields(
            _capsule(thread_descriptor={"lifecycle": "superseded", "superseded_by": "thread-next"}),
            None,
        )

        self.assertEqual(
            changed_fields,
            [
                "top_priorities",
                "open_loops",
                "active_constraints",
                "active_concerns",
                "stance_summary",
                "thread_descriptor.lifecycle",
                "thread_descriptor.superseded_by",
            ],
        )

    def test_pre_compaction_real_upsert_path_persists_lifecycle_transition(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _make_dirs(root)
            auth = _AuthStub()
            gm = _GitManagerStub(root)
            seed_payload = _capsule_payload()
            seed_payload["source"]["update_reason"] = "manual"
            seed_payload["thread_descriptor"]["scope_anchors"] = ["thread:issue-215"]
            candidate_payload = _capsule_payload()
            candidate_payload["source"]["update_reason"] = "manual"
            candidate_payload["updated_at"] = "2026-04-22T12:05:00Z"
            candidate_payload["verified_at"] = "2026-04-22T12:05:00Z"
            candidate_payload["thread_descriptor"]["scope_anchors"] = ["thread:issue-215"]
            candidate_payload["thread_descriptor"]["lifecycle"] = "suspended"

            continuity_upsert_service(
                repo_root=root,
                gm=gm,
                auth=auth,
                req=ContinuityUpsertRequest(
                    subject_kind="thread",
                    subject_id="issue-215",
                    capsule=_capsule_from_payload(seed_payload),
                ),
                audit=_noop_audit,
            )

            deps = HookExecutionDependencies(
                continuity_read=lambda req, auth_ctx: continuity_read_service(
                    repo_root=root,
                    auth=auth_ctx,
                    req=req,
                    now=datetime.now(timezone.utc),
                    audit=_noop_audit,
                ),
                context_retrieve=self.recorder.context_retrieve,
                continuity_upsert=lambda req, auth_ctx: continuity_upsert_service(
                    repo_root=root,
                    gm=gm,
                    auth=auth_ctx,
                    req=req,
                    audit=_noop_audit,
                ),
                handoff_create=self.recorder.handoff_create,
            )

            result = execute_pre_compaction_or_handoff_hook(
                capsule=_capsule_from_payload(candidate_payload),
                auth=auth,
                deps=deps,
            )

            self.assertEqual(result.local_step, HookLocalStep.WROTE)
            stored = continuity_read_service(
                repo_root=root,
                auth=auth,
                req=ContinuityReadRequest(
                    subject_kind="thread",
                    subject_id="issue-215",
                    allow_fallback=True,
                ),
                now=datetime.now(timezone.utc),
                audit=_noop_audit,
            )
            self.assertEqual(stored["capsule"]["thread_descriptor"]["lifecycle"], "suspended")
            self.assertNotIn("superseded_by", stored["capsule"]["thread_descriptor"])

    def test_pre_compaction_real_upsert_path_persists_superseded_by_transition(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _make_dirs(root)
            auth = _AuthStub()
            gm = _GitManagerStub(root)
            seed_payload = _capsule_payload()
            seed_payload["source"]["update_reason"] = "manual"
            seed_payload["thread_descriptor"]["scope_anchors"] = ["thread:issue-215"]
            candidate_payload = _capsule_payload()
            candidate_payload["source"]["update_reason"] = "manual"
            candidate_payload["updated_at"] = "2026-04-22T12:05:00Z"
            candidate_payload["verified_at"] = "2026-04-22T12:05:00Z"
            candidate_payload["thread_descriptor"]["scope_anchors"] = ["thread:issue-215"]
            candidate_payload["thread_descriptor"]["lifecycle"] = "superseded"
            candidate_payload["thread_descriptor"]["superseded_by"] = "thread-next"

            continuity_upsert_service(
                repo_root=root,
                gm=gm,
                auth=auth,
                req=ContinuityUpsertRequest(
                    subject_kind="thread",
                    subject_id="issue-215",
                    capsule=_capsule_from_payload(seed_payload),
                ),
                audit=_noop_audit,
            )

            deps = HookExecutionDependencies(
                continuity_read=lambda req, auth_ctx: continuity_read_service(
                    repo_root=root,
                    auth=auth_ctx,
                    req=req,
                    now=datetime.now(timezone.utc),
                    audit=_noop_audit,
                ),
                context_retrieve=self.recorder.context_retrieve,
                continuity_upsert=lambda req, auth_ctx: continuity_upsert_service(
                    repo_root=root,
                    gm=gm,
                    auth=auth_ctx,
                    req=req,
                    audit=_noop_audit,
                ),
                handoff_create=self.recorder.handoff_create,
            )

            result = execute_pre_compaction_or_handoff_hook(
                capsule=_capsule_from_payload(candidate_payload),
                auth=auth,
                deps=deps,
            )

            self.assertEqual(result.local_step, HookLocalStep.WROTE)
            stored = continuity_read_service(
                repo_root=root,
                auth=auth,
                req=ContinuityReadRequest(
                    subject_kind="thread",
                    subject_id="issue-215",
                    allow_fallback=True,
                ),
                now=datetime.now(timezone.utc),
                audit=_noop_audit,
            )
            self.assertEqual(stored["capsule"]["thread_descriptor"]["lifecycle"], "superseded")
            self.assertEqual(stored["capsule"]["thread_descriptor"]["superseded_by"], "thread-next")

    def test_real_handoff_runs_after_explicit_local_skip(self) -> None:
        capsule = _capsule(continuity={"working_hypotheses": ["non-eligible only"]})
        handoff = CoordinationHandoffCreateRequest(
            recipient_peer="agent-beta",
            subject_kind="thread",
            subject_id="issue-215",
            thread_id="issue-215",
        )

        result = execute_pre_compaction_or_handoff_hook(
            capsule=capsule,
            real_handoff=handoff,
            auth=self.auth,
            deps=self.deps,
        )

        self.assertEqual(result.local_step, HookLocalStep.SKIPPED)
        self.assertTrue(result.handoff_created)
        self.assertEqual([name for name, _ in self.recorder.calls], ["continuity_read", "handoff_create"])

    def test_real_handoff_runs_only_after_local_write_completes(self) -> None:
        capsule = _capsule(continuity={"open_loops": ["verify handoff ordering"]})
        handoff = CoordinationHandoffCreateRequest(
            recipient_peer="agent-beta",
            subject_kind="thread",
            subject_id="issue-215",
            thread_id="issue-215",
        )

        result = execute_pre_compaction_or_handoff_hook(
            capsule=capsule,
            session_end_snapshot=_snapshot(),
            real_handoff=handoff,
            auth=self.auth,
            deps=self.deps,
        )

        self.assertEqual(result.local_step, HookLocalStep.WROTE)
        self.assertTrue(result.handoff_created)
        self.assertEqual(
            [name for name, _ in self.recorder.calls],
            ["continuity_read", "continuity_upsert", "handoff_create"],
        )


if __name__ == "__main__":
    unittest.main()
