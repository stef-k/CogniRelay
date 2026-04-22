"""Runtime hook orchestration tests for issue #215 slice 2."""

from __future__ import annotations

import copy
import unittest
from dataclasses import dataclass

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
    execute_post_prompt_hook,
    execute_pre_compaction_or_handoff_hook,
    execute_pre_prompt_hook,
    execute_startup_hook,
)


class _AuthStub:
    peer_id = "agent-alpha"


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

    def test_pre_compaction_omits_snapshot_when_lifecycle_changed(self) -> None:
        capsule = _capsule(thread_descriptor={"lifecycle": "suspended"})

        result = execute_pre_compaction_or_handoff_hook(
            capsule=capsule,
            session_end_snapshot=_snapshot(),
            auth=self.auth,
            deps=self.deps,
        )

        self.assertEqual(result.changed_fields, ["thread_descriptor.lifecycle"])
        self.assertFalse(result.used_session_end_snapshot)

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
            session_end_snapshot=_snapshot(),
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
