"""No-static-replies contract tests (owner directive: the patient never receives a
canned line — when nothing model-authored exists the turn suppresses and dispatches
a HIGH-priority handoff instead)."""
from __future__ import annotations

import asyncio

import app.api.v1.message as runner_mod
import app.db.repository as repo
from app.api.v1.message import _run
from tests.test_runner_flow import stub_io, valid_payload


def test_total_model_failure_suppresses_and_dispatches_handoff(monkeypatch):
    stub_io(monkeypatch)

    handoffs = []

    async def spy_handoff(payload):
        handoffs.append(payload)
        return {"success": True}

    async def failing_agent(um, context):
        raise RuntimeError("gateway down")

    async def failing_repair(prompt):
        raise RuntimeError("gateway down")

    async def failing_compose(context):
        raise RuntimeError("gateway down")

    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool", failing_agent)
    monkeypatch.setattr(runner_mod.dialogue, "call_repair_model", failing_repair)
    monkeypatch.setattr(runner_mod.dialogue, "compose_patient_reply", failing_compose)
    monkeypatch.setattr(runner_mod.handoff_service, "create_or_reuse_handoff", spy_handoff)

    r = asyncio.run(_run(valid_payload(message_text="أهلا"), {}))
    # No canned line: the reply suppresses and a human is dispatched.
    assert r.get("reply_text") is None
    assert r.get("suppress_reply") is True
    assert len(handoffs) == 1
    assert handoffs[0].reason_code == "MODEL_UNAVAILABLE"
    assert handoffs[0].priority == "high"


def test_claim_blocked_static_notice_never_reaches_the_patient(monkeypatch):
    """The deterministic claim-status notices are ledger metadata — a composer failure
    on a claim-blocked turn must not ship them as the patient's reply."""
    stub_io(monkeypatch)

    static_texts_seen = []
    handoffs = []

    async def failing_agent(um, context):
        raise RuntimeError("gateway down")

    async def failing_repair(prompt):
        raise RuntimeError("gateway down")

    async def failing_compose(context):
        raise RuntimeError("gateway down")

    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool", failing_agent)
    monkeypatch.setattr(runner_mod.dialogue, "call_repair_model", failing_repair)
    monkeypatch.setattr(runner_mod.dialogue, "compose_patient_reply", failing_compose)

    # claim-blocked: the ledger row answers with the deterministic IN_PROGRESS notice
    async def claimed(ctx):
        return {"operation_id": "op-1", "decision": "IN_PROGRESS",
                "child_execution_allowed": False,
                "response_json": None, "mutation_status": None}

    monkeypatch.setattr(repo, "claim_operation", claimed)

    async def spy_handoff(payload):
        handoffs.append(payload)
        return {"success": True}

    monkeypatch.setattr(runner_mod.handoff_service, "create_or_reuse_handoff", spy_handoff)

    r = asyncio.run(_run(valid_payload(message_text="أيوه أكد", source_event_id="evt-claim"), {}))
    for static in ("قيد التنفيذ", "لن أكرر", "تعذر التحقق", "لم يتم تنفيذ"):
        assert static not in (r.get("reply_text") or ""), \
            "deterministic status notices are ledger metadata, not patient replies"
    assert r.get("suppress_reply") is True and r.get("reply_text") is None
    assert handoffs, "a suppressed turn must dispatch a follow-up handoff"


def test_completed_mutation_reply_without_booking_number_fails_grounding():
    """Same rule as the composer (2026-09-19): on a completed mutation the booking
    number from THIS TURN's authoritative facts must surface — a reply that cites
    nothing may not dodge the requirement (the old cited-only check let a bare
    acknowledgment ship without it)."""
    from app.core.response_context import try_ground_primary_reply

    context = {
        "response_code": "APPOINTMENT_CREATED",
        "fact_ids": ["patient.current_message", "execution.create"],
        "facts": [
            {"id": "patient.current_message", "value": {"message": "أيوه أكد"}},
            {"id": "execution.create", "value": {"success": True, "booking_number": "BK-240918-01"}},
        ],
    }
    assert try_ground_primary_reply("أيوه صح ✅", context) is None
    grounded = try_ground_primary_reply("تم الحجز بنجاح، رقم الحجز BK-240918-01", context)
    assert grounded is not None and grounded["grounding_status"] == "supported"


def test_ungrounded_fallback_suppressed_when_composer_down(monkeypatch):
    """Invariant-3 gate (2026-09-19): with the composer down, the fallback candidate is
    the SAME draft the primary grounding rejected — it must not ship ungrounded. The
    turn suppresses and the handoff follows up instead."""
    import json as _json

    stub_io(monkeypatch)

    # A draft carrying a clock value that exists in NO fact — grounding rejects it.
    ungrounded = _json.dumps({
        "schema_version": "k2.dialogue.v4", "reply": "تمام، الحجز الساعة 15:00 ✅",
        "turn": {"intent": "booking_continuation", "relation_to_previous_turn": "follow_up"},
        "confidence": 0.95, "ambiguous": [], "confirmation": {"intent": "none"},
        "selection": {"kind": "none", "rank": None}, "entities": {},
        "operation_proposal": {"type": "none", "requested": False}, "escalate": None,
    }, ensure_ascii=False)

    handoffs = []

    async def spy_handoff(payload):
        handoffs.append(payload)
        return {"success": True}

    async def failing_compose(context):
        raise RuntimeError("gateway down")

    async def agent_turn(um, context):
        from app.services.dialogue import AgentTurnText
        return AgentTurnText(ungrounded, tool_events=[], llm_calls=1, usage=[])

    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool", agent_turn)
    monkeypatch.setattr(runner_mod.dialogue, "call_repair_model",
                        lambda prompt: _async_helper(ungrounded))
    monkeypatch.setattr(runner_mod.dialogue, "compose_patient_reply", failing_compose)
    monkeypatch.setattr(runner_mod.handoff_service, "create_or_reuse_handoff", spy_handoff)

    r = asyncio.run(_run(valid_payload(message_text="احجزلي بكرة"), {}))
    assert r.get("reply_text") is None
    assert r.get("suppress_reply") is True
    assert len(handoffs) == 1, "an ungrounded fallback must dispatch the human handoff"


def _async_helper(value):
    import asyncio

    async def _a():
        return value
    return _a()
