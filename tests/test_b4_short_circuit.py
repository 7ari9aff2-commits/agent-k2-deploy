"""B4 cheap-turn short-circuit — regression tests (2026-09-25).

A clean affirm/deny of a live confirmation target must skip the primary LLM call
entirely (the contract is synthesized deterministically), while everything else
still goes through the normal path.
"""
from __future__ import annotations

import asyncio
import json

import app.api.v1.message as runner_mod
import app.db.repository as repo
from app.api.v1.message import _run
from app.services.dialogue import try_synthesize_confirmation_turn

CLINIC = "123e4567-e89b-42d3-a456-426614174000"
PATIENT = "123e4567-e89b-42d3-a456-426614174001"
CONVERSATION = "123e4567-e89b-42d3-a456-426614174002"
DOCTOR = "123e4567-e89b-42d3-a456-426614174004"
SLOT = "123e4567-e89b-42d3-a456-426614174005"
FUTURE = "2099-01-01T00:00:00Z"

STATE_STORE: dict = {}


def _contract(reply, intent, entities, proposal=None, relation="confirmation", confirm="affirmative",
              selection=None):
    return json.dumps({
        "schema_version": "k2.dialogue.v4", "reply": reply,
        "turn": {"intent": intent, "relation_to_previous_turn": relation},
        "confidence": 0.95, "ambiguous": [], "confirmation": {"intent": confirm},
        "selection": selection or {"kind": "none", "rank": None}, "entities": entities,
        "operation_proposal": proposal or {"type": "none", "requested": False},
        "escalate": None,
    }, ensure_ascii=False)


def _bound_state():
    return {
        "state_machine": {"current_state": "AWAIT_CONFIRMATION"},
        "operation_state": "AWAITING_CONFIRMATION",
        "operation_status": "awaiting_confirmation",
        "presented_offer": {
            "kind": "presented_offer", "expires_at": FUTURE,
            "clinic_id": CLINIC, "conversation_id": CONVERSATION,
            "alternatives": [{"rank": 1, "slot_id": SLOT, "local_date": "2026-09-24",
                              "local_time": "10:30"}],
        },
        "confirmation_state": "required",
        "confirmation_target": {
            "schema_version": 3, "action": "create_appointment",
            "clinic_id": CLINIC, "patient_id": PATIENT, "conversation_id": CONVERSATION,
            "doctor_id": DOCTOR, "doctor_name": "د. أحمد", "slot_id": SLOT,
            "date": "2026-09-24", "time": "10:30", "appointment_type": "NEW_VISIT",
            "patient_name": "حسام", "patient_phone": "+966500000000",
            "operation_id": "op-1", "confirmation_id": "c-1", "expires_at": FUTURE,
            "last_user_message_id_at_request": "m-1",
            "delivery": "pending", "source": "state_table_v3",
        },
        "booking_context": {"doctor_id": DOCTOR, "doctor_name": "د. أحمد",
                            "appointment_type": "NEW_VISIT", "date": "2026-09-24",
                            "time": "10:30", "slot_id": SLOT,
                            "patient_name": "حسام", "patient_phone": "+966500000000"},
    }


def _payload(text, event_id):
    return {"clinic_id": CLINIC, "patient_id": PATIENT, "conversation_id": CONVERSATION,
            "message_text": text, "channel_type": "whatsapp", "channel_id": "201000000000",
            "wamid": event_id, "source_event_id": event_id,
            "time_context": {"source": "runtime_now", "now_iso": "2026-09-18T12:00:00Z",
                             "now_local_date": "2026-09-18", "schema_version": 2}}


def _harness(monkeypatch):
    async def _async(v):
        return v

    finalize_calls = []
    monkeypatch.setattr(repo, "verify_k2_inbound_signature", lambda ctx: _async({"accepted": True}))
    monkeypatch.setattr(repo, "log_incoming_message", lambda n: _async({"id": "in", "duplicate": False}))
    monkeypatch.setattr(repo, "get_clinic_context", lambda n: _async({
        "clinic_id": CLINIC, "clinic_name": "عيادة النور", "clinic_timezone": "Asia/Riyadh",
        "clinic_found": True, "ownership_valid": True, "conversation_patient_id": PATIENT,
        "doctor_count": 1, "single_doctor_id": DOCTOR,
        "doctor_directory": [{"id": DOCTOR, "doctor_name": "د. أحمد"}]}))
    monkeypatch.setattr(repo, "k2_inbound_burst_rate_gate", lambda ctx: _async({"allowed": True}))
    monkeypatch.setattr(repo, "log_k2_rate_decision", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "mark_k2_burst_message_deferred", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "get_conversation_state", lambda n: _async({"state_data": STATE_STORE.get("state_data", {})}))
    monkeypatch.setattr(repo, "get_recent_window_2h", lambda ctx: _async({"conversation_history": []}))
    monkeypatch.setattr(repo, "get_active_handoff_request", lambda n: _async({}))
    monkeypatch.setattr(repo, "resolve_branch_inquiry", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "resolve_doctor_inquiry", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "resolve_service_fact", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "resolve_booking_ids", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "lookup_business_time_context", lambda ctx: _async({
        "timezone": "Asia/Riyadh", "timezone_configured": True,
        "business_hours": [{"day_of_week": 4, "open_time": "09:00", "close_time": "18:00"}],
        "slot_found": True,
        "start_time": "2026-09-24T10:30:00Z", "end_time": "2026-09-24T11:00:00Z"}))
    monkeypatch.setattr(repo, "persist_pending_confirmation", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "read_fresh_offer_midturn",
                        lambda ctx: _async({"presented_offer": STATE_STORE.get("state_data", {}).get("presented_offer")}))
    monkeypatch.setattr(repo, "log_agent_audit_entry", lambda e: _async({}))
    monkeypatch.setattr(repo, "insert_ai_request_usage", lambda u: _async("x"))
    monkeypatch.setattr(repo, "log_outgoing_message", lambda p: _async({"id": "out"}))
    monkeypatch.setattr(repo, "get_outgoing_reply", lambda n: _async(None))
    monkeypatch.setattr(repo, "save_conversation_state_with_retry", lambda n, b: _async({"initial": {"saved": True}}))
    monkeypatch.setattr(repo, "find_active_appointment_for_slot", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "claim_operation", lambda ctx: _async({
        "operation_id": "op-1", "decision": "OWNER", "child_execution_allowed": True}))

    async def fake_create(ctx):
        import uuid as _uuid
        apt = str(_uuid.UUID("123e4567-e89b-42d3-a456-426614174003"))
        return {"id": apt, "success": True, "response_code": "APPOINTMENT_CREATED",
                "appointment_id": apt, "booking_number": "BK-TEST-01", "public_id": "BK-TEST-01"}
    monkeypatch.setattr(repo, "execute_approved_create_appointment", fake_create)
    monkeypatch.setattr(repo, "finalize_operation",
                        lambda c: finalize_calls.append(dict(c)) or _async({"operation_id": c.get("finalize_operation_id")}))

    llm_calls = []

    def fake_llm(um, context):
        llm_calls.append(um)
        return _async(_contract("رد النموذج", "confirmation", {}, relation="confirmation",
                                confirm="affirmative", selection={"kind": "presented_match", "rank": 1}))
    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool", fake_llm)
    monkeypatch.setattr(runner_mod.dialogue, "compose_patient_reply",
                        lambda ctx2: _async({"reply": "تم الحجز ✅ رقم BK-TEST-01",
                                             "evidence_ids": ["execution.create"], "missing_information": [],
                                             "unsupported_claims": [], "grounding_status": "supported",
                                             "raw_output": "{}", "usage": {}}))
    return finalize_calls, llm_calls


def test_synthesizer_classification():
    st = {"confirmation_target": {"confirmation_id": "c1", "action": "create_appointment"}}
    assert try_synthesize_confirmation_turn(st, "أيوه أكد") is not None
    assert try_synthesize_confirmation_turn(st, "تمام") is not None
    assert try_synthesize_confirmation_turn(st, "لأ استنى") is not None
    # extra content → normal path
    assert try_synthesize_confirmation_turn(st, "أيوه بس غير الوقت") is None
    assert try_synthesize_confirmation_turn(st, "أيوه وأنا عايز أغير الدكتور كمان") is None
    # no live target → never synthesize
    assert try_synthesize_confirmation_turn({}, "أيوه") is None


def test_affirm_turn_skips_llm_and_executes(monkeypatch):
    finalize_calls, llm_calls = _harness(monkeypatch)
    STATE_STORE["state_data"] = _bound_state()
    r = asyncio.run(_run(_payload("أيوه أكد", "evt-b4"), {}))
    assert llm_calls == [], "the primary LLM must be skipped on a clean affirm"
    assert any(c.get("finalize_status") == "COMPLETED" for c in finalize_calls), finalize_calls
    assert "BK-TEST-01" in (r.get("reply_text") or "")


def test_ambiguous_affirm_still_uses_llm(monkeypatch):
    finalize_calls, llm_calls = _harness(monkeypatch)
    STATE_STORE["state_data"] = _bound_state()
    r = asyncio.run(_run(_payload("أيوه بس غير الوقت", "evt-b4b"), {}))
    assert len(llm_calls) == 1, "ambiguous content must go through the model"
