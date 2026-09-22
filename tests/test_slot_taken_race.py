"""Slot-taken race (55P03) — the concurrent-booking outcome must be a graceful,
deterministic reply, never a raw 500 (deep review 2026-09-23).

Scenario: two confirmations race the same slot. The DB function locks the slot
row FOR UPDATE and raises errcode 55P03 ('Slot is no longer available') for the
loser — the transaction rolled back, NOTHING was created. The runner must build
the SLOT_UNAVAILABLE failure envelope (which the ledger closes as
FAILED_FINAL/NOT_EXECUTED and the policy reports to the composer) instead of
letting the exception bubble as INTERNAL_ERROR.
"""
from __future__ import annotations

import asyncio
import json

import app.api.v1.message as runner_mod
import app.db.repository as repo
from app.api.v1.message import _run

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


def _reply_of(text):
    def _compose(context):
        return asyncio.sleep(0, {"reply": text, "evidence_ids": ["execution.create"],
                                 "missing_information": [], "unsupported_claims": [],
                                 "grounding_status": "supported", "raw_output": "{}",
                                 "usage": {}})
    return _compose


def _bound_state():
    return {
        "state_machine": {"current_state": "AWAIT_CONFIRMATION"},
        "operation_state": "AWAITING_CONFIRMATION",
        "operation_status": "awaiting_confirmation",
        "confirmation_state": "required",
        "confirmation_target": {
            "schema_version": 3, "action": "create_appointment",
            "clinic_id": CLINIC, "patient_id": PATIENT, "conversation_id": CONVERSATION,
            "doctor_id": DOCTOR, "doctor_name": "د. أحمد", "slot_id": SLOT,
            "date": "2026-09-24", "time": "10:30", "appointment_type": "NEW_VISIT",
            "patient_name": "حسام", "patient_phone": "+966500000000",
            "operation_id": "op-1", "confirmation_id": "c-1", "expires_at": FUTURE,
            "delivery": "pending", "source": "state_table_v3",
        },
        "presented_offer": {
            "kind": "presented_offer", "expires_at": FUTURE,
            "clinic_id": CLINIC, "conversation_id": CONVERSATION,
            "alternatives": [{"rank": 1, "slot_id": SLOT, "local_date": "2026-09-24", "local_time": "10:30"}],
        },
        "booking_context": {"doctor_id": DOCTOR, "doctor_name": "د. أحمد",
                            "appointment_type": "NEW_VISIT",
                            "date": "2026-09-24", "time": "10:30", "slot_id": SLOT,
                            "patient_name": "حسام", "patient_phone": "+966500000000",
                            "patient_age": 35, "patient_address": "القاهرة"},
        "facts": {"patient": {"name": "حسام"}},
        "recent_turns": [],
        "last_updated": "2026-09-18T12:00:00Z",
    }


class _SlotTakenError(RuntimeError):
    sqlstate = "55P03"


def test_slot_taken_55p03_is_graceful_not_500(monkeypatch):
    async def _async(v):
        return v

    saved_bodies = []

    async def fake_save(normalized, save_body):
        saved_bodies.append(save_body)
        STATE_STORE["state_data"] = save_body.get("state_data") or {}
        return {"initial": {"saved": True}, "retry": None}

    async def fake_state(normalized):
        return {"state_data": STATE_STORE.get("state_data", {}), "state_version": 1}

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
    monkeypatch.setattr(repo, "get_conversation_state", fake_state)
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
    monkeypatch.setattr(repo, "save_conversation_state_with_retry", fake_save)

    async def fake_create_raises(ctx):
        raise _SlotTakenError("Slot is no longer available")

    monkeypatch.setattr(repo, "execute_approved_create_appointment", fake_create_raises)
    monkeypatch.setattr(repo, "find_active_appointment_for_slot", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "finalize_operation",
                        lambda c: finalize_calls.append(dict(c)) or _async({"operation_id": c.get("finalize_operation_id")}))
    monkeypatch.setattr(repo, "claim_operation", lambda ctx: _async({
        "operation_id": "op-race", "decision": "OWNER", "child_execution_allowed": True}))
    monkeypatch.setattr(runner_mod.dialogue, "call_repair_model",
                        lambda p: _async(_contract("حصل تعارض", "confirmation", {})))
    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool",
                        lambda um, context: _async(_contract(
                            "أيوه أكد", "confirmation", {},
                            proposal={"type": "create_appointment", "requested": True},
                            relation="confirmation", confirm="affirmative",
                            selection={"kind": "presented_match", "rank": 1})))
    monkeypatch.setattr(runner_mod.dialogue, "compose_patient_reply", _reply_of(
        "الموعد ده اتاخد قبل ما تأكد — تحب أعرض لك المواعيد المتاحة تاني؟"))

    STATE_STORE["state_data"] = _bound_state()

    payload = {"clinic_id": CLINIC, "patient_id": PATIENT, "conversation_id": CONVERSATION,
               "message_text": "أيوه أكد", "channel_type": "whatsapp",
               "channel_id": "201000000000", "wamid": "evt-race",
               "source_event_id": "evt-race",
               "time_context": {"source": "runtime_now", "now_iso": "2026-09-18T12:00:00Z",
                                "now_local_date": "2026-09-18", "schema_version": 2}}

    r = asyncio.run(_run(payload, {}))   # must NOT raise

    assert r.get("response_code") == "SLOT_UNAVAILABLE", r.get("response_code")
    # the ledger closed deterministically: the DB rolled back → nothing executed
    failed = [c for c in finalize_calls if c.get("finalize_status") == "FAILED_FINAL"]
    assert failed, finalize_calls
    assert any(c.get("finalize_mutation_status") == "NOT_EXECUTED" for c in failed)
    # the dead target must not survive into the saved state
    assert not (STATE_STORE["state_data"].get("confirmation_target") or {}).get("slot_id")


def test_non_55p03_executor_error_still_bubbles(monkeypatch):
    """A real infrastructure crash must keep the existing behavior: ledger closed
    INCONCLUSIVE and the exception re-raised (500 to the sender) — the graceful
    SLOT_UNAVAILABLE path is reserved for the DB's 55P03 slot-taken outcome."""
    async def _async(v):
        return v

    saved_bodies = []

    async def fake_save(normalized, save_body):
        saved_bodies.append(save_body)
        STATE_STORE["state_data"] = save_body.get("state_data") or {}
        return {"initial": {"saved": True}, "retry": None}

    async def fake_state(normalized):
        return {"state_data": STATE_STORE.get("state_data", {}), "state_version": 1}

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
    monkeypatch.setattr(repo, "get_conversation_state", fake_state)
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
    monkeypatch.setattr(repo, "save_conversation_state_with_retry", fake_save)

    class _RealCrash(RuntimeError):
        sqlstate = "XX000"

    async def fake_create_real_crash(ctx):
        raise _RealCrash("connection to storage server lost")

    monkeypatch.setattr(repo, "execute_approved_create_appointment", fake_create_real_crash)
    monkeypatch.setattr(repo, "find_active_appointment_for_slot", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "finalize_operation",
                        lambda c: finalize_calls.append(dict(c)) or _async({"operation_id": c.get("finalize_operation_id")}))
    monkeypatch.setattr(repo, "claim_operation", lambda ctx: _async({
        "operation_id": "op-2", "decision": "OWNER", "child_execution_allowed": True}))
    monkeypatch.setattr(runner_mod.dialogue, "call_repair_model",
                        lambda p: _async(_contract("حصل تعارض", "confirmation", {})))
    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool",
                        lambda um, context: _async(_contract(
                            "أيوه أكد", "confirmation", {},
                            proposal={"type": "create_appointment", "requested": True},
                            relation="confirmation", confirm="affirmative",
                            selection={"kind": "presented_match", "rank": 1})))

    STATE_STORE["state_data"] = _bound_state()

    payload = {"clinic_id": CLINIC, "patient_id": PATIENT, "conversation_id": CONVERSATION,
               "message_text": "أيوه أكد", "channel_type": "whatsapp",
               "channel_id": "201000000000", "wamid": "evt-crash",
               "source_event_id": "evt-crash",
               "time_context": {"source": "runtime_now", "now_iso": "2026-09-18T12:00:00Z",
                                "now_local_date": "2026-09-18", "schema_version": 2}}
    raised = False
    try:
        asyncio.run(_run(payload, {}))
    except _RealCrash:
        raised = True
    assert raised, "the infrastructure crash must bubble (500 path preserved)"
    # ledger closed INCONCLUSIVE (outcome unknown) — never stranded IN_PROGRESS
    assert any(c.get("finalize_status") == "INCONCLUSIVE" for c in finalize_calls)
