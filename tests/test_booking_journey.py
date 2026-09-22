"""Full booking journey test — the convergence artifact.

Five consecutive turns drive _run with a FAKE STATE STORE: whatever turn N saves is
what turn N+1 loads — mirroring the production sequence
greeting → request → availability offer → confirmation proposal → affirmation/execution.
This is the only test that exercises every fix from 2026-09-18 TOGETHER (entity
recovery → wiring → envelope/finalize → state identity → composer grounding →
booking-number rule → dedupe gating → guard/state pairing).
"""
from __future__ import annotations

import asyncio
import json
import re
import uuid

import app.api.v1.message as runner_mod
import app.db.repository as repo
from app.api.v1.message import _run
from app.services.dialogue import AgentTurnText

CLINIC = str(uuid.UUID("123e4567-e89b-42d3-a456-426614174000"))
PATIENT = str(uuid.UUID("123e4567-e89b-42d3-a456-426614174001"))
CONVERSATION = str(uuid.UUID("123e4567-e89b-42d3-a456-426614174002"))
APPOINTMENT = str(uuid.UUID("123e4567-e89b-42d3-a456-426614174003"))
DOCTOR = str(uuid.UUID("123e4567-e89b-42d3-a456-426614174004"))
SLOT = str(uuid.UUID("123e4567-e89b-42d3-a456-426614174005"))

STATE_STORE: dict = {}


def _contract(reply, intent, entities, proposal=None, relation="new_request", confirm="none",
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
    """Stub composer that states the booking number ONLY from cited facts — the same
    rule the real model is held to."""
    def _compose(context):
        code = context.get("response_code") or ""
        number = ""
        if code in ("APPOINTMENT_CREATED", "IDEMPOTENT_REPLAY"):
            for fact in context.get("facts") or []:
                if str(fact.get("id") or "").startswith(("execution.", "policy.", "decision.")):
                    values = json.dumps(fact.get("value"), ensure_ascii=False)
                    if "BK-" in values:
                        number = re.search(r"BK-[0-9-]+", values).group(0)
                        break
        evidence = ["patient.current_message"]
        if number:
            return asyncio.sleep(0, {
                "reply": f"تم الحجز بنجاح ✅ رقم الحجز {number}",
                "evidence_ids": ["execution.create"], "missing_information": [],
                "unsupported_claims": [], "grounding_status": "supported",
                "raw_output": "{}", "usage": {}})
        return asyncio.sleep(0, {
            "reply": text, "evidence_ids": evidence, "missing_information": [],
            "unsupported_claims": [], "grounding_status": "supported",
            "raw_output": "{}", "usage": {}})
    return _compose


def test_full_booking_journey(monkeypatch):
    async def _async(v):
        return v

    saved_bodies = []

    async def fake_save(normalized, save_body):
        saved_bodies.append(save_body)
        STATE_STORE["state_data"] = save_body.get("state_data") or {}
        return {"initial": {"saved": True}, "retry": None}

    async def fake_state(normalized):
        return {"state_data": STATE_STORE.get("state_data", {}), "state_version": len(saved_bodies) + 1}

    finalize_calls = []
    exec_calls = {}

    monkeypatch.setattr(repo, "verify_k2_inbound_signature", lambda ctx: _async({"accepted": True}))
    monkeypatch.setattr(repo, "log_incoming_message", lambda n: _async({"id": "in", "duplicate": False}))
    monkeypatch.setattr(repo, "get_clinic_context", lambda n: _async({
        "clinic_id": CLINIC, "clinic_name": "عيادة النور", "clinic_timezone": "Asia/Riyadh",
        "clinic_found": True, "ownership_valid": True, "conversation_patient_id": PATIENT,
        "doctor_count": 1, "clinic_phone": "+966500000000", "single_doctor_id": DOCTOR,
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
        # Production-shaped row: the gate verifies the SLOT against clinic hours —
        # an empty row means TIMEZONE_NOT_CONFIGURED and blocks every execution.
        # Slot 2026-09-24T10:30Z = Thursday 13:30 Riyadh (day_of_week 4, Sun=0).
        "timezone": "Asia/Riyadh", "timezone_configured": True,
        "business_hours": [{"day_of_week": 4, "open_time": "09:00", "close_time": "18:00"}],
        "slot_found": True,
        "start_time": "2026-09-24T10:30:00Z", "end_time": "2026-09-24T11:00:00Z"}))
    monkeypatch.setattr(repo, "persist_pending_confirmation", lambda ctx: _async({}))
    async def fake_fresh_offer(ctx):
        # Production parity: the availability tool persists a presented_offer mid-turn,
        # and Read Fresh Offer (Midturn) returns it so the state save preserves it.
        offer = STATE_STORE.get("state_data", {}).get("presented_offer")
        return {"presented_offer": offer} if offer else {}

    monkeypatch.setattr(repo, "read_fresh_offer_midturn", fake_fresh_offer)
    monkeypatch.setattr(repo, "log_agent_audit_entry", lambda e: _async({}))
    monkeypatch.setattr(repo, "insert_ai_request_usage", lambda u: _async("x"))
    monkeypatch.setattr(repo, "log_outgoing_message", lambda p: _async({"id": "out"}))
    monkeypatch.setattr(repo, "get_outgoing_reply", lambda n: _async(None))
    monkeypatch.setattr(repo, "save_conversation_state_with_retry", fake_save)

    async def fake_create(ctx):
        exec_calls["ran"] = True
        exec_calls["slot_id"] = (ctx or {}).get("slot_id")
        exec_calls["patient_phone"] = (ctx or {}).get("patient_phone")
        return {"id": APPOINTMENT, "success": True, "response_code": "APPOINTMENT_CREATED",
                "appointment_id": APPOINTMENT, "booking_number": "BK-240918-01",
                "public_id": "BK-240918-01"}

    monkeypatch.setattr(repo, "execute_approved_create_appointment", fake_create)
    # the double-booking guard is a REAL DB call — mock it (slot free in this scenario).
    # Without this mock the test silently hits production Supabase (CI caught it).
    monkeypatch.setattr(repo, "find_active_appointment_for_slot", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "finalize_operation",
                        lambda c: finalize_calls.append(dict(c)) or _async({"operation_id": c.get("finalize_operation_id")}))
    monkeypatch.setattr(repo, "claim_operation", lambda ctx: _async({
        "operation_id": "op-1", "decision": "OWNER", "child_execution_allowed": True}))
    monkeypatch.setattr(runner_mod.dialogue, "call_repair_model", lambda p: _async(_contract(
        "أهلاً بيك 🌸", "small_talk", {})))
    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool",
                        lambda um, context: _async(SMALL_CONTRACT))
    monkeypatch.setattr(runner_mod.dialogue, "compose_patient_reply", _reply_of("أهلاً بيك 🌸"))

    SMALL_CONTRACT = _contract("أهلاً بيك 🌸", "small_talk", {})

    def payload(text, event_id):
        return {"clinic_id": CLINIC, "patient_id": PATIENT, "conversation_id": CONVERSATION,
                "message_text": text, "channel_type": "whatsapp", "channel_id": "201000000000",
                "wamid": event_id, "source_event_id": event_id,
                "time_context": {"source": "runtime_now", "now_iso": "2026-09-18T12:00:00Z",
                                 "now_local_date": "2026-09-18", "schema_version": 2}}

    # ── T1: greeting ──
    r1 = asyncio.run(_run(payload("أهلا", "evt-1"), {}))
    assert r1["reply_text"] == "أهلاً بيك 🌸"
    assert "أهلاً بيك 🌸" in json.dumps(saved_bodies[-1], ensure_ascii=False), \
        "assistant reply text must persist (the historical loss)"

    # ── T2: booking request; tool round with the doctor NAME; contract drops it ──
    booking_no_doctor = _contract(
        "يسعدنا نحجز مع د. أحمد، نوع الزيارة؟", "booking_request",
        {"doctor_name": None}, relation="new_request")
    t2_turn = AgentTurnText(booking_no_doctor, tool_events=[{
        "name": "Check_Doctor_Availability",
        "arguments": {"doctor_id": "د. أحمد", "requested_date": "2026-09-19"},
        "result": {"success": True, "matched": False, "error_code": "DOCTOR_NOT_WORKING_THAT_DAY"},
    }], llm_calls=2)
    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool",
                        lambda um, context: _async(t2_turn))
    r2 = asyncio.run(_run(payload("احجز مع الدكتور احمد", "evt-2"), {}))
    assert "د. أحمد" in json.dumps(saved_bodies[-1], ensure_ascii=False), \
        "recovered doctor must reach the saved state"

    # the patient is known to the clinic (facts survive into the next turns)
    sd2 = STATE_STORE["state_data"]
    sd2.setdefault("facts", {})["patient"] = {"name": "حسام", "name_source": "user_entered",
                                              "phone": "+966500000000", "age": "30",
                                              "address": "شارع الملك فهد"}
    sd2["booking_context"].update({"patient_age": "30", "patient_address": "شارع الملك فهد"})

    # ── T3: availability — the tool returns the offer for Thursday ──
    STATE_STORE["state_data"]["presented_offer"] = {
        "kind": "presented_offer", "alternatives": [
            {"rank": 1, "slot_id": SLOT, "start_time": "2026-09-24T10:30:00Z",
             "local_date": "2026-09-24", "local_time": "10:30"}],
        "expires_at": "2099-01-01T00:00:00Z"}
    offer_contract = _contract("متاح الخميس 10:30 🌸", "availability_inquiry",
                               {"doctor_name": "د. أحمد", "date": "2026-09-24", "time": "10:30"},
                               relation="follow_up")
    t3_turn = AgentTurnText(offer_contract, tool_events=[{
        "name": "Check_Doctor_Availability",
        "arguments": {"doctor_id": "د. أحمد", "requested_date": "2026-09-24"},
        "result": {"success": True, "matched": True, "error_code": None,
                   "nearest_slots": [{"slot_id": SLOT, "start_time": "2026-09-24T10:30:00Z",
                                      "local_date": "2026-09-24", "local_time": "10:30"}]},
    }], llm_calls=2)
    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool",
                        lambda um, context: _async(t3_turn))
    r3 = asyncio.run(_run(payload("الخميس", "evt-3"), {}))
    assert "2026-09-24" in json.dumps(saved_bodies[-1], ensure_ascii=False), \
        "the offered date must persist for the confirmation turn"

    # ── T4: the patient picks the slot → the proposal turn saves AWAIT_CONFIRMATION ──
    proposal_contract = _contract(
        "تمام، أأكد لك حجز الكشف مع د. أحمد الخميس 10:30؟ 🌸", "booking_continuation",
        {"doctor_name": "د. أحمد", "date": "2026-09-24", "time": "10:30",
         "visit_type": "NEW_VISIT", "appointment_type": "NEW_VISIT",
         "patient_name": "حسام", "patient_phone": "+966500000000",
         "patient_age": "30", "patient_address": "شارع الملك فهد"},
        proposal={"type": "create_appointment", "requested": True},
        relation="follow_up",
        selection={"kind": "presented_match", "rank": 1})
    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool",
                        lambda um, context: _async(proposal_contract))
    r4 = asyncio.run(_run(payload("أيوه حجز", "evt-4"), {}))
    saved_after_proposal = STATE_STORE["state_data"]
    # The C1 patient-data gate fires first: the flow asks the patient to confirm
    # their identity fields before binding the offered slot (production order).
    assert ((saved_after_proposal.get("required_next_step") or {}).get("type")
            == "confirm_patient_data"), \
        "proposal turn with a pending data review must ask for data confirmation"

    # ── T5: the patient confirms their data → the target binds (AWAIT_CONFIRMATION) ──
    confirm_contract = _contract("أيوه صح ✅", "confirmation",
                                 {"patient_name": "حسام", "appointment_type": "NEW_VISIT",
                                  "date": "2026-09-24", "time": "10:30"},
                                 proposal={"type": "create_appointment", "requested": True},
                                 relation="answer", confirm="affirmative",
                                 selection={"kind": "presented_match", "rank": 1})
    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool",
                        lambda um, context: _async(confirm_contract))
    STATE_STORE["state_data"]["presented_offer"] = {
        "kind": "presented_offer", "alternatives": [
            {"rank": 1, "slot_id": SLOT, "start_time": "2026-09-24T10:30:00Z",
             "local_date": "2026-09-24", "local_time": "10:30"}],
        "expires_at": "2099-01-01T00:00:00Z"}
    r5 = asyncio.run(_run(payload("أيوه صح", "evt-5"), {}))
    bound = STATE_STORE["state_data"]
    # The binding arm (added 2026-09-18) offers the live slot for final confirmation
    # right after the data confirm — no wasted date re-ask, no CONVERSATION dead-end.
    assert bound.get("response_code") == "CONFIRMATION_REQUIRED", bound.get("response_code")
    assert (bound.get("confirmation_target") or {}).get("slot_id") == SLOT,         json.dumps(bound.get("confirmation_target"), ensure_ascii=False)[:200]

    # ── T6: the patient affirms the booking → executor → identity persists ──
    # FIXED 2026-09-19 (P42c same-slot execute + offered payload guard): the affirm
    # echo at AWAIT_CONFIRMATION now executes the bound target through the C1 gate.
    # The asserts below are STRICT — any regression here fails the suite.
    r6 = asyncio.run(_run(payload("أيوه أكد", "evt-6"), {}))
    assert exec_calls.get("ran") is True, "the affirm turn must execute the create"
    assert exec_calls.get("slot_id") == SLOT, \
        "the executor must receive the bound slot, got " + json.dumps(exec_calls, default=str)[:300]
    assert finalize_calls and finalize_calls[-1].get("finalize_status") == "COMPLETED", \
        json.dumps(finalize_calls, ensure_ascii=False)[:300]
    assert "BK-240918-01" in json.dumps(saved_bodies[-1], ensure_ascii=False), \
        "the booking number must persist into the saved state"
    assert "BK-240918-01" in (r6.get("reply_text") or ""), \
        "the patient reply must carry the booking number"


def test_double_booking_guard_refuses_recreate(monkeypatch):
    """Double-booking guard (2026-09-19): a create that committed but died before the
    state save leaves the conversation at AWAIT_CONFIRMATION; the patient's re-affirm
    mints a NEW operation_id the ledger never saw. The slot-scoped guard must refuse
    the second booking — the existing appointment becomes the result, no re-create."""
    import asyncio as _aio

    async def _async(v):
        return v

    store: dict = {"state_data": {
        "state_machine": {"current_state": "AWAIT_CONFIRMATION"},
        "confirmation_state": "required",
        "confirmation_target": {
            "schema_version": 3, "action": "create_appointment",
            "clinic_id": CLINIC, "patient_id": PATIENT, "conversation_id": CONVERSATION,
            "doctor_id": DOCTOR, "doctor_name": "د. أحمد", "slot_id": SLOT,
            "date": "2026-09-24", "time": "10:30", "appointment_type": "NEW_VISIT",
            "patient_name": "حسام", "patient_phone": "+966500000000",
            "operation_id": "op-old", "last_user_message_id_at_request": "evt-0",
            "confirmation_id": "c-old", "expires_at": "2099-01-01T00:00:00Z",
            "delivery": "pending", "source": "state_table_v3"},
        "presented_offer": {"kind": "presented_offer", "expires_at": "2099-01-01T00:00:00Z",
                            "clinic_id": CLINIC, "conversation_id": CONVERSATION,
                            "alternatives": [{"rank": 1, "slot_id": SLOT,
                                              "local_date": "2026-09-24", "local_time": "10:30"}]},
        "booking_context": {"doctor_id": DOCTOR, "doctor_name": "د. أحمد",
                            "appointment_type": "NEW_VISIT", "date": "2026-09-24",
                            "time": "10:30", "slot_id": SLOT,
                            "patient_name": "حسام", "patient_phone": "+966500000000"},
    }}
    saved: list = []

    async def fake_save(normalized, save_body):
        saved.append(save_body)
        return {"initial": {"saved": True}, "retry": None}

    async def fake_state(normalized):
        return {"state_data": store["state_data"], "state_version": 7}

    create_called: list = []

    async def spy_create(ctx):
        create_called.append(dict(ctx or {}))
        return {"id": APPOINTMENT, "success": True, "response_code": "APPOINTMENT_CREATED",
                "appointment_id": APPOINTMENT, "booking_number": "BK-DOUBLE-00",
                "public_id": "BK-DOUBLE-00"}

    finalize: list = []

    monkeypatch.setattr(repo, "verify_k2_inbound_signature", lambda ctx: _async({"accepted": True}))
    monkeypatch.setattr(repo, "log_incoming_message", lambda n: _async({"id": "in", "duplicate": False}))
    monkeypatch.setattr(repo, "get_clinic_context", lambda n: _async({
        "clinic_id": CLINIC, "clinic_name": "عيادة النور", "clinic_timezone": "Asia/Riyadh",
        "clinic_found": True, "ownership_valid": True, "conversation_patient_id": PATIENT,
        "doctor_count": 1, "clinic_phone": "+966500000000", "single_doctor_id": DOCTOR,
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
    monkeypatch.setattr(repo, "read_fresh_offer_midturn", lambda ctx: _async(
        {"presented_offer": store["state_data"].get("presented_offer")}))
    monkeypatch.setattr(repo, "log_agent_audit_entry", lambda e: _async({}))
    monkeypatch.setattr(repo, "insert_ai_request_usage", lambda u: _async("x"))
    monkeypatch.setattr(repo, "log_outgoing_message", lambda p: _async({"id": "out"}))
    monkeypatch.setattr(repo, "get_outgoing_reply", lambda n: _async(None))
    monkeypatch.setattr(repo, "save_conversation_state_with_retry", fake_save)
    monkeypatch.setattr(repo, "claim_operation", lambda ctx: _async({
        "operation_id": "op-new", "decision": "OWNER", "child_execution_allowed": True}))
    # THE GUARD: the slot is already booked for this patient.
    monkeypatch.setattr(repo, "find_active_appointment_for_slot", lambda ctx: _async(
        {"id": "123e4567-e89b-42d3-a456-426614174006", "public_id": "BK-240918-99",
         "booking_number": "BK-240918-99"}))
    monkeypatch.setattr(repo, "execute_approved_create_appointment", spy_create)
    monkeypatch.setattr(repo, "finalize_operation",
                        lambda c: finalize.append(dict(c)) or _async({"operation_id": c.get("finalize_operation_id")}))
    affirm_contract = _contract(
        "أيوه صح ✅", "confirmation",
        {"patient_name": "حسام", "appointment_type": "NEW_VISIT",
         "date": "2026-09-24", "time": "10:30"},
        proposal={"type": "create_appointment", "requested": True},
        relation="answer", confirm="affirmative",
        selection={"kind": "presented_match", "rank": 1})
    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool",
                        lambda um, context: _async(affirm_contract))
    monkeypatch.setattr(runner_mod.dialogue, "call_repair_model", lambda p: _async(SMALL_CONTRACT))
    monkeypatch.setattr(runner_mod.dialogue, "compose_patient_reply", _reply_of("أهلاً بيك 🌸"))

    def _payload(text, event_id):
        return {"clinic_id": CLINIC, "patient_id": PATIENT, "conversation_id": CONVERSATION,
                "message_text": text, "channel_type": "whatsapp", "channel_id": "201000000000",
                "wamid": event_id, "source_event_id": event_id,
                "time_context": {"source": "runtime_now", "now_iso": "2026-09-18T12:00:00Z",
                                 "now_local_date": "2026-09-18", "schema_version": 2}}

    r = _aio.run(_run(_payload("أيوه أكد", "evt-g"), {}))
    assert create_called == [], "the second booking must never run"
    assert finalize and finalize[-1].get("finalize_status") == "COMPLETED"
    assert "BK-240918-99" in (r.get("reply_text") or ""), \
        "the patient must be told the existing booking number, got " + str(r.get("reply_text"))[:200]
