"""Offline end-to-end flow test: the real pipeline runner with stubbed I/O.

All stage functions (normalize, orchestrator, response policy, reply guard, agent output,
contract adapter, gates, resolvers' contexts...) execute for real; only the DB repository,
LLM calls, and sub-workflow services are stubbed. This validates the wiring of the 80-hop
n8n main path without touching the production database.
"""
import asyncio
import uuid
from unittest import mock

import app.api.v1.message as runner_mod
from app.api.v1.message import _run

CLINIC = str(uuid.UUID("123e4567-e89b-42d3-a456-426614174000"))
PATIENT = str(uuid.UUID("123e4567-e89b-42d3-a456-426614174001"))
CONVERSATION = str(uuid.UUID("123e4567-e89b-42d3-a456-426614174002"))

SMALL_TALK_CONTRACT = (
    '{"schema_version":"k2.dialogue.v4","reply":"أهلاً بك! كيف أقدر أساعدك؟",'
    '"turn":{"intent":"small_talk","relation_to_previous_turn":"new_request"},"confidence":0.9,'
    '"ambiguous":[],"confirmation":{"intent":"none"},"selection":{"kind":"none","rank":null},'
    '"entities":{},"operation_proposal":{"type":"none","requested":false},"escalate":null}'
)


def valid_payload(**over):
    payload = {
        "clinic_id": CLINIC,
        "patient_id": PATIENT,
        "conversation_id": CONVERSATION,
        "message_text": "اهلا",
        "channel_type": "whatsapp",
        "channel_id": "201000000000",
        "wamid": "wamid.TEST1",
        "source_event_id": "evt-1",
    }
    payload.update(over)
    return payload


def stub_io(monkeypatch, *, signature=None, incoming=None, gate=None, llm_output=SMALL_TALK_CONTRACT):
    import app.db.repository as repo

    async def _async(value):
        return value

    monkeypatch.setattr(repo, "verify_k2_inbound_signature", lambda ctx: _async(signature or {"accepted": True, "security_reject": False}))
    monkeypatch.setattr(repo, "log_incoming_message", lambda normalized: _async(incoming or {"id": "in-1", "duplicate": False}))
    # Mirrors the real Get Clinic Context SQL row: clinic_found + ownership_valid are
    # computed in-database and consumed by Validate Patient Ownership.
    monkeypatch.setattr(repo, "get_clinic_context", lambda normalized: _async({
        "clinic_id": CLINIC, "clinic_name": "عيادة مرنة", "clinic_timezone": "Asia/Riyadh",
        "clinic_found": True, "ownership_valid": True, "conversation_patient_id": PATIENT,
        "doctor_count": 1}))
    monkeypatch.setattr(repo, "k2_inbound_burst_rate_gate", lambda ctx: _async(gate or {"allowed": True}))
    monkeypatch.setattr(repo, "log_k2_rate_decision", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "mark_k2_burst_message_deferred", lambda ctx: _async({"batch_id": "b1", "batch_message_count": 1}))
    monkeypatch.setattr(repo, "get_conversation_state", lambda normalized: _async({"state_data": {}, "state_version": 3}))
    monkeypatch.setattr(repo, "get_recent_window_2h", lambda ctx: _async({"conversation_history": []}))
    monkeypatch.setattr(repo, "get_active_handoff_request", lambda normalized: _async({}))
    monkeypatch.setattr(repo, "resolve_branch_inquiry", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "resolve_doctor_inquiry", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "resolve_service_fact", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "persist_pending_confirmation", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "resolve_booking_ids", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "lookup_business_time_context", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "claim_operation", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "finalize_operation", lambda ctx: _async({}))
    monkeypatch.setattr(repo, "log_agent_audit_entry", lambda entry: _async({}))
    monkeypatch.setattr(repo, "insert_ai_request_usage", lambda usage: _async("INSERT 0 1"))
    monkeypatch.setattr(repo, "log_outgoing_message", lambda params: _async({"id": "out-1"}))
    monkeypatch.setattr(repo, "get_outgoing_reply", lambda normalized: _async("أهلاً بك! كيف أقدر أساعدك؟"))
    monkeypatch.setattr(repo, "save_conversation_state_with_retry",
                        lambda normalized, save_body: _async({"initial": {"saved": True}, "retry": None}))
    monkeypatch.setattr(repo, "read_fresh_offer_midturn", lambda ctx: _async({}))

    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool", lambda user_message, context: _async(llm_output))
    monkeypatch.setattr(runner_mod.dialogue, "call_repair_model", lambda prompt: _async(SMALL_TALK_CONTRACT))
    monkeypatch.setattr(runner_mod.dialogue, "compose_patient_reply", lambda context: _async({
        "reply": "أهلاً بيك، تحت أمرك. تحب أساعدك في إيه؟",
        "evidence_ids": ["patient.current_message"],
        "missing_information": [],
        "unsupported_claims": [],
        "grounding_status": "supported",
        "raw_output": "{}",
    }))
    monkeypatch.setattr(runner_mod.handoff_service, "create_or_reuse_handoff", lambda payload: _async({"success": True}))


def test_full_happy_path_small_talk(monkeypatch):
    stub_io(monkeypatch)
    result = asyncio.run(_run(valid_payload(), {}))
    assert isinstance(result, dict)
    assert result["conversation_id"] == CONVERSATION
    assert result["clinic_id"] == CLINIC
    assert result["outgoing_message_id"] == "out-1"
    assert isinstance(result["reply_text"], str) and result["reply_text"]
    assert result["response_code"]
    assert result["metadata"]["processed_at"]
    assert "deterministic_override" in result["_debug"]


def test_duplicate_message_exits_early(monkeypatch):
    stub_io(monkeypatch, incoming={"id": "in-1", "duplicate": True})
    from app.api.v1.message import _Exit
    try:
        asyncio.run(_run(valid_payload(), {}))
        raised = False
    except _Exit as exc:
        raised = True
        assert exc.status_code == 200
        assert exc.body["duplicate"] is True
        assert exc.body["idempotency_key"].endswith("evt-1")
        assert exc.body["message"] == "already_processed"
    assert raised


def test_burst_gate_defers_message(monkeypatch):
    stub_io(monkeypatch, gate={"allowed": False, "priority_allow": False})
    from app.api.v1.message import _Exit
    try:
        asyncio.run(_run(valid_payload(), {}))
        raised = False
    except _Exit as exc:
        raised = True
        assert exc.status_code == 200
        assert exc.body["response_code"] == "QUEUED_DEBOUNCED"
        assert exc.body["suppress_reply"] is True
        assert exc.body["rate_limited"] is True
        assert exc.body["batch_id"] == "b1"
    assert raised


def test_failed_tool_round_does_not_drop_known_doctor(monkeypatch):
    """Regression (2026-09-17 production incident): the model called Check_Doctor_Availability
    with the doctor name, then its final contract omitted entities.doctor_name — the saved
    state lost the doctor and the next turn re-asked the patient. The runner must recover
    entities the model itself carried in its tool-call arguments."""
    import json

    from app.services.dialogue import AgentTurnText

    booking_contract = json.dumps({
        "schema_version": "k2.dialogue.v4",
        "reply": "أهلاً وسهلاً 🌸 نساعدك مع د. أحمد الحنكشلاوي، من نوع الزيارة؟",
        "turn": {"intent": "booking_request", "relation_to_previous_turn": "new_request"},
        "confidence": 0.95, "ambiguous": [], "confirmation": {"intent": "none"},
        "selection": {"kind": "none", "rank": None},
        "entities": {"doctor_name": None, "service_name": None, "visit_type": None,
                     "date": None, "time": None, "patient_name": None, "patient_phone": None,
                     "patient_age": None, "reference": None},
        "operation_proposal": {"type": "none", "requested": False},
        "escalate": None,
    }, ensure_ascii=False)
    turn = AgentTurnText(booking_contract, tool_events=[{
        "name": "Check_Doctor_Availability",
        "arguments": {"doctor_id": "د. أحمد الحنكشلاوي", "requested_date": "2026-09-20",
                      "service_id": None},
        "result": {"error": "INVALID_OR_MISSING_IDENTIFIER"},
        "cache_hit": False,
    }], llm_calls=2)

    stub_io(monkeypatch)
    import app.api.v1.message as runner_mod
    import app.db.repository as repo

    # Production-shaped persisted state (state_version + recent_turns live INSIDE
    # state_data; an empty state_data reads as conversation start and drops booking context).
    async def _state(normalized):
        return {"state_data": {
            "state_version": 5,
            "recent_turns": [{"role": "assistant", "content": "أهلاً بحسام"}],
            "last_updated": "2026-09-17T19:34:53Z",
            "booking_context": {"patient_name": "حسام عادل"},
        }}

    monkeypatch.setattr(repo, "get_conversation_state", _state)

    async def _turn(*args, **kwargs):
        return turn

    captured = {}

    async def _save(normalized, save_body):
        captured["body"] = save_body
        return {"initial": {"saved": True}, "retry": None}

    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool", _turn)
    monkeypatch.setattr(repo, "save_conversation_state_with_retry", _save)

    result = asyncio.run(_run(valid_payload(message_text="احجز مع الدكتور احمد"), {}))
    assert result["response_code"]
    saved = json.dumps(captured["body"], ensure_ascii=False, default=str)
    assert "د. أحمد الحنكشلاوي" in saved, "doctor from tool args must reach the saved state"


def test_invalid_payload_returns_normalize_error(monkeypatch):
    stub_io(monkeypatch)
    from app.api.v1.message import _Exit
    broken = valid_payload()
    del broken["message_text"]
    try:
        asyncio.run(_run(broken, {}))
        raised = False
    except _Exit as exc:
        raised = True
        assert exc.status_code == 400
        assert exc.body["response_code"] == "INVALID_INBOUND_PAYLOAD"
        assert "message_text" in exc.body["metadata"]["missing_fields"]
    assert raised
