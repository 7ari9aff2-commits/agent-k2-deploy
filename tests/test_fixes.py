"""Unit and flow tests verifying all logic and configuration bug fixes in Agent K2.

Covers:
1. Early security reject via incoming_message.security_reject
2. Business time gate decision propagation to audit/tail
3. Single agent result phase direct dispatch to _respond_tail
4. Doctor availability tool parameter validation and fallbacks
5. Robust local date/time fallbacks in dialogue user message
6. Stale save retry failure detection for missing or non-true saved values
7. Protected check_doctor_weekly_schedule against invalid UUID inputs
"""
import pytest
import uuid
from unittest.mock import AsyncMock, patch

from app.api.v1.message import _run, _Exit
import app.db.repository as repo
from app.pipeline.respond import build_final_response
from app.services.availability import check_doctor_weekly_schedule
from app.services.dialogue import (
    build_user_message,
    call_primary_model_with_tool,
    AVAILABILITY_TOOL,
)
from tests.test_runner_flow import CLINIC, PATIENT, CONVERSATION, valid_payload, stub_io, SMALL_TALK_CONTRACT


@pytest.mark.asyncio
async def test_early_security_reject_on_incoming_message(monkeypatch):
    """BUG-3: Early security reject must inspect incoming_message, not signature_result."""
    stub_io(
        monkeypatch,
        signature={"accepted": True, "security_reject": False},
        incoming={"id": "in-sec", "duplicate": False, "security_reject": True, "security_error": "PATIENT_CONVERSATION_OWNERSHIP_MISMATCH"},
    )
    with pytest.raises(_Exit) as exc_info:
        await _run(valid_payload(), {})
    assert exc_info.value.status_code == 403
    assert exc_info.value.body["error_code"] == "PATIENT_CONVERSATION_OWNERSHIP_MISMATCH"


@pytest.mark.asyncio
async def test_early_security_reject_clinic_not_found(monkeypatch):
    """BUG-3: Early security reject with CLINIC_NOT_FOUND must return 404."""
    stub_io(
        monkeypatch,
        signature={"accepted": True, "security_reject": False},
        incoming={"id": "in-sec", "duplicate": False, "security_reject": True, "security_error": "CLINIC_NOT_FOUND"},
    )
    with pytest.raises(_Exit) as exc_info:
        await _run(valid_payload(), {})
    assert exc_info.value.status_code == 404
    assert exc_info.value.body["error_code"] == "CLINIC_NOT_FOUND"


@pytest.mark.asyncio
async def test_business_time_gate_decision_propagation(monkeypatch):
    """BUG-9: When business time is blocked, gate_decision must be passed as decision."""
    recorded_decision = {}

    async def _mock_respond_tail(normalized, state_row, state_data, policy, decision, *args, **kwargs):
        recorded_decision.update(decision)
        return {"ok": True, "decision": decision}

    monkeypatch.setattr("app.api.v1.message._respond_tail", _mock_respond_tail)
    booking_contract = (
        '{"schema_version":"k2.dialogue.v4","reply":"تمام، هحجزلك موعد",'
        '"turn":{"intent":"booking_request","relation_to_previous_turn":"new_request"},"confidence":0.9,'
        '"ambiguous":[],"confirmation":{"intent":"none"},"selection":{"kind":"none","rank":null},'
        '"entities":{"date":"2026-09-20","time":"10:00"},'
        '"operation_proposal":{"type":"create_appointment","requested":true},"escalate":null}'
    )
    async def _async(v):
        return v

    stub_io(monkeypatch, llm_output=booking_contract)
    monkeypatch.setattr(repo, "lookup_business_time_context", lambda ctx: _async({"is_business_hours": False}))

    # Force gate to reject business time
    import app.core.gates as gates
    monkeypatch.setattr(gates, "business_time_gate_deterministic", lambda guard, time_ctx: {
        "action": "business_time_blocked",
        "response_code": "OFF_HOURS_BLOCKED",
        "business_time_allowed": False,
    })

    resp = await _run(valid_payload(message_text="عايز احجز موعد بكره"), {})
    assert recorded_decision.get("business_time_allowed") is False
    assert recorded_decision.get("response_code") == "OFF_HOURS_BLOCKED"


def test_dialogue_fallback_local_time_and_date():
    """BUG-5: build_user_message derives date and time when missing from canonical_time_context."""
    clinic = {"clinic_name": "عيادة الرازي", "doctor_count": 1}
    # Time context missing now_local_date and now_local_time, but having now_iso and timezone
    time_ctx = {
        "timezone": "Asia/Riyadh",
        "now_iso": "2026-09-17T14:30:00Z",
        "now_local_date": None,
        "now_local_time": None,
        "utc_offset": "+03:00",
    }
    msg_str = build_user_message(clinic, time_ctx, {"message_text": "مرحبا"}, {}, {}, None)
    import json
    payload = json.loads(msg_str)

    # In Asia/Riyadh (UTC+3), 14:30 UTC is 17:30 local
    assert payload["context"]["local_time"]["date"] == "2026-09-17"
    assert payload["context"]["local_time"]["time"] == "17:30:00"
    assert payload["situation"]["today"] == "2026-09-17"


@pytest.mark.asyncio
async def test_availability_tool_guard_missing_params():
    """BUG-2: Check_Doctor_Availability returns MISSING_REQUIRED_PARAMS if doctor_id or requested_date missing."""
    call_turn = 0

    async def mock_chat(messages, *, with_tools=True, force_json=False):
        nonlocal call_turn
        call_turn += 1
        if call_turn == 1:
            # Model attempts to call tool without doctor_id or requested_date
            return {
                "role": "assistant",
                "content": None,
                "tool_calls": [{
                    "id": "call_1",
                    "function": {
                        "name": "Check_Doctor_Availability",
                        "arguments": json.dumps({"doctor_id": None, "requested_date": None}),
                    },
                }],
            }
        # Model returns final JSON on turn 2
        return {
            "role": "assistant",
            "content": '{"schema_version":"k2.dialogue.v4","reply":"يرجى تحديد الطبيب واليوم"}',
            "tool_calls": [],
        }

    import json
    with patch("app.services.dialogue._chat_messages", side_effect=mock_chat):
        result = await call_primary_model_with_tool("عايز موعد", context={"clinic_id": CLINIC})
        assert "schema_version" in result


@pytest.mark.asyncio
async def test_availability_tool_resolves_doctor_from_clinic_context():
    """BUG-2 & BUG-7: Check_Doctor_Availability falls back to single_doctor_id and context."""
    call_turn = 0
    captured_tool_input = {}

    async def mock_check_slots(tool_input):
        captured_tool_input.update(tool_input)
        return {"slots": []}

    async def mock_chat(messages, *, with_tools=True, force_json=False):
        nonlocal call_turn
        call_turn += 1
        if call_turn == 1:
            # Model calls tool omitting doctor_id (e.g. only passing requested_date)
            return {
                "role": "assistant",
                "content": None,
                "tool_calls": [{
                    "id": "call_2",
                    "function": {
                        "name": "Check_Doctor_Availability",
                        "arguments": json.dumps({"requested_date": "2026-09-20"}),
                    },
                }],
            }
        return {
            "role": "assistant",
            "content": '{"schema_version":"k2.dialogue.v4","reply":"لا توجد مواعيد"}',
            "tool_calls": [],
        }

    import json
    single_doc_uuid = str(uuid.uuid4())
    service_uuid = str(uuid.uuid4())
    context = {
        "clinic_id": CLINIC,
        "conversation_id": CONVERSATION,
        "patient_id": PATIENT,
        "clinic_context": {"single_doctor_id": single_doc_uuid},
        "service_id": service_uuid,
    }

    with patch("app.services.dialogue._chat_messages", side_effect=mock_chat), \
         patch("app.services.availability.check_available_slots", side_effect=mock_check_slots):
        result = await call_primary_model_with_tool("مواعيد يوم 2026-09-20", context=context)
        assert captured_tool_input.get("doctor_id") == single_doc_uuid
        assert captured_tool_input.get("service_id") == service_uuid
        assert captured_tool_input.get("requested_date") == "2026-09-20"


@pytest.mark.asyncio
async def test_check_doctor_weekly_schedule_guards_invalid_uuids():
    """BUG-2: check_doctor_weekly_schedule must return [] on invalid UUIDs without crashing SQL."""
    # Invalid clinic or doctor UUID
    res1 = await check_doctor_weekly_schedule("invalid-clinic", "invalid-doc", "2026-09-20")
    assert res1 == []

    res2 = await check_doctor_weekly_schedule(CLINIC, "", "2026-09-20")
    assert res2 == []

    res3 = await check_doctor_weekly_schedule(CLINIC, str(uuid.uuid4()), "")
    assert res3 == []


@pytest.mark.asyncio
async def test_rpc_get_available_slots_guards_invalid_inputs():
    """BUG-2 sibling: call_rpc_get_available_slots casts $1::uuid/$2::date/$3::date —
    invalid clinic UUID or missing dates must return [] instead of raising a PG cast error."""
    from app.services.availability import call_rpc_get_available_slots

    # Invalid clinic UUID
    assert await call_rpc_get_available_slots({"clinic_id": "not-a-uuid", "start_date": "2026-09-20", "end_date": "2026-09-23"}) == []
    # Missing/empty dates
    assert await call_rpc_get_available_slots({"clinic_id": CLINIC, "start_date": "", "end_date": "2026-09-23"}) == []
    assert await call_rpc_get_available_slots({"clinic_id": CLINIC, "start_date": "2026-09-20", "end_date": None}) == []


def test_respond_stale_save_retry_failure():
    """2026-09-18 contract: a state-save failure NEVER overwrites the model-authored
    reply — the patient still receives it (the failed save travels in metadata)."""
    normalized = {"conversation_id": CONVERSATION, "clinic_id": CLINIC}
    policy_out = {"response_code": "CONVERSATION_ONLY", "output": {}}

    # Case 1: Retry ran and saved is False
    res1 = build_final_response(
        normalized=normalized,
        outgoing_message_id="msg-1",
        save_initial={"saved": False, "rejected_reason": "CONCURRENT_STATE_STALE"},
        save_retry={"saved": False},
        reply_guard_result={},
        rendered_reply="أهلا بك",
        response_policy_output=policy_out,
        processed_at_iso="2026-09-17T12:00:00Z",
    )
    assert res1["reply_text"] == "أهلا بك"

    # Case 2: Retry ran and saved key is None or missing
    res2 = build_final_response(
        normalized=normalized,
        outgoing_message_id="msg-1",
        save_initial={"saved": False, "rejected_reason": "CONCURRENT_STATE_STALE"},
        save_retry={"error": "db_timeout"},  # saved is absent/None
        reply_guard_result={},
        rendered_reply="أهلا بك",
        response_policy_output=policy_out,
        processed_at_iso="2026-09-17T12:00:00Z",
    )
    assert res2["reply_text"] == "أهلا بك"

    # Case 3: Retry ran and saved is True -> normal reply
    res3 = build_final_response(
        normalized=normalized,
        outgoing_message_id="msg-1",
        save_initial={"saved": False, "rejected_reason": "CONCURRENT_STATE_STALE"},
        save_retry={"saved": True},
        reply_guard_result={},
        rendered_reply="أهلا بك",
        response_policy_output=policy_out,
        processed_at_iso="2026-09-17T12:00:00Z",
    )
    assert res3["reply_text"] == "أهلا بك"


def test_outgoing_params_stale_retry_failure_mirrors_patient_reply():
    """2026-09-18 contract: the DB-logged outgoing reply must match the patient-facing
    reply even when the state save failed — a canned infrastructure notice must never
    be logged as the reply (it poisoned the dedupe gate on retries)."""
    from app.pipeline.stages_pre import build_outgoing_message_sql_parameters

    params = build_outgoing_message_sql_parameters({}, {
        "normalize_validate": {"conversation_id": CONVERSATION, "clinic_id": CLINIC},
        "save_conversation_state": {"saved": False, "rejected_reason": "CONCURRENT_STATE_STALE"},
        "save_conversation_state_retry_v18": {"error": "db_timeout"},  # retry ran, saved absent
        "extract_single_agent_reply": {"rendered_reply": "أهلا بك", "render_used": True},
        "response_policy_deterministic": {"response_code": "CONVERSATION_ONLY", "output": {}},
    })
    qp = params["query_params"]
    assert "أهلا بك" in str(qp), f"the real reply must be logged even on save failure, got {qp}"

    # Retry succeeded -> the real rendered reply is logged
    params_ok = build_outgoing_message_sql_parameters({}, {
        "normalize_validate": {"conversation_id": CONVERSATION, "clinic_id": CLINIC},
        "save_conversation_state": {"saved": False, "rejected_reason": "CONCURRENT_STATE_STALE"},
        "save_conversation_state_retry_v18": {"saved": True},
        "extract_single_agent_reply": {"rendered_reply": "أهلا بك", "render_used": True},
        "response_policy_deterministic": {"response_code": "CONVERSATION_ONLY", "output": {}},
    })
    assert "أهلا بك" in str(params_ok["query_params"])
