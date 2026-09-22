"""Regression: after a successful create, the saved state carries the booking identity.

Reviewer-verified P0: build_persistent_conversation_state was fed the 2-column
fresh-offer row as its $json item, so booking_number/COMPLETED never persisted —
the reply promised a number the next turn's state did not have.
"""
from __future__ import annotations

from app.pipeline.stages_pre import build_persistent_conversation_state

CLINIC = "123e4567-e89b-42d3-a456-426614174000"


def test_state_save_keeps_booking_identity_after_create():
    decision = {
        "system_decision": {"response_code": "CREATE_COMPLETED", "action": "create_appointment",
                            "booking_context": {"doctor_name": "د. أحمد",
                                                "booking_number": "24-0918-0142"}},
        "response_code": "CREATE_COMPLETED",
        "booking_number": "24-0918-0142",
        "booking_context": {"doctor_name": "د. أحمد", "booking_number": "24-0918-0142"},
        "operation_state": "COMPLETED",
        "operation_status": "completed",
        "confirmation_target": None,
        "confirmation_state": None,
    }
    policy = {"response_code": "APPOINTMENT_CREATED",
              "system_decision": decision["system_decision"],
              "booking_number": "24-0918-0142",
              "agent_reply": None, "facts": {"booking_number": "24-0918-0142"}}
    state_item = {**decision, **policy, "output": {"rendered_reply": "تم الحجز BK-24-0918-0142 ✅"}}
    state_row = {"state_data": {
        "state_version": 5,
        "recent_turns": [{"role": "assistant", "content": "أيوه"}],
        "booking_context": {"doctor_name": "د. أحمد"},
    }}

    persistent = build_persistent_conversation_state(state_item, {
        "normalize_validate": {"clinic_id": CLINIC, "message_text": "تم"},
        "get_clinic_context": {"clinic_name": "عيادة"},
        "system_orchestrator_policy": decision,
        "get_conversation_state": state_row,
        "read_fresh_offer_midturn": {}})

    blob = str(persistent)
    assert "24-0918-0142" in blob, "booking number must survive into the saved state"
    assert "EXECUTING" not in str(persistent.get("state_data", {}).get("operation_state", "")) and \
        "EXECUTING" not in str(persistent.get("operation_state", ""))
