"""P42c confirm-execute regression tests (journey root-cause fix, 2026-09-19).

The affirm/negative turn at AWAIT_CONFIRMATION whose contract echoes the offered
selection must be owned by the C1 confirm-execute semantics — execute the bound
target on affirmative, reject it on negative — never re-bound into another
CONFIRMATION_REQUIRED (the infinite proposal loop the journey hit at T6).
"""
from app.core.orchestrator import decide

CLINIC = "123e4567-e89b-42d3-a456-426614174000"
PATIENT = "123e4567-e89b-42d3-a456-426614174001"
CONVERSATION = "123e4567-e89b-42d3-a456-426614174002"
DOCTOR = "123e4567-e89b-42d3-a456-426614174004"
SLOT = "123e4567-e89b-42d3-a456-426614174005"

FUTURE = "2099-01-01T00:00:00Z"


def bound_target():
    return {
        "schema_version": 3, "action": "create_appointment",
        "clinic_id": CLINIC, "patient_id": PATIENT, "conversation_id": CONVERSATION,
        "doctor_id": DOCTOR, "doctor_name": "د. أحمد", "slot_id": SLOT,
        "date": "2026-09-24", "time": "10:30", "appointment_type": "NEW_VISIT",
        "patient_name": "حسام", "patient_phone": "+966500000000",
        "operation_id": "op-1", "last_user_message_id_at_request": "m-1",
        "confirmation_id": "c-1", "expires_at": FUTURE,
        "delivery": "pending", "source": "state_table_v3",
    }


def state_with_bound_target():
    return {
        "state_machine": {"current_state": "AWAIT_CONFIRMATION"},
        "confirmation_state": "required",
        "confirmation_target": bound_target(),
        "presented_offer": {
            "kind": "presented_offer", "expires_at": FUTURE,
            "clinic_id": CLINIC, "conversation_id": CONVERSATION,
            "alternatives": [{"rank": 1, "slot_id": SLOT, "local_date": "2026-09-24", "local_time": "10:30"}],
        },
        "booking_context": {
            "doctor_id": DOCTOR, "doctor_name": "د. أحمد", "appointment_type": "NEW_VISIT",
            "date": "2026-09-24", "time": "10:30", "slot_id": SLOT,
            "patient_name": "حسام", "patient_phone": "+966500000000",
        },
    }


def echo_contract(confirm_intent):
    """The affirm turn per the agent prompt: the offered list is still injected, so
    the model echoes the selection AND sets the confirmation intent."""
    return {
        "schema_version": "k2.dialogue.v3",
        "turn": {"intent": "confirmation", "relation_to_previous_turn": "confirmation"},
        "confirmation": {"intent": confirm_intent},
        "selection": {"kind": "presented_match", "rank": 1},
        "entities": {},
        "operation_proposal": {"type": "none", "requested": False},
        "escalate": None,
    }


def clinic_ctx():
    return {"clinic_id": CLINIC, "patient_id": PATIENT, "conversation_id": CONVERSATION,
            "doctor_count": 1, "now_iso": "2026-09-19T12:00:00Z"}


def test_affirm_with_selection_echo_executes_bound_target():
    out = decide(echo_contract("affirmative"), state_with_bound_target(), clinic_ctx(), now_ts=1758000000)
    sd = out["system_decision"]
    assert sd["decision_rule"] == "c1_confirm_execute", sd["decision_rule"]
    assert sd["response_code"] == "EXECUTE_APPROVED"
    assert sd["allowed"] is True
    assert sd["action"] == "create_appointment"
    assert (out["state_machine"] or {}).get("current_state") == "EXECUTING"
    assert (out["confirmation_target"] or {}).get("delivery") == "confirmed"
    assert (out["confirmation_target"] or {}).get("slot_id") == SLOT


def test_negative_with_selection_echo_rejects_bound_target():
    out = decide(echo_contract("negative"), state_with_bound_target(), clinic_ctx(), now_ts=1758000000)
    sd = out["system_decision"]
    assert sd["decision_rule"] == "confirm_rejected_offer_open", sd["decision_rule"]
    assert (out["state_machine"] or {}).get("current_state") == "AWAIT_SLOT_CHOICE"
    assert out["confirmation_target"] is None


def test_same_slot_affirm_never_rebinds():
    """The historical loop: an affirm echo used to re-bind CONFIRMATION_REQUIRED forever."""
    out = decide(echo_contract("affirmative"), state_with_bound_target(), clinic_ctx(), now_ts=1758000000)
    assert out["system_decision"]["response_code"] != "CONFIRMATION_REQUIRED"
    assert out["system_decision"]["decision_rule"] != "selection_bound_from_offer"
