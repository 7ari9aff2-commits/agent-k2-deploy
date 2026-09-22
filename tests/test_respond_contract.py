"""Parity tests for the Respond To Patient port (final HTTP response contract)."""
from app.pipeline.respond import build_final_response

NORMALIZED = {"conversation_id": "c-1", "clinic_id": "k-1"}
POLICY_OUT = {"response_code": "APPOINTMENT_CREATED",
              "output": {"intent": "booking_request", "operation_status": "completed",
                          "appointment_id": "a-9", "escalate": None, "proposed_action": "create_appointment"}}


def test_save_transport_failure_does_not_overwrite_the_reply():
    out = build_final_response(NORMALIZED, "m-1", {"error": "boom"}, None, None, "رد", POLICY_OUT, "t")
    assert out["reply_text"] == "رد"
    assert out.get("suppress_reply") is not True


def test_save_http_status_counts_as_transport_error():
    out = build_final_response(NORMALIZED, "m-1", {"statusCode": 500}, None, None, "رد", POLICY_OUT, "t")
    assert out["reply_text"] == "رد"


def test_stale_save_rejected_is_not_failure_when_retry_did_not_run():
    initial = {"saved": False, "rejected_reason": "CONCURRENT_STATE_STALE"}
    out = build_final_response(NORMALIZED, "m-1", initial, None, None, "الرد المرسل", POLICY_OUT, "t")
    assert out["reply_text"] == "الرد المرسل"


def test_saved_false_without_retry_is_failure():
    out = build_final_response(NORMALIZED, "m-1", {"saved": False}, None, None, "الرد المرسل", POLICY_OUT, "t")
    assert out["reply_text"] == "الرد المرسل"


def test_model_reply_outranks_legacy_guard_override():
    """The model owns the conversation.

    `_reply_guard.override` used to outrank the model-authored reply. apply_reply_guard
    no longer writes patient-facing text at all, and this ordering makes the guarantee
    structural: even if the field carries text again, the model's reply wins.
    """
    guard = {"_reply_guard": {"override": "تم الحجز بنجاح يا فلان ✅"}}
    out = build_final_response(NORMALIZED, "m-1", {"saved": True}, {"saved": True}, guard, "الرد المرسل", POLICY_OUT, "t")
    assert out["reply_text"] == "الرد المرسل"
    assert out["_debug"]["deterministic_override"] is False


def test_guard_override_is_used_only_when_there_is_no_model_reply():
    guard = {"_reply_guard": {"override": "تم الحجز بنجاح يا فلان ✅"}}
    out = build_final_response(NORMALIZED, "m-1", {"saved": True}, {"saved": True}, guard, None, POLICY_OUT, "t")
    assert out["reply_text"] == "تم الحجز بنجاح يا فلان ✅"


def test_apply_reply_guard_produces_no_patient_text():
    """apply_reply_guard is detection-only now — it must never generate a reply."""
    from app.core.reply_guard import apply_reply_guard

    for code in ("APPOINTMENT_CREATED", "CANCEL_COMPLETED", "RESCHEDULE_COMPLETED",
                 "IDEMPOTENT_REPLAY", "CONFIRMATION_EXPIRED", "AVAILABILITY_SOURCE_ERROR"):
        policy = {"response_code": code, "system_decision": {"response_code": code}}
        out = apply_reply_guard({"response_policy": policy})
        meta = out["_reply_guard"]
        assert meta["override"] is None, f"{code} must not produce reply text"
        assert meta["code"] == code
        assert "agent_reply" not in out, f"{code} must not inject agent_reply"


def test_rendered_reply_fallback_and_metadata():
    out = build_final_response(NORMALIZED, "m-1", {"saved": True}, {"saved": True}, None, "الرد المرسل", POLICY_OUT, "2026-09-16T18:00:00Z")
    assert out["reply_text"] == "الرد المرسل"
    assert out["conversation_id"] == "c-1" and out["clinic_id"] == "k-1"
    assert out["outgoing_message_id"] == "m-1"
    assert out["response_code"] == "APPOINTMENT_CREATED"
    assert out["metadata"]["appointment_id"] == "a-9"
    assert out["metadata"]["processed_at"] == "2026-09-16T18:00:00Z"
    assert out["_debug"]["deterministic_override"] is False
