"""State-builder compaction integration (2026-09-23, 2h window).

The journey tests cover _run end-to-end with fresh states; this test pins the
state builder's OWN contract with the compaction boundary: past 2h of inactivity
the prior raw turns must NOT survive into the saved state, and the structured
summary must travel in their place.
"""
import json

from app.pipeline.stages_pre import build_persistent_conversation_state


def _previous_state(hours_ago_iso):
    return {
        "state_machine": {"current_state": "IDLE"},
        "operation_state": "IDLE",
        "operation_status": "idle",
        "last_updated": hours_ago_iso,
        "recent_turns": [
            {"role": "user", "text": "عايز أحجز مع دكتور أحمد", "at": hours_ago_iso},
            {"role": "assistant", "text": "تمام يا فندم", "at": hours_ago_iso},
        ],
        "booking_context": {"doctor_name": "د. أحمد", "appointment_type": "NEW_VISIT"},
        "facts": {"patient": {"name": "حسام"}, "booking": {}},
        "last_intent": "booking",
    }


def _inputs(previous, now_iso):
    item = {"_normalization": {}, "output": None}
    return item, {
        "normalize_validate": {"channel_type": "telegram", "clinic_id": "cl-1",
                               "patient_id": "pa-1", "conversation_id": "co-1",
                               "message_text": "أهلا", "received_at": now_iso},
        "get_clinic_context": {"clinic_id": "cl-1"},
        "system_orchestrator_policy": {"system_decision": {}, "booking_context": {}},
        "get_conversation_state": {"state_data": previous},
        "read_fresh_offer_midturn": {},
    }


def test_boundary_drops_prior_turns_and_writes_summary():
    prev = _previous_state("2026-09-22T12:00:00Z")
    item, inputs = _inputs(prev, "2026-09-22T16:00:00Z")   # 4h gap
    result = build_persistent_conversation_state(item, inputs)
    sd = result["state_data"]
    turns = sd.get("recent_turns") or []
    user_texts = [t.get("text") for t in turns if t.get("role") == "user"]
    assert user_texts == ["أهلا"], json.dumps(turns, ensure_ascii=False)[:300]
    assert not any("عايز أحجز مع دكتور أحمد" in (t.get("text") or "") for t in turns)
    summary = sd.get("previous_session_summary") or {}
    assert summary.get("patient_name") == "حسام"
    assert summary.get("booking_context", {}).get("doctor_name") == "د. أحمد"


def test_no_boundary_keeps_prior_turns():
    prev = _previous_state("2026-09-22T12:00:00Z")
    item, inputs = _inputs(prev, "2026-09-22T12:30:00Z")   # 30m gap
    result = build_persistent_conversation_state(item, inputs)
    sd = result["state_data"]
    turns = sd.get("recent_turns") or []
    assert any("عايز أحجز مع دكتور أحمد" in (t.get("text") or "") for t in turns)
    assert "previous_session_summary" not in sd
