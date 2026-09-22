"""Parity tests for the Booking Assistant Agent user-message template port.

Mirrors agent_user_message_template.js branch by branch.
Persona fallbacks use correct UTF-8 Arabic strings after mojibake fix.
FAQ facts are injected whenever results are available, regardless of prompt profile.
"""
from app.services.dialogue import build_user_message


CLINIC = "123e4567-e89b-42d3-a456-426614174000"


def clinic_ctx(persona=None, doctor_count=2):
    return {"clinic_id": CLINIC, "clinic_name": "عيادة مرنة", "persona": persona or {}, "doctor_count": doctor_count}


def time_ctx():
    return {"timezone": "Asia/Riyadh", "now_local_date": "2026-09-16", "now_local_time": "18:00", "utc_offset": "+03:00"}


def test_persona_arabic_fallbacks_correct():
    """Persona fallback values must be proper UTF-8 Arabic (mojibake was a bug, now fixed)."""
    msg = build_user_message(clinic_ctx(), time_ctx(), {"message_text": "مرحبا"}, {}, {}, None)
    assert '"assistant": "نور"' in msg and '"role": "مساعدة استقبال وحجوزات"' in msg


def test_persona_values_used_when_present():
    persona = {"name": "سارة", "role": "موظفة استقبال", "tone": "friendly", "dialect": "egyptian"}
    msg = build_user_message(clinic_ctx(persona), time_ctx(), {"message_text": "x"}, {}, {}, None)
    assert '"assistant": "سارة"' in msg and '"dialect": "egyptian"' in msg


def test_faq_injected_whenever_results_available():
    """FAQ facts must always be injected when results exist, regardless of prompt profile.
    The old profile-gate (clinic_query only) was a logic bug — fixed.
    """
    faq = {"clinic_id": CLINIC, "results": [{"answer": "aa"}], "count": 1}
    # clinic_query profile: FAQ included
    msg = build_user_message(clinic_ctx(), time_ctx(), {"message_text": "x"}, {"agent_prompt_profile": "clinic_query"}, {}, faq)
    assert '"faq_facts"' in msg and "aa" in msg
    # booking profile: FAQ still included (profile gate was wrong)
    msg2 = build_user_message(clinic_ctx(), time_ctx(), {"message_text": "x"}, {"agent_prompt_profile": "booking"}, {}, faq)
    assert '"faq_facts"' in msg2 and "aa" in msg2
    # no results -> null faq_facts regardless of profile
    msg3 = build_user_message(clinic_ctx(), time_ctx(), {"message_text": "x"}, {"agent_prompt_profile": "clinic_query"}, {}, None)
    assert '"faq_facts": null' in msg3


def test_next_ask_visit_type_when_collecting_without_type():
    msg = build_user_message(clinic_ctx(), time_ctx(), {"message_text": "عايز أحجز"}, {}, {}, None)
    assert '"next_ask": "visit_type"' in msg


def test_next_ask_appointment_reference_when_requested():
    st = {"turn_directive": {"must_ask": ["appointment_id"]}}
    msg = build_user_message(clinic_ctx(), time_ctx(), {"message_text": "x"}, {}, st, None)
    assert '"next_ask": "appointment_reference"' in msg


def test_next_ask_follows_missing_order():
    # The order-based missing selection only runs when appointment_type is already known;
    # without it the template returns "visit_type" first (JS branch order preserved).
    st = {"missing_human_fields": ["patient_phone", "date", "patient_name"]}
    msg = build_user_message(clinic_ctx(), time_ctx(), {"message_text": "x"}, {}, st, None)
    assert '"next_ask": "visit_type"' in msg
    st2 = {"booking_context": {"appointment_type": "NEW_VISIT"},
           "missing_human_fields": ["patient_phone", "date", "patient_name"]}
    msg2 = build_user_message(clinic_ctx(), time_ctx(), {"message_text": "x"}, {}, st2, None)
    assert '"next_ask": "date"' in msg2  # order: date beats phone/name


def test_pending_confirmation_only_when_state_required():
    conf = {"action": "create_appointment", "doctor_name": "أحمد", "date": "2026-09-20", "time": "17:30", "expires_at": "x"}
    st_on = {"confirmation_target": conf, "confirmation_state": "required"}
    msg = build_user_message(clinic_ctx(), time_ctx(), {"message_text": "x"}, {}, st_on, None)
    assert '"pending_confirmation"' in msg and "أحمد" in msg and '"next_ask": null' in msg
    st_off = {"confirmation_target": conf, "confirmation_state": "none"}
    msg2 = build_user_message(clinic_ctx(), time_ctx(), {"message_text": "x"}, {}, st_off, None)
    assert '"pending_confirmation": null' in msg2 and '"next_ask": "visit_type"' in msg2


def test_offered_maps_rank_date_time_only():
    st = {"pending_offer": {"alternatives": [{"rank": 2, "slot_id": "s", "local_date": "2026-09-20", "local_time": "17:30", "extra": 1}]}}
    msg = build_user_message(clinic_ctx(), time_ctx(), {"message_text": "x"}, {}, st, None)
    assert '{"rank": 2, "date": "2026-09-20", "time": "17:30"}' in msg and "slot_id" not in msg.split('"offered":')[1].split("]")[0]


def test_offered_suppressed_while_confirmation_pending():
    """P42c payload guard (2026-09-19): with a pending booking confirmation the offered
    list must NOT reach the model — prompt precedence puts "offered + accepts →
    selection" above "pending confirmation", which reclassified the affirm turn as a
    slot pick and re-bound CONFIRMATION_REQUIRED forever (journey T6)."""
    import json

    st = {"confirmation_target": {"action": "create_appointment", "date": "2026-09-24"},
          "confirmation_state": "required",
          "pending_offer": {"alternatives": [{"rank": 1, "local_date": "2026-09-24", "local_time": "10:30"}]}}
    msg = build_user_message(clinic_ctx(), time_ctx(), {"message_text": "أيوه أكد"}, {}, st, None)
    payload = json.loads(msg)
    assert payload["situation"]["offered"] == []
    assert payload["situation"]["pending_confirmation"] is not None


def test_offered_present_without_pending_confirmation():
    import json

    st = {"pending_offer": {"alternatives": [{"rank": 1, "local_date": "2026-09-24", "local_time": "10:30"}]}}
    msg = build_user_message(clinic_ctx(), time_ctx(), {"message_text": "الخميس"}, {}, st, None)
    payload = json.loads(msg)
    assert len(payload["situation"]["offered"]) == 1
    assert payload["situation"]["pending_confirmation"] is None
