"""Regression tests for the post-review fixes (2026-09-18, second quality pass)."""
from __future__ import annotations

import asyncio
import json
import uuid

from app.core.agent_output import _absorb_day_word_to_iso
from app.core.response_context import build_reply_context, validate_composer_output
from app.pipeline import stages_post


def test_glued_and_diacritic_badad_variants():
    assert _absorb_day_word_to_iso("بعدبكرة", "2026-09-18") == "2026-09-20"
    assert _absorb_day_word_to_iso("بعدبكره", "2026-09-18") == "2026-09-20"
    # Double space and kasra diacritic previously slipped to the bare بكرة rule (+1)
    # or missed absorption entirely.
    assert _absorb_day_word_to_iso("بعد  بكرة", "2026-09-18") == "2026-09-20"
    assert _absorb_day_word_to_iso("بعد بكِرة", "2026-09-18") == "2026-09-20"


def test_same_day_deictic_marks_today():
    # 2026-09-19 is a Saturday: "السبت ده" = today, "السبت" alone = next week.
    assert _absorb_day_word_to_iso("السبت ده", "2026-09-19") == "2026-09-19"
    assert _absorb_day_word_to_iso("السبت", "2026-09-19") == "2026-09-26"


def test_grounding_accepts_paraphrase_renders_of_cited_values():
    ctx = build_reply_context(
        normalized={"message_text": "ميعاد بكرة"},
        clinic_context={"clinic_name": "عيادة"},
        state_data={},
        policy={"response_code": "CONVERSATION_ONLY"}, decision={},
        normalized_agent_output={}, repaired_result={},
        tool_events=[{"name": "Check_Doctor_Availability", "arguments": {},
                      "result": {"success": True, "nearest_slots": [
                          {"local_date": "2026-09-20", "local_time": "14:00"}]}}],
        execution_results={}, faq_result={}, guard={})
    # 2:00 = 14:00 rendered in 12h form; 20/9 = date paraphrase — both legitimate.
    parsed, errors = validate_composer_output(
        {"reply": "متاح يوم 20/9 الساعة 2:00 🌸", "evidence_ids": ["tool.0.Check_Doctor_Availability"],
         "missing_information": [], "unsupported_claims": [], "grounding_status": "supported"}, ctx)
    assert parsed is not None, errors


def test_grounding_still_rejects_fabricated_clock_and_date():
    ctx = build_reply_context(
        normalized={"message_text": "ميعاد بكرة"},
        clinic_context={"clinic_name": "عيادة"},
        state_data={},
        policy={"response_code": "CONVERSATION_ONLY"}, decision={},
        normalized_agent_output={}, repaired_result={},
        tool_events=[{"name": "Check_Doctor_Availability", "arguments": {},
                      "result": {"success": True, "nearest_slots": [
                          {"local_date": "2026-09-20", "local_time": "14:00"}]}}],
        execution_results={}, faq_result={}, guard={})
    parsed, errors = validate_composer_output(
        {"reply": "متاح يوم 2026-10-01 الساعة 9:15 🌸", "evidence_ids": ["tool.0.Check_Doctor_Availability"],
         "missing_information": [], "unsupported_claims": [], "grounding_status": "supported"}, ctx)
    assert parsed is None
    assert any("2026-10-01" in e for e in errors)
    assert any("9:15" in e for e in errors)


def test_numeric_paraphrases_are_not_enforced():
    """Counts and durations ('3 مواعيد', 'ساعة') are paraphrase territory — deliberately
    not value-enforced so the repair budget is spent on real fabrications only."""
    ctx = build_reply_context(
        normalized={"message_text": "ميعاد"},
        clinic_context={"clinic_name": "عيادة"},
        state_data={},
        policy={"response_code": "CONVERSATION_ONLY"}, decision={},
        normalized_agent_output={}, repaired_result={},
        tool_events=[{"name": "Check_Doctor_Availability", "arguments": {},
                      "result": {"success": True, "nearest_slots": [{"local_date": "2026-09-20", "local_time": "14:00"}]}}],
        execution_results={}, faq_result={}, guard={})
    parsed, errors = validate_composer_output(
        {"reply": "لقيت 3 مواعيد يوم 2026-09-20 والجلسة ساعة 🌸",
         "evidence_ids": ["tool.0.Check_Doctor_Availability"],
         "missing_information": [], "unsupported_claims": [], "grounding_status": "supported"}, ctx)
    assert parsed is not None, errors


def test_replay_surfaces_the_stored_booking_number():
    decision = {"allowed": True, "booking_context": {"doctor_name": "د. أحمد"},
                "response_code": "APPOINTMENT_CREATED", "system_decision": {}}
    ledger_row = {"decision": "IDEMPOTENT_REPLAY", "operation_id": "op-9",
                  "response_json": json.dumps({"success": True, "final_reply": "تم",
                                               "booking_number": "BK-8842",
                                               "appointment_id": str(uuid.uuid4())})}
    out = stages_post.apply_operation_claim_deterministic(ledger_row, decision)
    assert out["booking_number"] == "BK-8842"
    assert out["system_decision"]["booking_context"]["booking_number"] == "BK-8842"


def test_lock_key_mirrors_normalize_conversation_chain():
    import importlib
    from app.api.v1 import message as runner
    importlib.reload(runner) if not hasattr(runner, "_lock_key_of") else None
    body = {"thread_id": "thread-9", "patient_id": "p"}
    assert runner._lock_key_of(body) == "thread-9"
    assert runner._lock_key_of({"conversation_id": "c1", "thread_id": "t"}) == "c1"
    assert runner._lock_key_of({}) == ""
