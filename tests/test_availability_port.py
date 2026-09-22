"""Parity tests for the availability sub-workflow port (k2 - get_available_slots).

Fixtures are hand-transcribed from the n8n reference JS so every branch below mirrors
a literal branch in Prepare_Search_Window.js / Match_Analyze_Slots_Deterministic.js /
Build_Final_Response_Deterministic.js / Build_Presented_Offer_Deterministic.js.
"""
import pytest

from app.services.availability import (
    build_final_response,
    build_presented_offer,
    match_and_analyze_slots,
    prepare_search_window,
)


CLINIC = "123e4567-e89b-42d3-a456-426614174000"
DOCTOR = "123e4567-e89b-42d3-a456-426614174001"


def base_item(**over):
    item = {
        "clinic_id": CLINIC,
        "doctor_id": DOCTOR,
        "service_id": None,
        "requested_date": "2026-09-20",
        "requested_time": "",
        "search_range_days": "",
        "timezone": "Asia/Riyadh",
        "search_mode": "",
        "conversation_id": CLINIC,
        "patient_id": "",
        "clinic_timezone": "Asia/Riyadh",
    }
    item.update(over)
    return item


def test_prepare_window_bounds_nearby_to_7_min_14_max():
    nearby = prepare_search_window(base_item(search_mode="nearby_alternatives", search_range_days="2"))
    assert nearby["start_date"] == "2026-09-20" and nearby["end_date"] == "2026-09-27"  # max(7, 2)
    far = prepare_search_window(base_item(search_mode="nearby_alternatives", search_range_days="40"))
    assert far["end_date"] == "2026-10-04"  # min(14, 40)
    plain = prepare_search_window(base_item(search_mode="", search_range_days=""))
    assert plain["end_date"] == "2026-09-23"  # default 3


def test_prepare_window_invalid_uuid_branch():
    out = prepare_search_window(base_item(doctor_id="not-a-uuid"))
    assert out["input_error"] == "INVALID_OR_MISSING_IDENTIFIER"
    assert out["start_date"] is None and out["end_date"] is None


def test_prepare_window_bad_timezone_branch():
    out = prepare_search_window(base_item(clinic_timezone="Not/AZone", timezone="Not/AZone"))
    assert out["input_error"] == "CLINIC_TIMEZONE_NOT_CONFIGURED"
    assert out["timezone"] is None and out["timezone_source"] == "clinic_timezone_invalid_or_missing"


def test_prepare_window_invalid_date_branch():
    out = prepare_search_window(base_item(requested_date="20/09/2026"))
    assert out["input_error"] == "INVALID_OR_MISSING_DATE"
    assert out["timezone"] == "Asia/Riyadh" and out["timezone_source"] == "clinic_configuration"


def test_prepare_window_requested_datetime_uses_local_offset():
    out = prepare_search_window(base_item(requested_time="17:30"))
    assert out["requested_datetime"] == "2026-09-20T17:30:00+03:00"


def test_match_error_message_for_input_error():
    prep = prepare_search_window(base_item(doctor_id="bad"))
    out = match_and_analyze_slots(prep, [prep])
    assert out["success"] is False and out["error_code"] == "INVALID_OR_MISSING_IDENTIFIER"
    assert out["message"] == "تاريخ الحجز غير واضح، محتاجين توضيح من المريض"


def test_match_rpc_error_branch():
    prep = prepare_search_window(base_item())
    out = match_and_analyze_slots(prep, [{"error": "boom"}])
    assert out["error_code"] == "RPC_ERROR" and out["message"] == "تعذر جلب الأوقات المتاحة حاليا"


def test_match_no_slots_triggers_schedule_fallback():
    prep = prepare_search_window(base_item())
    out = match_and_analyze_slots(prep, [])
    assert out["success"] is True and out["needs_schedule_fallback"] is True
    assert out["error_code"] == "NO_AVAILABLE_SLOTS"
    assert out["message"] == "لا توجد مواعيد متاحة في نافذة البحث"
    assert out["verification_status"] == "verified_unavailable"


def test_match_exact_slot_by_wall_clock_minute_precision():
    prep = prepare_search_window(base_item(requested_time="17:30"))
    slot = {"slot_id": "s1", "clinic_id": CLINIC, "doctor_id": DOCTOR, "service_id": None,
            "start_time": "2026-09-20T14:30:00+00:00", "end_time": "2026-09-20T15:00:00+00:00"}
    out = match_and_analyze_slots(prep, [slot])
    assert out["matched"] is True and out["message"] == "الوقت المطلوب متاح"
    assert out["exact_slot"]["slot_id"] == "s1" and out["exact_slot"]["slot_status"] == "available"
    assert out["verification_status"] == "verified_available"


def test_match_requested_time_unavailable_lists_nearest():
    prep = prepare_search_window(base_item(requested_time="09:00"))
    slot = {"slot_id": "s2", "start_time": "2026-09-20T14:30:00+00:00", "end_time": "2026-09-20T15:00:00+00:00"}
    out = match_and_analyze_slots(prep, [slot])
    assert out["error_code"] == "REQUESTED_TIME_NOT_AVAILABLE"
    assert out["message"] == "الوقت المطلوب غير متاح، وهذه أقرب المواعيد المتاحة"
    assert out["nearest_slots"][0]["slot_id"] == "s2" and "diff_minutes" in out["nearest_slots"][0]


def test_final_response_doctor_not_working_that_day():
    match = match_and_analyze_slots(prepare_search_window(base_item()), [])
    out = build_final_response(match, schedule_rows=[])
    assert out["error_code"] == "DOCTOR_NOT_WORKING_THAT_DAY"
    assert out["message"] == "الدكتور ما يشتغلش في اليوم ده جرب يوم تاني"
    assert out["doctor_works_that_day"] is False


def test_final_response_doctor_works_but_no_slots():
    match = match_and_analyze_slots(prepare_search_window(base_item()), [])
    rows = [{"day_of_week": 6, "start_time": "09:00:00", "end_time": "17:00:00"}]
    out = build_final_response(match, schedule_rows=rows)
    assert out["error_code"] == "NO_AVAILABLE_SLOTS"
    assert out["message"] == "الدكتور يعمل في هذا اليوم، لكن لا توجد مواعيد متاحة مؤكدة حاليًا"
    assert out["doctor_works_that_day"] is True


def test_final_response_nearby_window_empty():
    match = match_and_analyze_slots(prepare_search_window(base_item(search_mode="nearby_alternatives")), [])
    rows = [{"day_of_week": 6, "start_time": "09:00:00", "end_time": "17:00:00"}]
    out = build_final_response(match, schedule_rows=rows)
    assert out["error_code"] == "NO_AVAILABLE_SLOTS_IN_WINDOW"
    assert out["message"] == "لا توجد مواعيد متاحة مؤكدة في الأيام القريبة التي تم البحث فيها"


def test_presented_offer_only_when_verified():
    prep = prepare_search_window(base_item(requested_time="17:30"))
    slot = {"slot_id": "s1", "clinic_id": CLINIC, "doctor_id": DOCTOR, "service_id": None,
            "start_time": "2026-09-20T14:30:00+00:00", "end_time": "2026-09-20T15:00:00+00:00"}
    match = match_and_analyze_slots(prep, [slot])
    final = build_final_response(match, None)
    offer = build_presented_offer(final, base_item())
    assert offer is not None and offer["kind"] == "presented_offer" and offer["schema_version"] == 2
    assert len(offer["alternatives"]) == 1 and offer["alternatives"][0]["rank"] == 1
    assert offer["alternatives"][0]["local_date"] == "2026-09-20"
    assert offer["alternatives"][0]["local_time"] == "17:30"
    assert offer["expires_at"].endswith("Z") and offer["offered_at"].endswith("Z")
    # Unavailable results must not produce an offer
    empty = build_final_response(match_and_analyze_slots(prepare_search_window(base_item()), []), [])
    assert build_presented_offer(empty, base_item()) is None
