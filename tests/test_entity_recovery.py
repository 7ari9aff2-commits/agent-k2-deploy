"""Unit tests for recover_entities_from_tool_events (dialogue.py)."""
from __future__ import annotations

import json

from app.services.dialogue import recover_entities_from_tool_events


def _contract(entities: dict) -> str:
    return json.dumps({
        "schema_version": "k2.dialogue.v4", "reply": "رد",
        "turn": {"intent": "booking_request"}, "confidence": 0.95, "ambiguous": [],
        "confirmation": {"intent": "none"}, "selection": {"kind": "none", "rank": None},
        "entities": entities, "operation_proposal": {"type": "none", "requested": False},
        "escalate": None,
    }, ensure_ascii=False)


AVAILABILITY_EVENT = {
    "name": "Check_Doctor_Availability",
    "arguments": {"doctor_id": "د. أحمد الحنكشلاوي", "requested_date": "2026-09-20", "service_id": None},
    "result": {"success": True},
}


def test_fills_empty_entities_from_successful_tool_arguments():
    out = json.loads(recover_entities_from_tool_events(_contract({"doctor_name": None}), [AVAILABILITY_EVENT]))
    assert out["entities"]["doctor_name"] == "د. أحمد الحنكشلاوي"
    # An assumed requested_date is NOT hardened into entities.date (reviewer finding:
    # the patient never stated it, and a failed/assumed call must not book a day).
    assert out["entities"].get("date") in (None, "")


def test_skips_failed_tool_calls_entirely():
    failed = dict(AVAILABILITY_EVENT, result={"error": "connection timeout"})
    out = json.loads(recover_entities_from_tool_events(_contract({"doctor_name": None}), [failed]))
    assert out["entities"]["doctor_name"] is None


def test_last_matching_call_wins():
    old_call = {"name": "Check_Doctor_Availability",
                "arguments": {"doctor_id": "د. أحمد", "requested_date": "2026-09-20"},
                "result": {"success": True}}
    corrected_call = {"name": "Check_Doctor_Availability",
                      "arguments": {"doctor_id": "د. سارة", "requested_date": "2026-09-21"},
                      "result": {"success": True}}
    out = json.loads(recover_entities_from_tool_events(
        _contract({"doctor_name": None}), [old_call, corrected_call]))
    assert out["entities"]["doctor_name"] == "د. سارة"


def test_never_overwrites_values_the_contract_already_carries():
    entities = {"doctor_name": "د. سارة", "date": "2026-10-01"}
    out = json.loads(recover_entities_from_tool_events(_contract(entities), [AVAILABILITY_EVENT]))
    assert out["entities"]["doctor_name"] == "د. سارة"
    assert out["entities"]["date"] == "2026-10-01"


def test_ignores_resolved_uuid_doctor_ids():
    event = {"name": "Check_Doctor_Availability",
             "arguments": {"doctor_id": "e9acc542-92d7-49a7-bf0a-4d61c49ef98e",
                           "requested_date": "2026-09-20"}}
    out = json.loads(recover_entities_from_tool_events(_contract({"doctor_name": None}), [event]))
    assert out["entities"]["doctor_name"] is None


def test_returns_non_json_text_untouched():
    raw = "رد نصي مش JSON"
    assert recover_entities_from_tool_events(raw, [AVAILABILITY_EVENT]) == raw


def test_handles_json_fence_wrapped_contracts():
    fenced = "```json\n" + _contract({"doctor_name": None}) + "\n```"
    out = json.loads(recover_entities_from_tool_events(fenced, [AVAILABILITY_EVENT]))
    assert out["entities"]["doctor_name"] == "د. أحمد الحنكشلاوي"
