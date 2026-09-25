"""Session inactivity compaction (2026-09-22, 2h window) — regression tests.

Owner directive: after 2 hours since the conversation's last activity the raw
chat log is compacted — the prior turns do NOT flow into the next turn's model
context, and a structured summary (الخلاصة) travels instead.
"""
import json

from app.core import session_compact
from app.services import dialogue as dialogue_mod


def _state(last_updated, turns=None, **extra):
    st = {
        "last_updated": last_updated,
        "recent_turns": turns if turns is not None else [
            {"role": "user", "text": "السلام عليكم", "at": "2026-09-22T16:59:00Z"},
            {"role": "assistant", "text": "وعليكم السلام", "at": "2026-09-22T16:59:10Z"},
        ],
        "booking_context": {"doctor_name": "د. أحمد", "date": "2026-09-24", "time": "10:30"},
        "facts": {"patient": {"name": "حسام"}},
        "last_intent": "booking",
    }
    st.update(extra)
    return st


def test_boundary_after_two_hours():
    assert session_compact.session_boundary(_state("2026-09-22T17:00:00Z"), "2026-09-22T19:00:01Z") is True


def test_no_boundary_within_two_hours():
    assert session_compact.session_boundary(_state("2026-09-22T17:00:00Z"), "2026-09-22T18:59:00Z") is False


def test_no_boundary_without_last_activity():
    assert session_compact.session_boundary({}) is False
    assert session_compact.session_boundary({"recent_turns": []}) is False


def test_summary_extracts_essence_only():
    s = session_compact.build_session_summary(_state("2026-09-22T17:00:00Z"))
    assert s.get("patient_name") == "حسام"
    assert s.get("last_intent") == "booking"
    assert s.get("booking_context", {}).get("doctor_name") == "د. أحمد"
    # no raw chat text leaks into the summary
    assert "وعليكم السلام" not in json.dumps(s, ensure_ascii=False)


def test_compact_false_shape():
    out = session_compact.compact(_state("2026-09-22T17:00:00Z"), "2026-09-22T17:30:00Z")
    assert out == {"boundary": False, "summary": None}


def _iso_offset(hours):
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_agent_payload_compacts_after_gap():
    st = _state(_iso_offset(3))
    payload_raw = dialogue_mod.build_user_message({}, {}, {}, {}, st, None)
    payload = json.loads(payload_raw)
    # past the 2h window the turn is a FRESH session: no raw chat AND no injected
    # summary — the agent retrieves history via Recall_Session_History when the
    # patient explicitly references the past (owner directive 2026-09-25).
    assert "recent_dialogue" not in payload
    assert "previous_session_summary" not in payload


def test_agent_payload_keeps_dialogue_within_window():
    st = _state(_iso_offset(1))
    payload_raw = dialogue_mod.build_user_message({}, {}, {}, {}, st, None)
    payload = json.loads(payload_raw)
    recent = payload.get("recent_dialogue") or []
    assert len(recent) == 2
    assert "previous_session_summary" not in payload


def test_recall_executor_found_and_not_found():
    """Recall_Session_History: found=True with the stored summary; found=False with
    an honest not-found note — never fabricated history."""
    from app.core import session_compact as sc
    from app.services.dialogue import recall_session_history

    st = _state(_iso_offset(3))
    summary = sc.build_session_summary(st)
    st["previous_session_summary"] = summary
    out = recall_session_history(st)
    assert out["found"] is True
    assert out["previous_session_summary"]["patient_name"] == "حسام"

    out = recall_session_history({})
    assert out["found"] is False
    assert "restate" in out["note"]


def test_clinic_info_tool_registered_and_executor_wired():
    """Get_Clinic_Info (B2): the 6th tool — hours/branches from DB, grounded."""
    from app.services import dialogue as dlg
    tools = [t["function"]["name"] for t in dlg.RECEPTIONIST_TOOLS]
    assert "Get_Clinic_Info" in tools
    # executor branch reads the clinic context — verify the repository fn exists
    import app.db.repository as repo_mod
    assert hasattr(repo_mod, "get_clinic_info")
