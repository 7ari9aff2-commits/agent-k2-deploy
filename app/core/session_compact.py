"""Session inactivity compaction (owner directive 2026-09-22: 2-hour window).

After 2 hours pass since the conversation's last activity, the raw chat log must
NOT keep flowing into the model context turn after turn. The prior turns are
compacted into a structured summary (الخلاصة) derived strictly from saved state
fields — no text generation, no patient-facing output. The next turn starts a
fresh raw session; the summary travels alongside it for continuity.

The compaction is context engineering for the dialogue model: it never enters a
patient-facing reply, so the grounding gate is not involved.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

SESSION_INACTIVITY_GAP_SECONDS = 2 * 60 * 60

_SUMMARY_BOOKING_FIELDS = ("doctor_name", "appointment_type", "date", "time",
                           "booking_number", "status", "action")


def _parse_iso(value: Any) -> Optional[datetime]:
    s = str(value or "").strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _now_dt(now_iso: Optional[str]) -> datetime:
    dt = _parse_iso(now_iso)
    return dt or datetime.now(timezone.utc)


def _last_activity_dt(previous_state: Dict[str, Any]) -> Optional[datetime]:
    if not isinstance(previous_state, dict):
        return None
    candidates = [previous_state.get("last_updated")]
    turns = previous_state.get("recent_turns")
    if isinstance(turns, list):
        for t in reversed(turns):
            if isinstance(t, dict) and t.get("at"):
                candidates.append(t.get("at"))
                break
    for c in candidates:
        dt = _parse_iso(c)
        if dt is not None:
            return dt
    return None


def session_boundary(previous_state: Dict[str, Any], now_iso: Optional[str] = None) -> bool:
    """True when more than SESSION_INACTIVITY_GAP_SECONDS passed since last activity.

    No last-activity timestamp (first turn ever, or a state without one) is NOT a
    boundary: there is nothing to compact.
    """
    last = _last_activity_dt(previous_state)
    if last is None:
        return False
    gap = (_now_dt(now_iso) - last).total_seconds()
    return gap > SESSION_INACTIVITY_GAP_SECONDS


def build_session_summary(previous_state: Dict[str, Any]) -> Dict[str, Any]:
    """الخلاصة: the essence of the prior chat, extracted from structured state only."""
    if not isinstance(previous_state, dict):
        return {}
    bc = previous_state.get("booking_context")
    bc = bc if isinstance(bc, dict) else {}
    booking = {k: bc.get(k) for k in _SUMMARY_BOOKING_FIELDS if bc.get(k) not in (None, "")}
    facts = previous_state.get("facts")
    facts = facts if isinstance(facts, dict) else {}
    patient = facts.get("patient")
    patient = patient if isinstance(patient, dict) else {}
    patient_name = patient.get("name") or previous_state.get("patient_name")
    ct = previous_state.get("confirmation_target")
    ct = ct if isinstance(ct, dict) else {}
    pending = {
        "booking_number": ct.get("booking_number") or booking.get("booking_number"),
        "appointment_type": ct.get("appointment_type") or booking.get("appointment_type"),
        "doctor_name": ct.get("doctor_name") or booking.get("doctor_name"),
        "date": ct.get("date") or booking.get("date"),
        "time": ct.get("time") or booking.get("time"),
    }
    pending = {k: v for k, v in pending.items() if v not in (None, "")}
    summary: Dict[str, Any] = {}
    if patient_name:
        summary["patient_name"] = patient_name
    if previous_state.get("last_intent") or previous_state.get("current_intent"):
        summary["last_intent"] = previous_state.get("last_intent") or previous_state.get("current_intent")
    if booking:
        summary["booking_context"] = booking
    if pending:
        summary["pending_appointment"] = pending
    return summary


def compact(previous_state: Dict[str, Any], now_iso: Optional[str] = None) -> Dict[str, Any]:
    """Single entry point: {'boundary': bool, 'summary': dict|None}."""
    if not session_boundary(previous_state, now_iso):
        return {"boundary": False, "summary": None}
    return {"boundary": True, "summary": build_session_summary(previous_state)}
