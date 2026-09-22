"""Faithful port of the two execution-gate n8n code nodes (pure Python, no I/O).

Source nodes (extracted/code/):
  Execution_Transition_Guard_Deterministic.js -> execution_transition_guard_deterministic
  Business_Time_Gate_Deterministic.js         -> business_time_gate_deterministic
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# JS-semantics shims (private to this module; same contracts as llm_safety)
# ---------------------------------------------------------------------------

_UNDEFINED = object()
_NAN = float("nan")

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.IGNORECASE)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?$")


def _js_truthy(value: Any) -> bool:
    if value is None or value is _UNDEFINED:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return not (value == 0 or (isinstance(value, float) and value != value))
    if isinstance(value, str):
        return value != ""
    return True


def _js_string(value: Any) -> str:
    if value is _UNDEFINED:
        return "undefined"
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        import math

        if value != value:
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        if value.is_integer() and abs(value) < 1e21:
            return str(int(value))
        return repr(value)
    if isinstance(value, list):
        parts = []
        for item in value:
            if item is None or item is _UNDEFINED:
                parts.append("")
            elif isinstance(item, str):
                parts.append(item)
            else:
                parts.append(_js_string(item))
        return ",".join(parts)
    return "[object Object]"


def _prop(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, _UNDEFINED)
    return _UNDEFINED


def _dig(obj: Any, *keys: str) -> Any:
    cur = obj
    for k in keys:
        if cur is None or cur is _UNDEFINED:
            return _UNDEFINED
        cur = _prop(cur, k)
    return cur


def _first_truthy(*values: Any) -> Any:
    for v in values[:-1]:
        if _js_truthy(v):
            return v
    return values[-1] if values else None


def _is_finite_number(value: Any) -> bool:
    import math

    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _coalesce(*values: Any) -> Any:
    """JS `a ?? b` — first non-nullish value."""
    for v in values:
        if v is not None and v is not _UNDEFINED:
            return v
    return values[-1] if values else None


_NUM_RE = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")


def _js_number(value: Any) -> float:
    """JS Number() coercion (NaN where JS produces NaN)."""
    import math

    if value is _UNDEFINED:
        return _NAN
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if s == "":
            return 0.0
        if s.lower().startswith(("0x", "-0x", "+0x")):
            try:
                return float(int(s, 16))
            except ValueError:
                return _NAN
        if s in ("Infinity", "+Infinity"):
            return math.inf
        if s == "-Infinity":
            return -math.inf
        if _NUM_RE.match(s):
            try:
                return float(s)
            except ValueError:
                return _NAN
        return _NAN
    if isinstance(value, list):
        if len(value) == 0:
            return 0.0
        if len(value) == 1:
            return _js_number(value[0])
        return _NAN
    return _NAN


def _date_parse(value: Any) -> float:
    from datetime import timezone

    s = value if isinstance(value, str) else _js_string(value)
    t = s.strip()
    if not t:
        return _NAN
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", t):
            dt = datetime.strptime(t, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return dt.timestamp() * 1000.0
        iso = t[:-1] + "+00:00" if t.endswith(("Z", "z")) else t
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.astimezone()
        return dt.timestamp() * 1000.0
    except (ValueError, OSError, OverflowError):
        return _NAN


def _parse_json_or(value: Any, fallback: Any) -> Any:
    """JS parseJson: arrays/objects pass through; JSON strings are parsed; else fallback."""
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return value
    try:
        return json.loads(_js_string(value) if _js_truthy(value) else "")
    except ValueError:
        return fallback


def _valid_uuid(value: Any) -> bool:
    return bool(_UUID_RE.match(_js_string(_first_truthy(value, "")).strip()))


def _valid_date(value: Any) -> bool:
    return bool(_DATE_RE.match(_js_string(_first_truthy(value, "")).strip()))


def _valid_time(value: Any) -> bool:
    return bool(_TIME_RE.match(_js_string(_first_truthy(value, "")).strip()))


# ---------------------------------------------------------------------------
# P1.1 Execution Transition Guard (v2)
# ---------------------------------------------------------------------------


def execution_transition_guard_deterministic(item: Dict[str, Any], conversation_state: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Execution Transition Guard (Deterministic) (extracted/code/Execution_Transition_Guard_Deterministic.js).

    Args:
        item: current pipeline item ($json) carrying system_decision.
        conversation_state: 'Get Conversation State' first().json (its
            .state_data is read as `previous`); {} when unavailable.
    Returns the item with a guarded system_decision / output envelope.
    """
    item = item if _js_truthy(item) else {}
    decision = _first_truthy(_prop(item, "system_decision"), {})
    previous = _first_truthy(_prop(conversation_state or {}, "state_data"), {})
    action = _js_string(_first_truthy(_prop(decision, "action"), "")).lower()
    execution_actions = {"create_appointment", "cancel_appointment", "reschedule_appointment"}
    is_execution_request = action in execution_actions and _js_string(_prop(decision, "allowed")).lower() == "true"

    def data_valid(dec: Any, act: str) -> bool:
        if not _js_truthy(dec):
            return False
        bc = _prop(dec, "booking_context")
        bc = bc if isinstance(bc, (dict, list)) else {}
        if act == "create_appointment":
            return (
                _valid_uuid(_prop(bc, "doctor_id"))
                and _valid_uuid(_prop(bc, "slot_id"))
                and _valid_date(_prop(bc, "date"))
                and _valid_time(_prop(bc, "time"))
            )
        if act == "cancel_appointment":
            appt_id = _first_truthy(
                _prop(dec, "appointment_id"),
                _prop(previous, "appointment_id"),
                _dig(previous, "facts", "booking", "appointment_id"),
            )
            return _valid_uuid(appt_id)
        if act == "reschedule_appointment":
            appt_id = _first_truthy(
                _prop(dec, "appointment_id"),
                _dig(dec, "confirmation_target", "appointment_id"),
                _dig(previous, "confirmation_target", "appointment_id"),
                _prop(previous, "appointment_id"),
                None,
            )
            old_slot_id = _first_truthy(
                _prop(dec, "expected_old_slot_id"),
                _dig(dec, "confirmation_target", "expected_old_slot_id"),
                _dig(previous, "confirmation_target", "expected_old_slot_id"),
                _dig(previous, "slot_state", "slot_id"),
                None,
            )
            new_slot_id = _first_truthy(
                _prop(dec, "new_slot_id"),
                _dig(dec, "confirmation_target", "new_slot_id"),
                _prop(bc, "new_slot_id"),
                _prop(bc, "slot_id"),
                None,
            )
            return _valid_uuid(appt_id) and _valid_uuid(old_slot_id) and _valid_uuid(new_slot_id)
        return False

    # ── Orchestrator trust ──
    response_code = _js_string(_first_truthy(_prop(decision, "response_code"), "")).upper()
    orchestrator_approved = response_code in ("EXECUTE_APPROVED", "CANCEL_APPROVED", "RESCHEDULE_APPROVED") and _prop(decision, "allowed") is True

    if not is_execution_request:
        # Not an execution request — pass through unchanged
        return dict(item)

    # ── Orchestrator explicitly approved + data is valid → ALLOW ──
    if orchestrator_approved and data_valid(decision, action):
        approved_decision = {
            **decision,
            "allowed": True,
            "response_code": response_code,
            "transition_guard": {"event": action, "from_state": "ORCHESTRATOR_APPROVED", "allowed": True, "bypass": True},
        }
        return {
            **item,
            "system_decision": approved_decision,
            "output": {**_first_truthy(_prop(item, "output"), {}), "system_decision": approved_decision},
        }

    # ── Otherwise: check state transition rules ──

    def canonical_from_legacy(state: Any) -> Optional[str]:
        canonical = _js_string(_first_truthy(_prop(state, "operation_state"), "")).upper()
        if canonical in ("IDLE", "DRAFT", "PAUSED", "AWAITING_CONFIRMATION", "REFRESH_REQUIRED", "EXECUTING", "COMPLETED", "CANCELLED", "FAILED_RETRYABLE", "FAILED_FINAL"):
            return canonical
        status = _js_string(_first_truthy(_prop(state, "operation_status"), "")).strip().lower()
        if status == "awaiting_confirmation":
            return "AWAITING_CONFIRMATION"
        if status in ("collecting_details", "draft"):
            return "DRAFT"
        if status == "completed":
            return "COMPLETED"
        if status == "cancelled":
            return "CANCELLED"
        return None

    from_state = canonical_from_legacy(previous)
    from_state_allowed = from_state in ("AWAITING_CONFIRMATION", "DRAFT")

    if not from_state_allowed or not data_valid(decision, action):
        guarded_decision = {
            **decision,
            "allowed": False,
            "response_code": "INVALID_STATE_TRANSITION",
            "transition_guard": {"event": action or "unknown", "from_state": from_state, "allowed": False},
            "audit_event": "invalid_transition",
            # No patient-facing text here: the model authors every reply (see reply_guard).
        "final_reply": None,
        }
        guarded_output = {
            **_first_truthy(_prop(item, "output"), {}),
            "response_code": "INVALID_STATE_TRANSITION",
            "operation_status": "refresh_required",
            "retryable": False,
            "final_reply": guarded_decision["final_reply"],
            "reply_text": guarded_decision["final_reply"],
            "system_decision": guarded_decision,
        }
        return {**item, "system_decision": guarded_decision, "output": guarded_output, "audit_event": "invalid_transition"}

    # ── Default: allow with state transition check ──
    approved_decision2 = {
        **decision,
        "allowed": True,
        "transition_guard": {"event": action, "from_state": from_state, "allowed": True},
    }
    return {
        **item,
        "system_decision": approved_decision2,
        "output": {**_first_truthy(_prop(item, "output"), {}), "system_decision": approved_decision2},
    }


# ---------------------------------------------------------------------------
# Business Time Gate (Deterministic)
# ---------------------------------------------------------------------------

_SLOT_TIME_PREFIX_RE = re.compile(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?")


def _timezone_is_valid(name: str) -> bool:
    """JS: try { new Intl.DateTimeFormat('en-GB', { timeZone }).format(now) } — IANA check."""
    if not name:
        return False
    try:
        ZoneInfo(name)
        return True
    except Exception:
        return False


def business_time_gate_deterministic(guard: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Business Time Gate (Deterministic) (extracted/code/Business_Time_Gate_Deterministic.js).

    Args:
        guard: 'Execution Transition Guard (Deterministic)' first().json
            (provides system_decision); {} when unavailable.
        row: current item ($json) — the clinic/slot row with timezone,
            timezone_configured, timezone_error_code, business_hours,
            start_time, end_time, slot_found.
    Returns the merged item with business_time_* fields and a possibly
    overridden system_decision (allowed=false + Arabic final_reply on gate
    violations).
    """
    guard_row = guard if _js_truthy(guard) else {}
    decision = _prop(guard_row, "system_decision")
    decision = decision if isinstance(decision, (dict, list)) else {}
    row = row if isinstance(row, (dict, list)) else {}
    action = _js_string(
        _first_truthy(_prop(decision, "action"), _dig(decision, "confirmation_target", "action"), "")
    ).lower()
    should_check = _prop(decision, "allowed") is True and action in ("create_appointment", "reschedule_appointment")

    raw_timezone = _js_string(_coalesce(_prop(row, "timezone"), "")).strip()
    timezone_configured = _prop(row, "timezone_configured") is True and _timezone_is_valid(raw_timezone)
    timezone: Optional[str] = raw_timezone if timezone_configured else None
    if timezone_configured:
        timezone_error_code: Optional[str] = None
    else:
        timezone_error_code = (
            _js_string(_first_truthy(_prop(row, "timezone_error_code"), "")).strip()
            or ("CLINIC_TIMEZONE_INVALID" if _js_truthy(raw_timezone) else "CLINIC_TIMEZONE_NOT_CONFIGURED")
        )

    hours = _parse_json_or(_prop(row, "business_hours"), [])

    def local_parts(iso: Any) -> Optional[Dict[str, Any]]:
        if not timezone_configured:
            return None
        ms = _date_parse(iso)
        if not _is_finite_number(ms):
            return None
        try:
            dt = datetime.fromtimestamp(ms / 1000.0, tz=ZoneInfo(timezone))
        except Exception:
            return None
        # JS Intl weekday: Sun=0..Sat=6; Python weekday(): Mon=0..Sun=6
        day_of_week = (dt.weekday() + 1) % 7
        minute_of_day = dt.hour * 60 + dt.minute + dt.second / 60.0
        return {"dayOfWeek": day_of_week, "minuteOfDay": minute_of_day}

    def parse_time(value: Any) -> Optional[float]:
        m = _SLOT_TIME_PREFIX_RE.match(_js_string(_coalesce(value, "")))
        if not m:
            return None
        hour = _js_number(m.group(1))
        minute = _js_number(m.group(2))
        second = _js_number(m.group(3) if m.group(3) is not None else 0)
        if hour > 23 or minute > 59 or second > 59:
            return None
        return hour * 60 + minute + second / 60.0

    status = "NOT_CHECKED"
    code: Optional[str] = None
    allowed = True
    checked = False
    if should_check:
        checked = True
        if not timezone_configured:
            status = "TIMEZONE_NOT_CONFIGURED"
            code = timezone_error_code
            allowed = False
        else:
            start = local_parts(_prop(row, "start_time"))
            end = local_parts(_prop(row, "end_time"))
            if (not _js_truthy(_prop(row, "slot_found"))) or start is None or end is None or not isinstance(hours, list) or len(hours) == 0:
                if _prop(row, "slot_found") is not True:
                    status = "NOT_APPLICABLE"
                    code = None
                    allowed = True
                else:
                    status = "UNAVAILABLE"
                    code = "BUSINESS_HOURS_UNAVAILABLE"
                    allowed = False
            else:
                candidates = [
                    hour for hour in hours
                    if _js_number(_prop(hour, "day_of_week")) == start["dayOfWeek"] and _prop(hour, "is_off_day") is not True
                ]

                def hour_covers(hour: Any) -> bool:
                    open_m = parse_time(_prop(hour, "open_time"))
                    close_m = parse_time(_prop(hour, "close_time"))
                    if open_m is None or close_m is None or open_m == close_m:
                        return False
                    if close_m > open_m:
                        return (
                            start["minuteOfDay"] >= open_m
                            and start["minuteOfDay"] < close_m
                            and end["dayOfWeek"] == start["dayOfWeek"]
                            and end["minuteOfDay"] <= close_m
                        )
                    next_day = (start["dayOfWeek"] + 1) % 7
                    return start["minuteOfDay"] >= open_m and end["dayOfWeek"] == next_day and end["minuteOfDay"] <= close_m

                inside = any(hour_covers(hour) for hour in candidates)
                status = "VALID" if inside else "VIOLATION"
                code = "BUSINESS_TIME_VALID" if inside else "BUSINESS_HOURS_VIOLATION"
                allowed = inside

    business_time_source = "clinic_business_hours" if timezone_configured else "clinic_configuration_error"
    if should_check and not allowed:
        if status == "TIMEZONE_NOT_CONFIGURED":
            final_reply = None  # timezone not configured - reason only, no canned text
        elif status == "UNAVAILABLE":
            final_reply = None  # business hours unverified - reason only, no canned text
        else:
            final_reply = None  # outside business hours - reason only, no canned text
        final_decision = {
            **decision,
            "allowed": False,
            "response_code": code,
            "business_time_status": status,
            "business_time_checked": checked,
            "business_time_timezone": timezone,
            "business_time_source": business_time_source,
            "business_time_error_code": code,
            "final_reply": final_reply,
        }
    else:
        final_decision = {
            **decision,
            "business_time_status": status,
            "business_time_checked": checked,
            "business_time_timezone": timezone,
            "business_time_source": business_time_source,
            "business_time_error_code": code,
        }

    return {
        **guard_row,
        **row,
        "timezone_configured": timezone_configured,
        "timezone_error_code": timezone_error_code,
        "business_time_allowed": allowed,
        "business_time_checked": checked,
        "business_time_status": status,
        "business_time_code": code,
        "business_time_timezone": timezone,
        "business_time_source": business_time_source,
        "business_time_error_code": code,
        "business_time_slot_start": _first_truthy(_prop(row, "start_time"), None),
        "business_time_slot_end": _first_truthy(_prop(row, "end_time"), None),
        "system_decision": final_decision,
    }
