"""Faithful port of the post-execution n8n code nodes (pure Python, no I/O).

Source nodes (extracted/code/) and their ports:
  Prepare_Single_Agent_Result_Context.js -> prepare_single_agent_result_context
  Extract_Single_Agent_Reply.js          -> extract_single_agent_reply
  Prepare_Operation_Claim_Input.js       -> prepare_operation_claim_input
  Apply_Operation_Claim_Deterministic.js -> apply_operation_claim_deterministic
  Prepare_Execute_Input.js               -> prepare_execute_input
  Prepare_Execute_Context.js             -> prepare_execute_context
  Prepare_Operation_Finalize_Input.js    -> prepare_operation_finalize_input
  Merge_Operation_Completion.js          -> merge_operation_completion
  Apply_Resolved_Booking_IDs_Deterministic.js -> apply_resolved_booking_ids_deterministic
  Prepare_Handoff_Input.js               -> prepare_handoff_input
  Validate_Child_Envelope.js             -> validate_child_envelope
  Restore_Handoff_Context.js             -> restore_handoff_context

Every $(NodeName).first().json read in the JS is an explicitly documented input
key (see each docstring). Arabic strings are byte-identical to the JS, including
the mojibake quirk in Prepare_Single_Agent_Result_Context (U+FFFD U+0085 where
the source lost the م byte).
"""
from __future__ import annotations

import base64
import json
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from app.core.js_semantics import is_finite_num as _is_finite_num, obj_or_empty as _obj_or_empty

# ---------------------------------------------------------------------------
# JS-semantics shims (private to this module; same contracts as app/core/llm_safety)
# ---------------------------------------------------------------------------

_UNDEFINED = object()
_NAN = float("nan")


def _js_truthy(value: Any) -> bool:
    if value is None or value is _UNDEFINED:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return not (value == 0 or (isinstance(value, float) and value != value))
    if isinstance(value, str):
        return value != ""
    return True  # dict/list are always truthy in JS


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


_NUM_RE = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")


def _js_number(value: Any) -> float:
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
        low = s.lower()
        if low.startswith(("0x", "-0x", "+0x")):
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


def _js_parse_int(value: Any, radix: int = 10) -> float:
    """JS parseInt: leading integer parse; NaN when no digits."""
    s = _js_string(value).strip()
    sign = 1.0
    if s[:1] in ("+", "-"):
        if s[0] == "-":
            sign = -1.0
        s = s[1:]
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"[:radix]
    out = ""
    for ch in s:
        if ch.lower() in digits:
            out += ch
        else:
            break
    if not out:
        return _NAN
    return sign * float(int(out, radix))


def _coalesce(*values: Any) -> Any:
    """JS `a ?? b` — first non-nullish value."""
    for v in values:
        if v is not None and v is not _UNDEFINED:
            return v
    return values[-1] if values else None


def _first_truthy(*values: Any) -> Any:
    """JS `a || b` — first JS-truthy value, else the last value."""
    for v in values[:-1]:
        if _js_truthy(v):
            return v
    return values[-1] if values else None


def _prop(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, _UNDEFINED)
    return _UNDEFINED


def _dig(obj: Any, *keys: str) -> Any:
    """JS optional chaining a?.b?.c."""
    cur = obj
    for k in keys:
        if cur is None or cur is _UNDEFINED:
            return _UNDEFINED
        cur = _prop(cur, k)
    return cur


def _has_key(obj: Any, key: str) -> bool:
    """JS `key !== undefined` / `'key' in obj` on a dict item."""
    return isinstance(obj, dict) and obj.get(key, _UNDEFINED) is not _UNDEFINED


def _imul(a: int, b: int) -> int:
    return ((a & 0xFFFFFFFF) * (b & 0xFFFFFFFF)) & 0xFFFFFFFF


def _now_ms() -> float:
    return datetime.now(timezone.utc).timestamp() * 1000.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _date_parse(value: Any) -> float:
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


# ---------------------------------------------------------------------------
# Prepare Single Agent Result Context
# ---------------------------------------------------------------------------

# Source mojibake kept byte-identical: in the extracted JS every م byte pair
# became U+FFFD U+0085 (e.g. 'مارس' -> '\ufffd\u0085ارس').
_AR_MONTHS_063 = [
    "يناير", "فبراير", "\ufffd\u0085ارس", "أبريل", "\ufffd\u0085ايو", "يونيو",
    "يوليو", "أغسطس", "سبت\ufffd\u0085بر", "أكتوبر", "نوف\ufffd\u0085بر", "ديس\ufffd\u0085بر",
]
_AR_WEEKDAYS_063 = [
    "الأحد", "الاثنين", "الثلاثاء", "الأربعاء", "الخ\ufffd\u0085يس", "الج\ufffd\u0085عة", "السبت",
]


# ---------------------------------------------------------------------------
# Prepare Single Agent Result Context / Extract Single Agent Reply
# REMOVED 2026-09-17. Those two ports only ever ran inside the
# `if_single_agent_result_phase` branch of app/api/v1/message.py, which is
# unreachable (route_single_agent_phase always yields phase="understand").
# They also carried `_stage_plan()` - a per-response-code Arabic reply table.
# Replies are authored by the model from the fact catalog; see
# app/core/response_context.py and dialogue.compose_patient_reply.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Prepare Operation Claim Input
# ---------------------------------------------------------------------------

_LOOSE_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)


def prepare_operation_claim_input(item: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Prepare Operation Claim Input (extracted/code/Prepare_Operation_Claim_Input.js).

    Args:
        item: current pipeline item ($input.item.json).
        inputs keys:
          normalize_validate <- 'Normalize & Validate' (clinic_id/patient_id/conversation_id)
    Returns the item plus claim_* keys (claim_required, claim_action,
    claim_clinic_id, claim_patient_id, claim_conversation_id,
    claim_confirmation_id, claim_target_fingerprint, claim_schema_version).
    """
    source = item or {}
    normalize = _first_truthy(inputs.get("normalize_validate", _UNDEFINED), {})
    decision = _prop(source, "system_decision")
    decision = decision if isinstance(decision, (dict, list)) else {}
    target = _prop(decision, "confirmation_target")
    target = target if isinstance(target, (dict, list)) else {}
    action = _js_string(_first_truthy(_prop(decision, "action"), _prop(target, "action"), "")).lower()
    # flow_up_operation_ledger.confirmation_id references the persisted
    # flow_up_confirmations.confirmation_id. Pass the deterministic target id only;
    # never substitute a prompt-message id or invent an id.
    raw_confirmation_id = _js_string(_first_truthy(_prop(target, "confirmation_id"), "")).strip()
    claim_confirmation_id = raw_confirmation_id if _LOOSE_UUID_RE.match(raw_confirmation_id) else None
    side_effect_action = action in ("create_appointment", "cancel_appointment", "reschedule_appointment")
    # Claim only approved side effects; non-execution turns bypass the ledger.
    claim_required = _prop(decision, "allowed") is True and side_effect_action
    target_fingerprint = _js_string(
        _first_truthy(_prop(target, "context_fingerprint"), _prop(source, "business_time_target_fingerprint"), "")
    ).strip()
    return {
        **source,
        "claim_required": claim_required,
        "claim_action": action,
        "claim_clinic_id": _first_truthy(_prop(normalize, "clinic_id"), None),
        "claim_patient_id": _first_truthy(_prop(normalize, "patient_id"), None),
        "claim_conversation_id": _first_truthy(_prop(normalize, "conversation_id"), None),
        "claim_confirmation_id": claim_confirmation_id,
        "claim_target_fingerprint": target_fingerprint or None,
        "claim_schema_version": 1,
    }


# ---------------------------------------------------------------------------
# Apply Operation Claim (Deterministic)
# ---------------------------------------------------------------------------


def _parse_json_or_null(value: Any) -> Any:
    if _js_truthy(value) and isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(_js_string(_first_truthy(value, "")))
    except ValueError:
        return None


def apply_operation_claim_deterministic(item: Dict[str, Any], claim_input: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Source node: Apply Operation Claim (Deterministic) (extracted/code/Apply_Operation_Claim_Deterministic.js).

    Args:
        item: current pipeline item ($input.item.json — the ledger RPC row).
        claim_input: 'Prepare Operation Claim Input' first().json. Pass None only
            when that node did not run (mirrors the JS `|| $input.item.json`
            fallback onto the current item).
    Returns the merged item with operation_claim_*, operation_replay_*, the
    final system_decision and the Arabic final_reply for blocked/replayed claims.
    """
    raw = item or {}
    input_item = claim_input if claim_input is not None else raw or {}
    decision = _prop(input_item, "system_decision")
    decision = decision if isinstance(decision, (dict, list)) else {}
    claim_decision = _js_string(_first_truthy(_prop(raw, "decision"), "")).upper()
    ledger_operation_id = _js_string(_first_truthy(_prop(raw, "operation_id"), "")).strip() or None
    stored_response = _parse_json_or_null(_prop(raw, "response_json"))
    replay = claim_decision in ("IDEMPOTENT_REPLAY", "REPLAY_FINAL")
    claim_owner = claim_decision in ("OWNER", "OWNER_RETRY")
    blocked = not claim_owner
    ledger_alert = _prop(raw, "ledger_alert") is True or claim_decision == "INCONCLUSIVE"
    ledger_age_seconds_num = _js_number(_prop(raw, "ledger_age_seconds"))
    ledger_age_seconds = ledger_age_seconds_num if _is_finite_num(ledger_age_seconds_num) else None

    response_code: Optional[str] = None
    final_reply: Optional[str] = None
    allowed = _prop(decision, "allowed") is True
    retryable = False
    operation_state: Optional[str] = None

    if claim_decision == "IN_PROGRESS":
        response_code = "OPERATION_IN_PROGRESS"
        final_reply = "العملية نفسها قيد التنفيذ حالياً. لن أكرر الإجراء، وسأعيد لك النتيجة عند اكتمالها."
        allowed = False
        retryable = True
        operation_state = "in_progress"
    elif claim_decision == "INCONCLUSIVE":
        response_code = "OPERATION_INCONCLUSIVE"
        final_reply = "تعذر التحقق بأمان من نتيجة المحاولة السابقة، لذلك لن أعيد تنفيذ العملية تلقائياً."
        allowed = False
        retryable = False
        operation_state = "inconclusive"
    elif claim_decision == "OPERATION_CONFLICT":
        response_code = "OPERATION_ID_CONFLICT"
        final_reply = "تعذر مطابقة هوية العملية بأمان، لذلك لم يتم تنفيذ أي إجراء."
        allowed = False
        retryable = False
        operation_state = "conflict"
    elif claim_decision == "UNSUPPORTED_ACTION":
        response_code = "UNSUPPORTED_OPERATION"
        final_reply = "لا يمكن تنفيذ هذه العملية من خلال هذا المسار."
        allowed = False
        retryable = False
        operation_state = "failed_final"
    elif claim_decision == "REPLAY_FINAL":
        response_code = "REPLAY_FINAL"
        final_reply = _first_truthy(
            _prop(stored_response, "final_reply"),
            _prop(stored_response, "message"),
            _prop(decision, "final_reply"),
            "تمت معالجة العملية سابقاً ولا يمكن إعادة تنفيذها.",
        )
        allowed = False
        retryable = False
        operation_state = "failed_final"
    elif claim_decision == "IDEMPOTENT_REPLAY":
        response_code = "IDEMPOTENT_REPLAY"
        final_reply = _first_truthy(
            _prop(stored_response, "final_reply"),
            _prop(stored_response, "message"),
            _prop(decision, "final_reply"),
            "تم تنفيذ العملية سابقاً.",
        )
        allowed = False
        retryable = False
        operation_state = "completed"
    elif not claim_owner:
        response_code = "CLAIM_NOT_GRANTED"
        final_reply = "لم يتم الحصول على ملكية العملية بأمان، لذلك لم يتم تنفيذ أي إجراء."
        allowed = False
        retryable = False
        operation_state = "conflict"

    # Replay surfacing (added 2026-09-18): the stored ledger envelope carries the real
    # booking identity — surface it so the composer can state the booking number and
    # the state save keeps it (a bare "تم تنفيذ العملية سابقاً." leaves the patient
    # without their reference after a mid-turn crash retry).
    replay_surfaces: Dict[str, Any] = {}
    if replay and isinstance(stored_response, dict):
        stored_booking_number = _first_truthy(
            _prop(stored_response, "booking_number"),
            _dig(stored_response, "data", "booking_number"),
            _dig(stored_response, "booking_context", "booking_number"),
            None)
        stored_appointment_id = _first_truthy(
            _prop(stored_response, "appointment_id"),
            _dig(stored_response, "data", "appointment_id"), None)
        if _js_truthy(stored_booking_number):
            replay_surfaces["booking_number"] = stored_booking_number
        if _js_truthy(stored_appointment_id):
            replay_surfaces["appointment_id"] = stored_appointment_id

    if replay or blocked:
        final_decision = {
            **decision,
            "allowed": False,
            "response_code": response_code,
            "final_reply": final_reply,
            "operation_id": ledger_operation_id,
            "operation_state": operation_state,
            "claim_decision": claim_decision,
            "claim_granted": False,
            "resume_eligible": False,
            "retryable": retryable,
            "ledger_alert": ledger_alert,
            "escalate": ledger_alert or _prop(decision, "escalate") is True,
        }
        if replay and replay_surfaces:
            merged_booking_context = decision.get("booking_context") if isinstance(decision, dict) else {}
            merged_booking_context = dict(merged_booking_context) if isinstance(merged_booking_context, dict) else {}
            merged_booking_context.update(replay_surfaces)
            final_decision["booking_context"] = merged_booking_context
    else:
        final_decision = {
            **decision,
            "operation_id": ledger_operation_id,
            "claim_decision": claim_decision,
            "claim_granted": True,
        }

    return {
        **input_item,
        **raw,
        "operation_id": ledger_operation_id,
        "operation_claim_decision": claim_decision,
        "operation_claim_granted": claim_owner,
        "operation_claim_required": _prop(input_item, "claim_required") is True,
        "operation_claim_blocked": blocked,
        "operation_replay": replay,
        "operation_replay_response": stored_response,
        "operation_ledger_status": _first_truthy(_prop(raw, "operation_status"), None),
        "operation_mutation_status": _first_truthy(_prop(raw, "mutation_status"), None),
        "operation_state": operation_state,
        "retryable": retryable,
        "success": (_prop(stored_response, "success") is True) if replay else False,
        "appointment_id": (
            _first_truthy(_prop(stored_response, "appointment_id"), _dig(stored_response, "data", "appointment_id"), None)
            if replay
            else None
        ),
        "booking_number": (
            _first_truthy(replay_surfaces.get("booking_number"), None) if replay
            else _prop(input_item, "booking_number")
        ),
        "system_decision": final_decision,
        "child_execution_allowed": claim_owner,
        "response_code": _first_truthy(response_code, _prop(final_decision, "response_code"), None),
        "final_reply": _first_truthy(final_reply, _prop(final_decision, "final_reply"), None),
        "ledger_alert": ledger_alert,
        "ledger_age_seconds": ledger_age_seconds,
        "escalate": ledger_alert or _prop(final_decision, "escalate") is True,
    }


# ---------------------------------------------------------------------------
# Prepare Execute Input
# ---------------------------------------------------------------------------


def _dig_prior_state(prior_state: Any) -> Any:
    """The JSON round-trip Prepare_Execute_Input performs on state_data.

    JS: prior = parsed.state_data ? (typeof === 'string' ? JSON.parse(...) : value)
        : (parsed.data || parsed)
    """
    prior_state_data = _first_truthy(_prop(prior_state, "state_data"), _prop(prior_state, "data"), prior_state)
    prior: Any = {}
    try:
        raw = prior_state_data if isinstance(prior_state_data, str) else json.dumps(prior_state_data, ensure_ascii=False)
        parsed = json.loads(raw)
        state_val = _prop(parsed, "state_data")
        if _js_truthy(state_val):
            prior = json.loads(_js_string(state_val)) if isinstance(state_val, str) else state_val
        else:
            data_val = _prop(parsed, "data")
            prior = (_js_string(data_val) if isinstance(data_val, str) else data_val) if _js_truthy(data_val) else parsed
    except (ValueError, TypeError):
        prior = {}
    return prior


def prepare_execute_input(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Prepare Execute Input (extracted/code/Prepare_Execute_Input.js).

    Args (no $json in this node):
      conversation_state        <- 'Get Conversation State'
      resolve_booking_ids       <- 'Resolve Booking IDs (Deterministic)'
      apply_resolved_booking_ids <- 'Apply Resolved Booking IDs (Deterministic)'
      system_orchestrator       <- 'System Orchestrator (Policy)'
      normalize_validate        <- 'Normalize & Validate'
    Returns the child-workflow execute input dict (clinic/patient/conversation
    ids, resolved doctor/service/slot identity, operation_id, correlation_id,
    patient_data_complete).
    """
    inputs = inputs or {}
    prior_state = _first_truthy(inputs.get("conversation_state", _UNDEFINED), {})
    prior = _dig_prior_state(prior_state)
    resolve_node = _first_truthy(inputs.get("resolve_booking_ids", _UNDEFINED), {})
    apply_node = _first_truthy(inputs.get("apply_resolved_booking_ids", _UNDEFINED), {})
    orchestrator_node = _first_truthy(inputs.get("system_orchestrator", _UNDEFINED), {})
    sd = _first_truthy(_prop(orchestrator_node, "system_decision"), {})
    target = _prop(sd, "confirmation_target")
    target = target if isinstance(target, (dict, list)) else {}
    sd_bc = _prop(sd, "booking_context")
    sd_bc = sd_bc if isinstance(sd_bc, (dict, list)) else {}
    nv = _first_truthy(inputs.get("normalize_validate", _UNDEFINED), {})

    def ctx(value: Any, fallback: Any = None) -> Any:
        if value is not None and value is not _UNDEFINED and _js_string(value) != "":
            return value
        return fallback

    booking = {
        **(_obj_or_empty(_prop(prior, "booking_context")) if isinstance(_prop(prior, "booking_context"), dict) else {}),
        **(_obj_or_empty(_prop(apply_node, "booking_context")) if isinstance(_prop(apply_node, "booking_context"), dict) else {}),
        **(sd_bc if isinstance(sd_bc, dict) else {}),
    }
    slot_state = {
        **(_obj_or_empty(_prop(prior, "slot_state")) if isinstance(_prop(prior, "slot_state"), dict) else {}),
        **(_obj_or_empty(_prop(apply_node, "slot_state")) if isinstance(_prop(apply_node, "slot_state"), dict) else {}),
    }
    slot_id = ctx(
        _prop(target, "slot_id"),
        ctx(_prop(sd_bc, "slot_id"), ctx(_prop(slot_state, "slot_id"), ctx(_prop(resolve_node, "slot_id"), _prop(booking, "slot_id")))),
    )
    branch_id = ctx(
        _prop(target, "branch_id"),
        ctx(_prop(sd_bc, "branch_id"), ctx(_prop(slot_state, "branch_id"), ctx(_prop(resolve_node, "branch_id"), _prop(booking, "branch_id")))),
    )
    operation_id = ctx(
        _prop(target, "operation_id"),
        ctx(_prop(apply_node, "operation_id"), ctx(_prop(nv, "operation_id"), _js_string(_first_truthy(_prop(nv, "idempotency_key"), "")) + ":create_appointment")),
    )
    patient_values = [
        _prop(booking, "patient_name"),
        _prop(booking, "patient_phone"),
        _prop(booking, "patient_age"),
        _prop(booking, "patient_address"),
    ]
    return {
        "clinic_id": _first_truthy(_prop(nv, "clinic_id"), None),
        "patient_id": _first_truthy(_prop(nv, "patient_id"), None),
        "conversation_id": _first_truthy(_prop(nv, "conversation_id"), None),
        "doctor_id": ctx(_prop(sd_bc, "doctor_id"), ctx(_prop(booking, "doctor_id"), _prop(resolve_node, "doctor_id"))),
        "service_id": ctx(_prop(sd_bc, "service_id"), ctx(_prop(booking, "service_id"), _prop(resolve_node, "service_id"))),
        "service_name": ctx(_prop(sd_bc, "service_name"), ctx(_prop(booking, "service_name"), _prop(resolve_node, "service_name"))),
        "date": ctx(_prop(sd_bc, "date"), ctx(_prop(booking, "date"), _prop(resolve_node, "date"))),
        "time": ctx(_prop(sd_bc, "time"), ctx(_prop(booking, "time"), _prop(resolve_node, "time"))),
        "slot_id": slot_id,
        "branch_id": branch_id,
        "notes": ctx(_prop(target, "notes"), ctx(_prop(sd, "notes"), None)),
        "operation": _first_truthy(_prop(sd, "action"), _prop(sd, "operation"), _prop(target, "action"), "create_appointment"),
        "operation_id": operation_id,
        "correlation_id": _first_truthy(_prop(nv, "correlation_id"), _prop(nv, "message_id"), None),
        "channel_type": _first_truthy(_prop(nv, "channel_type"), "whatsapp"),
        "patient_data_complete": all(
            v is not None and v is not _UNDEFINED and _js_string(v).strip() != "" for v in patient_values
        ),
    }


# ---------------------------------------------------------------------------
# Prepare Execute Context
# ---------------------------------------------------------------------------

_ARABIC_INDIC_DIGITS = "٠١٢٣٤٥٦٧٨٩"
_PLACEHOLDER_NAMES = {"مريض", "patient", "unknown", "غير معروف"}


def _normalize_age(value: Any) -> Optional[int]:
    s = re.sub(
        "[٠-٩]",
        lambda d: str(_ARABIC_INDIC_DIGITS.index(d.group(0))),
        _js_string(_coalesce(value, "")),
    ).strip()
    if not _js_truthy(s):
        return None
    n = _js_number(s)
    if _is_finite_num(n) and float(n).is_integer() and 0 <= n <= 130:
        return int(n)
    return None


def _usable_text(value: Any) -> Optional[str]:
    text = _js_string(_coalesce(value, "")).strip()
    if not _js_truthy(text):
        return None
    return None if text.lower() in _PLACEHOLDER_NAMES else text


def prepare_execute_context(item: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Prepare Execute Context (extracted/code/Prepare_Execute_Context.js).

    Args:
        item: current pipeline item ($json; patient fields may live here).
        inputs keys:
          conversation_state         <- 'Get Conversation State' (None-safe)
          resolve_booking_ids        <- 'Resolve Booking IDs (Deterministic)'
          apply_resolved_booking_ids <- 'Apply Resolved Booking IDs (Deterministic)'
          system_orchestrator        <- 'System Orchestrator (Policy)'
          normalize_validate         <- 'Normalize & Validate'
          slot_lookup_result         <- 'Apply Deterministic Slot Lookup Result'
          clinic_context             <- 'Get Clinic Context'
    Returns the execute context dict with fallback-chained ids and the patient
    identity fields (current statement/review authoritative over the clinic row).
    """
    item = item or {}
    inputs = inputs or {}
    prior_state = inputs.get("conversation_state")
    if _js_truthy(prior_state):
        inner = _first_truthy(_prop(prior_state, "state_data"), _prop(prior_state, "data"))
        prior_state_data = _first_truthy(inner, prior_state, {})
    else:
        # JS: (priorState && (priorState.state_data || priorState.data)) || priorState || {}
        prior_state_data = {}
    prior: Any = {}
    try:
        raw = prior_state_data if isinstance(prior_state_data, str) else json.dumps(prior_state_data, ensure_ascii=False)
        parsed = json.loads(raw)
        # JS: if (parsed.state_data) ... else if (parsed.data) ... else prior = parsed
        state_val = _prop(parsed, "state_data")
        if _js_truthy(state_val):
            prior = json.loads(_js_string(state_val)) if isinstance(state_val, str) else state_val
        else:
            data_val = _prop(parsed, "data")
            if _js_truthy(data_val):
                prior = json.loads(_js_string(data_val)) if isinstance(data_val, str) else data_val
            else:
                prior = parsed
    except (ValueError, TypeError):
        prior = {}
    prior = prior if isinstance(prior, dict) else {}

    bc = _prop(prior, "booking_context")
    if not _js_truthy(bc):
        facts_val = _prop(prior, "facts")
        bc = _prop(facts_val, "booking") if _js_truthy(facts_val) else None
    if not _js_truthy(bc):
        prior_sd = _prop(prior, "system_decision")
        bc = _prop(prior_sd, "booking_context") if _js_truthy(prior_sd) else None
    if not _js_truthy(bc):
        bc = prior if _js_truthy(prior) else {}
    resolve_node = _first_truthy(inputs.get("resolve_booking_ids", _UNDEFINED), {})
    apply_node = _first_truthy(inputs.get("apply_resolved_booking_ids", _UNDEFINED), {})
    orchestrator_node = _first_truthy(inputs.get("system_orchestrator", _UNDEFINED), {})

    sd = _first_truthy(_prop(orchestrator_node, "system_decision"), {})
    sd_bc = _prop(sd, "booking_context")
    sd_bc = sd_bc if isinstance(sd_bc, (dict, list)) else {}
    slot_lookup_node = _first_truthy(inputs.get("slot_lookup_result", _UNDEFINED), {})

    normalize_inbound = _first_truthy(inputs.get("normalize_validate", _UNDEFINED), {})
    clinic_id = _prop(normalize_inbound, "clinic_id")
    patient_id = _prop(normalize_inbound, "patient_id")
    conversation_id = _prop(normalize_inbound, "conversation_id")

    apply_bc = _prop(apply_node, "booking_context")
    apply_bc_ok = _js_truthy(apply_node) and _js_truthy(apply_bc)
    apply_bc_val = apply_bc if apply_bc_ok else None

    doctor_id = _first_truthy(
        _prop(sd_bc, "doctor_id"),
        _prop(bc, "doctor_id"),
        _prop(resolve_node, "doctor_id"),
        _prop(apply_bc_val, "doctor_id"),
        None,
    )
    service_id = _first_truthy(
        _prop(sd_bc, "service_id"),
        _prop(bc, "service_id"),
        _prop(resolve_node, "service_id"),
        _prop(apply_bc_val, "service_id"),
        None,
    )
    service_name = _first_truthy(
        _prop(sd_bc, "service_name"),
        _prop(bc, "service_name"),
        _prop(resolve_node, "service_name"),
        _prop(apply_bc_val, "service_name"),
        None,
    )
    appointment_type = _first_truthy(
        _prop(sd_bc, "appointment_type"),
        _prop(bc, "appointment_type"),
        _prop(apply_bc_val, "appointment_type"),
        None,
    )
    date = _first_truthy(_prop(sd_bc, "date"), _prop(bc, "date"), _prop(resolve_node, "date"), _dig(apply_node, "booking_context", "date"), None)
    time = _first_truthy(
        _prop(sd_bc, "time"), _prop(bc, "time"), _prop(resolve_node, "time"),
        _dig(apply_node, "booking_context", "time"), _dig(slot_lookup_node, "booking_context", "time"), None,
    )
    slot_id = _first_truthy(
        _prop(sd_bc, "slot_id"), _prop(bc, "slot_id"), _prop(resolve_node, "slot_id"), _prop(apply_node, "slot_id"),
        _dig(apply_node, "booking_context", "slot_id"), _prop(slot_lookup_node, "slot_id"),
        _dig(slot_lookup_node, "slot_state", "slot_id"), _dig(slot_lookup_node, "booking_context", "slot_id"), None,
    )
    branch_id = _first_truthy(
        _prop(sd_bc, "branch_id"), _prop(bc, "branch_id"), _prop(resolve_node, "branch_id"), _prop(apply_node, "branch_id"),
        _dig(apply_node, "booking_context", "branch_id"), _prop(slot_lookup_node, "branch_id"),
        _dig(slot_lookup_node, "slot_state", "branch_id"), _dig(slot_lookup_node, "booking_context", "branch_id"), None,
    )
    branch_name = _first_truthy(
        _prop(sd_bc, "branch_name"), _prop(bc, "branch_name"), _prop(resolve_node, "branch_name"), _prop(apply_node, "branch_name"),
        _dig(apply_node, "booking_context", "branch_name"), _prop(slot_lookup_node, "branch_name"),
        _dig(slot_lookup_node, "slot_state", "branch_name"), _dig(slot_lookup_node, "booking_context", "branch_name"), None,
    )
    clinic_context = _first_truthy(inputs.get("clinic_context", _UNDEFINED), {})
    current_patient = sd_bc if isinstance(sd_bc, dict) else {}
    prior_review = _prop(prior, "patient_data_review")
    review_fields = _prop(prior_review, "fields") if (_js_truthy(prior_review) and isinstance(_prop(prior_review, "fields"), (dict, list))) else {}
    prior_facts = _prop(prior, "facts")
    fact_patient = _prop(prior_facts, "patient") if (_js_truthy(prior_facts) and isinstance(_prop(prior_facts, "patient"), (dict, list))) else {}
    patient_name = (
        _first_truthy(
            _usable_text(_prop(current_patient, "patient_name")),
            _usable_text(_prop(item, "patient_name")),
            _usable_text(_prop(bc, "patient_name")),
            _usable_text(_prop(review_fields, "name")),
            _usable_text(_prop(fact_patient, "name")),
            _usable_text(_prop(prior, "patient_name")),
            None,
        )
    )
    patient_phone = _first_truthy(
        _prop(current_patient, "patient_phone"), _prop(item, "patient_phone"), _prop(bc, "patient_phone"),
        _prop(review_fields, "phone"), _prop(fact_patient, "phone"), _prop(prior, "patient_phone"),
        _prop(clinic_context, "patient_phone"), None,
    )
    patient_age = _normalize_age(
        _coalesce(
            _prop(current_patient, "patient_age"), _prop(item, "patient_age"), _prop(bc, "patient_age"),
            _prop(review_fields, "age"), _prop(fact_patient, "age"), _prop(prior, "patient_age"),
            _prop(clinic_context, "patient_age"),
        )
    )
    patient_address = _first_truthy(
        _prop(current_patient, "patient_address"), _prop(item, "patient_address"), _prop(bc, "patient_address"),
        _prop(review_fields, "address"), _prop(fact_patient, "address"), _prop(prior, "patient_address"),
        _prop(clinic_context, "patient_address"), None,
    )
    return {
        "clinic_id": clinic_id,
        "patient_id": patient_id,
        "conversation_id": conversation_id,
        "doctor_id": doctor_id,
        "service_id": service_id,
        "service_name": service_name,
        "appointment_type": appointment_type,
        "date": date,
        "time": time,
        "slot_id": slot_id,
        "branch_id": branch_id,
        "branch_name": branch_name,
        "patient_name": patient_name,
        "patient_phone": patient_phone,
        "patient_age": patient_age,
        "patient_address": patient_address,
        "operation": _first_truthy(_prop(sd, "operation"), _prop(sd_bc, "operation"), "create_appointment"),
        "channel_type": _first_truthy(_prop(normalize_inbound, "channel_type"), "whatsapp"),
    }


# ---------------------------------------------------------------------------
# Prepare Operation Finalize Input
# ---------------------------------------------------------------------------

_DEFINITELY_NOT_EXECUTED_CODES = {
    "SLOT_UNAVAILABLE",
    "CANCELLATION_NOT_ALLOWED",
    "APPOINTMENT_NOT_FOUND_OR_NOT_OWNED",
    "SAME_SLOT",
    "MISSING_REQUIRED_FIELDS",
    "CONFIDENCE_REVIEW_REQUIRED",
    # Reviewer-verified: these two no-mutation rejections finalized INCONCLUSIVE,
    # leaving the ledger unclaimable for the operation id ("تعذر التحقق..." forever).
    "RESCHEDULE_NOT_ALLOWED",
    "CANCEL_NOT_ALLOWED",
}
_RETRYABLE_CODES = {"CREATE_RETRYABLE", "CANCEL_RETRYABLE", "RESCHEDULE_RETRYABLE"}


def prepare_operation_finalize_input(item: Dict[str, Any], inputs: Dict[str, Any], execution_id: Optional[Any] = None) -> Dict[str, Any]:
    """Source node: Prepare Operation Finalize Input (extracted/code/Prepare_Operation_Finalize_Input.js).

    Args:
        item: current pipeline item ($input.item.json — the child execution row).
        inputs keys:
          normalize_validate <- 'Normalize & Validate' (clinic_id for finalize_clinic_id)
        execution_id: n8n $execution.id equivalent. PORT-TODO(n8n): the n8n
            runtime execution id is not available to a pure function; the runner
            may pass it, otherwise None keeps the JS null branch.
    Returns the item plus finalize_* keys (status, mutation_status,
    response_b64, child_execution_id, last_error_b64).
    """
    raw = item or {}
    code = _js_string(_first_truthy(_prop(raw, "response_code"), _prop(raw, "error_code"), "")).upper()
    contract_valid = _prop(raw, "child_contract_checked") is True and _prop(raw, "child_contract_valid") is True
    success = _prop(raw, "success") is True
    definitely_not_executed = code in _DEFINITELY_NOT_EXECUTED_CODES
    retryable_code = code in _RETRYABLE_CODES
    explicit_not_executed = _js_string(
        _first_truthy(_prop(raw, "mutation_status"), _prop(raw, "operation_mutation_status"), "")
    ).upper() == "NOT_EXECUTED"
    retryable_not_executed = retryable_code or (_prop(raw, "retryable") is True and explicit_not_executed)
    ledger_status = "INCONCLUSIVE"
    mutation_status = "UNKNOWN"
    if contract_valid and success:
        ledger_status = "COMPLETED"
        mutation_status = "EXECUTED"
    elif contract_valid and definitely_not_executed:
        ledger_status = "FAILED_FINAL"
        mutation_status = "NOT_EXECUTED"
    elif contract_valid and retryable_not_executed:
        # A retryable child code is an explicit proof that the provider mutation
        # was not executed; keep the ledger retryable and claimable.
        ledger_status = "IN_PROGRESS"
        mutation_status = "NOT_EXECUTED"
    response_json = {
        "response_code": _first_truthy(_prop(raw, "response_code"), code, "CHILD_CONTRACT_INVALID"),
        "success": success,
        "retryable": _prop(raw, "retryable") is True,
        "final_reply": _first_truthy(_prop(raw, "final_reply"), _prop(raw, "message"), None),
        "appointment_id": _first_truthy(_prop(raw, "appointment_id"), _prop(raw, "id"), None),
        "booking_id": _first_truthy(_prop(raw, "booking_id"), None),
        "booking_number": _first_truthy(_prop(raw, "booking_number"), None),
        "branch_id": _first_truthy(_prop(raw, "branch_id"), None),
        "queue_number": _coalesce(_prop(raw, "queue_number"), None),
        "queue_path": _first_truthy(_prop(raw, "queue_path"), None),
        "queue_expires_at": _first_truthy(_prop(raw, "queue_expires_at"), None),
        "operation_id": _first_truthy(_prop(raw, "operation_id"), None),
        "child_contract_checked": _prop(raw, "child_contract_checked") is True,
        "child_contract_valid": _prop(raw, "child_contract_valid") is True,
        "contract_error": _first_truthy(_prop(raw, "contract_error"), None),
    }
    response_b64 = base64.b64encode(
        json.dumps(response_json, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    execution_id_value = execution_id if _js_truthy(execution_id) else None
    normalize_item = _first_truthy(inputs.get("normalize_validate", _UNDEFINED), {})
    detail = _first_truthy(
        _prop(raw, "last_error"), _prop(raw, "error_message"), _prop(raw, "error"),
        _prop(raw, "contract_error"), _prop(raw, "error_code"), None,
    )
    if _js_truthy(detail):
        last_error_payload = {
            "error_code": _first_truthy(_prop(raw, "error_code"), _prop(raw, "response_code"), None),
            "message": _js_string(detail),
        }
        last_error_b64 = base64.b64encode(
            json.dumps(last_error_payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).decode("ascii")
    else:
        last_error_b64 = ""
    return {
        **raw,
        "finalize_clinic_id": _first_truthy(_prop(normalize_item, "clinic_id"), None),
        "finalize_operation_id": _first_truthy(_prop(raw, "operation_id"), None),
        "finalize_status": ledger_status,
        "finalize_mutation_status": mutation_status,
        "finalize_response_b64": response_b64,
        "finalize_child_execution_id": _first_truthy(_prop(raw, "child_execution_id"), execution_id_value, None),
        "finalize_last_error_b64": last_error_b64,
    }


# ---------------------------------------------------------------------------
# Merge Operation Completion
# ---------------------------------------------------------------------------


def merge_operation_completion(validate_child_envelope_item: Optional[Dict[str, Any]], item: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Merge Operation Completion (extracted/code/Merge_Operation_Completion.js).

    Args:
        validate_child_envelope_item: 'Validate Child Envelope' first().json.
            Pass None only when that node did not run (mirrors the JS catch that
            falls back to $json).
        item: current pipeline item ($input.item.json — the completion row).
    Returns the merged envelope (operation ids/statuses, finalize response,
    operation_finalized, child_execution_id).
    """
    original = (
        validate_child_envelope_item
        if validate_child_envelope_item is not None
        else (item or {})
    )
    completion = item or {}
    return {
        **original,
        "operation_id": _first_truthy(_prop(completion, "operation_id"), _prop(original, "operation_id"), None),
        "operation_ledger_status": _first_truthy(_prop(completion, "operation_status"), _prop(original, "operation_ledger_status"), None),
        "operation_mutation_status": _first_truthy(_prop(completion, "mutation_status"), _prop(original, "operation_mutation_status"), None),
        "operation_finalize_response": _first_truthy(_prop(completion, "response_json"), None),
        "operation_finalized": bool(_js_truthy(_prop(completion, "operation_id"))),
        "child_execution_id": _first_truthy(_prop(completion, "child_execution_id"), _prop(original, "child_execution_id"), None),
    }


# ---------------------------------------------------------------------------
# Apply Resolved Booking IDs (Deterministic)
# ---------------------------------------------------------------------------


def apply_resolved_booking_ids_deterministic(item: Dict[str, Any], inputs: Dict[str, Any], now_ms: Optional[float] = None) -> Dict[str, Any]:
    """Source node: Apply Resolved Booking IDs (Deterministic) (extracted/code/Apply_Resolved_Booking_IDs_Deterministic.js).

    Args:
        item: current pipeline item ($json — the resolver output row).
        inputs keys:
          validate_repaired_contract <- 'Validate Repaired Contract (Deterministic)';
              used only when its _contract_status === 'VALID'
          normalize_agent_output     <- 'Normalize Agent Output (Deterministic)' (fallback base)
          conversation_state         <- 'Get Conversation State' (offer/target liveness guard)
        now_ms: JS Date.now() equivalent; defaults to the current wall clock.
    Returns the normalized item with resolver-merged contract entities,
    contract_v3 mirror, booking_context and slot_state; the unchanged normalized
    item when the operation should not apply.
    """
    inputs = inputs or {}
    vrc = inputs.get("validate_repaired_contract")
    nao = _first_truthy(inputs.get("normalize_agent_output", _UNDEFINED), {})
    normalized = (
        vrc
        if (vrc is not None and _js_truthy(vrc) and _prop(vrc, "_contract_status") == "VALID")
        else (nao or {})
    )
    resolved = item if _js_truthy(item) and isinstance(item, dict) else {}
    contract = _obj_or_empty(_prop(normalized, "contract"))
    entities = _obj_or_empty(_prop(contract, "entities"))
    operation_type = _js_string(
        _first_truthy(_dig(contract, "operation_proposal", "type"), _dig(normalized, "_normalization", "turn_intent"), "")
    ).lower()
    should_apply = operation_type in ("create_appointment", "cancel_appointment", "reschedule_appointment", "check_availability") or _js_string(
        _first_truthy(_dig(normalized, "_normalization", "turn_intent"), "")
    ).lower() in ("cancellation_request", "reschedule_request", "availability_inquiry")
    normalized_scope = _obj_or_empty(_prop(normalized, "_normalization"))
    current_turn_date = _first_truthy(
        _dig(normalized_scope, "query_scope", "date"),
        _dig(normalized_scope, "raw_temporal_input", "date"),
        _prop(entities, "date"),
        None,
    )
    current_turn_time = _first_truthy(
        _dig(normalized_scope, "query_scope", "time"),
        _dig(normalized_scope, "raw_temporal_input", "time"),
        _prop(entities, "time"),
        None,
    )
    current_turn_has_window_evidence = bool(
        _prop(normalized_scope, "current_message_temporal") is True
        or _js_truthy(_dig(normalized_scope, "query_scope", "date"))
        or _js_truthy(_dig(normalized_scope, "query_scope", "time"))
        or _js_truthy(_prop(entities, "date"))
        or _js_truthy(_prop(entities, "time"))
    )
    fresh_booking_turn = operation_type == "create_appointment" and current_turn_has_window_evidence
    fresh_scheduling_turn = operation_type in ("create_appointment", "check_availability", "reschedule_appointment") and current_turn_has_window_evidence
    # A slot returned by a read-only availability lookup is only a candidate.
    availability_only = bool(
        _dig(normalized, "_normalization", "query_is_availability") is True
        or _dig(normalized, "_normalization", "query_scope", "type") == "availability"
        or _dig(contract, "query", "type") == "availability"
        or _dig(contract, "next_step", "type") == "show_availability"
        or _dig(normalized, "_normalization", "next_step_type") == "show_availability"
    )
    # P-SLOT-GUARD v40: prior-state slots may only survive when this conversation
    # still has an open offer or a pending confirmation.
    state_row_guard = _first_truthy(inputs.get("conversation_state", _UNDEFINED), {})
    guard_state = _prop(state_row_guard, "state_data")
    guard_state = guard_state if isinstance(guard_state, (dict, list)) else {}
    guard_offer = _first_truthy(_prop(guard_state, "presented_offer"), _prop(guard_state, "pending_offer"), None)
    now = now_ms if now_ms is not None else _now_ms()
    offer_exp = _date_parse(_js_string(_first_truthy(_prop(guard_offer, "expires_at"), ""))) if _js_truthy(guard_offer) else _NAN
    live_offer = bool(
        _js_truthy(guard_offer)
        and isinstance(guard_offer, dict)
        and _is_finite_num(offer_exp)
        and offer_exp > now
    )
    guard_target = _prop(guard_state, "confirmation_target") if _js_truthy(guard_state) else None
    guard_target_exp = _date_parse(_js_string(_first_truthy(_prop(guard_target, "expires_at"), ""))) if _js_truthy(guard_target) else _NAN
    live_target = bool(
        _js_truthy(guard_target)
        and isinstance(guard_target, dict)
        and ((not _is_finite_num(guard_target_exp)) or guard_target_exp > now)
    )
    may_carry_slot = live_offer or live_target
    if not should_apply:
        return dict(normalized)
    resolved_doctor_id = _first_truthy(_prop(resolved, "doctor_id"), None)
    resolved_doctor_name = _first_truthy(_prop(resolved, "doctor_name"), None)
    resolved_service_id = _first_truthy(_prop(resolved, "service_id"), None)
    resolved_service_name = _first_truthy(_prop(resolved, "service_name"), None)
    merged_entities = {
        **entities,
        "doctor_id": _first_truthy(resolved_doctor_id, _prop(entities, "doctor_id"), None),
        "doctor_name": _first_truthy(resolved_doctor_name, _prop(entities, "doctor_name"), None),
        "service_id": _first_truthy(resolved_service_id, _prop(entities, "service_id"), None),
        "service_name": _first_truthy(resolved_service_name, _prop(entities, "service_name"), None),
        "appointment_type": _first_truthy(_prop(entities, "appointment_type"), None),
        "appointment_id": _first_truthy(_prop(entities, "appointment_id"), _prop(resolved, "appointment_id"), None),
        "booking_number": _first_truthy(_prop(entities, "booking_number"), _prop(resolved, "booking_number"), None),
        "expected_old_slot_id": _first_truthy(_prop(entities, "expected_old_slot_id"), _prop(resolved, "expected_old_slot_id"), None),
        "new_slot_id": _first_truthy(_prop(entities, "new_slot_id"), _prop(resolved, "new_slot_id"), None),
        "slot_id": (
            None
            if availability_only
            else _first_truthy(_prop(entities, "slot_id"), _prop(resolved, "slot_id"), _prop(resolved, "new_slot_id"), None)
        ),
        "branch_id": _first_truthy(_prop(entities, "branch_id"), _prop(resolved, "branch_id"), None),
    }
    prior_booking = _prop(normalized, "booking_context")
    prior_booking = prior_booking if isinstance(prior_booking, (dict, list)) else {}
    slot_state_in = _prop(normalized, "slot_state")
    slot_state_in = slot_state_in if isinstance(slot_state_in, (dict, list)) else {}
    resolved_slot_start = _prop(resolved, "resolved_slot_start_time")

    def _start_date_part() -> Optional[str]:
        # Reviewer-verified: the resolver row carries a raw asyncpg datetime for the
        # timestamptz — _js_string(datetime) produced "[object Object]" and the date/
        # time halves in booking_context became garbage. Coerce to ISO first.
        if not _js_truthy(resolved_slot_start):
            return None
        value = resolved_slot_start.isoformat() if hasattr(resolved_slot_start, "isoformat") else _js_string(resolved_slot_start)
        return value[:10]

    def _start_time_part() -> Optional[str]:
        if not _js_truthy(resolved_slot_start):
            return None
        value = resolved_slot_start.isoformat() if hasattr(resolved_slot_start, "isoformat") else _js_string(resolved_slot_start)
        return value[11:19]

    # Resolver output is the tenant-scoped database authority for IDs and labels;
    # keep doctor/service pairs together so an old state label cannot survive
    # with a different database ID. Reviewer-verified split: when the resolver
    # names NO doctor for this turn (0 or 2+ matches) while the patient's text
    # named one, the carried-over prior doctor_id must drop WITH the name —
    # otherwise the booking confirms with the doctor the patient replaced.
    entities_name_this_turn = _text_value(_prop(entities, "doctor_name") or "").strip()
    prior_name_text = _text_value(_prop(prior_booking, "doctor_name") or "").strip()
    resolver_named_doctor = _js_truthy(resolved_doctor_id) or _js_truthy(resolved_doctor_name)
    same_doctor = bool(
        prior_name_text
        and (entities_name_this_turn in prior_name_text or prior_name_text in entities_name_this_turn)
    )
    drop_prior_doctor = bool(entities_name_this_turn and not resolver_named_doctor and not same_doctor)
    booking_context = {
        **prior_booking,
        "doctor_id": None if drop_prior_doctor else _first_truthy(resolved_doctor_id, _prop(prior_booking, "doctor_id"), None),
        "doctor_name": None if drop_prior_doctor else _first_truthy(resolved_doctor_name, _prop(prior_booking, "doctor_name"), None),
        "service_id": _first_truthy(resolved_service_id, _prop(prior_booking, "service_id"), None),
        "service_name": _first_truthy(resolved_service_name, _prop(prior_booking, "service_name"), None),
        "appointment_type": _first_truthy(_prop(prior_booking, "appointment_type"), _prop(entities, "appointment_type"), None),
        "appointment_id": _first_truthy(_prop(prior_booking, "appointment_id"), _prop(resolved, "appointment_id"), None),
        "booking_number": _first_truthy(_prop(prior_booking, "booking_number"), _prop(resolved, "booking_number"), None),
        "expected_old_slot_id": _first_truthy(_prop(prior_booking, "expected_old_slot_id"), _prop(resolved, "expected_old_slot_id"), None),
        "new_slot_id": (
            None
            if availability_only
            else (
                _first_truthy(_prop(resolved, "new_slot_id"), None)
                if fresh_booking_turn
                else (
                    _first_truthy(_prop(prior_booking, "new_slot_id"), _prop(resolved, "new_slot_id"), None)
                    if may_carry_slot
                    else None
                )
            )
        ),
        "slot_id": (
            None
            if availability_only
            else (
                _first_truthy(_prop(resolved, "slot_id"), _prop(resolved, "new_slot_id"), None)
                if fresh_booking_turn
                else (
                    _first_truthy(_prop(prior_booking, "slot_id"), _prop(resolved, "slot_id"), _prop(resolved, "new_slot_id"), None)
                    if may_carry_slot
                    else None
                )
            )
        ),
        "branch_id": _first_truthy(_prop(prior_booking, "branch_id"), _prop(resolved, "branch_id"), None),
        "date": (
            (_first_truthy(current_turn_date, None))
            if availability_only
            else (
                _first_truthy(current_turn_date, _start_date_part())
                if fresh_scheduling_turn
                else (
                    _first_truthy(_prop(prior_booking, "date"), _start_date_part())
                    if may_carry_slot
                    else _first_truthy(current_turn_date, None)
                )
            )
        ),
        "time": (
            (_first_truthy(current_turn_time, None))
            if availability_only
            else (
                _first_truthy(current_turn_time, _start_time_part())
                if fresh_scheduling_turn
                else (
                    _first_truthy(_prop(prior_booking, "time"), _start_time_part())
                    if may_carry_slot
                    else _first_truthy(current_turn_time, None)
                )
            )
        ),
    }
    contract_v3_in = _prop(normalized, "contract_v3")
    if isinstance(contract_v3_in, dict):
        v3_entities = _obj_or_empty(_prop(contract_v3_in, "entities"))
        contract_v3_out = {
            **contract_v3_in,
            "entities": {
                **v3_entities,
                "appointment_id": _coalesce(_prop(merged_entities, "appointment_id"), _prop(v3_entities, "appointment_id"), None),
                "booking_number": _coalesce(_prop(merged_entities, "booking_number"), _prop(v3_entities, "booking_number"), None),
                "expected_old_slot_id": _coalesce(_prop(merged_entities, "expected_old_slot_id"), _prop(v3_entities, "expected_old_slot_id"), None),
                "new_slot_id": (
                    None
                    if (availability_only or not may_carry_slot)
                    else _coalesce(_prop(merged_entities, "new_slot_id"), _prop(v3_entities, "new_slot_id"), None)
                ),
                "slot_id": (
                    None
                    if (availability_only or not may_carry_slot)
                    else _coalesce(_prop(merged_entities, "slot_id"), _prop(v3_entities, "slot_id"), None)
                ),
                "doctor_id": _coalesce(_prop(merged_entities, "doctor_id"), _prop(v3_entities, "doctor_id"), None),
                "doctor_name": _coalesce(_prop(merged_entities, "doctor_name"), _prop(v3_entities, "doctor_name"), None),
                "service_id": _coalesce(_prop(merged_entities, "service_id"), _prop(v3_entities, "service_id"), None),
                "service_name": _coalesce(_prop(merged_entities, "service_name"), _prop(v3_entities, "service_name"), None),
                "branch_id": _coalesce(_prop(merged_entities, "branch_id"), _prop(v3_entities, "branch_id"), None),
            },
        }
    else:
        contract_v3_out = contract_v3_in
    slot_state_out = {
        **slot_state_in,
        **merged_entities,
        "doctor_id": _first_truthy(_prop(booking_context, "doctor_id"), _prop(slot_state_in, "doctor_id"), None),
        "doctor_name": _first_truthy(_prop(booking_context, "doctor_name"), _prop(slot_state_in, "doctor_name"), None),
        "service_id": _first_truthy(_prop(booking_context, "service_id"), _prop(slot_state_in, "service_id"), None),
        "service_name": _first_truthy(_prop(booking_context, "service_name"), _prop(slot_state_in, "service_name"), None),
        "appointment_type": _first_truthy(_prop(booking_context, "appointment_type"), _prop(slot_state_in, "appointment_type"), None),
        # Availability candidates must not leak into the persistent slot selection state.
        "date": (_first_truthy(current_turn_date, None) if availability_only else _first_truthy(_prop(booking_context, "date"), _prop(slot_state_in, "date"), None)),
        "time": (_first_truthy(current_turn_time, None) if availability_only else _first_truthy(_prop(booking_context, "time"), _prop(slot_state_in, "time"), None)),
        "slot_id": (None if availability_only else _first_truthy(_prop(booking_context, "slot_id"), _prop(slot_state_in, "slot_id"), None)),
        "patient_name": _first_truthy(_prop(booking_context, "patient_name"), _prop(slot_state_in, "patient_name"), None),
        "patient_phone": _first_truthy(_prop(booking_context, "patient_phone"), _prop(slot_state_in, "patient_phone"), None),
        "patient_age": _coalesce(_prop(booking_context, "patient_age"), _prop(slot_state_in, "patient_age"), None),
        "patient_address": _first_truthy(_prop(booking_context, "patient_address"), _prop(slot_state_in, "patient_address"), None),
    }
    return {
        **normalized,
        "contract": {**contract, "entities": merged_entities},
        # BUGFIX (2026-09-09): the orchestrator reads contract_v3.entities, which
        # previously kept the pre-resolver values; mirror the resolver results there too.
        "contract_v3": contract_v3_out,
        "booking_context": booking_context,
        # Preserve the canonical window and patient fields for downstream readiness and state persistence.
        "slot_state": slot_state_out,
        "appointment_id": _first_truthy(_prop(resolved, "appointment_id"), _prop(normalized, "appointment_id"), None),
        "booking_number": _first_truthy(
            _prop(resolved, "booking_number"),
            _prop(normalized, "booking_number"),
            _dig(normalized, "booking_context", "booking_number"),
            None,
        ),
        "expected_old_slot_id": _first_truthy(_prop(resolved, "expected_old_slot_id"), _prop(normalized, "expected_old_slot_id"), None),
        "new_slot_id": (
            None
            if availability_only
            else _first_truthy(_prop(resolved, "new_slot_id"), _prop(normalized, "new_slot_id"), None)
        ),
        "branch_id": _first_truthy(_prop(resolved, "branch_id"), _prop(normalized, "branch_id"), None),
        "resolver_result": resolved,
        "resolver_contract_version": 2,
    }


# ---------------------------------------------------------------------------
# Prepare Handoff Input
# ---------------------------------------------------------------------------

_RE_PATIENT_HUMAN = re.compile(r"(?:موظف|بشري|إنسان|انسان|حد من الاستقبال|كلموني|عايز اكلم حد|human|agent|reception|customer service)", re.IGNORECASE)
_RE_COMPLAINT = re.compile(r"(?:شكوى|اشتك|مش مبسوط|مش راضي|غير راض|complaint|not happy|unhappy)", re.IGNORECASE)
_RE_PAYMENT = re.compile(r"(?:دفع|فاتورة|سداد|payment|invoice)", re.IGNORECASE)
_RE_SYSTEM = re.compile(r"(?:خطأ تقني|مشكلة تقنية|النظام|تعطل|مش شغال|system|technical|error|not working)", re.IGNORECASE)
_RE_APPOINTMENT = re.compile(r"(?:موعد|حجز|تعديل|إلغاء|الغاء|appointment|booking|cancel|reschedul)", re.IGNORECASE)
_RE_URGENT = re.compile(r"(?:طارئ|عاجل|عاجل جداً|ضروري|urgent|emergency)", re.IGNORECASE)


def _js_char_code_units(s: str) -> List[int]:
    """JS charCodeAt iteration: UTF-16 code units (surrogate pairs for astral chars)."""
    codes: List[int] = []
    for ch in s:
        cp = ord(ch)
        if cp > 0xFFFF:
            cp -= 0x10000
            codes.append(0xD800 + (cp >> 10))
            codes.append(0xDC00 + (cp & 0x3FF))
        else:
            codes.append(cp)
    return codes


def _to_deterministic_uuid(value: Any) -> str:
    raw = _js_string(_first_truthy(value, "")).strip()
    if re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", raw, re.IGNORECASE):
        return raw.lower()
    source = raw or "agents-k2-handoff"

    def fnv(seed: int) -> str:
        h = seed & 0xFFFFFFFF
        for code in _js_char_code_units(source):
            h ^= code
            h = _imul(h, 16777619)
        return format(h & 0xFFFFFFFF, "08x")

    hex_str = "".join(fnv(seed) for seed in (0x811C9DC5, 0x9E3779B9, 0x85EBCA6B, 0xC2B2AE35))
    hex_str = hex_str[:12] + "5" + hex_str[13:16] + "a" + hex_str[17:]
    return hex_str[0:8] + "-" + hex_str[8:12] + "-" + hex_str[12:16] + "-" + hex_str[16:20] + "-" + hex_str[20:32]


def prepare_handoff_input(item: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Prepare Handoff Input (extracted/code/Prepare_Handoff_Input.js).

    Args:
        item: current pipeline item ($json) — the parent payload.
        inputs keys:
          system_orchestrator <- 'System Orchestrator (Policy)' (system_decision, proposal)
          normalize_validate  <- 'Normalize & Validate'
          conversation_state  <- 'Get Conversation State' (state_data)
    Returns the parent payload plus the handoff_input envelope (reason_code /
    priority inference from the Arabic+English keyword ladders, deterministic
    correlation/channel UUIDs, context_snapshot, metadata).
    """
    parent_payload = dict(item or {})
    inputs = inputs or {}
    output = _first_truthy(_prop(parent_payload, "output"), {})
    orchestrator = _first_truthy(inputs.get("system_orchestrator", _UNDEFINED), {})
    decision = _first_truthy(_prop(orchestrator, "system_decision"), {})
    proposal = _first_truthy(_prop(orchestrator, "proposal"), {})
    ctx = _first_truthy(inputs.get("normalize_validate", _UNDEFINED), {})
    prior_node = _first_truthy(inputs.get("conversation_state", _UNDEFINED), {})
    prior = _first_truthy(_prop(prior_node, "state_data"), {})
    text = _js_string(_first_truthy(_prop(ctx, "message_text"), "")).strip()
    correlation_source = _first_truthy(
        _prop(ctx, "idempotency_key"),
        _prop(ctx, "message_id"),
        _js_string(_first_truthy(_prop(ctx, "conversation_id"), "")) + ":" + text,
    )
    correlation_id = _to_deterministic_uuid(correlation_source)
    raw_channel_id = _js_string(_first_truthy(_prop(ctx, "channel_id"), "")).strip()
    normalized_channel_id = _to_deterministic_uuid(
        _js_string(_first_truthy(_prop(ctx, "channel_type"), "unknown")) + ":" + (raw_channel_id or "unknown")
    )
    raw_reason = _js_string(
        _first_truthy(
            _prop(output, "handoff_reason"),
            _prop(output, "escalation_reason"),
            _prop(proposal, "handoff_reason"),
            _prop(proposal, "escalation_reason"),
            "",
        )
    ).strip()
    reason_text = " ".join(part for part in (raw_reason, text) if _js_truthy(part)).lower()

    def has(pattern: "re.Pattern[str]") -> bool:
        return pattern.search(reason_text) is not None

    reason_code = "AI_UNABLE_TO_HELP"
    if has(_RE_PATIENT_HUMAN):
        reason_code = "PATIENT_REQUESTED_HUMAN"
    elif has(_RE_COMPLAINT):
        reason_code = "COMPLAINT"
    elif has(_RE_PAYMENT):
        reason_code = "PAYMENT_ISSUE"
    elif has(_RE_SYSTEM):
        reason_code = "SYSTEM_EXCEPTION"
    elif has(_RE_APPOINTMENT):
        reason_code = "APPOINTMENT_EXCEPTION"
    elif _js_number(_coalesce(_prop(output, "confidence"), _prop(proposal, "confidence"), 1)) < _js_number(
        _coalesce(_prop(decision, "confidence_threshold"), 0.75)
    ):
        reason_code = "LOW_CONFIDENCE"
    priority = "NORMAL"
    if has(_RE_URGENT):
        priority = "URGENT"
    elif reason_code in ("COMPLAINT", "PAYMENT_ISSUE", "APPOINTMENT_EXCEPTION"):
        priority = "HIGH"
    booking = _first_truthy(
        _prop(output, "booking_context"), _prop(output, "slot_state"),
        _prop(prior, "booking_context"), _prop(prior, "slot_state"), {},
    )
    prior_turns = _prop(prior, "recent_turns")
    context_snapshot = {
        "message_text": text,
        "latest_reply": _first_truthy(
            _prop(output, "final_reply"), _prop(output, "reply_text"), _prop(decision, "final_reply"), ""
        ),
        "conversation_summary": _first_truthy(_prop(prior, "conversation_summary"), ""),
        "recent_turns": prior_turns[-4:] if isinstance(prior_turns, list) else [],
        "booking_context": booking,
        "appointment_id": _first_truthy(_prop(output, "appointment_id"), _prop(prior, "appointment_id"), None),
    }
    metadata = {
        "source": "agents_k2",
        "response_code": _first_truthy(_prop(output, "response_code"), _prop(decision, "response_code"), "HANDOFF_REQUIRED"),
        "intent": _first_truthy(_prop(output, "intent"), _prop(proposal, "intent"), None),
        "proposed_action": _first_truthy(_prop(output, "proposed_action"), _prop(proposal, "proposed_action"), None),
        "confidence": _coalesce(_prop(output, "confidence"), _prop(proposal, "confidence"), None),
        "reason_inferred": not _js_truthy(raw_reason),
        "source_idempotency_key": _first_truthy(_prop(ctx, "idempotency_key"), None),
        "raw_channel_id": raw_channel_id or None,
        "normalized_channel_id": normalized_channel_id,
        "workflow_version": "handoff-link-v2",
    }
    return {
        **parent_payload,
        "handoff_input": {
            "clinic_id": _first_truthy(_prop(ctx, "clinic_id"), ""),
            "conversation_id": _first_truthy(_prop(ctx, "conversation_id"), ""),
            "patient_id": _first_truthy(_prop(ctx, "patient_id"), ""),
            "channel_type": _first_truthy(_prop(ctx, "channel_type"), ""),
            "channel_id": normalized_channel_id,
            "handoff_reason": raw_reason or "agent_escalation",
            "reason_code": reason_code,
            "reason_note": raw_reason or None,
            "correlation_id": correlation_id,
            "source_message_id": _first_truthy(_prop(ctx, "message_id"), ""),
            "priority": priority,
            "context_snapshot": context_snapshot,
            "metadata": metadata,
        },
    }


# ---------------------------------------------------------------------------
# Validate Child Envelope
# ---------------------------------------------------------------------------

_ENVELOPE_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)

_ALLOWED_SUCCESS = {
    "create_appointment": {"CREATE_COMPLETED", "IDEMPOTENT_REPLAY"},
    "cancel_appointment": {"CANCEL_COMPLETED", "IDEMPOTENT_REPLAY"},
    "reschedule_appointment": {"RESCHEDULE_COMPLETED", "IDEMPOTENT_REPLAY"},
}
_ALLOWED_FAILURE = {
    "create_appointment": {
        "CONTRACT_INVALID", "PATIENT_NOT_FOUND", "PATIENT_DATA_REQUIRED", "DOCTOR_INVALID",
        "SERVICE_INVALID", "SLOT_INVALID", "MISSING_REQUIRED_FIELDS", "SLOT_UNAVAILABLE",
        "RPC_ERROR", "UNKNOWN_RESPONSE", "CREATE_APPOINTMENT_FAILED", "CREATE_RETRYABLE",
        "SLOT_ALREADY_BOOKED", "SLOT_INCOMPATIBLE", "CURRENT_SLOT_STATE_INVALID",
        "CURRENT_SLOT_MISSING", "SLOT_STALE", "OPERATION_IN_PROGRESS",
        "DAILY_BOOKING_SEQUENCE_EXHAUSTED", "CLINIC_TIMEZONE_NOT_CONFIGURED",
        "PROVIDER_SUCCESS_INVALID", "REPLAY_CONTRACT_INVALID",
    },
    "cancel_appointment": {
        "CONTRACT_INVALID", "MISSING_REQUIRED_FIELD", "PATIENT_NOT_FOUND",
        "APPOINTMENT_NOT_FOUND_OR_NOT_OWNED", "CANCELLATION_NOT_ALLOWED", "CANCEL_RETRYABLE",
        "CANCEL_NOT_ALLOWED", "CURRENT_APPOINTMENT_STATE_INVALID", "OPERATION_IN_PROGRESS",
        "RPC_ERROR", "UNKNOWN_RESPONSE", "PROVIDER_SUCCESS_INVALID", "REPLAY_CONTRACT_INVALID",
    },
    "reschedule_appointment": {
        "CONTRACT_INVALID", "MISSING_REQUIRED_FIELD", "INVALID_UUID", "INVALID_OPERATION_ID",
        "APPOINTMENT_NOT_FOUND_OR_NOT_OWNED", "RESCHEDULE_NOT_ALLOWED", "SLOT_UNAVAILABLE",
        "SAME_SLOT", "RESCHEDULE_CONFLICT", "RESCHEDULE_RETRYABLE", "CURRENT_SLOT_STATE_INVALID",
        "CURRENT_SLOT_MISSING", "SLOT_INCOMPATIBLE", "OPERATION_IN_PROGRESS", "RPC_ERROR",
        "MISSING_REQUIRED_FIELDS", "UNKNOWN_RESPONSE", "PROVIDER_SUCCESS_INVALID",
    },
}


def _text_value(value: Any) -> str:
    return "" if value is None or value is _UNDEFINED else _js_string(value).strip()


def validate_child_envelope(item: Dict[str, Any], inputs: Dict[str, Any], execution_id: Optional[Any] = None) -> Dict[str, Any]:
    """Source node: Validate Child Envelope (extracted/code/Validate_Child_Envelope.js).

    Args:
        item: current pipeline item ($input.item.json — the child response row).
        inputs keys:
          system_orchestrator  <- 'System Orchestrator (Policy)' (uses .system_decision)
          normalize_validate   <- 'Normalize & Validate'
          apply_operation_claim <- 'Apply Operation Claim (Deterministic)' (operation_id)
        execution_id: n8n $execution.id equivalent. PORT-TODO(n8n): runtime value;
            the runner may pass it, otherwise the JS empty-string branch is used.
    Returns the validated child response ({..., child_contract_checked: true,
    child_contract_valid: true}) or the CHILD_CONTRACT_INVALID envelope with the
    Arabic message 'تعذر التحقق من عقد نتيجة العملية قبل التنفيذ النهائي'.
    """
    raw = item or {}
    inputs = inputs or {}
    decision = {}
    try:
        decision = _first_truthy(_prop(inputs.get("system_orchestrator", _UNDEFINED), "system_decision"), {})
    except Exception:
        decision = {}
    context = {}
    try:
        context = _first_truthy(inputs.get("normalize_validate", _UNDEFINED), {})
    except Exception:
        context = {}

    schema_version = 1
    action = _text_value(_first_truthy(_prop(decision, "action"), _dig(decision, "confirmation_target", "action"))).lower()
    operation_by_action = {
        "create_appointment": "create_appointment",
        "cancel_appointment": "cancel_appointment",
        "reschedule_appointment": "reschedule_appointment",
    }
    expected_operation = operation_by_action.get(action, "")
    target = _prop(decision, "confirmation_target")
    target = target if isinstance(target, (dict, list)) else {}
    claimed_operation_id = ""
    try:
        claimed_node = inputs.get("apply_operation_claim")
        claimed_operation_id = _text_value(_prop(claimed_node, "operation_id")) if _js_truthy(claimed_node) else ""
    except Exception:
        claimed_operation_id = ""
    if action == "create_appointment":
        create_branch = _first_truthy(
            _prop(context, "operation_id"),
            (_js_string(_first_truthy(_prop(context, "idempotency_key"), "")) + ":create_appointment") if _js_truthy(_prop(context, "idempotency_key")) else "",
        )
    else:
        create_branch = ""
    if action == "cancel_appointment":
        cancel_branch = (_js_string(_first_truthy(_prop(context, "idempotency_key"), "")) + ":cancel_appointment") if _js_truthy(_prop(context, "idempotency_key")) else ""
    else:
        cancel_branch = ""
    expected_operation_id = _text_value(_first_truthy(claimed_operation_id, _prop(target, "operation_id"), create_branch, cancel_branch))
    expected_correlation_id = _text_value(_prop(context, "correlation_id")) or _text_value(claimed_operation_id)

    def invalid(reason: str) -> Dict[str, Any]:
        return {
            "schema_version": schema_version,
            "operation": expected_operation or None,
            "correlation_id": expected_correlation_id or None,
            "operation_id": expected_operation_id or None,
            "response_code": "CHILD_CONTRACT_INVALID",
            "success": False,
            "retryable": False,
            "appointment_id": None,
            "error_code": "CHILD_CONTRACT_INVALID",
            "operation_status": "failed_final",
            "child_contract_checked": True,
            "child_contract_valid": False,
            "contract_error": reason,
            "message": "تعذر التحقق من عقد نتيجة العملية قبل التنفيذ النهائي",
        }

    if not _js_truthy(expected_operation):
        return invalid("UNEXPECTED_OPERATION")
    response = _prop(raw, "child_response")
    response = response if isinstance(response, (dict, list)) else raw
    response_code = _text_value(_prop(response, "response_code")).upper()
    response_operation = _text_value(_prop(response, "operation")).lower()
    response_correlation = _text_value(_prop(response, "correlation_id"))
    response_operation_id = _text_value(_prop(response, "operation_id"))
    success = _prop(response, "success") is True
    retryable = _prop(response, "retryable")
    appointment_id = _text_value(_prop(response, "appointment_id"))

    # EXEC-SQL-ENVELOPE-PATCH: a direct Execute SQL output {id, public_id} maps to
    # the expected success envelope.
    sql_direct = _has_key(raw, "id") or (isinstance(raw, dict) and len(raw) <= 4 and ("id" in raw or "public_id" in raw))
    direct_create_success = bool(
        expected_operation == "create_appointment"
        and isinstance(raw, dict)
        and _prop(raw, "success") is True
        and _js_string(_first_truthy(_prop(raw, "response_code"), "")).upper() == "APPOINTMENT_CREATED"
        and _ENVELOPE_UUID_RE.match(_js_string(_first_truthy(_prop(raw, "appointment_id"), _prop(raw, "id"), "")))
        and bool(_js_truthy(_prop(raw, "booking_number")) or _js_truthy(_prop(raw, "public_id")))
    )
    if (sql_direct and isinstance(raw, dict) and len(raw) <= 4) or direct_create_success:
        direct_id = _js_string(_first_truthy(_prop(raw, "appointment_id"), _prop(raw, "id"), ""))
        if _ENVELOPE_UUID_RE.match(direct_id):
            direct_code_by_action = {
                "create_appointment": "CREATE_COMPLETED",
                "cancel_appointment": "CANCEL_COMPLETED",
                "reschedule_appointment": "RESCHEDULE_COMPLETED",
            }
            raw = {
                "schema_version": schema_version,
                "operation": expected_operation,
                "correlation_id": expected_correlation_id,
                "operation_id": expected_operation_id,
                "response_code": direct_code_by_action.get(expected_operation, "UNKNOWN_RESPONSE"),
                "success": True,
                "retryable": False,
                "appointment_id": direct_id,
                "booking_number": _first_truthy(_prop(raw, "booking_number"), _prop(raw, "public_id"), None),
                "mutation_status": _first_truthy(_prop(raw, "mutation_status"), "succeeded"),
                "child_execution_id": _js_string(execution_id) if _js_truthy(execution_id) else "",
            }
            response = raw
            response_code = _text_value(_prop(response, "response_code")).upper()
            response_operation = _text_value(_prop(response, "operation")).lower()
            response_correlation = _text_value(_prop(response, "correlation_id"))
            response_operation_id = _text_value(_prop(response, "operation_id"))
            success = _prop(response, "success") is True
            retryable = _prop(response, "retryable")
            appointment_id = _text_value(_prop(response, "appointment_id"))

    if _js_number(_prop(response, "schema_version")) != schema_version:
        return invalid("SCHEMA_VERSION_MISMATCH")
    if response_operation != expected_operation:
        return invalid("OPERATION_MISMATCH")
    if not _js_truthy(expected_correlation_id) or response_correlation != expected_correlation_id:
        return invalid("CORRELATION_ID_MISMATCH")
    if not _js_truthy(expected_operation_id) or response_operation_id != expected_operation_id:
        return invalid("OPERATION_ID_MISMATCH")
    if not isinstance(success, bool) or not isinstance(retryable, bool) or not _js_truthy(response_code):
        return invalid("MISSING_EXPLICIT_CONTRACT_FIELD")

    if success:
        if response_code not in _ALLOWED_SUCCESS.get(expected_operation, set()):
            return invalid("SUCCESS_CODE_NOT_ALLOWED")
        if not _ENVELOPE_UUID_RE.match(appointment_id):
            return invalid("SUCCESS_APPOINTMENT_ID_INVALID")
        if retryable is not False:
            return invalid("SUCCESS_RETRYABLE_INCONSISTENT")
    else:
        if response_code not in _ALLOWED_FAILURE.get(expected_operation, set()):
            return invalid("FAILURE_CODE_NOT_ALLOWED")
        if _js_truthy(appointment_id):
            return invalid("FAILURE_APPOINTMENT_ID_PRESENT")

    return {
        **response,
        "schema_version": schema_version,
        "operation": expected_operation,
        "child_contract_checked": True,
        "child_contract_valid": True,
    }


# ---------------------------------------------------------------------------
# Restore Handoff Context
# ---------------------------------------------------------------------------


def restore_handoff_context(item: Dict[str, Any], prepare_handoff_output: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Restore Handoff Context (extracted/code/Restore_Handoff_Context.js).

    Args:
        item: current pipeline item ($json — the handoff child workflow result).
        prepare_handoff_output: 'Prepare Handoff Input' first().json ({} when
            unavailable, mirroring the JS `|| {}`).
    Returns the parent payload plus handoff_result / handoff_created /
    handoff_request_id. Raises ValueError('K2_HANDOFF_CHILD_FAILED: <reason>')
    when the handoff was not created, matching the JS throw that halts the turn
    and fires the K2 Error Monitor (P-HANDOFF v40: failures must be loud).
    """
    parent_payload = prepare_handoff_output if _js_truthy(prepare_handoff_output) else {}
    handoff_result = item if _js_truthy(item) else {}
    result = _prop(handoff_result, "result")
    result = result if isinstance(result, (dict, list)) else handoff_result
    handoff_created_flag = (
        _prop(result, "success") is True or _prop(result, "created") is True or _prop(result, "reused") is True
    )
    if handoff_created_flag is not True:
        error_val = _prop(result, "error")
        error_branch = _first_truthy(_prop(error_val, "message"), error_val) if _js_truthy(error_val) else None
        inner = _first_truthy(_prop(result, "code"), error_branch) if _js_truthy(result) else None
        fail_reason = _js_string(_first_truthy(inner, "unknown_error"))
        raise ValueError("K2_HANDOFF_CHILD_FAILED: " + fail_reason)
    return {
        **parent_payload,
        "handoff_result": result,
        "handoff_created": handoff_created_flag,
        "handoff_request_id": _first_truthy(
            _prop(result, "handoff_request_id"), _prop(result, "request_id"), _prop(result, "id"), None
        ),
    }
