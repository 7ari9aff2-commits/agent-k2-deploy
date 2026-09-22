"""Faithful port of the K2 LLM-safety chain (n8n code nodes -> pure Python).

Source nodes (extracted/code/):
  R3_LLM_Response_Safety.js        -> r3_llm_response_safety
  R2_LLM_Error_Detection.js        -> r2_llm_error_detection
  R1_Agent_Reply_Recovery.js       -> r1_agent_reply_recovery
  Build_Repair_Prompt_Deterministic.js -> build_repair_prompt_deterministic
  Validate_Repaired_Contract_Deterministic.js -> validate_repaired_contract_deterministic

Public flow (same order as the n8n main path):
  1. r3_llm_response_safety(item)   - sanitize the raw model output in place
      (strips DeepSeek <think>/<reasoning> blocks and markdown fences; sets
      data.output = data.text = sanitized text and p17_llm_safety metadata).
  2. r2_llm_error_detection(item)   - classify the sanitized output:
      explicit_error | implicit_error | empty_output | no_error; sets
      p17_llm_error and p17_recovery_needed.
  3. r1_agent_reply_recovery(item)  - empty / <5 chars / unterminated-JSON
      outputs are NOT replaced by any canned text; they are flagged
      p17_empty_output_deferred + p17_recovery_applied so the one-shot
      self-repair chain takes over (v2 behavior, see source header comment).
  4. The agent-contract parser (Normalize Agent Output, app/core/agent_output.py)
      runs on the sanitized output. When the contract is structurally invalid:
      build_repair_prompt_deterministic(inputs) builds the single repair prompt,
      the repair model (DeepSeek Repair Chain) produces new raw output, and
      validate_repaired_contract_deterministic(item, inputs) parses/validates it
      (the same validator also parses the primary output on the normal path and
      emits _contract_repair_needed / _contract_status used by
      IF Contract Needs Repair and downstream stages).
  No DB access, no I/O; stdlib only; Arabic strings kept byte-identical to the JS.
"""
from __future__ import annotations

import json
import math
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from app.core.js_semantics import obj_or_empty as _obj_or_empty

# ---------------------------------------------------------------------------
# JS-semantics shims (private to this module)
# ---------------------------------------------------------------------------

_UNDEFINED = object()  # JS `undefined`
_NAN = float("nan")


def _js_truthy(value: Any) -> bool:
    """JS truthiness: 0/NaN/''/null/undefined falsy; {} and [] are TRUTHY."""
    if value is None or value is _UNDEFINED:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return not (value == 0 or (isinstance(value, float) and value != value))
    if isinstance(value, str):
        return value != ""
    return True  # dict, list, object


def _js_string(value: Any) -> str:
    """JS String() coercion."""
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
        # JS Array.prototype.toString: elements joined with ',', nullish -> ''
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
    """JS Number() coercion (returns NaN where JS would produce NaN)."""
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
        if s == "Infinity" or s == "+Infinity":
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


def _is_finite_number(value: Any) -> bool:
    """JS Number.isFinite."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _coalesce(*values: Any) -> Any:
    """JS `a ?? b ?? c` — first value that is neither null nor undefined."""
    for v in values:
        if v is not None and v is not _UNDEFINED:
            return v
    return values[-1] if values else None


def _first_truthy(*values: Any) -> Any:
    """JS `a || b || c` — first JS-truthy value, else the last value."""
    for v in values[:-1]:
        if _js_truthy(v):
            return v
    return values[-1] if values else None


def _prop(obj: Any, key: str) -> Any:
    """JS property access returning undefined (None) for non-objects/missing keys."""
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


def _imul(a: int, b: int) -> int:
    """JS Math.imul (32-bit integer multiply) as an unsigned bit pattern."""
    return ((a & 0xFFFFFFFF) * (b & 0xFFFFFFFF)) & 0xFFFFFFFF


def _now_ms() -> float:
    """JS Date.now()."""
    return datetime.now(timezone.utc).timestamp() * 1000.0


def _utc_now_iso() -> str:
    """JS new Date().toISOString() — millisecond precision, trailing Z."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _date_parse(value: Any) -> float:
    """JS Date.parse — ISO strings only in this codebase; NaN when invalid.

    - 'YYYY-MM-DD'            -> UTC midnight (JS date-only ISO form)
    - '...T..Z' / '..+03:00'  -> respected offset
    - date-time w/o offset    -> JS treats it as LOCAL time (mirrored via astimezone)
    """
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


def _keep(value: Any, old: Any) -> Any:
    """JS: v !== undefined && v !== null && String(v) !== '' ? v : (old ?? null)."""
    if value is not None and value is not _UNDEFINED and _js_string(value) != "":
        return value
    return _coalesce(old, None)


# ---------------------------------------------------------------------------
# R3 — LLM response safety (sanitize raw model output)
# ---------------------------------------------------------------------------

_THINK_RE = re.compile(r"<think>[\s\S]*?</think>", re.IGNORECASE)
_REASONING_RE = re.compile(r"<reasoning>[\s\S]*?</reasoning>", re.IGNORECASE)
_FENCE_JSON_RE = re.compile(r"```json\s*", re.IGNORECASE)
_FENCE_RE = re.compile(r"```\s*", re.IGNORECASE)
_LEAD_WS_RE = re.compile(r"^[\s\n]+")
_TRAIL_WS_RE = re.compile(r"[\s\n]+$")


def r3_llm_response_safety(item: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: R3 LLM Response Safety (extracted/code/R3_LLM_Response_Safety.js).

    Input item keys read: output (string or {text|output} object), text (string).
    Output item keys added/overwritten: output, text, p17_llm_safety
    (original_length, sanitized_length, had_reasoning, stripped_chars).
    """
    data = dict(item or {})
    out_v = data.get("output", _UNDEFINED)
    if isinstance(out_v, str):
        raw = out_v
    elif _js_truthy(out_v) and isinstance(out_v, (dict, list)):
        raw = _js_string(_first_truthy(_prop(out_v, "text"), _prop(out_v, "output"), json.dumps(out_v, ensure_ascii=False, separators=(",", ":"))))
    elif isinstance(data.get("text", _UNDEFINED), str):
        raw = data["text"]
    else:
        raw = ""

    original = raw
    had_reasoning = bool(_THINK_RE.search(raw)) or bool(_REASONING_RE.search(raw))
    raw = _THINK_RE.sub("", raw)
    raw = _REASONING_RE.sub("", raw)
    raw = _FENCE_JSON_RE.sub("", raw)
    raw = _FENCE_RE.sub("", raw)
    raw = _LEAD_WS_RE.sub("", raw)
    raw = _TRAIL_WS_RE.sub("", raw)

    data["output"] = raw
    data["text"] = raw
    data["p17_llm_safety"] = {
        "original_length": len(original),
        "sanitized_length": len(raw),
        "had_reasoning": had_reasoning,
        "stripped_chars": len(original) - len(raw),
    }
    return data


# ---------------------------------------------------------------------------
# R2 — LLM error detection
# ---------------------------------------------------------------------------

_ERROR_HINT_RE = re.compile(r"rate limit|timeout|503|504|429|temporar|unavailable|try again|exceeded", re.IGNORECASE)


def r2_llm_error_detection(item: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: R2 LLM Error Detection (extracted/code/R2_LLM_Error_Detection.js).

    Input item keys read: error, error_message, last_error, llm_error (top level and
    inside `metadata`), output, text. Output item keys added: p17_llm_error
    (detected, detected_message, looks_like_error, output_length, classification,
    reason) and p17_recovery_needed (true only on persistent failure).
    """
    data = dict(item or {})
    error_fields = ["error", "error_message", "last_error", "llm_error"]
    detected_error: Optional[str] = None
    for f in error_fields:
        v = data.get(f, _UNDEFINED)
        if _js_truthy(v) and isinstance(v, str) and len(v) > 0:
            detected_error = v
            break
    if detected_error is None:
        meta = _first_truthy(data.get("metadata", _UNDEFINED), {})
        for f in error_fields:
            v = _prop(meta, f)
            if _js_truthy(v) and isinstance(v, str) and len(v) > 0:
                detected_error = v
                break

    out_v = data.get("output", _UNDEFINED)
    txt_v = data.get("text", _UNDEFINED)
    raw = (out_v if isinstance(out_v, str) else "") or (txt_v if isinstance(txt_v, str) else "")
    # Only short outputs (< 200 chars) are candidate error messages; a long Arabic
    # reply that mentions "503" is a normal explanation, not an error.
    looks_like_error = bool(0 < len(raw) < 200 and _ERROR_HINT_RE.search(raw))
    is_empty_output = len(raw) == 0
    is_persistent_failure = bool(detected_error) or looks_like_error or is_empty_output
    if detected_error:
        classification = "explicit_error"
    elif looks_like_error:
        classification = "implicit_error"
    elif is_empty_output:
        classification = "empty_output"
    else:
        classification = "no_error"

    data["p17_llm_error"] = {
        "detected": is_persistent_failure,
        "detected_message": detected_error,
        "looks_like_error": looks_like_error,
        "output_length": len(raw),
        "classification": classification,
        "reason": "empty_output_after_sanitization" if is_empty_output else None,
    }
    if is_persistent_failure:
        data["p17_recovery_needed"] = True
    return data


# ---------------------------------------------------------------------------
# R1 — empty agent reply recovery (v2: never fabricate a greeting)
# ---------------------------------------------------------------------------


def r1_agent_reply_recovery(item: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: R1 Agent Reply Recovery (extracted/code/R1_Agent_Reply_Recovery.js).

    Input item keys read: output, text, p17_llm_error. Output item keys added:
    p17_recovery_applied (reason, recovery_type, r2_classification,
    output_unchanged, recovered_length) and p17_empty_output_deferred (true only
    when the output is empty / too short / an unterminated JSON attempt).
    The output itself is never modified here.
    """
    data = dict(item or {})
    out_v = data.get("output", _UNDEFINED)
    txt_v = data.get("text", _UNDEFINED)
    raw = (out_v if isinstance(out_v, str) else "") or (txt_v if isinstance(txt_v, str) else "")
    trimmed = raw.strip()
    is_empty = len(trimmed) == 0
    is_too_short = 0 < len(trimmed) < 5
    starts_open = trimmed[:1] == "{" or trimmed[:1] == "["
    ends_close = trimmed[-1:] == "}" or trimmed[-1:] == "]"
    looks_like_json_attempt = starts_open and not ends_close
    if is_empty or is_too_short or looks_like_json_attempt:
        r2 = data.get("p17_llm_error", _UNDEFINED)
        r2 = r2 if isinstance(r2, (dict, list)) else {}
        reason = "empty_output" if is_empty else ("too_short" if is_too_short else "malformed_json_attempt")
        data["p17_recovery_applied"] = {
            "reason": reason,
            "recovery_type": "defer_to_self_repair",
            "r2_classification": _coalesce(_prop(r2, "classification"), None),
            "output_unchanged": True,
            "recovered_length": len(raw),
        }
        data["p17_empty_output_deferred"] = True
        # Output stays empty/unmodified so NAO flags contract_missing and
        # IF Contract Needs Repair routes to the one-shot self-repair chain.
    else:
        data["p17_recovery_applied"] = {"reason": "none", "recovery_type": "none", "recovered_length": len(raw)}
    return data


# ---------------------------------------------------------------------------
# Build Repair Prompt (Deterministic)
# ---------------------------------------------------------------------------

# The contract text below is byte-identical to the runtime JS string in
# Build_Repair_Prompt_Deterministic.js line 23 (real newlines, plain quotes).
_REPAIR_CONTRACT_TEXT = (
    "Output contract k2.dialogue.v3 — JSON only, no Markdown, no commentary.\n"
    "Top-level keys exactly: schema_version, phase, reply, turn, confirmation, selection, entities, operation_proposal, references_prior_conversation, escalate, handoff_reason.\n"
    "schema_version = \"k2.dialogue.v3\". phase = \"understand\".\n"
    "reply: one short natural Arabic sentence(s) answering the patient in the clinic dialect. Never expose internal labels, enums, IDs, or stage names. Never claim a booking, cancellation, availability, or any execution result — only the Result phase may report results. Affirmation of a pending confirmation gets a brief acknowledgment only, no restated details.\n"
    "turn: { intent, relation_to_previous_turn, certainty, confidence }.\n"
    "  intent ∈ {booking_request, booking_continuation, availability_inquiry, cancellation_request, reschedule_request, confirmation, correction, small_talk, clinic_query, greeting, unclear, other}.\n"
    "  relation_to_previous_turn ∈ {new_request, answer, confirmation, correction, change_details, follow_up, none, unclear}.\n"
    "  certainty ∈ {certain, probable, uncertain}. confidence ∈ [0..1] or null.\n"
    "confirmation: { intent } with intent ∈ {affirmative, negative, question, conditional, none}.\n"
    "selection: { kind, rank, date, time } — use ONLY when the assistant previously presented concrete appointment options (numbered alternatives or day/time offers) and the current message picks one. kind ∈ {presented_rank, presented_match, any, none}. rank = the presented option number 1..4 when chosen by number. date (ISO) / time (HH:MM) when the patient echoes a specific presented day/time. kind=any when the patient accepts whatever was offered without naming one. Otherwise kind=none with rank=null, date=null, time=null.\n"
    "entities: exactly { doctor_name, service_name, date, time, visit_type, patient_name, patient_phone, patient_age, patient_address, appointment_id, booking_number }. Every absent value = null. Entities are NEW values stated in the current message only; null never deletes a known value.\n"
    "  date: ISO YYYY-MM-DD in the clinic's local calendar. Convert relative wording (today, tomorrow, weekday names, \"after tomorrow\") using context.local_time.date as today. Only a date you can resolve with certainty; never guess. Allowed window: today through today+60 days; outside that, set null.\n"
    "  time: 24h HH:MM only when stated clearly; convert morning/evening wording. null otherwise.\n"
    "  visit_type ∈ {NEW_VISIT, FOLLOW_UP, null}. NEW_VISIT = first/regular visit, FOLLOW_UP = review/follow-up. Never inferred from service_name.\n"
    "  patient_phone: copy exactly as the patient wrote it; a deterministic layer normalizes it.\n"
    "  patient_age: integer 0..130 or null.\n"
    "  appointment_id / booking_number: only when the patient explicitly provides one.\n"
    "operation_proposal: { type, requested }. type ∈ {create_appointment, cancel_appointment, reschedule_appointment, check_availability, \"\"}. requested=true only when this message asks for that operation; a proposal is never execution.\n"
    "references_prior_conversation: true only on a clear reference to an earlier conversation or appointment.\n"
    "escalate: true only when the request is unsafe, abusive, medical-emergency, or beyond K2 abilities; set handoff_reason (short) with it, else null."
)


def build_repair_prompt_deterministic(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Build Repair Prompt (Deterministic) (extracted/code/Build_Repair_Prompt_Deterministic.js).

    `inputs` keys (each = $(NodeName).first().json, {} when the node did not run):
      normalize_agent_output  <- 'Normalize Agent Output (Deterministic)'
      normalize_validate      <- 'Normalize & Validate'
      clinic_persona_context  <- 'Build Clinic Persona Context (Deterministic)'
      clinic_context          <- 'Get Clinic Context'
      patient_ownership       <- 'Validate Patient Ownership'
    Returns { repair_attempt, errors, prompt } — exactly one repair attempt.
    """
    nao = _obj_or_empty((inputs or {}).get("normalize_agent_output", _UNDEFINED))
    inbound = _obj_or_empty((inputs or {}).get("normalize_validate", _UNDEFINED))
    persona = _obj_or_empty((inputs or {}).get("clinic_persona_context", _UNDEFINED))
    clinic = _obj_or_empty((inputs or {}).get("clinic_context", _UNDEFINED))
    ownership = _obj_or_empty((inputs or {}).get("patient_ownership", _UNDEFINED))

    nao_errors = _prop(nao, "_contract_errors")
    errors = list(nao_errors)[:8] if isinstance(nao_errors, list) else []
    prev_raw = _js_string(_first_truthy(_prop(nao, "agent_raw_output"), ""))[:600]
    ownership_time = _prop(ownership, "canonical_time_context")
    inbound_time = _prop(inbound, "time_context")
    time_ctx = (
        ownership_time
        if isinstance(ownership_time, (dict, list))
        else (inbound_time if isinstance(inbound_time, (dict, list)) else {})
    )
    compact_context = {
        "clinic_name": _coalesce(_first_truthy(_prop(clinic, "clinic_name"), None), None),
        "local_time": {
            "date": _coalesce(_first_truthy(_prop(time_ctx, "now_local_date"), None), None),
            "time": _coalesce(_first_truthy(_prop(time_ctx, "now_local_time"), None), None),
            "timezone": _coalesce(_first_truthy(_prop(time_ctx, "timezone"), None), None),
        },
        "doctor_count": _coalesce(_prop(clinic, "doctor_count"), None),
        "state": _coalesce(_first_truthy(_prop(persona, "agent_context_model"), None), None),
    }
    parts: List[Any] = [
        "K2_SELF_REPAIR: Your previous understanding output for the patient message below was structurally invalid and could not be parsed.",
        "Structural errors: " + (", ".join(_js_string(e) for e in errors) if errors else "unparseable_json") + ".",
        ("Your previous raw output (truncated): " + prev_raw) if _js_truthy(prev_raw) else None,
        "Produce the corrected output now, following the contract rules exactly.",
        _REPAIR_CONTRACT_TEXT,
        "Patient message: " + _js_string(_first_truthy(_prop(inbound, "message_text"), "")),
        "Context JSON: " + json.dumps(compact_context, ensure_ascii=False, separators=(",", ":")),
    ]
    prompt = "\n\n".join(p for p in parts if _js_truthy(p))
    return {"repair_attempt": 1, "errors": errors, "prompt": prompt}


# ---------------------------------------------------------------------------
# Validate Repaired Contract (Deterministic) — NAO v3 parser/validator
# ---------------------------------------------------------------------------

SCHEMA_VERSION = "k2.dialogue.v3"

TURN_INTENTS = ["booking_request", "booking_continuation", "availability_inquiry",
    "cancellation_request", "reschedule_request", "confirmation", "correction",
    "small_talk", "clinic_query", "greeting", "unclear", "other"]
RELATIONS = ["new_request", "answer", "confirmation", "correction",
    "change_details", "follow_up", "none", "unclear"]
CERTAINTIES = ["certain", "probable", "uncertain"]
CONFIRMATION_INTENTS = ["affirmative", "negative", "question", "conditional", "none"]
SELECTION_KINDS = ["presented_rank", "presented_match", "any", "none"]
OPERATION_TYPES = ["create_appointment", "cancel_appointment", "reschedule_appointment", "check_availability", ""]
VISIT_TYPES = ["NEW_VISIT", "FOLLOW_UP"]

PHONE_RULES: Dict[str, Dict[str, Any]] = {
    "SA": {"code": "+966", "lengths": [9], "prefixes": ["5"]},
    "AE": {"code": "+971", "lengths": [9], "prefixes": ["5"]},
    "KW": {"code": "+965", "lengths": [8], "prefixes": ["5", "6", "9"]},
    "QA": {"code": "+974", "lengths": [8], "prefixes": ["3", "5", "6", "7"]},
    "BH": {"code": "+973", "lengths": [8], "prefixes": ["3"]},
    "OM": {"code": "+968", "lengths": [8], "prefixes": ["7", "9"]},
    "EG": {"code": "+20", "lengths": [10], "prefixes": ["1", "2"]},
    "PH": {"code": "+63", "lengths": [10], "prefixes": ["9"]},
    "IN": {"code": "+91", "lengths": [10], "prefixes": ["6", "7", "8", "9"]},
    "PK": {"code": "+92", "lengths": [10], "prefixes": ["3"]},
    "BD": {"code": "+880", "lengths": [10], "prefixes": ["1"]},
    "ID": {"code": "+62", "lengths": [9, 10, 11, 12], "prefixes": ["8"]},
    "YE": {"code": "+967", "lengths": [9], "prefixes": ["7"]},
    "JO": {"code": "+962", "lengths": [9], "prefixes": ["7"]},
    "SD": {"code": "+249", "lengths": [9], "prefixes": ["9"]},
    "SY": {"code": "+963", "lengths": [9], "prefixes": ["9"]},
    "IQ": {"code": "+964", "lengths": [10], "prefixes": ["7"]},
    "LB": {"code": "+961", "lengths": [7, 8], "prefixes": ["3", "7"]},
    "TR": {"code": "+90", "lengths": [10], "prefixes": ["5"]},
    "US": {"code": "+1", "lengths": [10], "prefixes": ["2", "3", "4", "5", "6", "7", "8", "9"]},
    "GB": {"code": "+44", "lengths": [10], "prefixes": ["7"]},
}
_ARABIC_INDIC = {
    "\u0660": "0", "\u0661": "1", "\u0662": "2", "\u0663": "3", "\u0664": "4",
    "\u0665": "5", "\u0666": "6", "\u0667": "7", "\u0668": "8", "\u0669": "9",
}

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
TIME_24_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?$")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.IGNORECASE)
_FENCED_BLOCK_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.IGNORECASE)
_WS_MULTI_RE = re.compile(r"\s+")


def _to_english_digits(s: Any) -> str:
    return re.sub(r"[\u0660-\u0669]", lambda m: _ARABIC_INDIC[m.group(0)], _js_string(s))


def _normalize_phone(raw_input: Any, default_country: Any = None) -> Optional[str]:
    """JS normalizePhone — E.164 normalizer retained from P1.7 as a validator."""
    if default_country is None:
        default_country = "SA"
    if raw_input is None or raw_input is _UNDEFINED or raw_input == "":
        return None
    s = re.sub(r"[^\d+]", "", _to_english_digits(_js_string(raw_input)))
    if not s:
        return None
    if s[:1] == "+":
        # Source quirk kept: the lookup result is discarded and `s` is returned
        # unchanged in both branches.
        for _cc, rule in PHONE_RULES.items():
            if s.startswith(rule["code"]):
                return s
        return s
    for _cc, rule in PHONE_RULES.items():
        if any(len(s) in rule["lengths"] and s.startswith(p) for p in rule["prefixes"]):
            return rule["code"] + s
    if s[:1] == "0":
        s = s[1:]
    default_rule = PHONE_RULES.get(_js_string(default_country))
    if default_rule:
        return default_rule["code"] + s
    return "+" + s


def _iso_date_valid(date_iso: Any) -> bool:
    if not ISO_DATE_RE.match(_js_string(_first_truthy(date_iso, ""))):
        return False
    try:
        datetime.strptime(_js_string(date_iso), "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def _date_within_horizon(date_iso: Any, now_local_date: Any, horizon_days: Any) -> bool:
    if not _iso_date_valid(date_iso) or not _iso_date_valid(now_local_date):
        return False
    a = datetime.strptime(_js_string(now_local_date), "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000.0
    b = datetime.strptime(_js_string(date_iso), "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000.0
    days = round((b - a) / 86400000.0)
    return days >= 0 and days <= (_first_truthy(horizon_days, 60) or 60)


def _normalize_time(value: Any) -> Optional[str]:
    t = _js_string(_first_truthy(value, "")).strip()
    if not TIME_24_RE.match(t):
        return None
    # Source quirk kept: length 4 is unreachable given TIME_24, and 8 -> HH:MM.
    if len(t) == 8:
        return t[:5]
    if len(t) == 4:
        return t + ":00"
    return t


def _clean_str(value: Any) -> Optional[str]:
    # JS: String(value == null ? '' : value) — `== null` catches undefined AND null.
    s = _WS_MULTI_RE.sub(" ", _js_string("" if (value is None or value is _UNDEFINED) else value)).strip()
    return s or None


def _in_enum(value: Any, allowed: List[str], fallback: str, warnings: List[str]) -> str:
    # JS: String(value == null ? '' : value) — undefined and null both become ''.
    v = _js_string("" if (value is None or value is _UNDEFINED) else value).strip().lower()
    if not v:
        return fallback
    if v in allowed:
        return v
    warnings.append("enum_repaired:" + v)
    return fallback


def _extract_json_document(text: Any) -> Optional[Dict[str, Any]]:
    """Robust JSON extraction (no semantics): returns the LAST balanced object."""
    if _js_truthy(text) and isinstance(text, dict):
        return text
    if text is None or text is _UNDEFINED:
        return None
    t = _js_string(text).strip()
    fenced_blocks = _FENCED_BLOCK_RE.findall(t)
    if fenced_blocks:
        # Reviewer fix: first-fence-wins inverted the last-object-wins contract when
        # the repair model emitted draft+corrected fences — the corrected block wins.
        t = fenced_blocks[-1].strip()

    def parse_at(start: int) -> Optional[Tuple[Dict[str, Any], int]]:
        depth = 0
        in_string = False
        escaped = False
        i = start
        while i < len(t):
            ch = t[i]
            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
                i += 1
                continue
            if ch == '"':
                in_string = True
                i += 1
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        value = json.loads(t[start:i + 1])
                        if _js_truthy(value) and isinstance(value, dict):
                            return value, i
                    except ValueError:
                        pass
                    return None
            i += 1
        return None

    candidates: List[Dict[str, Any]] = []
    i = 0
    while i < len(t):
        if t[i] != "{":
            i += 1
            continue
        parsed = parse_at(i)
        if parsed is not None:
            candidates.append(parsed[0])
            i = parsed[1] + 1
        else:
            i += 1
    if not candidates:
        return None
    return candidates[-1]


def _validate_contract(raw: Any, ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Validate + deterministically repair a model contract.

    ctx keys: now_local_date, clinic_country_code, date_horizon_days.
    Returns { valid, errors, warnings, contract }: errors are structural (they
    trigger the one-shot model self-repair retry), warnings are field repairs.
    Every branch of the source validator is preserved, in source order.
    """
    ctx = ctx or {}
    errors: List[str] = []
    warnings: List[str] = []
    c = raw
    if isinstance(c, str):
        try:
            c = json.loads(c)
        except ValueError:
            c = None
        if not _js_truthy(c) or not isinstance(c, (dict, list)):
            errors.append("contract_unparseable")
    if not _js_truthy(c) or not isinstance(c, (dict, list)):
        return {"valid": False, "errors": errors if errors else ["contract_missing"], "warnings": warnings, "contract": None}

    out: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "phase": "understand",
        "reply": _clean_str(_prop(c, "reply")),
    }

    turn = _obj_or_empty(_prop(c, "turn"))
    intent_raw = _prop(turn, "intent")
    intent = _in_enum(intent_raw, TURN_INTENTS, "unclear", warnings)
    intent_str = _js_string(_first_truthy(intent_raw, "")).strip()
    if intent == "unclear" and intent_str != "" and intent_str.lower() not in TURN_INTENTS:
        warnings.append("turn_intent_repaired")
    confidence_num = _js_number(_prop(turn, "confidence"))
    confidence = max(0.0, min(1.0, confidence_num)) if _is_finite_number(confidence_num) else None
    out["turn"] = {
        "intent": intent,
        "relation_to_previous_turn": _in_enum(_prop(turn, "relation_to_previous_turn"), RELATIONS, "none", warnings),
        "certainty": _in_enum(_prop(turn, "certainty"), CERTAINTIES, "uncertain", warnings),
        "confidence": confidence,
    }

    conf = _obj_or_empty(_prop(c, "confirmation"))
    out["confirmation"] = {"intent": _in_enum(_prop(conf, "intent"), CONFIRMATION_INTENTS, "none", warnings)}

    sel = _obj_or_empty(_prop(c, "selection"))
    sel_kind = _in_enum(_prop(sel, "kind"), SELECTION_KINDS, "none", warnings)
    sel_rank_num = _js_number(_prop(sel, "rank"))
    sel_rank: Optional[int] = None
    if _is_finite_number(sel_rank_num) and float(sel_rank_num).is_integer() and 1 <= sel_rank_num <= 4:
        sel_rank = int(sel_rank_num)
    sel_date_val = _prop(sel, "date")
    if _iso_date_valid(sel_date_val):
        sel_date = _js_string(sel_date_val)
    elif _js_truthy(sel_date_val):
        warnings.append("selection_date_invalid")
        sel_date = None
    else:
        sel_date = None
    sel_time_val = _prop(sel, "time")
    sel_time = _normalize_time(sel_time_val)
    if sel_time is None and _js_truthy(sel_time_val):
        warnings.append("selection_time_invalid")
    out["selection"] = {"kind": sel_kind, "rank": sel_rank, "date": sel_date, "time": sel_time}
    if (
        sel_kind != "none"
        and sel_kind != "any"
        and not _js_truthy(sel_rank)
        and not _js_truthy(sel_date)
        and not _js_truthy(sel_time)
        and not _js_truthy(_prop(sel, "time"))
        and not _js_truthy(_prop(sel, "date"))
    ):
        warnings.append("selection_without_anchor")

    ent = _obj_or_empty(_prop(c, "entities"))
    ent_date = _clean_str(_prop(ent, "date"))
    if _js_truthy(ent_date) and not _date_within_horizon(ent_date, _prop(ctx, "now_local_date"), _first_truthy(_prop(ctx, "date_horizon_days"), 60)):
        warnings.append("entities_date_out_of_horizon:" + _js_string(ent_date))
        ent_date = None
    ent_time = _normalize_time(_prop(ent, "time"))
    if ent_time is None and _js_truthy(_prop(ent, "time")):
        warnings.append("entities_time_invalid")

    age_val = _prop(ent, "patient_age")
    if age_val is not None and age_val is not _UNDEFINED and age_val != "":
        digits = re.sub(r"[^0-9]", "", _to_english_digits(_js_string(age_val)))
        n = float(int(digits)) if digits else _NAN
        if _is_finite_number(n) and 0 <= n <= 130:
            age: Optional[int] = int(n)
        else:
            warnings.append("patient_age_out_of_range")
            age = None
    else:
        age = None

    phone = _clean_str(_prop(ent, "patient_phone"))
    if _js_truthy(phone):
        normalized = _normalize_phone(phone, _first_truthy(_prop(ctx, "clinic_country_code"), "SA"))
        if _js_truthy(normalized):
            phone = normalized
        else:
            warnings.append("patient_phone_invalid")
            phone = None

    visit_raw = _js_string(_first_truthy(_prop(ent, "visit_type"), _prop(ent, "appointment_type"), ""))
    # Reviewer fix: "FOLLOW-UP" (hyphen) previously missed the enum and nulled
    # the visit type mid-booking.
    v = _WS_MULTI_RE.sub("_", visit_raw.strip().upper().replace("-", " "))
    if not v:
        visit_type: Optional[str] = None
    elif v in VISIT_TYPES:
        visit_type = v
    else:
        warnings.append("visit_type_repaired:" + v)
        visit_type = None

    appointment_id = _clean_str(_prop(ent, "appointment_id"))
    if _js_truthy(appointment_id) and not UUID_RE.match(appointment_id):
        warnings.append("appointment_id_not_uuid")
        appointment_id_out: Optional[str] = None
    else:
        appointment_id_out = appointment_id

    out["entities"] = {
        "doctor_name": _clean_str(_prop(ent, "doctor_name")),
        "service_name": _clean_str(_prop(ent, "service_name")),
        "date": ent_date,
        "time": ent_time,
        "visit_type": visit_type,
        "patient_name": _clean_str(_prop(ent, "patient_name")),
        "patient_phone": phone,
        "patient_age": age,
        "patient_address": _clean_str(_prop(ent, "patient_address")),
        "appointment_id": appointment_id_out,
        "booking_number": _clean_str(_first_truthy(_prop(ent, "booking_number"), _prop(ent, "reference"), None)),
    }

    op = _obj_or_empty(_prop(c, "operation_proposal"))
    out["operation_proposal"] = {
        "type": _in_enum(_prop(op, "type"), OPERATION_TYPES, "", warnings),
        "requested": _prop(op, "requested") is True,
    }

    out["references_prior_conversation"] = _prop(c, "references_prior_conversation") is True
    out["escalate"] = _prop(c, "escalate") is True
    out["handoff_reason"] = _clean_str(_prop(c, "handoff_reason"))
    return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings, "contract": out}


def _message_class(contract: Any, state: Any) -> str:
    """Message classification exclusively from contract fields — never text.

    (The source signature receives `state` but never reads it; kept for fidelity.)
    """
    c = contract or {}
    turn = _first_truthy(_prop(c, "turn"), {})
    intent = _js_string(_first_truthy(_prop(turn, "intent"), "unclear"))
    conf_intent = _js_string(_first_truthy(_prop(_first_truthy(_prop(c, "confirmation"), {}), "intent"), "none"))
    sel_kind = _js_string(_first_truthy(_prop(_first_truthy(_prop(c, "selection"), {}), "kind"), "none"))
    op_type = _js_string(_first_truthy(_prop(_first_truthy(_prop(c, "operation_proposal"), {}), "type"), ""))
    if op_type == "cancel_appointment" or intent == "cancellation_request":
        return "cancel_request"
    if op_type == "reschedule_appointment" or intent == "reschedule_request":
        return "reschedule_request"
    if sel_kind != "none":
        return "selection_presented"
    if conf_intent == "affirmative":
        return "confirmation_affirm"
    if conf_intent == "negative":
        return "confirmation_negative"
    if conf_intent == "question" or conf_intent == "conditional":
        return "confirmation_question"
    if intent == "confirmation":
        return "confirmation_affirm"
    if intent == "booking_request" or intent == "booking_continuation" or op_type == "create_appointment":
        return "booking"
    if intent == "availability_inquiry" or op_type == "check_availability":
        return "availability_inquiry"
    if intent == "correction":
        return "correction"
    if intent == "small_talk" or intent == "greeting":
        return "small_talk"
    if intent == "clinic_query":
        return "clinic_query"
    return "unclear"


def _stable_hash(value: Any) -> str:
    """FNV-1a over JS code points; 8-char zero-padded hex of the uint32 hash."""
    h = 2166136261
    for ch in _js_string(_coalesce(value, "")):
        h ^= ord(ch)
        h = _imul(h, 16777619)
    return format(h & 0xFFFFFFFF, "08x")


def _normalize_arabic_user_text(value: Any) -> str:
    """Observability-only Arabic hygiene view (never used for decisions)."""
    s = _js_string(_coalesce(value, ""))
    s = unicodedata.normalize("NFKC", s)
    s = re.sub("[أإآٱ]", "ا", s)
    s = s.replace("ى", "ي")
    s = s.replace("ؤ", "و")
    s = s.replace("ئ", "ي")
    s = s.replace("ة", "ه")
    s = re.sub("[ًٌٍَُِّْـ]", "", s)
    s = re.sub("[٠-٩]", lambda d: str("٠١٢٣٤٥٦٧٨٩".index(d.group(0))), s)
    s = _WS_MULTI_RE.sub(" ", s)
    return s.strip().lower()


# JS \b is ASCII-\w based ([A-Za-z0-9_]); Python \b is Unicode-aware. These
# lookarounds reproduce the source quirk exactly: the Arabic day/time words in
# the first pattern can only match when glued to ASCII word chars (in practice a
# no-op on natural Arabic text), while the ASCII digit patterns behave normally.
_TEMPORAL_DAY_WORDS_RE = re.compile(
    "(?<=[0-9A-Za-z_])(?:اليوم|بكره|بكرة|غدا|غدًا|بعد بكره|بعد بكرة|بعد غد|باچر|السبت|الاحد|الأحد|الاثنين|الثلاثاء|الاربعاء|الأربعاء|الخميس|الجمعة)(?=[0-9A-Za-z_])",
    re.IGNORECASE,
)
_TEMPORAL_SAA_RE = re.compile(r"الساعة\s+\d{1,2}(?::\d{2})?\s*(?:صباحا|صباحًا|مساء|مساءً|ص|م)?", re.IGNORECASE)
_TEMPORAL_CLOCK_RE = re.compile(r"(?<![0-9A-Za-z_])\d{1,2}:\d{2}(?![0-9A-Za-z_])")
_TEMPORAL_DATE_RE = re.compile(r"(?<![0-9A-Za-z_])\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?(?![0-9A-Za-z_])")
_SPACES_2PLUS_RE = re.compile(r"\s{2,}")


def validate_repaired_contract_deterministic(item: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Validate Repaired Contract (Deterministic) (extracted/code/Validate_Repaired_Contract_Deterministic.js).

    `item` is the current pipeline item ($json): on the primary path the Normalize
    Agent Output item, on the repair path the DeepSeek Repair Chain output. Raw
    model text is read from keys text / raw_output / output / response (nullish
    chain, first present wins).

    `inputs` keys (each = $(NodeName).first().json; {} / None when not available):
      conversation_state     <- 'Get Conversation State' (uses .state_data)
      clinic_context         <- 'Get Clinic Context'
      patient_ownership      <- 'Validate Patient Ownership'
      clinic_persona_context <- 'Build Clinic Persona Context (Deterministic)'
      normalize_validate     <- 'Normalize & Validate'
      repair_prompt          <- 'Build Repair Prompt (Deterministic)'; pass None
                                only when the node did not run this turn (its
                                truthiness decides _repair_source).
    Returns the enriched item dict (contract, contract_v3, slot_state,
    booking_context, _contract_status, _contract_errors, _contract_warnings,
    _contract_repair_needed, _normalization, ...).
    """
    item = item or {}
    inputs = inputs or {}
    state = _first_truthy(_prop(inputs.get("conversation_state", _UNDEFINED), "state_data"), {})
    clinic_row = _first_truthy(inputs.get("clinic_context", _UNDEFINED), {})
    ownership = _first_truthy(inputs.get("patient_ownership", _UNDEFINED), {})
    persona_ctx = _first_truthy(inputs.get("clinic_persona_context", _UNDEFINED), {})
    ctx = _first_truthy(inputs.get("normalize_validate", _UNDEFINED), {})
    repair_prompt = inputs.get("repair_prompt")

    raw_output = _js_string(
        _coalesce(
            _prop(item, "text"),
            _prop(item, "raw_output"),
            _prop(item, "output"),
            _prop(item, "response"),
            "",
        )
    ).strip()

    # ── Model-call failure detection (transport errors only) ──
    agent_call_failed = bool(
        _js_truthy(item)
        and (
            _js_truthy(_prop(item, "error"))
            or _js_truthy(_prop(item, "errorMessage"))
            or (_js_number(_prop(item, "statusCode")) >= 400)
        )
    )

    parsed_doc = _extract_json_document(raw_output)
    if agent_call_failed:
        validation = {"valid": False, "errors": ["MODEL_CALL_FAILED"], "warnings": [], "contract": None}
    else:
        # ── Validator context: clinic-local calendar + country for format checks ──
        ownership_time = _prop(ownership, "canonical_time_context")
        ctx_time = _prop(ctx, "time_context")
        time_ctx = (
            ownership_time
            if isinstance(ownership_time, (dict, list))
            else (ctx_time if isinstance(ctx_time, (dict, list)) else {})
        )
        now_local_date = (
            _js_string(_first_truthy(_prop(time_ctx, "now_local_date"), ""))
            if ISO_DATE_RE.match(_js_string(_first_truthy(_prop(time_ctx, "now_local_date"), "")))
            else _utc_now_iso()[:10]
        )
        validator_ctx = {
            "now_local_date": now_local_date,
            "clinic_country_code": _js_string(_first_truthy(_prop(clinic_row, "country_code"), _prop(clinic_row, "clinic_country_code"), "SA")),
            "date_horizon_days": 60,
        }
        validation = _validate_contract(parsed_doc, validator_ctx)

    # JS truthiness of the repair node json: any object (even {}) counts as "ran";
    # only a missing node (exception -> None here) yields False.
    repair_ran = repair_prompt is not None and _js_truthy(repair_prompt)
    structural_failure = (not agent_call_failed) and (not validation["valid"])
    repair_needed = structural_failure and not repair_ran
    contract_v3 = validation["contract"]
    if agent_call_failed:
        model_call_status = "MODEL_CALL_FAILED"
    elif validation["valid"]:
        model_call_status = "VALID"
    elif repair_ran:
        model_call_status = "INVALID_AFTER_REPAIR"
    else:
        model_call_status = "INVALID_OR_INCOMPLETE_CONTRACT"

    # ── Contract-derived facts (never from message text) ──
    turn_v3 = _prop(contract_v3, "turn") if contract_v3 is not None else None
    turn_intent_v3 = _js_string(_first_truthy(_prop(turn_v3, "intent"), "unclear")) if turn_v3 is not None else "unclear"
    relation_v3 = _js_string(_first_truthy(_prop(turn_v3, "relation_to_previous_turn"), "none")) if turn_v3 is not None else "none"
    confirmation_intent_v3 = (
        _js_string(_first_truthy(_prop(_prop(contract_v3, "confirmation"), "intent"), "none"))
        if contract_v3 is not None
        else "none"
    )
    operation_type_v3 = (
        _js_string(_first_truthy(_prop(_prop(contract_v3, "operation_proposal"), "type"), ""))
        if contract_v3 is not None
        else ""
    )
    operation_requested = (
        _prop(_prop(contract_v3, "operation_proposal"), "requested") is True
        if contract_v3 is not None
        else False
    )
    references_prior_conversation = (
        _prop(contract_v3, "references_prior_conversation") is True if contract_v3 is not None else False
    )
    escalation_requested = _prop(contract_v3, "escalate") is True if contract_v3 is not None else False
    handoff_reason = (
        (_js_string(_first_truthy(_prop(contract_v3, "handoff_reason"), "")).strip() or None)
        if contract_v3 is not None
        else None
    )
    cls = _message_class(contract_v3, state) if contract_v3 is not None else "unclear"
    availability_inquiry = cls == "availability_inquiry"

    # ── Temporal-claim guard (patient-safety, stage-driven, not text-driven) ──
    current_pre_agent_stage = _first_truthy(_prop(persona_ctx, "pre_agent_stage_contract"), None)
    temporal_claim_guard_active = (
        _prop(current_pre_agent_stage, "type") == "confirm_patient_data"
        and _prop(current_pre_agent_stage, "date_allowed") is False
    )

    def _sanitize_unsupported_temporal_claims(value: Any) -> str:
        raw = _js_string(_coalesce(value, ""))
        if not temporal_claim_guard_active:
            return raw.strip()
        out = _TEMPORAL_DAY_WORDS_RE.sub("", raw)
        out = _TEMPORAL_SAA_RE.sub("", out)
        out = _TEMPORAL_CLOCK_RE.sub("", out)
        out = _TEMPORAL_DATE_RE.sub("", out)
        out = _SPACES_2PLUS_RE.sub(" ", out)
        return out.strip()

    extracted_reply = (
        (_sanitize_unsupported_temporal_claims(_js_string(_prop(contract_v3, "reply"))) or None)
        if (contract_v3 is not None and _js_truthy(_prop(contract_v3, "reply")))
        else None
    )

    # ── Entities for plumbing (IDs resolve later via Resolve Booking IDs) ──
    ent_v3 = _prop(contract_v3, "entities") if contract_v3 is not None else {}
    clean_entities: Dict[str, Any] = {
        "doctor_name": _coalesce(_prop(ent_v3, "doctor_name"), None),
        "doctor_id": None,
        "service_name": _coalesce(_prop(ent_v3, "service_name"), None),
        "service_id": None,
        "appointment_type": _coalesce(_prop(ent_v3, "visit_type"), None),
        "date": _coalesce(_prop(ent_v3, "date"), None),
        "time": _coalesce(_prop(ent_v3, "time"), None),
        "slot_id": None,
        "branch_id": None,
        "branch_name": None,
        "appointment_id": _coalesce(_prop(ent_v3, "appointment_id"), None),
        "booking_number": _coalesce(_prop(ent_v3, "booking_number"), None),
        "patient_name": _coalesce(_prop(ent_v3, "patient_name"), None),
        "patient_phone": _coalesce(_prop(ent_v3, "patient_phone"), None),
        "patient_age": _coalesce(_prop(ent_v3, "patient_age"), None),
        "patient_address": _coalesce(_prop(ent_v3, "patient_address"), None),
        "references_prior_conversation": references_prior_conversation,
    }
    invalid_date_input = any(_js_string(w).startswith("entities_date_out_of_horizon") for w in validation["warnings"])
    invalid_time_input = any(_js_string(w) == "entities_time_invalid" for w in validation["warnings"])

    # ── Booking context management (deterministic merge, kept for the legacy bridge) ──
    prior_activity_ms0 = _date_parse(
        _first_truthy(_prop(state, "last_updated"), _prop(state, "updated_at"), _prop(state, "last_message_at"), "")
    )
    received_at_str = _js_string(_first_truthy(_prop(ctx, "received_at"), ""))
    if not _js_truthy(received_at_str):
        received_at_str = _utc_now_iso()
    current_activity_ms0 = _date_parse(received_at_str)
    session_context_stale = (
        _is_finite_number(prior_activity_ms0)
        and _is_finite_number(current_activity_ms0)
        and (current_activity_ms0 - prior_activity_ms0) >= 2 * 60 * 60 * 1000
    )
    session_context_usable = (not session_context_stale) or references_prior_conversation is True
    prior_slot = (
        _prop(state, "slot_state")
        if (session_context_usable and isinstance(_prop(state, "slot_state"), (dict, list)))
        else {}
    )
    prior_context = (
        _prop(state, "booking_context")
        if (session_context_usable and isinstance(_prop(state, "booking_context"), (dict, list)))
        else {}
    )
    state_facts = _prop(state, "facts")
    state_patient_facts = (
        _prop(state_facts, "patient")
        if (_js_truthy(state_facts) and isinstance(state_facts, (dict, list)) and isinstance(_prop(state_facts, "patient"), (dict, list)))
        else {}
    )

    prior_operation_action = _js_string(_first_truthy(_prop(state, "active_operation"), _prop(state, "operation_action"), "")).strip().lower()
    prior_operation_state = _js_string(_first_truthy(_prop(state, "operation_state"), _prop(state, "operation_status"), "")).strip().upper()
    prior_draft_expiry_ms = _date_parse(_first_truthy(_prop(state, "draft_expires_at"), ""))
    prior_draft_expired = _is_finite_number(prior_draft_expiry_ms) and prior_draft_expiry_ms <= current_activity_ms0
    prior_create_is_live = (
        prior_operation_action == "create_appointment"
        and prior_operation_state not in ("COMPLETED", "CANCELLED", "FAILED_FINAL", "IDLE")
        and not (
            prior_draft_expired
            and prior_operation_state in ("DRAFT", "COLLECTING_DETAILS", "COLLECTING_APPOINTMENT_DETAILS", "")
        )
    )

    fresh_doctor_name_given = _js_string(_first_truthy(clean_entities.get("doctor_name"), "")).strip() != ""
    fresh_service_name_given = _js_string(_first_truthy(clean_entities.get("service_name"), "")).strip() != ""

    def _kept_entity(field: str, old: Any) -> Any:
        return _keep(clean_entities.get(field, _UNDEFINED), old)

    slot_state: Dict[str, Any] = {
        "branch_id": _kept_entity("branch_id", _prop(prior_slot, "branch_id")),
        "branch_name": _kept_entity("branch_name", _prop(prior_slot, "branch_name")),
        "doctor_id": (
            clean_entities["doctor_id"]
            if (clean_entities["doctor_id"] is not None and clean_entities["doctor_id"] is not _UNDEFINED)
            else ((None if fresh_doctor_name_given else _prop(prior_slot, "doctor_id")))
        ),
        "doctor_name": _kept_entity("doctor_name", _prop(prior_slot, "doctor_name")),
        "service_id": (
            clean_entities["service_id"]
            if (clean_entities["service_id"] is not None and clean_entities["service_id"] is not _UNDEFINED)
            else ((None if fresh_service_name_given else _prop(prior_slot, "service_id")))
        ),
        "service_name": _kept_entity("service_name", _prop(prior_slot, "service_name")),
        "appointment_type": (
            clean_entities["appointment_type"]
            if (clean_entities["appointment_type"] is not None and clean_entities["appointment_type"] is not _UNDEFINED)
            else _prop(prior_slot, "appointment_type")
        ),
        "date": _kept_entity("date", _prop(prior_slot, "date")),
        "time": _kept_entity("time", _prop(prior_slot, "time")),
        "slot_id": _kept_entity("slot_id", _prop(prior_slot, "slot_id")),
    }
    booking_context: Dict[str, Any] = {
        "branch_id": _kept_entity("branch_id", _prop(prior_context, "branch_id")),
        "branch_name": _kept_entity("branch_name", _prop(prior_context, "branch_name")),
        "doctor_id": (
            clean_entities["doctor_id"]
            if (clean_entities["doctor_id"] is not None and clean_entities["doctor_id"] is not _UNDEFINED)
            else ((None if fresh_doctor_name_given else _prop(prior_context, "doctor_id")))
        ),
        "doctor_name": _kept_entity("doctor_name", _prop(prior_context, "doctor_name")),
        "service_id": (
            clean_entities["service_id"]
            if (clean_entities["service_id"] is not None and clean_entities["service_id"] is not _UNDEFINED)
            else ((None if fresh_service_name_given else _prop(prior_context, "service_id")))
        ),
        "service_name": _kept_entity("service_name", _prop(prior_context, "service_name")),
        "appointment_type": (
            clean_entities["appointment_type"]
            if (clean_entities["appointment_type"] is not None and clean_entities["appointment_type"] is not _UNDEFINED)
            else _prop(prior_context, "appointment_type")
        ),
        "slot_id": _kept_entity("slot_id", _prop(prior_context, "slot_id")),
        "date": _kept_entity("date", _prop(prior_context, "date")),
        "time": _kept_entity("time", _prop(prior_context, "time")),
        "patient_name": _kept_entity("patient_name", _first_truthy(_prop(prior_context, "patient_name"), _prop(state_patient_facts, "name"))),
        "patient_phone": _kept_entity(
            "patient_phone",
            _first_truthy(_prop(prior_context, "patient_phone"), _prop(state_patient_facts, "phone"), _prop(state_patient_facts, "mobile")),
        ),
        "patient_age": _keep(
            clean_entities["patient_age"] if clean_entities["patient_age"] is not _UNDEFINED else None,
            (
                _prop(prior_context, "patient_age")
                if _prop(prior_context, "patient_age") is not _UNDEFINED
                else (_prop(state_patient_facts, "age") if _prop(state_patient_facts, "age") is not _UNDEFINED else None)
            ),
        ),
        "patient_address": _kept_entity("patient_address", _first_truthy(_prop(prior_context, "patient_address"), _prop(state_patient_facts, "address"))),
        "references_prior_conversation": references_prior_conversation,
    }

    if invalid_date_input or invalid_time_input:
        slot_state["slot_id"] = None
        booking_context["slot_id"] = None

    # ── Error-followup recovery (state-driven; preserves doctor identity after a failed reply) ──
    error_followup_recovered = False
    builder_recovery = _prop(persona_ctx, "error_followup_context")
    builder_recovery = builder_recovery if isinstance(builder_recovery, (dict, list)) else {}
    doctor_directory = _prop(clinic_row, "doctor_directory")
    normalize_clinic_doctors = doctor_directory if isinstance(doctor_directory, list) else []
    normalize_recovered_doctor = None
    for doctor in normalize_clinic_doctors:
        if _js_string(_first_truthy(_dig(doctor, "doctor_id"), _dig(doctor, "id"), "")) == _js_string(_first_truthy(_prop(builder_recovery, "doctor_id"), "")):
            normalize_recovered_doctor = doctor
            break
    if (
        _prop(builder_recovery, "active") is True
        and _js_truthy(_prop(builder_recovery, "doctor_name"))
        and not fresh_doctor_name_given
    ):
        error_followup_recovered = True
        clean_entities["doctor_name"] = _js_string(_prop(builder_recovery, "doctor_name")).strip()
        clean_entities["doctor_id"] = _first_truthy(
            _prop(builder_recovery, "doctor_id"),
            _dig(normalize_recovered_doctor, "doctor_id"),
            _dig(normalize_recovered_doctor, "id"),
            None,
        )
        slot_state["doctor_name"] = clean_entities["doctor_name"]
        slot_state["doctor_id"] = _first_truthy(clean_entities["doctor_id"], slot_state.get("doctor_id"), None)
        booking_context["doctor_name"] = clean_entities["doctor_name"]
        booking_context["doctor_id"] = _first_truthy(clean_entities["doctor_id"], booking_context.get("doctor_id"), None)
        if _js_truthy(_prop(builder_recovery, "appointment_type")) and not _js_truthy(booking_context.get("appointment_type")):
            clean_entities["appointment_type"] = _prop(builder_recovery, "appointment_type")
            slot_state["appointment_type"] = _prop(builder_recovery, "appointment_type")
            booking_context["appointment_type"] = _prop(builder_recovery, "appointment_type")

    # ── Fresh booking restart (contract-driven R1: intent + relation + live artifacts only) ──
    prior_offer_raw = _prop(state, "presented_offer") if isinstance(_prop(state, "presented_offer"), (dict, list)) else (
        _prop(state, "pending_offer") if isinstance(_prop(state, "pending_offer"), (dict, list)) else None
    )

    def _offer_is_live(offer: Any) -> bool:
        if offer is None:
            return False
        exp = _date_parse(_js_string(_first_truthy(_prop(offer, "expires_at"), "")))
        return (exp > current_activity_ms0) if _is_finite_number(exp) else False

    prior_offer_live = _offer_is_live(prior_offer_raw)
    prior_target_raw = _prop(state, "confirmation_target") if isinstance(_prop(state, "confirmation_target"), (dict, list)) else None

    def _target_is_live(target: Any) -> bool:
        if target is None or _prop(target, "invalidated") is True:
            return False
        status = _js_string(_first_truthy(_prop(target, "confirmation_delivery_status"), _prop(target, "delivery"), "pending")).lower()
        if status not in ("pending", "sent", "proposed"):
            return False
        exp = _date_parse(_js_string(_first_truthy(_prop(target, "expires_at"), "")))
        return (not _is_finite_number(exp)) or exp > current_activity_ms0

    prior_target_live = _target_is_live(prior_target_raw)
    new_booking_restart = (
        turn_intent_v3 == "booking_request"
        and relation_v3 == "new_request"
        and not references_prior_conversation
        and not prior_offer_live
        and not prior_target_live
    )

    if new_booking_restart:
        for field in ("date", "time", "slot_id"):
            slot_state[field] = _first_truthy(clean_entities.get(field), None)
            booking_context[field] = _first_truthy(clean_entities.get(field), None)
        for field in ("doctor_id", "doctor_name", "service_id", "service_name", "appointment_type", "date", "time", "slot_id"):
            current_value = _first_truthy(clean_entities.get(field), None)
            slot_state[field] = current_value
            booking_context[field] = current_value

    # ── Single-doctor lock (runs AFTER the restart wipe) ──
    clinic_doctor_count = _js_number(_first_truthy(_prop(clinic_row, "doctor_count"), 0))
    single_doctor_id = _js_string(_first_truthy(_prop(clinic_row, "single_doctor_id"), "")).strip() or None
    single_doctor_name = _js_string(_first_truthy(_prop(clinic_row, "single_doctor_name"), "")).strip() or None
    doctor_selection_turn = turn_intent_v3 in ("booking_request", "booking_continuation", "availability_inquiry")
    explicit_doctor_in_turn = bool(_js_truthy(clean_entities.get("doctor_id")) or _js_truthy(clean_entities.get("doctor_name")))
    doctor_context_already_set = bool(_js_truthy(booking_context.get("doctor_id")) or _js_truthy(booking_context.get("doctor_name")))
    can_auto_select_single_doctor = (
        clinic_doctor_count == 1
        and bool(single_doctor_id and single_doctor_name)
        and doctor_selection_turn
        and not explicit_doctor_in_turn
        and not doctor_context_already_set
    )
    if can_auto_select_single_doctor:
        clean_entities["doctor_id"] = single_doctor_id
        clean_entities["doctor_name"] = single_doctor_name
        slot_state["doctor_id"] = single_doctor_id
        slot_state["doctor_name"] = single_doctor_name
        booking_context["doctor_id"] = single_doctor_id
        booking_context["doctor_name"] = single_doctor_name

    # ── Canonical carry for continuation turns (state-driven) ──
    canonical_entities = dict(clean_entities)
    state_activity_ms = _date_parse(
        _first_truthy(_prop(state, "last_updated"), _prop(state, "updated_at"), _prop(state, "last_message_at"), "")
    )
    parsed_received_ms = _date_parse(_first_truthy(_prop(ctx, "received_at"), ""))
    current_activity_ms = (
        parsed_received_ms
        if (_is_finite_number(parsed_received_ms) and parsed_received_ms != 0)
        else _now_ms()
    )
    if _is_finite_number(state_activity_ms):
        state_conversation_fresh = (current_activity_ms - state_activity_ms) < 2 * 60 * 60 * 1000
    else:
        state_conversation_fresh = bool(prior_create_is_live)
    context_carry_allowed = (
        (not new_booking_restart)
        and state_conversation_fresh
        and (
            confirmation_intent_v3 == "affirmative"
            or turn_intent_v3 in ("small_talk", "greeting", "booking_continuation", "confirmation", "correction", "availability_inquiry")
            or operation_requested is False
            or relation_v3 in ("answer", "confirmation", "follow_up")
        )
    )
    if context_carry_allowed:
        for field in ("doctor_id", "doctor_name", "service_id", "service_name", "appointment_type", "date", "time", "slot_id", "branch_id", "branch_name"):
            if canonical_entities.get(field, _UNDEFINED) in (None, _UNDEFINED, "") and _js_truthy(booking_context.get(field)):
                canonical_entities[field] = booking_context[field]

    # ── Legacy v2 bridge projection (deterministic; consumed until Phase 3 replaces it) ──
    INTENT_TO_V2 = {
        "booking_request": "booking_request", "booking_continuation": "booking_continuation",
        "availability_inquiry": "availability_inquiry", "cancellation_request": "cancellation_request",
        "reschedule_request": "reschedule_request", "confirmation": "confirmation",
        "correction": "correction", "small_talk": "small_talk", "greeting": "small_talk",
        "clinic_query": "faq_inquiry", "unclear": "unclear", "other": "other",
    }
    RELATION_TO_V2 = {
        "new_request": "new_request", "answer": "answer", "confirmation": "confirmation",
        "correction": "correction", "change_details": "correction", "follow_up": "continuation",
        "none": "none", "unclear": "none",
    }
    CERTAINTY_TO_V2 = {"certain": "clear", "probable": "ambiguous", "uncertain": "uncertain"}
    ROUTING_BY_CLASS = {
        "cancel_request": "cancel", "reschedule_request": "reschedule", "selection_presented": "booking",
        "confirmation_affirm": "confirmation", "confirmation_negative": "confirmation",
        "confirmation_question": "confirmation", "booking": "booking", "availability_inquiry": "booking",
        "correction": "booking", "small_talk": "small_talk", "clinic_query": "faq", "unclear": "none",
    }
    v2_intent = INTENT_TO_V2.get(turn_intent_v3) or "unclear"
    v2_relation = RELATION_TO_V2.get(relation_v3) or "none"
    v2_certainty = CERTAINTY_TO_V2.get(_js_string(_first_truthy(_prop(turn_v3, "certainty"), "")) if turn_v3 is not None else "") or "uncertain"
    v2_routing = "escalation" if escalation_requested else (ROUTING_BY_CLASS.get(cls) or "none")
    confidence_raw = _js_number(_prop(turn_v3, "confidence") if turn_v3 is not None else _NAN)
    normalized_confidence = max(0.0, min(1.0, confidence_raw)) if _is_finite_number(confidence_raw) else None

    persona_clinic_query_type = _js_string(_first_truthy(_prop(persona_ctx, "clinic_query_type"), ""))
    if cls == "clinic_query":
        projected_query: Optional[Dict[str, Any]] = {
            "type": (
                "service_price"
                if persona_clinic_query_type == "service_price"
                else ("doctor_service" if persona_clinic_query_type in ("doctor_catalog", "doctor_fact") else "faq")
            )
        }
    elif cls == "availability_inquiry":
        projected_query = {
            "type": "availability",
            "date": _first_truthy(canonical_entities.get("date"), None),
            "time": _first_truthy(canonical_entities.get("time"), None),
        }
    else:
        projected_query = None
    query_scope = {
        "type": _coalesce(_prop(projected_query, "type"), None),
        "date": _coalesce(_prop(projected_query, "date"), None),
        "time": _coalesce(_prop(projected_query, "time"), None),
        "resolution_status": ("resolved" if (cls == "availability_inquiry" and _js_truthy(canonical_entities.get("date"))) else "unresolved"),
        "requires_resolution": False,
    }

    def _projected_next_step() -> Dict[str, Any]:
        if contract_v3 is None:
            return {"type": "none", "field": None}
        if cls == "confirmation_affirm":
            return {"type": "confirm_action", "field": None}
        if cls == "availability_inquiry":
            return {"type": "show_availability", "field": None if _js_truthy(canonical_entities.get("date")) else "date"}
        if cls == "clinic_query" or cls == "small_talk":
            return {"type": "provide_answer" if cls == "clinic_query" else "none", "field": None}
        if cls == "cancel_request" or cls == "reschedule_request":
            has_id = bool(_js_truthy(canonical_entities.get("appointment_id")) or _js_truthy(canonical_entities.get("booking_number")))
            return {"type": "confirm_action" if has_id else "ask_for_missing_information", "field": None if has_id else "booking_number"}
        if cls == "booking" or cls == "selection_presented":
            if not _js_truthy(_prop(booking_context, "appointment_type")):
                return {"type": "ask_for_missing_information", "field": "appointment_type"}
            for field in ("patient_name", "patient_age", "patient_phone", "patient_address"):
                value = _prop(booking_context, field)
                if value is None or value is _UNDEFINED or _js_string(value).strip() == "":
                    return {"type": "ask_for_missing_information", "field": field}
            if not _js_truthy(_prop(booking_context, "date")):
                return {"type": "ask_for_missing_information", "field": "date"}
            return {"type": "show_availability", "field": None}
        return {"type": "none", "field": None}

    projected_next_step = _projected_next_step()

    fallback_v3 = {
        "schema_version": "k2.dialogue.v3",
        "phase": "understand",
        "reply": None,
        "turn": {"intent": "unclear", "relation_to_previous_turn": "none", "certainty": "uncertain", "confidence": None},
        "confirmation": {"intent": "none"},
        "selection": {"kind": "none", "rank": None, "date": None, "time": None},
        "entities": {"doctor_name": None, "service_name": None, "date": None, "time": None, "visit_type": None, "patient_name": None, "patient_phone": None, "patient_age": None, "patient_address": None, "appointment_id": None, "booking_number": None},
        "operation_proposal": {"type": "", "requested": False},
        "references_prior_conversation": False,
        "escalate": False,
        "handoff_reason": None,
    }

    output_contract = {
        "schema_version": "k2.dialogue.v2",
        "turn": {
            "intent": v2_intent,
            "relation_to_previous_turn": v2_relation,
            "certainty": v2_certainty,
            "confidence": normalized_confidence,
            "answer_to": None,
        },
        "operation_proposal": {"type": operation_type_v3, "requested": operation_requested},
        "routing": {"target": v2_routing},
        "entities": canonical_entities,
        "query": projected_query,
        "confirmation": {"intent": confirmation_intent_v3, "target_operation": operation_type_v3 or None},
        "next_step": projected_next_step,
    }

    effective_model_call_status = "ERROR_FOLLOWUP_RECOVERED" if error_followup_recovered else model_call_status

    # ── Context + lineage ──
    current_turn_id = _js_string(_first_truthy(_prop(ctx, "message_id"), _prop(ctx, "source_event_id"), _prop(ctx, "idempotency_key"), "")).strip()
    current_turn_key = "|".join(
        _js_string(_coalesce(v, "")).strip()
        for v in (
            _prop(ctx, "clinic_id"),
            _first_truthy(_prop(ctx, "channel_key"), _js_string(_first_truthy(_prop(ctx, "channel_type"), "")) + ":" + _js_string(_first_truthy(_prop(ctx, "channel_id"), ""))),
            _prop(ctx, "conversation_id"),
            current_turn_id,
        )
    )
    current_message_fingerprint = _stable_hash(
        "|".join(_js_string(_coalesce(v, "")) for v in (_prop(ctx, "message_text"), _prop(ctx, "message_id"), _prop(ctx, "source_event_id"), _prop(ctx, "received_at")))
    )
    current_turn_lineage = {
        "schema_version": 3,
        "turn_id": current_turn_id or None,
        "turn_key": current_turn_key,
        "message_id": _first_truthy(_prop(ctx, "message_id"), None),
        "source_event_id": _first_truthy(_prop(ctx, "source_event_id"), None),
        "conversation_id": _first_truthy(_prop(ctx, "conversation_id"), None),
        "message_fingerprint": current_message_fingerprint,
        "created_at": _first_truthy(_prop(ctx, "received_at"), _utc_now_iso()),
    }
    normalized_message = _normalize_arabic_user_text(_js_string(_first_truthy(_prop(ctx, "message_text"), "")).strip())

    if agent_call_failed:
        contract_status = "MODEL_CALL_FAILED"
    elif validation["valid"]:
        contract_status = "VALID"
    elif repair_ran:
        contract_status = "INVALID_AFTER_REPAIR"
    else:
        contract_status = "REPAIR_NEEDED"

    result: Dict[str, Any] = {"temporal_claim_guard_active": temporal_claim_guard_active}
    result.update(item)
    result.update({
        "agent_reply": extracted_reply,
        "agent_raw_output": raw_output,
        "contract": output_contract,
        "contract_v3": contract_v3 if contract_v3 is not None else fallback_v3,
        "slot_state": slot_state,
        "booking_context": booking_context,
        "new_booking_restart": new_booking_restart,
        "availability_inquiry": availability_inquiry,
        "model_call_failed": agent_call_failed,
        "model_call_status": effective_model_call_status,
        "error_followup_recovered": error_followup_recovered,
        "escalation_requested": escalation_requested,
        "handoff_reason": handoff_reason,
        "_contract_status": contract_status,
        "_contract_errors": validation["errors"],
        "_contract_warnings": validation["warnings"],
        "_contract_repair_needed": repair_needed,
        "_repair_source": "self_repair" if repair_ran else "primary",
        "_normalization": {
            "schema_version": "k2.dialogue.v3",
            "valid": validation["valid"],
            "errors": validation["errors"],
            "warnings": validation["warnings"],
            "certainty": v2_certainty,
            "confidence": normalized_confidence,
            "turn_intent": v2_intent,
            "relation_to_previous_turn": v2_relation,
            "confirmation_intent": confirmation_intent_v3,
            "operation_type": operation_type_v3,
            "operation_requested": operation_requested,
            "routing_target": v2_routing,
            "next_step_type": _prop(projected_next_step, "type"),
            "next_step_field": _coalesce(_prop(projected_next_step, "field"), None),
            "message_class": cls,
            "model_turn_intent": turn_intent_v3,
            "model_relation": relation_v3,
            "invalid_temporal_input": invalid_date_input or invalid_time_input,
            "invalid_temporal_fields": [
                f for f in ("date" if invalid_date_input else None, "time" if invalid_time_input else None) if _js_truthy(f)
            ],
            "new_booking_restart": new_booking_restart,
            "escalation": escalation_requested,
            "booking_signal": turn_intent_v3 in ("booking_request", "booking_continuation"),
            "query_is_faq": _coalesce(_prop(projected_query, "type"), None) == "faq",
            "query_is_price": _coalesce(_prop(projected_query, "type"), None) == "service_price",
            "query_is_availability": availability_inquiry,
            "query_is_doctor_service": _coalesce(_prop(projected_query, "type"), None) == "doctor_service",
            "query_scope": query_scope,
            "turn_lineage": current_turn_lineage,
            "normalized_language_views": {"arabic": normalized_message},
            "model_call_failed": agent_call_failed,
            "model_call_status": effective_model_call_status,
            "repair_attempted": repair_ran,
            "repair_needed": repair_needed,
        },
    })
    return result
