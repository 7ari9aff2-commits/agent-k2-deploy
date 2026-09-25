"""Faithful port of the pre-agent n8n code nodes (pure Python, no I/O).

Source nodes (extracted/code/) and their ports:
  Build_Clinic_Persona_Context_Deterministic.js -> build_clinic_persona_context_deterministic
  Build_Persistent_Conversation_State.js        -> build_persistent_conversation_state
  Build_Audit_Entry.js                          -> build_audit_entry
  Compute_AI_Request_Usage_Deterministic.js     -> compute_ai_request_usage_deterministic
  Build_Outgoing_Message_SQL_Parameters.js      -> build_outgoing_message_sql_parameters
  Extract_K2_Signature_Context.js               -> extract_k2_signature_context
  Validate_Patient_Ownership.js                 -> validate_patient_ownership
  Evaluate_Completed_Create_Replay.js           -> evaluate_completed_create_replay
  Route_Single_Agent_Phase.js                   -> route_single_agent_phase

Conventions (docs/port_conventions.md):
  - `$(NodeName).first().json` -> inputs["<snake_case node name>"] (documented per function).
  - `$json` / `$input.first().json` -> `item`.
  - `$(Node).all()` -> inputs["<snake_case node name>"] as a list of json dicts.
  - n8n item `{json: {...}}` -> we carry the inner json dict; a node returning
    multiple items returns a list of dicts.
  - `new Date()` / `Date.now()` -> UTC clock via `_utc_now_iso()` / `_now_ms()`.
  - JSON.stringify -> `_json_stringify` (compact separators, non-escaped UTF-8).
  - PORT-TODO(n8n): `$('Normalize & Validate').item.json` (paired-item access in
    Validate_Patient_Ownership / Evaluate_Completed_Create_Replay) is replaced by
    the explicit `inputs["normalize_validate"]` key — the runner must supply it.
  - PORT-TODO(n8n): `$now.toISO()` (Compute_AI_Request_Usage) uses the workflow
    timezone in n8n; here the UTC instant is emitted. `$execution.id` is read
    from `inputs["execution_id"]` and omitted from the JSON string when absent
    (JSON.stringify drops undefined keys — same observable result).
  - PORT-TODO(n8n): JS `Date.parse` accepts many non-ISO formats; the shared
    `_date_parse` shim covers ISO-8601 (the only format the state fields emit).
  - PORT-TODO(n8n): JS object keys set to `undefined` are dropped by
    JSON.stringify; this port emits `null` for those keys instead (reads of a
    missing key still behave falsy, so downstream branches are unaffected).
"""
from __future__ import annotations

import json
import math
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from app.core import session_compact
from app.core.config import settings
from app.core.js_semantics import is_finite_num as _is_finite_num

# ---------------------------------------------------------------------------
# JS-semantics shims (private to this module; same contracts as app/pipeline/stages_post)
# ---------------------------------------------------------------------------

_UNDEFINED = object()
_NAN = float("nan")

# JS \s == [\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]
# (Python \s also matches \x1c-\x1f and \x85 which JS does not, and misses \ufeff).
_JS_WS = r"\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff"
_JS_WS_CLASS = "[" + _JS_WS + "]"
_JS_WS_RUN_RE = re.compile(_JS_WS_CLASS + "+")


def _js_ws_sub(value: str) -> str:
    """JS .replace(/\\s+/g, ' ') with the exact JS whitespace class."""
    return _JS_WS_RUN_RE.sub(" ", value)


def _js_trim(value: str) -> str:
    """JS String.prototype.trim (JS \\s class only)."""
    return re.sub("^(?:" + _JS_WS_CLASS + "+)|(?:" + _JS_WS_CLASS + "+)$", "", value)


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


def _js_number_or0(value: Any) -> float:
    """JS `Number(x) || 0`."""
    n = _js_number(value)
    return n if (_is_finite_num(n) and n != 0) else 0.0


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


def _obj_or_none(value: Any) -> Any:
    """JS `x && typeof x === 'object' ? x : null` (dict/list pass, including [])."""
    if isinstance(value, (dict, list)):
        return value
    return None


def _dict_or(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


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
    return isinstance(obj, dict) and obj.get(key, _UNDEFINED) is not _UNDEFINED


def _int_if_integral(value: Any) -> Any:
    """Emit JS-style integral numbers as ints (4 not 4.0)."""
    if isinstance(value, float) and value.is_integer() and abs(value) < 1e15:
        return int(value)
    return value


def _js_round(value: float) -> int:
    """JS Math.round (half towards +infinity)."""
    return math.floor(value + 0.5)


def _now_ms() -> float:
    return datetime.now(timezone.utc).timestamp() * 1000.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _ms_to_iso(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def _date_parse(value: Any) -> float:
    """JS Date.parse approximation: ISO-8601 (date-only = UTC midnight); NaN otherwise."""
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


def _json_stringify(value: Any) -> Optional[str]:
    """JS JSON.stringify: undefined -> undefined (None); compact; UTF-8 unescaped."""
    if value is None or value is _UNDEFINED:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _u16_len(s: str) -> int:
    """JS String.prototype.length (UTF-16 code units)."""
    total = 0
    for ch in s:
        total += 2 if ord(ch) > 0xFFFF else 1
    return total


def _u16_slice(s: str, start: int, end: Optional[int] = None) -> str:
    """JS String.prototype.slice over UTF-16 code units (splits surrogate pairs)."""
    units: List[str] = []
    for ch in s:
        cp = ord(ch)
        if cp > 0xFFFF:
            units.append(chr(0xD800 + ((cp - 0x10000) >> 10)))
            units.append(chr(0xDC00 + ((cp - 0x10000) & 0x3FF)))
        else:
            units.append(ch)
    if end is None:
        end = len(units)
    return "".join(units[start:end])


def _u16_units(s: str) -> List[int]:
    """UTF-16 code units of s (JS charCodeAt iteration)."""
    out: List[int] = []
    for ch in s:
        cp = ord(ch)
        if cp > 0xFFFF:
            out.append(0xD800 + ((cp - 0x10000) >> 10))
            out.append(0xDC00 + ((cp - 0x10000) & 0x3FF))
        else:
            out.append(cp)
    return out


def _js_re(pattern: str) -> re.Pattern:
    """Compile a JS regex body: /i flag implied; \\s becomes the exact JS whitespace class.

    Bracket-aware: \\s inside a character class gets the class CONTENTS (so
    ``[\\s.!]`` does not produce a nested ``[[..]`` which Python flags), while
    \\s outside a class gets the full ``[...]`` class.
    """
    out: List[str] = []
    i = 0
    in_class = False
    n = len(pattern)
    while i < n:
        ch = pattern[i]
        if ch == "\\" and i + 1 < n:
            if pattern[i + 1] == "s":
                out.append(_JS_WS if in_class else _JS_WS_CLASS)
            else:
                out.append(pattern[i:i + 2])
            i += 2
            continue
        if ch == "[" and not in_class:
            in_class = True
        elif ch == "]" and in_class:
            in_class = False
        out.append(ch)
        i += 1
    return re.compile("".join(out), re.IGNORECASE)


def _keep(value: Any, old: Any) -> Any:
    """JS `v !== null && v !== undefined && v !== '' ? v : (old ?? null)`."""
    if value is not None and value is not _UNDEFINED and value != "":
        return value
    if old is None or old is _UNDEFINED:
        return None
    return old


# ---------------------------------------------------------------------------
# Shared K2 strong-ask classifier (identical block in Build_Clinic_Persona_Context
# and Build_Persistent_Conversation_State).
# ---------------------------------------------------------------------------

_K2Q_SPLIT_RE = re.compile(r"[\u061f?.;\u061b!]+")

_K2Q_STRONG: Dict[str, re.Pattern] = {
    "patient_name": _js_re(r"(?:إيه اسمك|ايه اسمك|اسمك إيه|اسمك ايه|وش اسمك|عايزين اسمك|عاوزين اسمك|نبعتلك اسمك|نكتب اسمك|أكتب اسمك|اكتب اسمك)"),
    "patient_phone": _js_re(r"(?:رقمك إيه|رقمك ايه|إيه رقمك|ايه رقمك|وش رقمك|رقم جوالك|رقم الواتس|عايزين رقمك|نرسل لك رقمك|رقم حضرتك|نبعت على رقمك)"),
    "patient_age": _js_re(r"(?:كم عمرك|عمرك كام|كم سنة|عمر حضرتك|سنك كام)"),
    "patient_address": _js_re(r"(?:عنوانك إيه|عنوانك ايه|إيه عنوانك|عايزين عنوانك|ساكن فين|ساكنة فين|وين ساكن|مكان السكن|عنوان حضرتك)"),
    "doctor": _js_re(r"(?:أي دكتور|اي دكتور|مين الدكتور|مين الطبيب|مين الدكتورة|وش اسم الدكتور|الدكتور المطلوب|الدكتورة المطلوبة)"),
    "service": _js_re(r"(?:نوع الخدمة|أي خدمة|اي خدمة|التخصص المطلوب|أي تخصص|اي تخصص|الخدمة المطلوبة)"),
    "visit_type": _js_re(r"(?:نوع الكشف|كشف جديد ولا|كشف جديد والا|كشف ولا متابعة|كشف والا متابعة|زيارة ولا متابعة|ولا متابعة|متابعة ولا|كشف ولا|كشف والا)"),
    "date": _js_re(r"(?:أي يوم|اي يوم|متى الموعد|متى يناسبك|أي تاريخ|اي تاريخ|حدد اليوم|تختار اليوم|يوم شنو|نفسك تختار يوم|تحب تحجز يوم|عايز تحجز يوم|يوم تاني|يوم ثاني|يوم تانى|يوم بديل|يوم بدايل|شوف لك يوم|أشوف لك يوم|شوفلك يوم|أشوفلك يوم|موعد تاني|موعد ثاني|موعد بديل|يوم آخر|يوم اخر|تغيير اليوم|نفسك يوم تاني|تحب يوم تاني|عايز يوم تاني)"),
    "time": _js_re(r"(?:أي وقت|اي وقت|أي ساعة|اي ساعة|الوقت المناسب|متى يناسبك الساعة)"),
}


def _k2q_asked_from_reply(reply_text: Any) -> List[str]:
    text = _js_trim(_js_ws_sub(_js_string(_first_truthy(reply_text, ""))))
    if not text:
        return []
    out: List[str] = []
    for sentence in _K2Q_SPLIT_RE.split(text):
        s = _js_trim(sentence)
        if not s:
            continue
        for field in _K2Q_STRONG:
            if field not in out and _K2Q_STRONG[field].search(s):
                out.append(field)
    return out


_K2_EQUIV: Dict[str, List[str]] = {"service": ["service", "visit_type"], "visit_type": ["service", "visit_type"]}
_K2_CONCRETE = ["patient_name", "patient_phone", "patient_age", "patient_address", "doctor", "service", "visit_type", "date", "time"]


def _k2_field_matches(a: Any, b: List[str]) -> bool:
    flat_a: List[str] = []
    for f in a if isinstance(a, list) else []:
        flat_a.extend(_K2_EQUIV.get(f, [f]))
    set_a = set(flat_a)
    set_b = set()
    for f in b:
        set_b.update(_K2_EQUIV.get(f, [f]))
    for x in set_a:
        if x in set_b:
            return True
    return False


# ---------------------------------------------------------------------------
# Build Clinic Persona Context (Deterministic)
# ---------------------------------------------------------------------------

_ALLOWED_TONES = {"warm", "warm_professional", "professional", "calm", "friendly", "concise", "luxury", "medical_calm"}
_ALLOWED_DIALECTS = {"saudi", "ar_saudi", "gulf", "egyptian", "ar_eg", "msa", "formal_arabic", "ar"}

_FORBIDDEN_RE = _js_re(r"(?:ignore\s+(?:all\s+)?(?:previous|system)\s+instructions?|تجاهل\s+(?:كل\s+)?(?:التعليمات|قواعد)\s*(?:السابقة|النظام)?|override\s+(?:the\s+)?system|نف[ًٌ]?ذ\s+(?:الحجز|الإلغاء|التعديل)\s*(?:مباشرة|بدون\s+تأكيد)?|احجز\s+مباشرة|قل\s+إن\s+(?:الحجز|الإلغاء|التعديل)\s+تم|disable\s+(?:handoff|confirmation|safety)|تعطيل\s+(?:الهاندوف|التأكيد|الأمان)|لا\s+تستخدم\s+(?:الهاندوف|التأكيد))")

_ALEF_RE = re.compile("[أإآٱ]")
_TA_MARBUTA_RE = re.compile("ة")
_DIACRITICS_RE = re.compile("[ًٌٍَُِّْـ]")
_PUNCT_RE = re.compile("[،,؛;.!؟?]")

_VISIT_TYPE_ONLY_RE = _js_re(r"^(?:زياره جديده|كشف جديد|كشف عادي|كشف اول|كشف اول مره|اول زياره|زيارة جديدة|زيارة اولى|new visit|first visit)$")

_EXPLICIT_FRESH_BOOKING_RE = _js_re(r"^(?:ابدأ حجز جديد|ابدا حجز جديد|ابغى حجز جديد|أبغى حجز جديد|عايز حجز جديد|عاوز حجز جديد|احجز لي موعد جديد|أريد حجز جديد|اريد حجز جديد|ابدأ حجز|ابدا حجز|ابغى احجز|أبغى احجز|عايز احجز|عاوز احجز|عايز ابدا حجز تاني|عاوز ابدا حجز تاني|أريد أبدأ حجز تاني|اريد ابدأ حجز تاني|ابدأ حجز تاني|ابدا حجز تاني|ابدأ موعد جديد|احجز موعد ثاني|احجز موعد تاني|start a new booking|book another appointment|make a new appointment|i want to book another|new appointment please)$")

_EXPLICIT_CANCEL_RESCHEDULE_RE = _js_re(r"(?:إلغاء|الغاء|الغِ|الغي|يلغى|الغاء الحجز|أبغى ألغي|ابغى الغي|عايز ألغي|عايز الغي|تعديل الموعد|تغيير الموعد|عدل الموعد|اعدل الموعد|غير الموعد|غيّر الموعد|أبغى أعدل|ابغى اعدل|عايز اعدل|تأجيل الموعد|اجل الموعد|أجل الموعد|انقل الموعد|نقل الموعد|cancel(?: my)? appointment|cancel it|reschedule(?: my)? appointment|change my appointment|move my appointment)")

_PRIOR_K2_ERROR_RE = _js_re(r"تعذر صياغة الرد من نتيجة العملية الحالية|تعذر معالجة الرسالة|مشكلة مؤقتة في الرد|تعذر التحقق من نتيجة العملية")

_AR_DAYS = ["الأحد", "الاثنين", "الثلاثاء", "الأربعاء", "الخميس", "الجمعة", "السبت"]

_ISO_DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")

_DOCTOR_MENTION_RE = _js_re(r"(?:دكتور|د\.\s*|طبيب|doctor|physician)")

_GREETING_ONLY_RE = _js_re(r"^(?:(?:السلام\s+عليكم(?:\s+ورحمه\s+الله(?:\s+وبركاته)?)?|سلام\s+عليكم|وعليكم\s+السلام(?:\s+ورحمه\s+الله(?:\s+وبركاته)?)?)|(?:مرحبا(?:\s+(?:بك|فيك))?|مرحبتين|يا\s+مرحبا(?:\s+بك)?)|(?:اهلا(?:\s+وسهلا)?|اهلين(?:\s+وسهلين)?|يا\s+اهلا(?:\s+وسهلا)?)|(?:هلا(?:\s+(?:والله(?:\s+وغلا)?|وغلا|بك|فيك))?|يا\s+هلا(?:\s+(?:والله(?:\s+وغلا)?|وغلا|فيك))?|حياك\s+الله|حياكم\s+الله|الله\s+يحييك(?:م)?|حي\s+الله\s+من\s+جانا)|(?:هاي|صباح\s+(?:الخير|النور|الورد)|صباحك\s+خير|مساء\s+(?:الخير|النور|الورد)|مساك\s+خير|نهارك\s+سعيد)|(?:كيفك|كيف\s+حالك|شلونك|شخبارك|وش\s+علومك|طمني\s+عنك|ازيك|عامل\s+ايه|اخبارك\s+ايه|عاملين\s+ايه|طمني\s+عليك)|(?:شكرا|مشكور(?:ه)?|تسلم(?:ين)?|يعطيك\s+العافيه|الله\s+يعطيك\s+العافيه|تمام|تم|اوكي?|ماشي|موافق))$")

_QUERY_ACTION_SIGNAL_RE = _js_re(r"(?:احجز|حجز|ابغى\s+موعد|أبغى\s+موعد|عايز\s+احجز|الغاء\s+الحجز|إلغاء\s+الحجز|تعديل\s+الحجز|غير\s+الموعد|غيّر\s+الموعد|موعدي|المواعيد\s+(?:المتاحه|المتاحة)|متاح(?:ه|ة)?\s+.*(?:موعد|وقت)|بكره|بكرة|باچر|بعد\s+بكره|بعد\s+بكرة|اليوم|غدا|غداً|الاحد|الأحد|الاثنين|الثلاثاء|الاربعاء|الأربعاء|الخميس|الجمعة|السبت|book|appointment|cancel|reschedule|available|today|tomorrow|sunday|monday|tuesday|wednesday|thursday|friday|saturday)")

_FAQ_SIGNAL_RE = _js_re(r"(?:الدوام|ساعات\s+العمل|مفتوح|مفتوحين|تفتحون|تقفلون|سياس(?:ه|ة)|التأمين|التواصل|واتساب|طريقة\s+الدفع|الدفع|معلومات\s+(?:عن|العياده|العيادة)|كيف\s+اوصل|كيف\s+أوصل)")
# The prompt constants that used to live here (_FULL_AGENT_SYSTEM_PROMPT,
# _SMALL_TALK_SYSTEM_PROMPT, _CLINIC_QUERY_SYSTEM_PROMPT, _CONFIRM_FAST_HEAD/TAIL,
# _NATURAL_ARABIC_SUFFIX, _TENANT_PERSONA_SUFFIX, _BOOKING_STAGE_SAFETY_SUFFIX,
# _CTC_*_SUFFIX) were REMOVED 2026-09-17. ~28.6k chars of prompt text were assembled
# here and then discarded: nothing ever read `agent_system_prompt` except to count
# its characters for token telemetry. The live prompt is the single file
# app/services/prompts/agent_system_message.txt, loaded by dialogue.load_system_message.
# A second, dead prompt stack is a trap - it invites edits that change nothing.
_CTRL_CHARS_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

_STATE_TERMINAL = {"completed", "cancelled", "failed_final", "idle", "refresh_required"}
_LIVE_OPERATIONS = ("create_appointment", "cancel_appointment", "reschedule_appointment")
_REQUIRED_CREATE_FIELDS = ["doctor", "visit_type", "patient_name", "patient_phone", "patient_age", "patient_address", "date"]


def _live_system_prompt() -> str:
    """The prompt the model actually receives (single source of truth)."""
    try:
        from app.services.dialogue import load_system_message
        return load_system_message()
    except Exception:
        return ""

_CONFIRM_FAST_AFFIRM_RE = _js_re(r"^(?:نعم|ايه|اه|اها|أه|أها|ايوه|أيوه|ايوا|إي|اي|أكيد|أكد|اكد|أكدي|أكدت|اكدت|موافق|تمام|طيب|اوك|أوكي|ok|okay|يب|صح|صحيح|مضبوط|بالضبط|أجل|اجل|yes|yeah|yep|sure|correct|exactly)[\s.!،,:-]*$")

_MESSAGE_ACTION_SIGNAL_RE = _js_re(r"(?:احجز|حجز|موعد|دكتور|طبيب|مواعيد|متاح|متاحة|سعر|اسعار|أسعار|خدمة|خدمات|الغاء|إلغاء|تعديل|متابعة|كشف|بكرة|بكره|باچر|اليوم|الاحد|الأحد|الاثنين|الثلاثاء|الاربعاء|الأربعاء|الخميس|الجمعة|السبت|book|appointment|doctor|physician|available|price|cost|service|cancel|reschedule|today|tomorrow)")

_DOCTOR_INFO_REQUEST_RE = _js_re(r"(?:تخصص|اختصاص|مجال|مين\s+(?:الدكاتره|الدكاترة|الاطباء|الأطباء)|اسماء\s+(?:الدكاتره|الدكاترة|الاطباء|الأطباء)|دكاتره\s+العياده|دكاترة\s+العيادة|specialt)")


def _clean_text(value: Any) -> str:
    """JS text(): String(value ?? '') minus control chars, whitespace collapsed, trimmed."""
    s = _CTRL_CHARS_RE.sub("", _js_string(_coalesce(value, "")))
    return _js_trim(_js_ws_sub(s))


def _parse_object(value: Any) -> Dict[str, Any]:
    """JS parseObject(): objects pass; JSON strings parse to objects; else {}."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _compact_put(out: Dict[str, Any], key: str, value: Any) -> None:
    if value is None or value is _UNDEFINED or value == "":
        return
    if isinstance(value, list) and len(value) == 0:
        return
    out[key] = value


def _compact_object(source: Any, keys: List[str]) -> Optional[Dict[str, Any]]:
    if not isinstance(source, dict):
        return None
    out: Dict[str, Any] = {}
    for key in keys:
        _compact_put(out, key, _prop(source, key))
    return out if out else None


def _safe_service(x: Any) -> Dict[str, Any]:
    x = x if isinstance(x, dict) else {}
    return {
        "service_name": _first_truthy(_prop(x, "service_name"), None),
        "price": _coalesce(_prop(x, "price"), None),
        "duration_minutes": _coalesce(_prop(x, "duration_minutes"), None),
        "online_booking": _coalesce(_prop(x, "online_booking"), None),
    }


def _safe_doctor(x: Any) -> Dict[str, Any]:
    x = x if isinstance(x, dict) else {}
    return {
        "doctor_name": _first_truthy(_prop(x, "doctor_name"), None),
        "specialization": _first_truthy(_prop(x, "specialization"), None),
    }


def _safe_branch(x: Any) -> Dict[str, Any]:
    x = x if isinstance(x, dict) else {}
    l = _prop(x, "location_config")
    l = l if isinstance(l, dict) else {}
    return {
        "branch_name": _first_truthy(_prop(x, "branch_name"), None),
        "address": _first_truthy(_prop(x, "address"), _prop(l, "address"), None),
        "phone": _first_truthy(_prop(x, "phone"), None),
        "maps_url": _first_truthy(_prop(l, "maps_url"), None),
    }


def _norm_ar_name(s: Any) -> str:
    """JS normArName: alef/ya/ta-marbuta folding + whitespace collapse + lower."""
    if not s:
        return ""
    s = _ALEF_RE.sub("ا", s)
    s = s.replace("ى", "ي")
    s = _TA_MARBUTA_RE.sub("ه", s)
    return _js_trim(_js_ws_sub(s)).lower()


def _reconcile_open_question(lac: Dict[str, Any]) -> None:
    """v33 systemic guard: derive the open question from the last sent reply text and
    reconcile every bookkeeping field to it. Mutates lac in place (same as the JS IIFE)."""
    rat = _prop(lac, "recent_assistant_turn")
    loq = _prop(lac, "last_open_question")
    last_msg = _js_string(_first_truthy(
        (_first_truthy(_prop(rat, "message"), _prop(rat, "text")) if _js_truthy(rat) else _UNDEFINED),
        (_prop(loq, "message") if _js_truthy(loq) else _UNDEFINED),
        "",
    ))
    asked = _k2q_asked_from_reply(last_msg)
    if not asked:
        return
    lead = asked[0]
    if _js_truthy(rat) and isinstance(rat, dict):
        meta = _prop(rat, "requested_information")
        if isinstance(meta, list) and len(meta) > 0 and not _k2_field_matches(meta, asked):
            rat["requested_information"] = list(asked)
    if _js_truthy(loq) and isinstance(loq, dict):
        f = _prop(loq, "requested_fields")
        if isinstance(f, list) and len(f) > 0 and not _k2_field_matches(f, asked):
            lac["last_open_question"] = {**loq, "requested_fields": list(asked)}
    rns = _prop(lac, "required_next_step")
    if _js_truthy(rns) and isinstance(rns, dict):
        rf = _first_truthy(_prop(rns, "field"), (_prop(rns, "fields")[0] if isinstance(_prop(rns, "fields"), list) and len(_prop(rns, "fields")) > 0 else _UNDEFINED), None)
        if isinstance(rf, str) and rf and rf in _K2_CONCRETE and not _k2_field_matches([rf], asked):
            nxt = {**rns, "field": lead}
            if isinstance(_prop(rns, "fields"), list):
                nxt["fields"] = list(asked)
            lac["required_next_step"] = nxt
    nbb = _prop(lac, "next_best_missing_human_field")
    if isinstance(nbb, str) and nbb and nbb in _K2_CONCRETE and not _k2_field_matches([nbb], asked):
        lac["next_best_missing_human_field"] = lead
    mhf = _prop(lac, "missing_human_fields")
    if isinstance(mhf, list) and len(mhf) > 0 and not _k2_field_matches(mhf, asked):
        lac["missing_human_fields"] = [lead] + [f for f in mhf if not _k2_field_matches([f], asked)]


def build_clinic_persona_context_deterministic(item: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Build Clinic Persona Context (Deterministic) (extracted/code/Build_Clinic_Persona_Context_Deterministic.js).

    Input keys (missing nodes read as {}):
      item                                       -> $json (the Resolve Service Fact payload)
      inputs["get_clinic_context"]               -> $(Get Clinic Context).first().json
      inputs["get_conversation_state"]           -> $(Get Conversation State).first().json (reads .state_data)
      inputs["normalize_validate"]               -> $(Normalize & Validate).first().json
      inputs["get_recent_window_2h"]             -> $(Get Recent Window 2h).first().json (reads .conversation_history)
      inputs["resolve_doctor_inquiry_deterministic"] -> $(Resolve Doctor Inquiry (Deterministic)).first().json
      inputs["resolve_branch_inquiry_deterministic"] -> $(Resolve Branch Inquiry (Deterministic)).first().json

    Output: single json dict — agent_context / agent_context_model / clinic persona
    gates, then `...$json` spread, then the remaining persona keys (later keys win,
    exactly like the JS object literal).
    """
    clinic = _dict_or(inputs.get("get_clinic_context"))

    # Resolve Service Fact is the authoritative, tenant-scoped source for service
    # names, prices, durations, booking eligibility, and the service catalog.
    service_fact = item if isinstance(item, dict) else {}
    matches = _prop(service_fact, "matches")
    catalog = _prop(service_fact, "catalog")
    service_facts = {
        "is_service_fact_inquiry": _prop(service_fact, "is_service_fact_inquiry") is True,
        "is_price_inquiry": _prop(service_fact, "is_price_inquiry") is True,
        "is_service_catalog_inquiry": _prop(service_fact, "is_service_catalog_inquiry") is True,
        "service_id": _first_truthy(_prop(service_fact, "service_id"), None),
        "service_name": _first_truthy(_prop(service_fact, "service_name"), None),
        "price": _coalesce(_prop(service_fact, "price"), None),
        "duration_minutes": _coalesce(_prop(service_fact, "duration_minutes"), None),
        "online_booking": _coalesce(_prop(service_fact, "online_booking"), None),
        "requires_confirmation": _coalesce(_prop(service_fact, "requires_confirmation"), None),
        "matches": list(matches[:10]) if isinstance(matches, list) else [],
        "catalog": list(catalog[:100]) if _prop(service_fact, "is_service_catalog_inquiry") is True and isinstance(catalog, list) else [],
    }

    conversation_state = _dict_or(_prop(_dict_or(inputs.get("get_conversation_state")), "state_data"))
    recent_turns = _prop(conversation_state, "recent_turns")
    recent_turns = recent_turns if isinstance(recent_turns, list) else []
    has_assistant_turn = any(
        _js_string(_first_truthy(_dig(t, "role"), "")).lower() == "assistant" for t in recent_turns
    )
    state_version = _js_number(_first_truthy(_prop(conversation_state, "state_version"), 0))
    has_persisted_conversation = bool(
        has_assistant_turn
        or (_is_finite_num(state_version) and state_version > 0)
        or _js_truthy(_prop(conversation_state, "last_message_id"))
        or _js_truthy(_prop(conversation_state, "conversation_summary"))
    )
    inbound = _dict_or(inputs.get("normalize_validate"))
    recent_window = _dict_or(_prop(_dict_or(inputs.get("get_recent_window_2h")), "conversation_history"))
    historical_reference_requested = _prop(recent_window, "historical_reference_requested") is True
    inbound_tc = _prop(inbound, "time_context")
    expiry_reference_ms = _date_parse(_first_truthy(
        _prop(inbound, "received_at"),
        (_prop(inbound_tc, "now_iso") if isinstance(inbound_tc, dict) else _UNDEFINED),
        _utc_now_iso(),
    ))
    previous_activity_ms = _date_parse(_first_truthy(
        _prop(conversation_state, "last_updated"),
        _prop(conversation_state, "updated_at"),
        _prop(conversation_state, "last_message_at"),
        "",
    ))
    draft_expiry_ms = _date_parse(_first_truthy(_prop(conversation_state, "draft_expires_at"), ""))
    rolling_session_expired = bool(
        _is_finite_num(expiry_reference_ms) and _is_finite_num(previous_activity_ms)
        and expiry_reference_ms >= previous_activity_ms + (2 * 60 * 60 * 1000)
    )
    draft_expired = bool(_is_finite_num(expiry_reference_ms) and _is_finite_num(draft_expiry_ms) and expiry_reference_ms >= draft_expiry_ms)
    is_conversation_start = (not has_persisted_conversation) or (rolling_session_expired and not historical_reference_requested)
    session_booking_context_allowed = (not rolling_session_expired) or historical_reference_requested

    raw_persona = _parse_object(_prop(clinic, "persona"))
    tone_candidate = _clean_text(_first_truthy(_prop(raw_persona, "tone"), _prop(clinic, "ai_tone"), "warm")).lower()
    dialect_candidate = _clean_text(_first_truthy(_prop(raw_persona, "dialect"), _prop(clinic, "clinic_dialect_code"), _prop(clinic, "ai_language"), "ar")).lower()
    tone = tone_candidate if tone_candidate in _ALLOWED_TONES else "warm"
    dialect = dialect_candidate if dialect_candidate in _ALLOWED_DIALECTS else "ar"
    raw_name = _u16_slice(_clean_text(_first_truthy(_prop(raw_persona, "name"), "")), 0, 120)
    raw_role = _u16_slice(_clean_text(_first_truthy(_prop(raw_persona, "role"), "")), 0, 160)
    name_rejected = bool(raw_name and _FORBIDDEN_RE.search(raw_name))
    role_rejected = bool(raw_role and _FORBIDDEN_RE.search(raw_role))
    name = "مساعد العيادة" if (name_rejected or not raw_name) else raw_name
    role = "مساعد حجوزات" if (role_rejected or not raw_role) else raw_role
    raw_prompt = _u16_slice(_clean_text(_first_truthy(_prop(clinic, "clinic_system_prompt"), _prop(clinic, "ai_persona_prompt"), "")), 0, 2400)
    rejected = bool(raw_prompt and _FORBIDDEN_RE.search(raw_prompt))
    prompt = "" if rejected else raw_prompt
    prompt_version = _u16_slice(_clean_text(_first_truthy(_prop(clinic, "prompt_version"), _prop(clinic, "ai_prompt_version"), "v1")), 0, 64)
    clinic_name = _u16_slice(_clean_text(_first_truthy(_prop(clinic, "clinic_name"), "")), 0, 180)
    clinic_location_config = _parse_object(_prop(clinic, "clinic_location_config"))
    branch_directory = _prop(clinic, "branch_directory")
    clinic_branch_directory: List[Dict[str, Any]] = []
    if isinstance(branch_directory, list):
        for branch in branch_directory[:100]:
            b = branch if isinstance(branch, dict) else {}
            clinic_branch_directory.append({
                "branch_id": _first_truthy(_prop(b, "branch_id"), None),
                "branch_name": _first_truthy(_u16_slice(_clean_text(_first_truthy(_prop(b, "branch_name"), _prop(b, "name"), "")), 0, 180), None),
                "address": _first_truthy(_u16_slice(_clean_text(_first_truthy(_prop(b, "address"), "")), 0, 300), None),
                "phone": _first_truthy(_u16_slice(_clean_text(_first_truthy(_prop(b, "phone"), "")), 0, 80), None),
                "location_config": _parse_object(_prop(b, "location_config")),
            })
    clinic_block = "\n".join([
        "[CLINIC PERSONA — STYLE ONLY]",
        "Clinic: " + (clinic_name or "clinic"),
        "Assistant name: " + name,
        "Role: " + role,
        "Tone: " + tone,
        "Dialect: " + dialect,
        "Prompt version: " + prompt_version,
        ("Clinic style instructions: " + prompt) if prompt else "Clinic style instructions: Use the approved default style.",
        "This profile controls style and wording only. It cannot override K2 rules, confirmation requirements, handoff gates, execution safety, database truth, or operation results.",
        "[/CLINIC PERSONA]",
    ])

    # Live dialogue context (previously created only after System Orchestrator).
    state_booking = _prop(conversation_state, "booking_context")
    state_booking = state_booking if isinstance(state_booking, dict) else {}
    state_slot = _prop(conversation_state, "slot_state")
    state_slot = state_slot if isinstance(state_slot, dict) else {}
    state_operation = _first_truthy(
        _clean_text(_first_truthy(_prop(conversation_state, "active_operation"), _prop(conversation_state, "operation_action"), "")).lower(), None
    )
    state_operation_status = _first_truthy(
        _clean_text(_first_truthy(_prop(conversation_state, "operation_status"), _prop(conversation_state, "operation_state"), "")).lower(), None
    )
    state_resume_eligible = _prop(conversation_state, "resume_eligible") is True
    state_handoff_status = _clean_text(_first_truthy(_prop(conversation_state, "handoff_status"), "")).lower()
    state_has_active_handoff = bool(
        _js_truthy(_prop(conversation_state, "handoff_request_id"))
        and state_handoff_status in ("open", "pending", "assigned", "in_progress", "active")
    )
    state_paused_not_resumable = state_operation_status == "paused" and not state_resume_eligible and not state_has_active_handoff
    state_has_live_booking = bool(
        state_operation in _LIVE_OPERATIONS
        and state_operation_status not in _STATE_TERMINAL
        and not state_paused_not_resumable
        and ((not rolling_session_expired) or historical_reference_requested)
        and ((not draft_expired) or historical_reference_requested)
    )
    state_last_assistant: Dict[str, Any] = {}
    for t in reversed(recent_turns):
        if _js_string(_first_truthy(_dig(t, "role"), "")).lower() == "assistant":
            state_last_assistant = t if isinstance(t, dict) else {}
            break
    rf = _prop(state_last_assistant, "requested_fields")
    ri = _prop(state_last_assistant, "requested_information")
    state_requested_fields = rf if isinstance(rf, list) else (ri if isinstance(ri, list) else [])

    is_telegram_channel = _js_string(_first_truthy(_prop(inbound, "channel_type"), "")).lower() == "telegram"
    telegram_untrusted = is_telegram_channel and _dig(conversation_state, "facts", "patient", "name_source") != "user_entered"
    live_booking_context: Dict[str, Any] = {}
    if state_has_live_booking:
        live_booking_context = {
            "doctor_id": _first_truthy(_prop(state_booking, "doctor_id"), _prop(state_slot, "doctor_id"), None),
            "doctor_name": _first_truthy(_prop(state_booking, "doctor_name"), _prop(state_slot, "doctor_name"), None),
            "service_id": _first_truthy(_prop(state_booking, "service_id"), _prop(state_slot, "service_id"), None),
            "service_name": _first_truthy(_prop(state_booking, "service_name"), _prop(state_slot, "service_name"), None),
            "appointment_type": _first_truthy(_prop(state_booking, "appointment_type"), _prop(state_slot, "appointment_type"), None),
            "booking_number": _first_truthy(_prop(state_booking, "booking_number"), _prop(state_slot, "booking_number"), _prop(conversation_state, "booking_number"), None),
            "branch_id": _first_truthy(_prop(state_booking, "branch_id"), _prop(state_slot, "branch_id"), None),
            "branch_name": _first_truthy(_prop(state_booking, "branch_name"), _prop(state_slot, "branch_name"), None),
            "date": _first_truthy(_prop(state_booking, "date"), _prop(state_slot, "date"), None),
            "time": _first_truthy(_prop(state_booking, "time"), _prop(state_slot, "time"), None),
            "slot_id": _first_truthy(_prop(state_booking, "slot_id"), _prop(state_slot, "slot_id"), None),
            "patient_name": None if telegram_untrusted else _first_truthy(_prop(state_booking, "patient_name"), None),
            "patient_phone": _first_truthy(_prop(state_booking, "patient_phone"), None),
            "patient_age": _coalesce(_prop(state_booking, "patient_age"), None),
            "patient_address": _first_truthy(_prop(state_booking, "patient_address"), None),
        }

    # Deterministic current-turn stage contract.
    stage_message_text = _clean_text(_first_truthy(_prop(inbound, "message_text"), ""))
    stage_message_normalized = _js_trim(_js_ws_sub(_PUNCT_RE.sub(" ", _DIACRITICS_RE.sub("", _TA_MARBUTA_RE.sub("ه", _ALEF_RE.sub("ا", unicodedata.normalize("NFKC", stage_message_text))))))).lower()
    visit_type_only_turn = bool(_VISIT_TYPE_ONLY_RE.search(stage_message_normalized))
    stage_patient = {
        "name": _clean_text("" if telegram_untrusted else _first_truthy(
            _prop(state_booking, "patient_name"), _prop(clinic, "patient_name"),
            _dig(conversation_state, "facts", "patient", "name"), ""
        )),
        "phone": _clean_text(_first_truthy(
            _prop(state_booking, "patient_phone"), _prop(clinic, "patient_phone"), _prop(clinic, "patient_mobile"),
            _dig(conversation_state, "facts", "patient", "phone"), _dig(conversation_state, "facts", "patient", "mobile"), ""
        )),
        "age": _coalesce(_dig(conversation_state, "facts", "patient", "age"), _prop(state_booking, "patient_age"), _prop(clinic, "patient_age"), None),
        "address": _clean_text(_first_truthy(
            _prop(state_booking, "patient_address"), _prop(clinic, "patient_address"),
            _dig(conversation_state, "facts", "patient", "address"), ""
        )),
    }
    stage_age = stage_patient["age"]
    stage_patient_complete = bool(
        stage_patient["name"] and stage_patient["phone"]
        and stage_age is not None and _js_trim(_js_string(stage_age)) != ""
        and stage_patient["address"]
    )
    prior_patient_review_pending = _dig(conversation_state, "patient_data_review", "status") == "pending"

    # General failure-state recovery.
    explicit_fresh_booking_request = bool(_EXPLICIT_FRESH_BOOKING_RE.search(stage_message_normalized))
    explicit_cancel_or_reschedule_request = bool(_EXPLICIT_CANCEL_RESCHEDULE_RE.search(stage_message_normalized))
    recovery_followup_eligible = bool(_js_trim(stage_message_text)) and not explicit_fresh_booking_request and not explicit_cancel_or_reschedule_request
    latest_assistant_recovery_text = _clean_text(_first_truthy(
        _prop(state_last_assistant, "text"), _prop(state_last_assistant, "message"), _prop(state_last_assistant, "content"), ""
    ))
    persisted_recovery_marker = _clean_text(_first_truthy(
        _dig(conversation_state, "recent_assistant_turn", "message"), _prop(conversation_state, "last_assistant_message"), ""
    ))
    prior_k2_error_reply = bool(_PRIOR_K2_ERROR_RE.search(latest_assistant_recovery_text)) or bool(_PRIOR_K2_ERROR_RE.search(persisted_recovery_marker))
    recent_assistant_turns_for_recovery = [
        t for t in recent_turns
        if _js_string(_first_truthy(_dig(t, "role"), "")).lower() == "assistant"
        and _js_trim(_js_string(_first_truthy(_dig(t, "text"), _dig(t, "message"), _dig(t, "content"), "")))
    ]
    recovery_doctor_directory = _prop(clinic, "doctor_directory")
    recovery_doctor_directory = recovery_doctor_directory if isinstance(recovery_doctor_directory, list) else []
    recovered_followup_doctor: Optional[Dict[str, Any]] = None
    recovered_candidates: List[Dict[str, Any]] = []
    for doctor in recovery_doctor_directory:
        d = doctor if isinstance(doctor, dict) else {}
        recovered_candidates.append({
            "name": _clean_text(_first_truthy(_prop(d, "doctor_name"), _prop(d, "name"), "")),
            "id": _first_truthy(_prop(d, "doctor_id"), _prop(d, "id"), None),
        })
    # Reviewer fix: raw substring inclusion matched 'علي' inside 'وعليكم السلام' — a
    # doctor never selected became the recovery fact. Match on folded name tokens
    # with a minimum length instead.
    def _turn_tokens(text: str) -> set:
        return {tok for tok in re.split(r'[^؀-ۿ]+', _norm_ar_name(text)) if len(tok) >= 3}

    def _name_in_turn(name: str, turn_text: str) -> bool:
        # The LONGEST normalized name token must appear as a turn token — 'علي' no
        # longer matches inside 'وعليكم السلام' (reviewer collision fix), while
        # multi-word names match on their distinctive surname token.
        name_toks = [t for t in re.split(r'[^؀-ۿ]+', _norm_ar_name(name)) if len(t) >= 3]
        if not name_toks:
            return False
        turn_toks = _turn_tokens(turn_text)
        return max(name_toks, key=len) in turn_toks

    recovered_candidates = [
        d for d in recovered_candidates
        if d["name"] and any(
            _name_in_turn(d["name"], _clean_text(_first_truthy(
                _dig(t, "text"), _dig(t, "message"), _dig(t, "content"), "")))
            for t in recent_assistant_turns_for_recovery
        )
    ]
    recovered_candidates.sort(key=lambda d: -_u16_len(d["name"]))
    if recovered_candidates:
        recovered_followup_doctor = recovered_candidates[0]
    effective_recovery_doctor_name = _clean_text(_first_truthy(
        _prop(live_booking_context, "doctor_name"), _dig(recovered_followup_doctor, "name"), ""
    ))
    effective_recovery_doctor_id = _first_truthy(
        _prop(live_booking_context, "doctor_id"), _dig(recovered_followup_doctor, "id"), None
    )
    recovery_next_field = "date" if effective_recovery_doctor_name else "doctor"
    error_followup_context: Optional[Dict[str, Any]] = None
    if state_has_live_booking and recovery_followup_eligible and prior_k2_error_reply:
        error_followup_context = {
            "active": True,
            "reason": "previous_k2_reply_failed",
            "doctor_name": _first_truthy(effective_recovery_doctor_name, None),
            "doctor_id": effective_recovery_doctor_id,
            "appointment_type": _first_truthy(_prop(live_booking_context, "appointment_type"), None),
            "service_optional": True,
            "next_field": recovery_next_field,
            "preserve_booking_context": True,
            "do_not_greet": True,
        }
    pre_agent_stage_contract: Optional[Dict[str, Any]] = None
    if (
        state_has_live_booking
        and state_operation == "create_appointment"
        and visit_type_only_turn
        and stage_patient_complete
        and (
            prior_patient_review_pending
            or (
                _prop(conversation_state, "resume_eligible") is True
                and _js_string(_first_truthy(_dig(conversation_state, "patient_data_review", "status"), "")).lower() != "confirmed"
            )
        )
    ):
        pre_agent_stage_contract = {
            "type": "confirm_patient_data",
            "fields": ["name", "age", "address", "phone"],
            "patient_data": stage_patient,
            "service_optional": True,
            "date_allowed": False,
            "source": "deterministic_current_turn",
        }

    review_confirmed = bool(
        state_has_live_booking
        and isinstance(_prop(conversation_state, "patient_data_review"), dict)
        and _js_string(_first_truthy(_dig(conversation_state, "patient_data_review", "status"), "")).lower() == "confirmed"
    )
    # Computed in the JS but not emitted downstream; kept for parity.
    if state_has_live_booking and review_confirmed and _js_string(_first_truthy(_prop(conversation_state, "pending_action"), "")).lower() == "confirm_patient_data":
        effective_pending_action: Any = "ask_date"
    else:
        effective_pending_action = _first_truthy(_prop(conversation_state, "pending_action"), None)
    rns_check = _prop(conversation_state, "required_next_step")
    if state_has_live_booking and review_confirmed and _js_string(_first_truthy(_prop(rns_check, "type") if isinstance(rns_check, dict) else _UNDEFINED, "")).lower() == "confirm_patient_data":
        effective_required_next_step: Any = {"type": "ask_date", "fields": ["date"]}
    else:
        effective_required_next_step = _first_truthy(pre_agent_stage_contract, _prop(conversation_state, "required_next_step"), None)

    # v35 MODEL-FIRST facts.
    def _bc_field(f: str) -> Any:
        if f == "visit_type":
            return _prop(live_booking_context, "appointment_type")
        if f == "doctor":
            return _first_truthy(_prop(live_booking_context, "doctor_name"), _prop(live_booking_context, "doctor_id"))
        return _prop(live_booking_context, f)

    def _is_collected(f: str) -> bool:
        v = _bc_field(f)
        return v is not None and v is not _UNDEFINED and _js_trim(_js_string(v)) != ""

    collected_booking_data = [f for f in _REQUIRED_CREATE_FIELDS if _is_collected(f)]
    booking_data_missing_for_create = (
        [f for f in _REQUIRED_CREATE_FIELDS if not _is_collected(f)] if state_operation == "create_appointment" else []
    )
    live_agent_context: Dict[str, Any] = {
        "active_operation": state_operation if state_has_live_booking else None,
        "operation_status": state_operation_status if state_has_live_booking else None,
        "confirmation_state": _first_truthy(_prop(conversation_state, "confirmation_state"), None) if state_has_live_booking else None,
        "confirmation_target": _first_truthy(_prop(conversation_state, "confirmation_target"), None) if state_has_live_booking else None,
        "pending_action": None,
        "conversation_stage": None,
        "required_next_step": None,
        "missing_human_fields": [],
        "next_best_missing_human_field": None,
        "patient_data_review": _first_truthy(_prop(conversation_state, "patient_data_review"), None) if state_has_live_booking else None,
        "booking_context": live_booking_context if state_has_live_booking else None,
        "booking_progress": (
            {"collected": collected_booking_data, "missing": booking_data_missing_for_create}
            if state_has_live_booking and state_operation == "create_appointment"
            else None
        ),
        "last_open_question": (
            _first_truthy(_prop(conversation_state, "last_open_question"), {
                "message": _first_truthy(_prop(state_last_assistant, "text"), None),
                "requested_fields": state_requested_fields,
            })
            if state_has_live_booking else None
        ),
        "recent_assistant_turn": (
            {
                "message": _first_truthy(_prop(state_last_assistant, "text"), None),
                "requested_information": state_requested_fields,
            }
            if state_has_live_booking else None
        ),
    }

    # v37 VERIFIED-OFFER FACT.
    state_offer = _prop(conversation_state, "presented_offer")
    state_offer = state_offer if isinstance(state_offer, dict) else None
    offer_alternatives = _prop(state_offer, "alternatives") if state_offer is not None else _UNDEFINED
    offer_alternatives = offer_alternatives if isinstance(offer_alternatives, list) else []
    offer_live = False
    if state_offer is not None and len(offer_alternatives) > 0:
        exp = _date_parse(_js_string(_first_truthy(_prop(state_offer, "expires_at"), "")))
        offer_live = bool(_is_finite_num(exp) and exp > _now_ms())
    if offer_live:
        offered: List[Dict[str, Any]] = []
        for i, alt in enumerate(offer_alternatives[:4]):
            a = alt if isinstance(alt, dict) else {}
            iso_date = _u16_slice(_js_string(_first_truthy(_prop(a, "local_date"), _prop(a, "date"), "")), 0, 10)
            weekday: Optional[str] = None
            if _ISO_DATE_RE.search(iso_date):
                try:
                    js_day = (datetime.strptime(iso_date, "%Y-%m-%d").weekday() + 1) % 7
                    weekday = _AR_DAYS[js_day] or None
                except ValueError:
                    weekday = None
            rank_num = _js_number(_prop(a, "rank"))
            rank: Any = (i + 1) if not (_is_finite_num(rank_num) and rank_num != 0) else _int_if_integral(rank_num)
            offered.append({
                "rank": rank,
                "date": _first_truthy(iso_date, None),
                "day": weekday,
                "time": _first_truthy(_u16_slice(_js_string(_first_truthy(_prop(a, "local_time"), _prop(a, "time"), "")), 0, 5), None),
                "slot_id": _first_truthy(_prop(a, "slot_id"), None),
            })
        live_agent_context["offered_slots"] = [o for o in offered if _js_truthy(o["date"]) and _js_truthy(o["time"])]
        bp = live_agent_context.get("booking_progress")
        if isinstance(bp, dict) and isinstance(bp.get("missing"), list):
            bp["missing"] = [f for f in bp["missing"] if f != "date" and f != "time"]
        bp = live_agent_context.get("booking_progress")
        bc_obj = live_agent_context.get("booking_context")
        if isinstance(bp, dict) and isinstance(bp.get("collected"), list) and isinstance(bc_obj, dict):
            offered_dates = {o["date"] for o in offered if o["date"]}
            current_date = _u16_slice(_js_string(_prop(bc_obj, "date")), 0, 10) if _js_truthy(_prop(bc_obj, "date")) else None
            if current_date and current_date not in offered_dates:
                live_agent_context["booking_context"] = {**bc_obj, "date": None, "time": None}
                bp["collected"] = [f for f in bp["collected"] if f != "date" and f != "time"]

    # v36 DOCTOR-CHANGE FACT.
    if state_has_live_booking and isinstance(live_booking_context, dict):
        doctor_inquiry_for_change = _dict_or(inputs.get("resolve_doctor_inquiry_deterministic"))
        requested_doctor = (
            _js_trim(_js_string(_prop(doctor_inquiry_for_change, "doctor_name")))
            if (_prop(doctor_inquiry_for_change, "found") is True and _js_truthy(_prop(doctor_inquiry_for_change, "doctor_name")))
            else None
        )
        stored_doctor = (
            _js_trim(_js_string(_prop(live_booking_context, "doctor_name")))
            if _js_truthy(_prop(live_booking_context, "doctor_name"))
            else None
        )
        # Reviewer fixes: (a) the change fires on mere availability QUESTIONS about
        # another doctor ('هل دكتور خالد موجود؟') — require switch intent (a
        # correction or a booking verb), and (b) the rebuild spread the PRE-v37
        # snapshot, resurrecting a stale date the v37 phantom-date fix had just
        # nulled — spread the post-v37 agent-context copy instead.
        _switch_message = _js_string(_prop(_dict_or(inputs.get("normalize_validate")), "message_text") or "")
        switch_requested = (
            _prop(doctor_inquiry_for_change, "correction_detected") is True
            or bool(re.search(r"(?:احجز|بحجز|عايز\s+احجز|عاوز\s+احجز)", _switch_message))
        )
        if requested_doctor and stored_doctor and _norm_ar_name(requested_doctor) != _norm_ar_name(stored_doctor) and switch_requested:
            live_agent_context["doctor_change"] = {
                "from": stored_doctor,
                "to": requested_doctor,
                "doctor_id": _first_truthy(_prop(doctor_inquiry_for_change, "doctor_id"), None),
            }
            live_agent_context["booking_context"] = {
                **_dict_or(live_agent_context.get("booking_context")),
                "doctor_name": requested_doctor,
                "doctor_id": _first_truthy(_prop(doctor_inquiry_for_change, "doctor_id"), _prop(live_agent_context.get("booking_context"), "doctor_id"), None),
            }
            bp = live_agent_context.get("booking_progress")
            if _js_truthy(bp):
                missing_without_doctor = (
                    [f for f in bp["missing"] if f != "doctor"] if isinstance(bp.get("missing"), list) else []
                )
                collected_without_doctor = (
                    [f for f in bp["collected"] if f != "doctor"] if isinstance(bp.get("collected"), list) else []
                )
                live_agent_context["booking_progress"] = {
                    "collected": collected_without_doctor + ["doctor"],
                    "missing": missing_without_doctor,
                }

    if error_followup_context:
        # v35: recovery is a FACT, not a directive.
        old_bc = live_agent_context.get("booking_context")
        old_bc = old_bc if isinstance(old_bc, dict) else {}
        live_agent_context["booking_context"] = {
            **old_bc,
            "doctor_id": _first_truthy(error_followup_context.get("doctor_id"), _prop(old_bc, "doctor_id"), None),
            "doctor_name": _first_truthy(error_followup_context.get("doctor_name"), _prop(old_bc, "doctor_name"), None),
            "appointment_type": _first_truthy(error_followup_context.get("appointment_type"), _prop(old_bc, "appointment_type"), None),
            "date": _first_truthy(_prop(old_bc, "date"), None),
            "time": _first_truthy(_prop(old_bc, "time"), None),
        }
        bp = live_agent_context.get("booking_progress")
        if _js_truthy(bp) and state_operation == "create_appointment":
            bp["missing"] = booking_data_missing_for_create

    # v33 systemic guard: reconcile bookkeeping to the last sent reply.
    _reconcile_open_question(live_agent_context)

    # v35 agent_context_model: FACTS only, compact.
    c = live_agent_context or {}
    b = _obj_or_none(_prop(c, "booking_context"))
    target = _obj_or_none(_prop(c, "confirmation_target"))
    review = _obj_or_none(_prop(c, "patient_data_review"))
    state: Dict[str, Any] = {}
    for key in ("active_operation", "operation_status"):
        _compact_put(state, key, _prop(c, key))
    if _js_truthy(_prop(c, "booking_progress")):
        _compact_put(state, "booking_progress", _prop(c, "booking_progress"))
    if _js_truthy(_prop(c, "confirmation_state")):
        cs_val = _prop(c, "confirmation_state")
        _compact_put(state, "confirmation_state", cs_val if isinstance(cs_val, str) else _compact_object(cs_val, ["status", "valid", "pending"]))
    _compact_put(state, "confirmation_target", _compact_object(target, ["action", "operation", "target_operation", "doctor_name", "doctor_id", "service_name", "appointment_type", "date", "time", "branch_name", "patient_name", "patient_phone", "patient_age", "patient_address", "booking_number", "slot_id", "confirmation_id", "expires_at"]))
    if _js_truthy(_prop(c, "last_open_question")) and not _js_truthy(pre_agent_stage_contract):
        _compact_put(state, "last_open_question", _compact_object(_prop(c, "last_open_question"), ["requested_fields", "pending_action"]))
    if _js_truthy(review):
        _compact_put(state, "patient_data_review", _compact_object(review, ["status", "complete", "missing_fields", "corrections", "fields", "source"]))
    _compact_put(state, "booking_context", _compact_object(b, ["doctor_name", "service_name", "appointment_type", "booking_number", "branch_name", "date", "time", "patient_name", "patient_phone", "patient_age", "patient_address"]))
    if isinstance(_prop(c, "doctor_change"), dict):
        _compact_put(state, "doctor_change", _compact_object(_prop(c, "doctor_change"), ["from", "to"]))
    offered_slots_val = _prop(c, "offered_slots")
    if isinstance(offered_slots_val, list) and len(offered_slots_val) > 0:
        _compact_put(state, "offered_slots", [{"rank": _prop(o, "rank"), "date": _prop(o, "date"), "day": _prop(o, "day"), "time": _prop(o, "time")} for o in offered_slots_val])
    if not _js_truthy(pre_agent_stage_contract):
        _compact_put(state, "recent_assistant_turn", _compact_object(_prop(c, "recent_assistant_turn"), ["message", "requested_information"]))
    persisted_patient_facts = _dig(conversation_state, "facts", "patient")
    persisted_patient_facts = persisted_patient_facts if isinstance(persisted_patient_facts, dict) else {}
    profile = _compact_object({
        "name": (
            None
            if (is_telegram_channel and _prop(persisted_patient_facts, "name_source") != "user_entered")
            else _first_truthy(_prop(clinic, "patient_name"), _prop(persisted_patient_facts, "name"), None)
        ),
        "phone": _first_truthy(_prop(clinic, "patient_phone"), _prop(persisted_patient_facts, "phone"), _prop(persisted_patient_facts, "mobile"), None),
        "age": _coalesce(_prop(clinic, "patient_age"), _prop(persisted_patient_facts, "age"), None),
        "address": _first_truthy(_prop(clinic, "patient_address"), _prop(persisted_patient_facts, "address"), None),
    }, ["name", "phone", "age", "address"])
    if profile:
        _compact_put(state, "patient_profile", profile)
    agent_context_model = state if state else None

    clinic_persona_compact = {
        "clinic": _first_truthy(clinic_name, None),
        "assistant": name,
        "role": role,
        "tone": tone,
        "dialect": dialect,
        "style": _u16_slice(prompt, 0, 700) if prompt else None,
    }

    # Doctor/branch directory gating.
    doctor_inquiry = _dict_or(inputs.get("resolve_doctor_inquiry_deterministic"))
    branch_inquiry = _dict_or(inputs.get("resolve_branch_inquiry_deterministic"))
    clinic_doctor_directory_full = _prop(clinic, "doctor_directory")
    clinic_doctor_directory_full = clinic_doctor_directory_full if isinstance(clinic_doctor_directory_full, list) else []
    doctor_count = _js_number(_first_truthy(_prop(clinic, "doctor_count"), len(clinic_doctor_directory_full), 0))
    branch_count = len(clinic_branch_directory)
    current_message_for_directory = _clean_text(_first_truthy(_prop(inbound, "message_text"), "")).lower()
    doctor_mentioned_in_message = bool(_DOCTOR_MENTION_RE.search(current_message_for_directory))
    cs_missing_fields = _prop(conversation_state, "missing_human_fields")
    doctor_stage_needs_choice = bool(
        state_has_live_booking
        and not _js_truthy(_prop(live_booking_context, "doctor_name"))
        and isinstance(cs_missing_fields, list)
        and "doctor" in cs_missing_fields
    )
    needs_doctor_directory = bool(
        session_booking_context_allowed
        and (
            (_is_finite_num(doctor_count) and doctor_count <= 1)
            or _prop(doctor_inquiry, "is_doctor_inquiry") is True
            or _prop(doctor_inquiry, "is_doctor_catalog_inquiry") is True
            or doctor_mentioned_in_message
            or doctor_stage_needs_choice
        )
    )
    needs_branch_directory = bool(
        session_booking_context_allowed
        and (
            _prop(branch_inquiry, "is_branch_inquiry") is True
            or _prop(branch_inquiry, "is_branch_catalog_inquiry") is True
        )
    )
    greeting_signal_text = _js_trim(_js_ws_sub(_PUNCT_RE.sub(" ", _TA_MARBUTA_RE.sub("ه", _ALEF_RE.sub("ا", _DIACRITICS_RE.sub("", unicodedata.normalize("NFKC", current_message_for_directory)))))))
    is_greeting_only = bool(_GREETING_ONLY_RE.search(greeting_signal_text))
    query_action_signal = bool(_QUERY_ACTION_SIGNAL_RE.search(current_message_for_directory))
    faq_signal = bool(_FAQ_SIGNAL_RE.search(current_message_for_directory))
    clinic_query_type: Optional[str] = None
    if not state_has_live_booking and not query_action_signal:
        if service_facts["is_service_catalog_inquiry"] is True:
            clinic_query_type = "service_catalog"
        elif service_facts["is_price_inquiry"] is True:
            clinic_query_type = "service_price"
        elif service_facts["is_service_fact_inquiry"] is True:
            clinic_query_type = "service_fact"
        elif _prop(doctor_inquiry, "is_doctor_catalog_inquiry") is True:
            clinic_query_type = "doctor_catalog"
        elif _prop(doctor_inquiry, "is_doctor_inquiry") is True:
            clinic_query_type = "doctor_fact"
        elif _prop(branch_inquiry, "is_branch_catalog_inquiry") is True or _prop(branch_inquiry, "is_branch_inquiry") is True:
            clinic_query_type = "branch_location"
        elif faq_signal:
            clinic_query_type = "faq"
    if clinic_query_type == "service_catalog":
        clinic_query_context: Optional[Dict[str, Any]] = {
            "type": clinic_query_type,
            "services": [_safe_service(s) for s in service_facts["catalog"][:50]],
        }
    elif clinic_query_type in ("service_price", "service_fact"):
        clinic_query_context = {
            "type": clinic_query_type,
            "matches": [_safe_service(s) for s in service_facts["matches"][:5]],
            "selected": _safe_service(service_facts),
        }
    elif clinic_query_type == "doctor_catalog":
        di_catalog = _prop(doctor_inquiry, "catalog")
        clinic_query_context = {
            "type": clinic_query_type,
            "doctors": [_safe_doctor(s) for s in di_catalog[:20]] if isinstance(di_catalog, list) else [],
        }
    elif clinic_query_type == "doctor_fact":
        di_matches = _prop(doctor_inquiry, "matches")
        clinic_query_context = {
            "type": clinic_query_type,
            "matches": [_safe_doctor(s) for s in di_matches[:5]] if isinstance(di_matches, list) else [],
            "selected": _safe_doctor(doctor_inquiry),
        }
    elif clinic_query_type == "branch_location":
        bi_catalog = _prop(branch_inquiry, "catalog")
        bi_matches = _prop(branch_inquiry, "matches")
        clinic_query_context = {
            "type": clinic_query_type,
            "branches": [_safe_branch(s) for s in bi_catalog[:10]] if isinstance(bi_catalog, list) else [],
            "matches": [_safe_branch(s) for s in bi_matches[:5]] if isinstance(bi_matches, list) else [],
        }
    elif clinic_query_type == "faq":
        clinic_query_context = {"type": "faq", "use_faq_tool": True}
    else:
        clinic_query_context = None

    # Deterministic confirm fast-path.
    pending_confirm_target = bool(
        state_has_live_booking
        and isinstance(_prop(conversation_state, "confirmation_target"), dict)
        and _prop(_prop(conversation_state, "confirmation_target"), "invalidated") is not True
        and _js_string(_first_truthy(_dig(conversation_state, "confirmation_target", "confirmation_delivery_status"), "")).lower() in ("pending", "sent")
    )
    deterministic_confirm_turn = bool(pending_confirm_target and _CONFIRM_FAST_AFFIRM_RE.search(_js_trim(stage_message_text)))
    # Prompt assembly REMOVED 2026-09-17: the model has exactly one prompt,
    # app/services/prompts/agent_system_message.txt. agent_prompt_profile survives
    # as telemetry only, never as a prompt selector.
    agent_prompt_profile = (
        "small_talk" if (is_greeting_only and not state_has_live_booking)
        else ("clinic_query" if clinic_query_type else "standard_safe")
    )
    message_action_signal = bool(_MESSAGE_ACTION_SIGNAL_RE.search(current_message_for_directory))
    explicit_doctor_info_request = bool(
        _js_truthy(_prop(doctor_inquiry, "is_doctor_inquiry")) or _js_truthy(_prop(doctor_inquiry, "is_doctor_catalog_inquiry"))
    ) or bool(_DOCTOR_INFO_REQUEST_RE.search(current_message_for_directory))
    doctor_directory_for_model: List[Dict[str, Any]] = []
    if needs_doctor_directory:
        for doctor in clinic_doctor_directory_full:
            d = doctor if isinstance(doctor, dict) else {}
            row = {"doctor_name": _first_truthy(_prop(d, "doctor_name"), None)}
            if explicit_doctor_info_request and _js_truthy(_prop(d, "specialization")):
                row["specialization"] = _prop(d, "specialization")
            doctor_directory_for_model.append(row)
    filtered_doctor_directory = doctor_directory_for_model
    filtered_branch_directory = clinic_branch_directory if needs_branch_directory else []

    out: Dict[str, Any] = {
        "agent_context": live_agent_context,
        "agent_context_model": agent_context_model,
    }
    out.update(item if isinstance(item, dict) else {})
    out.update({
        "clinic_persona": {"name": name, "role": role, "tone": tone, "dialect": dialect},
        "agent_persona_compact": clinic_persona_compact,
        "agent1_is_greeting_only": is_greeting_only,
        "agent_prompt_profile": agent_prompt_profile,
        "agent_prompt_profile_reason": (
            "greeting_without_live_booking" if agent_prompt_profile == "small_talk"
            else (clinic_query_type if agent_prompt_profile == "clinic_query"
                  else ("live_booking_or_continuation" if state_has_live_booking else "non_greeting_or_ambiguous"))
        ),
        "clinic_query_type": clinic_query_type,
        "clinic_query_context": clinic_query_context,
        "pre_agent_stage_contract": pre_agent_stage_contract,
        "error_followup_context": error_followup_context,
        "patient_review_current_turn": bool(pre_agent_stage_contract),
        "deterministic_confirm_turn": deterministic_confirm_turn,
        # Measured from the prompt actually sent, not a locally assembled string.
        "agent_system_prompt_chars": _u16_len(_live_system_prompt()),
        "is_conversation_start": is_conversation_start,
        "context_session_expired": rolling_session_expired,
        "draft_context_expired": draft_expired,
        "historical_reference_requested": historical_reference_requested,
        "context_age_minutes": (
            _int_if_integral(max(0, _js_round((expiry_reference_ms - previous_activity_ms) / 60000)))
            if _is_finite_num(expiry_reference_ms) and _is_finite_num(previous_activity_ms) else None
        ),
        "clinic_prompt_block": clinic_block,
        "clinic_prompt_version": prompt_version,
        "clinic_prompt_rejected": rejected,
        "clinic_prompt_rejection_reason": "FORBIDDEN_INSTRUCTION_PATTERN" if rejected else None,
        "persona_name_rejected": name_rejected,
        "persona_role_rejected": role_rejected,
        "clinic_prompt_source": "clinic_settings" if prompt else "default",
        "clinic_location_config": clinic_location_config,
        "clinic_branch_directory": filtered_branch_directory,
        "clinic_doctor_directory": filtered_doctor_directory,
        "doctor_directory_model_mode": "with_specialties_for_doctor_info" if explicit_doctor_info_request else "names_only_for_booking",
        "needs_doctor_directory": needs_doctor_directory,
        "needs_branch_directory": needs_branch_directory,
        "service_facts": service_facts,
        "service_name": service_facts["service_name"],
        "service_id": service_facts["service_id"],
        "price": service_facts["price"],
        "duration_minutes": service_facts["duration_minutes"],
        "online_booking": service_facts["online_booking"],
    })
    return out


# ---------------------------------------------------------------------------
# Build Persistent Conversation State
# ---------------------------------------------------------------------------

_TERMINAL_RESPONSE_CODES = ["APPOINTMENT_CREATED", "CANCEL_COMPLETED", "CANCELLATION_NOT_ALLOWED", "APPOINTMENT_NOT_FOUND_OR_NOT_OWNED", "RESCHEDULE_COMPLETED", "IDEMPOTENT_REPLAY", "OPERATION_INCONCLUSIVE"]
_TRANSIENT_FAILURE_CODES = {"APPOINTMENT_CREATION_FAILED", "CANCEL_FAILED", "RESCHEDULE_FAILED", "PROVIDER_TIMEOUT", "PROVIDER_UNAVAILABLE", "SLOT_UNAVAILABLE", "SLOT_INVALID", "NETWORK_ERROR", "UPSTREAM_TIMEOUT"}
_FINAL_FAILURE_CODES = {"CANCELLATION_NOT_ALLOWED", "APPOINTMENT_NOT_FOUND_OR_NOT_OWNED", "OPERATION_INCONCLUSIVE"}
_TERMINAL_FOR_STATE_REPAIR = {"APPOINTMENT_CREATED", "CANCEL_COMPLETED", "RESCHEDULE_COMPLETED", "IDEMPOTENT_REPLAY", "CANCELLATION_NOT_ALLOWED", "APPOINTMENT_NOT_FOUND_OR_NOT_OWNED", "RESCHEDULE_NOT_ALLOWED", "OPERATION_INCONCLUSIVE"}

_TARGET_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.IGNORECASE)
_PROMPT_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
_DATE_ONLY_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_TIME_RE = re.compile(r"^(?:[01][0-9]|2[0-3]):[0-5][0-9](?::[0-5][0-9])?$")

_UNUSABLE_PATIENT_NAMES = {"مريض", "patient", "unknown", "غير معروف", "غير محدد"}

_FENCED_TAIL_RE = re.compile(r"```" + _JS_WS_CLASS + r"*$")


def _to_int32(x: int) -> int:
    return ((x + 0x80000000) & 0xFFFFFFFF) - 0x80000000


def _mint_uuid(seed: Any) -> str:
    """JS mintUuid: deterministic 8-4-4-4-12 hex id from a string seed (charCodeAt loop)."""
    s = _js_trim(_js_string(_coalesce(seed, "")))
    h1 = 0
    h2 = 0
    for code in _u16_units(s):
        h1 = _to_int32((h1 << 5) - h1 + code)
        h2 = _to_int32((h2 << 7) - h2 + code)
    a = format(abs(h1), "x").zfill(8)[:8]
    b = (format(abs(h2), "x") + "00000000")[:8][:4]
    c = format(abs((h1 ^ h2) & 0x0FFF), "x").zfill(4)[:4]
    d = ("a" + format(abs(h1 + h2) % 0x4000, "x")).ljust(4, "0")[:4]
    e = (format(abs(h1 * 31 + h2 * 17), "x") + "000000000000")[:12]
    return a + "-" + b + "-" + c + "-" + d + "-" + e


def _parse_agent_output(raw: Any) -> Any:
    """JS output parse: unfence ```json blocks, JSON.parse, `|| {}` on falsy."""
    try:
        if not raw:
            return {}
        if not isinstance(raw, str):
            return raw if isinstance(raw, dict) else {}
        t = _js_trim(raw)
        if t.startswith("```"):
            i = t.find("\n")
            t = t[i + 1:] if i >= 0 else t
            t = _js_trim(_FENCED_TAIL_RE.sub("", t))
        parsed = json.loads(t)
        return parsed or {}
    except Exception:
        return {}


def _usable_patient_name(value: Any) -> Optional[str]:
    text = _js_trim(_js_string(_first_truthy(value, "")))
    if text and text.lower() not in _UNUSABLE_PATIENT_NAMES:
        return text
    return None


def _same_field(a: Any, b: Any) -> bool:
    return a == b or (a == "visit_type" and b == "service") or (a == "service" and b == "visit_type")


def _json_safe(value: Any) -> Any:
    """JSON-safe guard at the state-save boundary (deep review 2026-09-25).

    Non-serializable strays (the _UNDEFINED sentinel, datetime.time/date from
    asyncpg rows, Decimal) are neutralized deterministically: sentinels drop to
    None, temporal values to their ISO string. Valid values pass untouched —
    this only fires on values json.dumps could not handle anyway.
    """
    if value is _UNDEFINED or type(value) is object:
        return None
    if isinstance(value, (str, bool, int, float)) or value is None:
        return value
    if isinstance(value, (datetime,)):
        return value.isoformat()
    try:
        import datetime as _dt
        if isinstance(value, (_dt.time, _dt.date)):
            return value.isoformat()
    except Exception:
        pass
    try:
        from decimal import Decimal
        if isinstance(value, Decimal):
            return float(value)
    except Exception:
        pass
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def build_persistent_conversation_state(item: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Build Persistent Conversation State (extracted/code/Build_Persistent_Conversation_State.js).

    Input keys (missing nodes read as {}):
      item                                   -> $json (Response Policy / downstream merged item)
      inputs["normalize_validate"]           -> $(Normalize & Validate).first().json
      inputs["get_clinic_context"]           -> $(Get Clinic Context).first().json
      inputs["system_orchestrator_policy"]   -> $(System Orchestrator (Policy)).first().json
      inputs["get_conversation_state"]       -> $(Get Conversation State).first().json (reads .state_data)
      inputs["read_fresh_offer_midturn"]     -> $(Read Fresh Offer (Midturn)).first().json

    Output: single json dict = `{...$json, state_data, ...top-level overrides}`
    (booking_context / booking_number / slot_state / patient_data_review /
    confirmation_target / confirmation_state / state_version, plus the
    newBookingRestart operation keys).
    """
    ctx = _dict_or(inputs.get("normalize_validate"))
    clinic = _dict_or(inputs.get("get_clinic_context"))
    orch_output = _dict_or(inputs.get("system_orchestrator_policy"))
    is_telegram_channel = _js_trim(_js_string(_first_truthy(_prop(ctx, "channel_type"), ""))).lower() == "telegram"

    orch_decision = _obj_or_none(_prop(orch_output, "system_decision")) or {}
    obc = _obj_or_none(_prop(orch_output, "booking_context"))
    orch_booking_context = obc if obc is not None else (_obj_or_none(_prop(orch_decision, "booking_context")) or {})
    orch_patient = orch_booking_context
    orch_patient_name = _first_truthy(_prop(orch_patient, "patient_name"), None)
    orch_patient_name_source = _first_truthy(_prop(orch_patient, "patient_name_source"), _prop(orch_booking_context, "patient_name_source"), None)
    orch_patient_phone = _first_truthy(_prop(orch_patient, "patient_phone"), None)
    orch_patient_age = _first_truthy(_prop(orch_patient, "patient_age"), None)
    orch_patient_address = _first_truthy(_prop(orch_patient, "patient_address"), None)
    orch_state_patches = _obj_or_none(_prop(orch_output, "state_patches")) or {}
    orch_state_patches = orch_state_patches if isinstance(orch_state_patches, dict) else {}

    previous = _dict_or(_first_truthy(_prop(_dict_or(inputs.get("get_conversation_state")), "state_data"), {}))
    context_session_reset = _prop(item, "context_session_reset") is True
    execution_booking_number: Any = _first_truthy(_prop(item, "booking_number"), _prop(previous, "booking_number"), None)
    output = _parse_agent_output(_prop(item, "output"))
    output = output if isinstance(output, dict) else {}
    upstream_decision = _first_truthy(_prop(orch_output, "system_decision"), {})
    decision = _first_truthy(_prop(item, "system_decision"), upstream_decision, {})

    abandon_current_booking = bool(
        (
            _prop(decision, "booking_draft_abandoned") is True
            or _prop(output, "booking_draft_abandoned") is True
            or _js_string(_first_truthy(_prop(decision, "response_code"), _prop(item, "response_code"), _prop(output, "response_code"), "")).upper() == "BOOKING_DRAFT_ABANDONED"
        )
        and not _js_truthy(_prop(item, "appointment_id"))
        and not _js_truthy(_prop(output, "appointment_id"))
        and not _js_truthy(_prop(item, "booking_number"))
        and not _js_truthy(_prop(output, "booking_number"))
    )
    normalization = _obj_or_none(_prop(item, "_normalization")) or {}
    normalization = normalization if isinstance(normalization, dict) else {}
    new_booking_restart = bool(
        _prop(normalization, "new_booking_restart") is True
        or _prop(output, "new_booking_restart") is True
        or _prop(decision, "new_booking_restart") is True
    )
    prior_draft_expiry_ms = _date_parse(_first_truthy(_prop(previous, "draft_expires_at"), ""))
    expired_prior_draft = bool(
        (not new_booking_restart)
        and _is_finite_num(prior_draft_expiry_ms)
        and prior_draft_expiry_ms <= _now_ms()
        and _js_string(_first_truthy(_prop(previous, "operation_state"), _prop(previous, "operation_status"), "")).upper()
        in ("DRAFT", "COLLECTING_DETAILS", "COLLECTING_APPOINTMENT_DETAILS", "")
    )
    superseded_condition = bool(
        (new_booking_restart or expired_prior_draft)
        and (
            _js_truthy(_prop(previous, "operation_id"))
            or _js_truthy(_prop(previous, "active_operation"))
            or _js_truthy(_prop(previous, "confirmation_target"))
            or _js_truthy(_prop(previous, "booking_context"))
        )
    )
    if superseded_condition:
        superseded_operation: Any = {
            "operation_id": _first_truthy(_prop(previous, "operation_id"), _dig(previous, "confirmation_target", "operation_id"), None),
            "active_operation": _first_truthy(_prop(previous, "active_operation"), _prop(previous, "operation_action"), None),
            "operation_state": _first_truthy(_prop(previous, "operation_state"), _prop(previous, "operation_status"), None),
            "confirmation_target": _first_truthy(_prop(previous, "confirmation_target"), None),
            "superseded_at": _utc_now_iso(),
            "reason": "EXPIRED_DRAFT" if expired_prior_draft else "SUPERSEDED_BY_NEW_REQUEST",
        }
    else:
        superseded_operation = _first_truthy(_prop(previous, "superseded_operation"), None)

    # Draft expiry invalidates the executable slot/confirmation but preserves stable facts.
    if expired_prior_draft:
        stable_previous_booking: Dict[str, Any] = {
            "doctor_id": _first_truthy(_dig(previous, "booking_context", "doctor_id"), None),
            "doctor_name": _first_truthy(_dig(previous, "booking_context", "doctor_name"), None),
            "service_id": _first_truthy(_dig(previous, "booking_context", "service_id"), None),
            "service_name": _first_truthy(_dig(previous, "booking_context", "service_name"), None),
            "appointment_type": _first_truthy(_dig(previous, "booking_context", "appointment_type"), None),
            "date": _first_truthy(_dig(previous, "booking_context", "date"), _dig(previous, "slot_state", "date"), _dig(previous, "facts", "booking", "date"), None),
            "time": _first_truthy(_dig(previous, "booking_context", "time"), _dig(previous, "slot_state", "time"), _dig(previous, "facts", "booking", "time"), None),
        }
    else:
        stable_previous_booking = {}
    previous_slot: Any = {} if (new_booking_restart or expired_prior_draft) else _first_truthy(_prop(previous, "slot_state"), _prop(previous, "booking_context"), {})
    if new_booking_restart:
        # A restart resets SCHEDULING facts (date/time/slot) but keeps the patient's
        # stable identity facts (doctor/service/type) when the new request names no
        # doctor — a full wipe made the next turn re-ask a doctor the patient had
        # already chosen. Two guards:
        #   1. If THIS turn's decision resolved a doctor (patient named one), reset
        #      fully — the decision's booking_context, not the fresh-offer `output`
        #      row, is where the resolved doctor lives.
        #   2. After a TERMINAL operation (completed/cancelled/failed) the previous
        #      booking is finished business — carrying its doctor into a fresh
        #      restart silently books the old doctor again.
        restart_names_doctor = _js_truthy(_first_truthy(
            _dig(orch_output, "booking_context", "doctor_name"),
            _dig(orch_output, "booking_context", "doctor_id"),
            _dig(orch_output, "system_decision", "booking_context", "doctor_name"),
            _dig(orch_output, "system_decision", "booking_context", "doctor_id"),
            None,
        ))
        # .lower(): _STATE_TERMINAL is lowercase; the saved operation_state is canonical
        # uppercase ("COMPLETED") — the .upper() form made this guard dead code
        # (reviewer-verified: a completed booking's doctor was resurrected on restart).
        previous_operation_terminal = _js_string(_first_truthy(
            _prop(previous, "operation_state"), _prop(previous, "operation_status"), ""
        )).lower() in _STATE_TERMINAL
        if restart_names_doctor or previous_operation_terminal:
            previous_booking_fallback: Any = {}
        else:
            previous_booking_fallback: Any = {k: v for k, v in {
                "doctor_id": _dig(previous, "booking_context", "doctor_id"),
                "doctor_name": _dig(previous, "booking_context", "doctor_name"),
                "service_id": _dig(previous, "booking_context", "service_id"),
                "service_name": _dig(previous, "booking_context", "service_name"),
                "appointment_type": _dig(previous, "booking_context", "appointment_type"),
            }.items() if _js_truthy(v)}
    else:
        previous_booking_fallback: Any = (
            stable_previous_booking if expired_prior_draft else _first_truthy(_prop(previous, "booking_context"), {})
        )
    current_slot: Any = _first_truthy(_prop(output, "slot_state"), {})
    # Orchestrator slot context (same node read three times in the JS).
    orch_resolved = _obj_or_none(_prop(orch_output, "system_decision")) or {}
    orch_slot_context = _first_truthy(
        _prop(orch_output, "slot_state"), _prop(orch_output, "booking_context"),
        _prop(orch_resolved, "slot_state"), _prop(orch_resolved, "booking_context"), {},
    )
    if not _js_truthy(_prop(current_slot, "doctor_name")):
        current_slot = _first_truthy(orch_slot_context, current_slot, {})
    current_context: Any = _first_truthy(_prop(output, "booking_context"), orch_booking_context, {})
    orch_context = _first_truthy(
        _prop(orch_output, "booking_context"), _prop(orch_resolved, "booking_context"),
        _prop(orch_output, "slot_state"), _prop(orch_resolved, "slot_state"), {},
    )
    if not (_js_truthy(_prop(current_context, "doctor_name")) or _js_truthy(_prop(current_context, "doctor_id"))):
        current_context = _first_truthy(orch_context, current_context, {})
    if new_booking_restart:
        safe_current = _obj_or_none(_prop(orch_output, "booking_context"))
        if safe_current is None:
            safe_current = _obj_or_none(_prop(orch_resolved, "booking_context"))
        safe_current = safe_current if isinstance(safe_current, dict) else {}
        current_context = {
            "doctor_id": _first_truthy(_prop(safe_current, "doctor_id"), None),
            "doctor_name": _first_truthy(_prop(safe_current, "doctor_name"), None),
            "service_id": _first_truthy(_prop(safe_current, "service_id"), None),
            "service_name": _first_truthy(_prop(safe_current, "service_name"), None),
            "appointment_type": _first_truthy(_prop(safe_current, "appointment_type"), None),
            "date": _first_truthy(_prop(safe_current, "date"), None),
            "time": _first_truthy(_prop(safe_current, "time"), None),
            "slot_id": _first_truthy(_prop(safe_current, "slot_id"), None),
            "booking_number": _first_truthy(_prop(safe_current, "booking_number"), None),
        }
    current_turn_lineage = (
        _obj_or_none(_prop(output, "turn_lineage"))
        or _obj_or_none(_prop(item, "turn_lineage"))
        or _obj_or_none(_prop(normalization, "turn_lineage"))
        or {}
    )
    current_lookup_lineage = (
        _obj_or_none(_prop(output, "availability_lineage"))
        or _obj_or_none(_dig(output, "deterministic_slot_lookup", "lineage"))
        or _obj_or_none(_prop(item, "availability_lineage"))
        or _first_truthy(_dig(item, "deterministic_slot_lookup", "lineage"), {})
    )
    lookup_matches_current_turn = bool(
        _js_truthy(_prop(current_turn_lineage, "turn_key"))
        and _js_truthy(_prop(current_lookup_lineage, "turn_key"))
        and _js_string(_prop(current_turn_lineage, "turn_key")) == _js_string(_prop(current_lookup_lineage, "turn_key"))
        and (_js_truthy(_prop(current_lookup_lineage, "message_id")) or _js_truthy(_prop(current_lookup_lineage, "turn_id")))
    )
    if new_booking_restart and not lookup_matches_current_turn:
        current_slot = {
            "doctor_id": _first_truthy(_prop(current_context, "doctor_id"), None),
            "doctor_name": _first_truthy(_prop(current_context, "doctor_name"), None),
            "service_id": _first_truthy(_prop(current_context, "service_id"), None),
            "service_name": _first_truthy(_prop(current_context, "service_name"), None),
            "appointment_type": _first_truthy(_prop(current_context, "appointment_type"), None),
            "date": _first_truthy(_prop(current_context, "date"), None),
            "time": _first_truthy(_prop(current_context, "time"), None),
            "slot_id": _first_truthy(_prop(current_context, "slot_id"), None),
        }
    # P42c companion (journey T6, 2026-09-19): the orchestrator's bound
    # confirmation_target is the slot authority — NAO's booking_context never sees a
    # tool-bound slot, so without this fallback the saved booking_context/slot_state
    # lost slot_id and the transition guard's create data_valid failed at the affirm
    # turn (INVALID_STATE_TRANSITION instead of execution).
    orch_bound_target: Any = _dig(decision, "confirmation_target")
    orch_bound_target = orch_bound_target if isinstance(orch_bound_target, dict) else {}
    slot_state: Dict[str, Any] = {
        "doctor_id": _keep(_prop(current_slot, "doctor_id"), _prop(previous_slot, "doctor_id")),
        "doctor_name": _keep(_prop(current_slot, "doctor_name"), _prop(previous_slot, "doctor_name")),
        "service_id": _keep(_prop(current_slot, "service_id"), _prop(previous_slot, "service_id")),
        "service_name": _keep(_prop(current_slot, "service_name"), _prop(previous_slot, "service_name")),
        "appointment_type": _keep(_prop(current_slot, "appointment_type"), _prop(previous_slot, "appointment_type")),
        "date": _keep(_prop(current_slot, "date"), _first_truthy(_prop(previous_slot, "date"), _prop(orch_bound_target, "date"), None)),
        "time": _keep(_prop(current_slot, "time"), _first_truthy(_prop(previous_slot, "time"), _prop(orch_bound_target, "time"), None)),
        "slot_id": _keep(_prop(current_slot, "slot_id"), _first_truthy(_prop(previous_slot, "slot_id"), _prop(orch_bound_target, "slot_id"), None)),
        "booking_number": _keep(_prop(current_slot, "booking_number"), _prop(previous_slot, "booking_number")),
    }
    booking_context: Dict[str, Any] = {
        "doctor_id": _keep(_prop(current_context, "doctor_id"), _first_truthy(_prop(previous_booking_fallback, "doctor_id"), _prop(slot_state, "doctor_id"), None)),
        "doctor_name": _keep(_prop(current_context, "doctor_name"), _first_truthy(_prop(previous_booking_fallback, "doctor_name"), _prop(slot_state, "doctor_name"), None)),
        "service_id": _keep(_prop(current_context, "service_id"), _first_truthy(_prop(previous_booking_fallback, "service_id"), _prop(slot_state, "service_id"), None)),
        "service_name": _keep(_prop(current_context, "service_name"), _first_truthy(_prop(previous_booking_fallback, "service_name"), _prop(slot_state, "service_name"), None)),
        "appointment_type": _keep(_prop(current_context, "appointment_type"), _first_truthy(_prop(previous_booking_fallback, "appointment_type"), _prop(slot_state, "appointment_type"), None)),
        "slot_id": _keep(_prop(current_context, "slot_id"), _first_truthy(_prop(previous_booking_fallback, "slot_id"), _prop(slot_state, "slot_id"), _prop(orch_bound_target, "slot_id"), None)),
        "date": _keep(_prop(current_context, "date"), _first_truthy(_prop(previous_booking_fallback, "date"), _prop(slot_state, "date"), _prop(orch_bound_target, "date"), None)),
        "time": _keep(_prop(current_context, "time"), _first_truthy(_prop(previous_booking_fallback, "time"), _prop(slot_state, "time"), _prop(orch_bound_target, "time"), None)),
        "booking_number": _keep(_prop(current_context, "booking_number"), _first_truthy(_prop(previous_booking_fallback, "booking_number"), _prop(slot_state, "booking_number"), None)),
    }
    persistent_prior_reference = bool(
        _dig(output, "contract", "entities", "references_prior_conversation") is True
        or _prop(output, "references_prior_conversation") is True
        or _js_trim(_js_string(_first_truthy(_dig(output, "contract", "entities", "references_prior_conversation"), ""))).lower() == "true"
    )
    persistent_operation_name = _js_trim(_js_string(_first_truthy(
        _prop(output, "active_operation"), _prop(decision, "active_operation"),
        _prop(output, "operation_action"), _prop(decision, "operation_action"),
        _prop(previous, "active_operation"), _prop(previous, "operation_action"), "",
    ))).lower()
    persistent_operation_state = _js_trim(_js_string(_first_truthy(
        _prop(output, "operation_status"), _prop(decision, "operation_status"), _prop(previous, "operation_status"), "",
    ))).upper()
    persistent_turn_intent = _js_trim(_js_string(_first_truthy(
        _prop(decision, "intent"), _dig(output, "contract", "turn", "intent"), _prop(output, "intent"),
        _prop(previous, "current_intent"), _prop(previous, "last_intent"), "",
    ))).lower()
    current_turn_requests_scheduling = bool(
        _js_truthy(_prop(current_context, "date")) or _js_truthy(_prop(current_context, "time"))
        or _prop(output, "availability_inquiry") is True
        or _prop(decision, "availability_inquiry") is True
        or _dig(output, "contract", "operation_proposal", "requested") is True
        or _dig(decision, "contract", "operation_proposal", "requested") is True
        or _dig(output, "query", "type") == "availability"
        or _dig(decision, "query", "type") == "availability"
    )
    clear_historical_context_on_small_talk = bool(
        (
            persistent_turn_intent == "small_talk"
            and not persistent_prior_reference
            and persistent_operation_name not in ("create_appointment", "cancel_appointment", "reschedule_appointment")
            and persistent_operation_state not in ("DRAFT", "PENDING", "CONFIRMATION_REQUIRED")
            and not _js_truthy(_prop(decision, "confirmation_target"))
        )
        or (expired_prior_draft and not persistent_prior_reference and not current_turn_requests_scheduling)
    )
    if clear_historical_context_on_small_talk:
        fields_to_clear = ["slot_id"] if expired_prior_draft else ["slot_id", "date", "time"]
        for field in fields_to_clear:
            booking_context[field] = None
            slot_state[field] = None
    previous_facts = _first_truthy(_prop(previous, "facts"), {})
    db_patient_record_present = bool(
        _js_truthy(_prop(clinic, "patient_id"))
        or _js_truthy(_prop(clinic, "patient_name"))
        or _js_truthy(_prop(clinic, "patient_phone"))
        or _js_truthy(_prop(clinic, "patient_mobile"))
        or (_prop(clinic, "patient_age") is not None and _prop(clinic, "patient_age") is not _UNDEFINED)
        or _js_truthy(_prop(clinic, "patient_address"))
    )
    previous_patient_name = _first_truthy(
        _usable_patient_name(_dig(previous_facts, "patient", "name")),
        _usable_patient_name(_dig(previous, "patient_data_review", "fields", "name")),
    )
    current_turn_patient_name = _usable_patient_name(orch_patient_name)
    clinic_patient_name = _usable_patient_name(_prop(clinic, "patient_name"))
    telegram_patient_name_trusted = bool(
        (not is_telegram_channel)
        or orch_patient_name_source == "user_entered"
        or _dig(previous_facts, "patient", "name_source") == "user_entered"
    )
    prior_patient_name_candidate = _first_truthy(previous_patient_name, None) if telegram_patient_name_trusted else None
    clinic_patient_name_for_state = clinic_patient_name if telegram_patient_name_trusted else None
    patient_record_for_review = bool((not is_telegram_channel) or telegram_patient_name_trusted)
    persisted_patient = {
        "patient_name": _first_truthy(
            current_turn_patient_name,
            prior_patient_name_candidate,
            (clinic_patient_name_for_state if db_patient_record_present else None),
            None,
        ),
        "patient_name_source": (
            _first_truthy(orch_patient_name_source, "user_entered")
            if current_turn_patient_name
            else (
                _first_truthy(_dig(previous_facts, "patient", "name_source"), "stored")
                if (patient_record_for_review and (prior_patient_name_candidate or clinic_patient_name_for_state))
                else None
            )
        ),
        "patient_phone": _first_truthy(
            orch_patient_phone,
            (
                _first_truthy(_prop(clinic, "patient_phone"), _prop(clinic, "patient_mobile"))
                if db_patient_record_present
                else _first_truthy(_dig(previous_facts, "patient", "phone"), _dig(previous_facts, "patient", "mobile"))
            ),
            None,
        ),
        "patient_age": _coalesce(
            orch_patient_age,
            (_prop(clinic, "patient_age") if db_patient_record_present else _dig(previous_facts, "patient", "age")),
            None,
        ),
        "patient_address": _first_truthy(
            orch_patient_address,
            (_prop(clinic, "patient_address") if db_patient_record_present else _dig(previous_facts, "patient", "address")),
            None,
        ),
    }
    for field in ("patient_name", "patient_name_source", "patient_phone", "patient_age", "patient_address"):
        booking_context[field] = persisted_patient[field]
    if abandon_current_booking:
        for field in ("doctor_id", "doctor_name", "service_id", "service_name", "appointment_type", "slot_id", "date", "time", "booking_number", "branch_id", "branch_name"):
            booking_context[field] = None
    sfc = _prop(output, "service_fact_context")
    service_fact_context = sfc if isinstance(sfc, dict) else {}
    current_service_catalog: Optional[List[Any]] = None
    if _prop(service_fact_context, "source") == "catalog":
        lsc = _prop(service_fact_context, "last_service_catalog")
        if isinstance(lsc, list) and len(lsc) > 0:
            current_service_catalog = lsc
    prev_facts_dict = previous_facts if isinstance(previous_facts, dict) else {}
    prev_patient_obj = _prop(previous_facts, "patient")
    prev_patient_obj = prev_patient_obj if isinstance(prev_patient_obj, dict) else {}
    prev_booking_obj = _prop(previous_facts, "booking")
    prev_booking_obj = prev_booking_obj if isinstance(prev_booking_obj, dict) else {}
    facts: Dict[str, Any] = {
        **prev_facts_dict,
        **({"last_service_catalog": current_service_catalog, "last_service_catalog_at": _utc_now_iso()} if current_service_catalog else {}),
        "patient": {
            **prev_patient_obj,
            "patient_id": _coalesce(_prop(ctx, "patient_id"), None),
            "name": persisted_patient["patient_name"],
            "name_source": persisted_patient["patient_name_source"],
            "phone": persisted_patient["patient_phone"],
            "age": persisted_patient["patient_age"],
            "address": persisted_patient["patient_address"],
        },
        "clinic": {
            "id": _coalesce(_prop(ctx, "clinic_id"), None),
            "name": _first_truthy(_prop(clinic, "clinic_name"), _dig(previous_facts, "clinic", "name"), None),
        },
        "channel": {
            "type": _coalesce(_prop(ctx, "channel_type"), None),
            "id": _coalesce(_prop(ctx, "channel_id"), None),
            "key": _coalesce(_prop(ctx, "channel_key"), None),
        },
        "booking": {
            **({} if (new_booking_restart or abandon_current_booking or clear_historical_context_on_small_talk) else prev_booking_obj),
            **booking_context,
            "booking_number": (
                None
                if (new_booking_restart or abandon_current_booking) and not _js_truthy(execution_booking_number)
                else _first_truthy(
                    execution_booking_number,
                    _dig(previous_facts, "booking", "booking_number"),
                    _prop(previous, "booking_number"),
                    _prop(booking_context, "booking_number"),
                    None,
                )
            ),
            "appointment_id": (
                None
                if (new_booking_restart or abandon_current_booking or clear_historical_context_on_small_talk)
                else _first_truthy(
                    _prop(output, "appointment_id"),
                    _dig(previous_facts, "booking", "appointment_id"),
                    _prop(previous, "appointment_id"),
                    None,
                )
            ),
        },
    }
    prior_turns = _prop(previous, "recent_turns")
    prior_turns = list(prior_turns) if isinstance(prior_turns, list) else []
    canonical_reply = _js_trim(_js_string(_first_truthy(
        _prop(item, "rendered_reply"), _prop(item, "final_reply"), _prop(item, "reply_text"),
        _prop(output, "rendered_reply"), _prop(output, "final_reply"), _prop(output, "reply_text"), "",
    )))
    requested_fields_from_actual_reply = _k2q_asked_from_reply(canonical_reply)
    dmhf = _prop(decision, "missing_human_fields")
    omhf = _prop(output, "missing_human_fields")
    current_missing_human_fields_for_question = dmhf if isinstance(dmhf, list) else (omhf if isinstance(omhf, list) else [])
    current_next_best_missing_field_for_question = _first_truthy(
        _js_trim(_js_string(_first_truthy(
            _prop(decision, "next_best_missing_human_field"), _prop(output, "next_best_missing_human_field"),
            _dig(output, "contract", "next_step", "field"), _dig(output, "next_step", "field"), "",
        ))),
        None,
    )
    current_requested_fields_for_question: List[str] = []
    for v in current_missing_human_fields_for_question:
        sv = _js_trim(_js_string(_first_truthy(v, "")))
        if sv and sv not in current_requested_fields_for_question:
            current_requested_fields_for_question.append(sv)
    if current_next_best_missing_field_for_question and current_next_best_missing_field_for_question not in current_requested_fields_for_question:
        current_requested_fields_for_question.append(current_next_best_missing_field_for_question)
    # v34: plan-first, strong-ask override.
    reply_strong_asked = [f for f in requested_fields_from_actual_reply if f != "confirmation"]
    if reply_strong_asked:
        if current_requested_fields_for_question and any(
            any(_same_field(f, g) for g in reply_strong_asked) for f in current_requested_fields_for_question
        ):
            open_question_fields = current_requested_fields_for_question
        else:
            open_question_fields = reply_strong_asked
    else:
        open_question_fields = current_requested_fields_for_question
    assistant_requested_fields = open_question_fields
    reply_failed = _prop(item, "reply_failed") is True or _prop(output, "reply_failed") is True
    user_turn = {
        "role": "user",
        "text": _coalesce(_prop(ctx, "message_text"), None),
        "at": _coalesce(_prop(ctx, "received_at"), None),
        "channel": _coalesce(_prop(ctx, "channel_type"), None),
    }
    assistant_turn = {
        "role": "assistant",
        "text": canonical_reply,
        "requested_fields": assistant_requested_fields,
        "requested_information": assistant_requested_fields,
        "at": _utc_now_iso(),
        "channel": _coalesce(_prop(ctx, "channel_type"), None),
    }
    # Session inactivity compaction (2026-09-22, 2h window): past the gap the prior
    # raw turns are NOT carried into the new session — their essence travels as the
    # structured previous_session_summary instead (see app/core/session_compact.py).
    _boundary = session_compact.session_boundary(previous, _prop(ctx, "received_at"))
    prior_turns = [] if _boundary else prior_turns
    recent_turns = [
        t for t in (list(prior_turns) + [user_turn] + ([assistant_turn] if not reply_failed else [None]))
        if t is not None
    ][-6:]
    session_recent_turns = recent_turns[-2:] if context_session_reset else recent_turns
    last_assistant_turn_for_patient_evidence: Optional[Dict[str, Any]] = None
    for t in reversed(recent_turns):
        if _js_truthy(t) and _prop(t, "role") == "assistant":
            last_assistant_turn_for_patient_evidence = t if isinstance(t, dict) else {}
            break
    intent = _first_truthy(_prop(output, "intent"), _prop(previous, "last_intent"), _prop(previous, "current_intent"), None)
    finalize_response = _prop(item, "operation_finalize_response")
    finalize_response = finalize_response if isinstance(finalize_response, dict) else {}
    response_code = _first_truthy(
        _prop(decision, "response_code"), _prop(item, "response_code"), _prop(output, "response_code"),
        _prop(finalize_response, "response_code"), None,
    )
    execution_appointment_id = _first_truthy(
        _prop(item, "appointment_id"), _prop(finalize_response, "appointment_id"), _prop(output, "appointment_id"), None,
    )
    execution_booking_number = _first_truthy(
        _prop(item, "booking_number"), _prop(finalize_response, "booking_number"), _prop(output, "booking_number"),
        _prop(booking_context, "booking_number"), _prop(previous, "booking_number"), None,
    )
    dt = _prop(decision, "confirmation_target")
    decision_target = dt if isinstance(dt, dict) else None
    configured_draft_ttl_raw = _coalesce(
        _prop(clinic, "draft_ttl_seconds"), _prop(clinic, "draft_ttl"),
        _prop(previous, "draft_ttl_seconds"), 1800,
    )
    ttl_num = _js_number(configured_draft_ttl_raw)
    if _is_finite_num(ttl_num):
        draft_ttl_seconds = _int_if_integral(max(60.0, min(86400.0, math.trunc(ttl_num))))
    else:
        draft_ttl_seconds = 1800
    decision_action = (
        _prop(decision, "action")
        if (_js_truthy(_prop(decision, "action")) and _prop(decision, "action") != "none")
        else _first_truthy(_prop(decision_target, "action"), None)
    )
    supplied_original_response_code = _first_truthy(
        _prop(output, "original_response_code"), _prop(decision, "original_response_code"),
        _prop(previous, "original_response_code"), None,
    )
    replay_original_by_action = (
        "CANCEL_COMPLETED" if decision_action == "cancel_appointment"
        else ("RESCHEDULE_COMPLETED" if decision_action == "reschedule_appointment"
              else ("APPOINTMENT_CREATED" if decision_action == "create_appointment" else None))
    )
    replay_original_response_code = (
        _first_truthy(supplied_original_response_code, replay_original_by_action, None)
        if response_code == "IDEMPOTENT_REPLAY" else None
    )
    replay_origin_unknown = bool(response_code == "IDEMPOTENT_REPLAY" and not replay_original_response_code)
    explicit_continuation = bool(
        _prop(output, "explicit_continuation") is True
        or _prop(decision, "event") == "USER_EXPLICIT_CONTINUATION"
        or _prop(output, "event") == "USER_EXPLICIT_CONTINUATION"
    )
    handoff_required = bool(
        response_code == "HANDOFF_REQUIRED"
        and _prop(decision, "response_code") == "HANDOFF_REQUIRED"
        and _prop(decision, "escalation_requested") is True
    )
    is_create_operation = bool(decision_action == "create_appointment" or _prop(decision_target, "action") == "create_appointment")
    clears_create_operation = bool(decision_action in ("cancel_appointment", "reschedule_appointment"))
    pt = _prop(previous, "confirmation_target")
    previous_target: Optional[Dict[str, Any]] = (
        None if (new_booking_restart or expired_prior_draft or abandon_current_booking) else (pt if isinstance(pt, dict) else None)
    )

    def _pt(key: str) -> Any:
        return _prop(previous_target, key) if previous_target is not None else _UNDEFINED

    previous_target_expires = _date_parse(_pt("expires_at")) if _js_truthy(_pt("expires_at")) else _NAN

    def _state_target_uuid(value: Any) -> bool:
        return bool(_TARGET_UUID_RE.search(_js_trim(_js_string(_first_truthy(value, "")))))

    def _state_prompt_uuid(value: Any) -> bool:
        return bool(_PROMPT_UUID_RE.search(_js_trim(_js_string(_first_truthy(value, "")))))

    previous_target_identity_valid = bool(
        previous_target is not None
        and _js_string(_first_truthy(_pt("clinic_id"), "")) == _js_string(_first_truthy(_prop(ctx, "clinic_id"), ""))
        and _js_string(_first_truthy(_pt("patient_id"), "")) == _js_string(_first_truthy(_prop(ctx, "patient_id"), ""))
        and _js_string(_first_truthy(_pt("conversation_id"), "")) == _js_string(_first_truthy(_prop(ctx, "conversation_id"), ""))
        and _js_truthy(_pt("operation_id"))
        and _js_truthy(_pt("last_user_message_id_at_request"))
    )
    if previous_target is not None and _pt("action") == "create_appointment":
        previous_target_shape_valid = bool(
            previous_target_identity_valid
            and _state_target_uuid(_pt("doctor_id"))
            and _state_target_uuid(_pt("slot_id"))
            and bool(_DATE_ONLY_RE.search(_js_string(_first_truthy(_pt("date"), ""))))
            and bool(_TIME_RE.search(_js_string(_first_truthy(_pt("time"), ""))))
            and _js_string(_first_truthy(_pt("appointment_type"), "")).upper() in ("NEW_VISIT", "FOLLOW_UP")
        )
    elif previous_target is not None and _pt("action") == "cancel_appointment":
        previous_target_shape_valid = bool(previous_target_identity_valid and _state_target_uuid(_pt("appointment_id")))
    elif previous_target is not None and _pt("action") == "reschedule_appointment":
        previous_target_shape_valid = bool(
            previous_target_identity_valid
            and _state_target_uuid(_pt("appointment_id"))
            and _state_target_uuid(_pt("expected_old_slot_id"))
            and _state_target_uuid(_pt("new_slot_id"))
        )
    else:
        previous_target_shape_valid = False
    previous_target_binding_valid = bool(
        _js_string(_first_truthy(_pt("confirmation_delivery_status"), "")).lower() in ("sent", "pending")
        and _js_truthy(_pt("confirmation_delivery_recorded_at"))
        and _state_prompt_uuid(_pt("confirmation_prompt_message_id"))
        and _js_truthy(_pt("last_user_message_id_at_request"))
        and _pt("invalidated") is not True
    )
    previous_target_created = _date_parse(_pt("created_at")) if _js_truthy(_pt("created_at")) else _NAN
    previous_target_ttl = _js_number(_first_truthy(_pt("confirmation_ttl_seconds"), _prop(previous, "confirmation_ttl_seconds"), 600))
    previous_target_computed_expiry = bool(
        _is_finite_num(previous_target_created)
        and _is_finite_num(previous_target_ttl)
        and previous_target_created + max(60, previous_target_ttl) * 1000 > _now_ms()
    )
    previous_target_live_expiry = bool(_is_finite_num(previous_target_expires) and previous_target_expires > _now_ms())
    previous_target_valid = bool(
        (not new_booking_restart)
        and _js_truthy(previous_target)
        and (previous_target_live_expiry or previous_target_computed_expiry)
        and previous_target_shape_valid
        and previous_target_binding_valid
        and _pt("invalidated") is not True
    )
    previous_target_invalid = bool(_js_truthy(previous_target) and not previous_target_valid)
    invalid_state_transition = bool(response_code == "INVALID_STATE_TRANSITION")
    previous_operation_action = (
        None
        if (new_booking_restart or expired_prior_draft or abandon_current_booking or context_session_reset)
        else _first_truthy(_prop(previous, "operation_action"), _prop(previous, "active_operation"), None)
    )
    canonical_operation_action: Any = (
        "create_appointment" if (new_booking_restart or expired_prior_draft)
        else _first_truthy(decision_action, previous_operation_action, None)
    )
    fresh_operation_id = _first_truthy(
        _prop(output, "operation_id"), _prop(decision, "operation_id"), _prop(ctx, "operation_id"),
        _js_string(_prop(ctx, "idempotency_key")) + ":create_appointment",
    )
    canonical_operation_id: Any = (
        None if abandon_current_booking
        else (
            fresh_operation_id if new_booking_restart
            else _first_truthy(
                _prop(output, "operation_id"), _prop(decision_target, "operation_id"),
                _prop(decision, "operation_id"), _prop(previous, "operation_id"),
                _dig(previous, "confirmation_target", "operation_id"), None,
            )
        )
    )
    if response_code == "APPOINTMENT_CREATED":
        original_response_code: Any = "APPOINTMENT_CREATED"
    elif response_code == "CANCEL_COMPLETED":
        original_response_code = "CANCEL_COMPLETED"
    elif response_code == "RESCHEDULE_COMPLETED":
        original_response_code = "RESCHEDULE_COMPLETED"
    elif response_code == "IDEMPOTENT_REPLAY":
        original_response_code = replay_original_response_code
    else:
        original_response_code = None if (is_create_operation or clears_create_operation) else _first_truthy(_prop(previous, "original_response_code"), None)
    output_status = _js_string(_first_truthy(
        _prop(item, "operation_status"), _prop(output, "operation_status"),
        _prop(finalize_response, "operation_status"), _prop(previous, "operation_status"), "",
    )).lower()
    operation_state: Any = "idle" if abandon_current_booking else _first_truthy(output_status, _prop(previous, "operation_status"), "idle")
    if response_code in ("APPOINTMENT_CREATED", "RESCHEDULE_COMPLETED"):
        operation_state = "completed"
    elif response_code == "CANCEL_COMPLETED":
        operation_state = "cancelled"
    elif response_code == "IDEMPOTENT_REPLAY":
        if replay_original_response_code == "CANCEL_COMPLETED" or canonical_operation_action == "cancel_appointment":
            operation_state = "cancelled"
        elif replay_original_response_code in ("APPOINTMENT_CREATED", "RESCHEDULE_COMPLETED") or canonical_operation_action in ("create_appointment", "reschedule_appointment"):
            operation_state = "completed"
        else:
            operation_state = "refresh_required"
    elif invalid_state_transition:
        operation_state = "refresh_required"
    elif handoff_required:
        operation_state = "paused"
    elif response_code in _TRANSIENT_FAILURE_CODES:
        operation_state = "failed_retryable"
    elif response_code in _FINAL_FAILURE_CODES:
        operation_state = "failed_final"
    elif response_code == "CONFIRMATION_REQUIRED":
        operation_state = "awaiting_confirmation"
    elif (new_booking_restart or response_code == "NEW_BOOKING_STARTED") and not previous_target_valid:
        operation_state = "collecting_details"
    elif response_code == "CONFIRMATION_EXPIRED":
        operation_state = "refresh_required"
    elif response_code in ("EXECUTE_APPROVED", "CANCEL_APPROVED"):
        operation_state = "executing"
    elif previous_target_invalid and operation_state == "awaiting_confirmation":
        operation_state = "refresh_required"
    faq_turn = bool(
        _prop(output, "intent") == "faq"
        or _prop(decision, "intent") == "faq"
        or _prop(normalization, "inferred_intent") == "faq"
    )
    has_live_operation = bool(
        (not context_session_reset) and (not new_booking_restart) and (not expired_prior_draft)
        and _js_truthy(_prop(previous, "active_operation"))
        and _js_string(_first_truthy(_prop(previous, "operation_status"), "")).lower() not in ("completed", "failed", "cancelled")
    )
    faq_pauses_operation = bool(faq_turn and has_live_operation)
    if faq_pauses_operation:
        operation_state = "paused"
    if abandon_current_booking:
        active_operation: Any = None
    elif new_booking_restart:
        active_operation = "create_appointment"
    elif response_code in _TRANSIENT_FAILURE_CODES:
        active_operation = _first_truthy(
            previous_operation_action, decision_action,
            ("create_appointment" if intent == "booking" else None), None,
        )
    elif response_code in _TERMINAL_RESPONSE_CODES:
        active_operation = None
    else:
        active_operation = _first_truthy(
            decision_action, _prop(previous, "active_operation"),
            ("create_appointment" if intent == "booking" else None), None,
        )
    if faq_pauses_operation:
        active_operation = _prop(previous, "active_operation")
    if abandon_current_booking:
        active_operation = None
    if abandon_current_booking:
        confirmation_state: Any = None
    elif previous_target_invalid and not (_js_truthy(_prop(decision, "confirmation_target")) and isinstance(_prop(decision, "confirmation_target"), dict)):
        confirmation_state = "expired"
    else:
        confirmation_state = _first_truthy(_prop(previous, "confirmation_state"), None)
    if response_code == "CONFIRMATION_REQUIRED":
        confirmation_state = "required"
    elif response_code == "CONFIRMATION_EXPIRED":
        confirmation_state = "expired"
    elif response_code in ("EXECUTE_APPROVED", "CANCEL_APPROVED"):
        confirmation_state = "executing"
    elif invalid_state_transition:
        confirmation_state = "expired"
    elif response_code in ("APPOINTMENT_CREATED", "RESCHEDULE_COMPLETED"):
        confirmation_state = "confirmed"
    elif response_code == "CANCEL_COMPLETED":
        confirmation_state = "cancelled"
    elif response_code == "IDEMPOTENT_REPLAY":
        confirmation_state = (
            "expired" if replay_origin_unknown
            else ("cancelled" if (replay_original_response_code == "CANCEL_COMPLETED" or canonical_operation_action == "cancel_appointment") else "confirmed")
        )
    elif response_code in _TRANSIENT_FAILURE_CODES or response_code in _FINAL_FAILURE_CODES:
        confirmation_state = "failed"
    clears_confirmation_for_response = bool(response_code in _TERMINAL_RESPONSE_CODES or invalid_state_transition)
    upstream_ct = _dig(item, "output", "confirmation_target")
    upstream_applied_target = upstream_ct if isinstance(upstream_ct, dict) else None
    target: Any = (
        None if clears_confirmation_for_response
        else _first_truthy(decision_target, upstream_applied_target, (previous_target if previous_target_valid else None), None)
    )
    if _js_truthy(target) and isinstance(target, dict):
        if not _js_truthy(_prop(target, "clinic_id")):
            target["clinic_id"] = _first_truthy(_prop(ctx, "clinic_id"), None)
        if not _js_truthy(_prop(target, "patient_id")):
            target["patient_id"] = _first_truthy(_prop(ctx, "patient_id"), None)
        if not _js_truthy(_prop(target, "conversation_id")):
            target["conversation_id"] = _first_truthy(_prop(ctx, "conversation_id"), None)
        if _js_truthy(_prop(target, "slot_id")):
            pmid = _js_string(_first_truthy(_prop(target, "confirmation_prompt_message_id"), ""))
            if not _PROMPT_UUID_RE.search(pmid):
                target["confirmation_prompt_message_id"] = _mint_uuid(_first_truthy(
                    _prop(target, "operation_id"),
                    _js_string(_prop(target, "clinic_id")) + ":" + _js_string(_first_truthy(_prop(target, "last_user_message_id_at_request"), "")),
                ))
    if _js_truthy(target) and isinstance(target, dict):
        target_bound = bool(
            _js_string(_first_truthy(_prop(target, "clinic_id"), "")) == _js_string(_first_truthy(_prop(ctx, "clinic_id"), ""))
            and _js_string(_first_truthy(_prop(target, "patient_id"), "")) == _js_string(_first_truthy(_prop(ctx, "patient_id"), ""))
            and _js_string(_first_truthy(_prop(target, "conversation_id"), "")) == _js_string(_first_truthy(_prop(ctx, "conversation_id"), ""))
            and _js_truthy(_prop(target, "operation_id"))
            and _js_truthy(_prop(target, "last_user_message_id_at_request"))
        )
        if not target_bound:
            target = None
    if faq_pauses_operation:
        target = None
        confirmation_state = "invalidated" if _js_truthy(previous_target) else _first_truthy(_prop(previous, "confirmation_state"), None)
    canonical_state_map = {
        "idle": "IDLE",
        "collecting_details": "DRAFT",
        "draft": "DRAFT",
        "paused": "PAUSED",
        "awaiting_confirmation": "AWAITING_CONFIRMATION",
        "refresh_required": "REFRESH_REQUIRED",
        "executing": "EXECUTING",
        "completed": "CANCELLED" if canonical_operation_action == "cancel_appointment" else "COMPLETED",
        "cancelled": "CANCELLED",
        "failed_retryable": "FAILED_RETRYABLE",
        "failed": "FAILED_FINAL",
        "failed_final": "FAILED_FINAL",
    }
    canonical_operation_state = _first_truthy(
        canonical_state_map.get(_js_string(operation_state).lower()),
        _first_truthy(_prop(previous, "operation_state"), "IDLE"),
    )
    has_booking_draft_context = bool(any(
        v is not None and v is not _UNDEFINED and _js_trim(_js_string(v)) != ""
        for v in booking_context.values()
    ))
    if canonical_operation_state == "IDLE" and _js_truthy(active_operation) and has_booking_draft_context and response_code not in _TERMINAL_FOR_STATE_REPAIR:
        operation_state = "collecting_details"
        canonical_operation_state = "DRAFT"
    if not _js_truthy(active_operation) and not has_booking_draft_context and not _js_truthy(target) and canonical_operation_state == "IDLE":
        operation_state = "idle"
    if handoff_required or faq_pauses_operation:
        canonical_operation_state = "PAUSED"
    if response_code == "CANCEL_COMPLETED" or (
        response_code == "IDEMPOTENT_REPLAY" and (replay_original_response_code == "CANCEL_COMPLETED" or canonical_operation_action == "cancel_appointment")
    ):
        canonical_operation_state = "CANCELLED"
    if response_code in ("APPOINTMENT_CREATED", "RESCHEDULE_COMPLETED") or (
        response_code == "IDEMPOTENT_REPLAY" and not replay_origin_unknown and replay_original_response_code != "CANCEL_COMPLETED" and canonical_operation_action != "cancel_appointment"
    ):
        canonical_operation_state = "COMPLETED"
    if response_code == "IDEMPOTENT_REPLAY" and replay_origin_unknown:
        canonical_operation_state = "REFRESH_REQUIRED"
    if invalid_state_transition:
        canonical_operation_state = "REFRESH_REQUIRED"
    migration_issues: List[str] = list(_prop(previous, "migration_issues")) if isinstance(_prop(previous, "migration_issues"), list) else []
    if previous_target_invalid and _js_string(operation_state) in ("awaiting_confirmation", "AWAITING_CONFIRMATION"):
        migration_issues.append("confirmation_target_invalid_or_undelivered")
    if handoff_required:
        migration_issues.append("handoff_required_paused")
    if replay_origin_unknown:
        migration_issues.append("replay_origin_unknown")
    if invalid_state_transition:
        migration_issues.append("invalid_transition_refresh_required")
    resume_eligible = bool(
        canonical_operation_state in ("DRAFT", "PAUSED", "AWAITING_CONFIRMATION", "REFRESH_REQUIRED")
        and not handoff_required
    )
    retryable = bool(canonical_operation_state == "FAILED_RETRYABLE" or response_code in _TRANSIENT_FAILURE_CODES)
    failure_code = _first_truthy(response_code, _prop(previous, "failure_code"), None) if (retryable or canonical_operation_state == "FAILED_FINAL") else None
    migration_status = _first_truthy(_prop(previous, "migration_status"), "REVIEW_REQUIRED" if migration_issues else "MIGRATED_V1")
    routing_action = "HANDOFF_REQUIRED" if handoff_required else (None if explicit_continuation else _first_truthy(_prop(previous, "routing_action"), None))
    conversation_stage = _first_truthy(_prop(decision, "conversation_stage"), _prop(previous, "conversation_stage"), None)
    required_next_step = _first_truthy(_prop(decision, "required_next_step"), _prop(previous, "required_next_step"), None)
    if conversation_stage == "WAITING_PATIENT_DATA_CONFIRMATION":
        pending_action: Any = "confirm_patient_data"
    elif conversation_stage == "COLLECTING_PATIENT_DATA":
        pending_action = "collect_patient_data"
    elif canonical_operation_state == "PAUSED":
        pending_action = "handoff_required" if routing_action == "HANDOFF_REQUIRED" else "resume_booking_operation"
    elif canonical_operation_state == "AWAITING_CONFIRMATION":
        pending_action = (
            "confirm_cancel_appointment" if active_operation == "cancel_appointment"
            else ("confirm_reschedule_appointment" if active_operation == "reschedule_appointment" else "confirm_create_appointment")
        )
    elif canonical_operation_state == "REFRESH_REQUIRED":
        pending_action = "refresh_booking_slot"
    elif canonical_operation_state == "DRAFT":
        pending_action = "collect_booking_details"
    else:
        pending_action = None
    should_persist_open_question = bool(
        response_code in ("MISSING_REQUIRED_FIELDS", "PATIENT_DATA_CONFIRMATION_REQUIRED", "CONFIRMATION_REQUIRED")
        or canonical_operation_state == "AWAITING_CONFIRMATION"
    )
    if open_question_fields:
        persisted_requested_fields: List[Any] = open_question_fields
    elif response_code == "PATIENT_DATA_CONFIRMATION_REQUIRED":
        persisted_requested_fields = ["name", "age", "address", "phone"]
    else:
        la_req = _prop(last_assistant_turn_for_patient_evidence, "requested_fields") if last_assistant_turn_for_patient_evidence is not None else _UNDEFINED
        loq_req = _dig(previous, "last_open_question", "requested_fields")
        persisted_requested_fields = la_req if (isinstance(la_req, list) and len(la_req) > 0) else (loq_req if isinstance(loq_req, list) else [])
    persisted_last_open_question = {
        "message": _first_truthy(canonical_reply, _dig(previous, "last_open_question", "message"), None) if should_persist_open_question else None,
        "requested_fields": persisted_requested_fields,
        "type": (
            ("confirmation" if (response_code == "CONFIRMATION_REQUIRED" or canonical_operation_state == "AWAITING_CONFIRMATION") else "missing_field")
            if should_persist_open_question else None
        ),
        "pending_action": pending_action,
    }
    previous_draft_started_ms = _date_parse(_first_truthy(_prop(previous, "draft_started_at"), ""))
    previous_draft_expires_ms = _date_parse(_first_truthy(_prop(previous, "draft_expires_at"), ""))
    previous_draft_active = _js_string(_first_truthy(_prop(previous, "operation_state"), "")).upper() == "DRAFT"
    draft_state_active = canonical_operation_state == "DRAFT"
    draft_restart = bool(_prop(normalization, "new_booking_restart") is True or _prop(output, "draft_restart") is True)
    if draft_state_active:
        if (not draft_restart) and previous_draft_active and _is_finite_num(previous_draft_started_ms):
            draft_started_at: Optional[str] = _ms_to_iso(previous_draft_started_ms)
        else:
            draft_started_at = _utc_now_iso()
    else:
        draft_started_at = None
    if draft_state_active:
        if (not draft_restart) and previous_draft_active and _is_finite_num(previous_draft_expires_ms):
            draft_expires_at: Optional[str] = _ms_to_iso(previous_draft_expires_ms)
        else:
            draft_expires_at = _ms_to_iso(_date_parse(draft_started_at) + draft_ttl_seconds * 1000)
    else:
        draft_expires_at = None
    dc = booking_context
    if _js_truthy(_prop(dc, "doctor_id")) and _js_truthy(_prop(dc, "service_id")):
        summary_part1 = (
            "المريض يحجز " + _js_string(_first_truthy(_prop(dc, "service_name"), "خدمة"))
            + " مع " + _js_string(_first_truthy(_prop(dc, "doctor_name"), "الطبيب"))
            + ((" يوم " + _js_string(_prop(dc, "date"))) if _js_truthy(_prop(dc, "date")) else "")
            + ((" الساعة " + _js_string(_prop(dc, "time"))) if _js_truthy(_prop(dc, "time")) else "")
        )
    else:
        summary_part1 = (
            "آخر طلب: " + ("محادثة عامة أو تحية" if intent == "other" else _js_string(intent))
        ) if _js_truthy(intent) else None
    if _js_truthy(active_operation):
        ao = active_operation if isinstance(active_operation, str) else _js_string(active_operation)
        summary_part2 = (
            "يوجد طلب إلغاء قيد التجهيز" if "cancel" in ao
            else ("يوجد طلب تغيير موعد قيد التجهيز" if "reschedule" in ao else "يوجد حجز جديد قيد التجهيز")
        )
    else:
        summary_part2 = None
    summary_part3 = (
        "مرحلة العملية: "
        + ("في انتظار الموعد المناسب" if canonical_operation_state == "DRAFT"
           else ("بانتظار تأكيد المريض النهائي" if canonical_operation_state == "CONFIRMED" else _js_string(canonical_operation_state)))
        + ","
    )
    summary_part4 = (
        "تاريخ ووقت متفق عليه مبدئيًا: " + _js_string(_prop(dc, "date")) + " " + _js_string(_prop(dc, "time"))
        if (_js_truthy(_prop(dc, "date")) and _js_truthy(_prop(dc, "time")))
        else None
    )
    summary_part5 = (
        "اسم المريض: " + _js_string(_prop(booking_context, "patient_name"))
        if _js_truthy(_prop(booking_context, "patient_name"))
        else None
    )
    summary_parts = " ".join([p for p in (summary_part1, summary_part2, summary_part3, summary_part4, summary_part5) if _js_truthy(p)]) + (
        "\r\nآخر التبادلات: " + _json_stringify(session_recent_turns[-4:])
        if _js_truthy(session_recent_turns) and len(session_recent_turns) > 0
        else ""
    )
    availability_inquiry = bool(
        _prop(normalization, "availability_inquiry") is True
        or _prop(output, "availability_inquiry") is True
        or _prop(decision, "availability_inquiry") is True
    )
    next_best_missing_human_field = _first_truthy(_prop(decision, "next_best_missing_human_field"), _prop(output, "next_best_missing_human_field"), None)
    missing_human_fields = dmhf if isinstance(dmhf, list) else (omhf if isinstance(omhf, list) else [])
    resolved_ct = target if isinstance(target, dict) else None
    previous_patient_review = {} if context_session_reset else (_obj_or_none(_prop(previous, "patient_data_review")) or {})
    previous_patient_review = previous_patient_review if isinstance(previous_patient_review, dict) else {}
    model_patient_review = _obj_or_none(_prop(output, "patient_data_review"))
    if model_patient_review is None:
        model_patient_review = _obj_or_none(_prop(orch_state_patches, "patient_data_review"))
    if model_patient_review is None:
        model_patient_review = _obj_or_none(_prop(decision, "patient_data_review"))
    model_patient_review = model_patient_review if isinstance(model_patient_review, dict) else {}
    prev_review_fields = _prop(previous_patient_review, "fields")
    model_review_fields = _prop(model_patient_review, "fields")
    review_fields: Dict[str, Any] = {
        **(prev_review_fields if isinstance(prev_review_fields, dict) else {}),
        **(model_review_fields if isinstance(model_review_fields, dict) else {}),
    }
    for field, value in (
        ("name", persisted_patient["patient_name"]),
        ("phone", persisted_patient["patient_phone"]),
        ("age", persisted_patient["patient_age"]),
        ("address", persisted_patient["patient_address"]),
    ):
        if value is not None and value is not _UNDEFINED and _js_trim(_js_string(value)) != "":
            review_fields[field] = value
    if is_telegram_channel and not telegram_patient_name_trusted:
        review_fields.pop("name", None)
    patient_review_requested = bool(
        len(model_patient_review) > 0
        or len(previous_patient_review) > 0
        or (patient_record_for_review and db_patient_record_present)
        or response_code == "PATIENT_DATA_CONFIRMATION_REQUIRED"
    )
    merged_patient_data_review: Optional[Dict[str, Any]] = None
    if patient_review_requested:
        merged_patient_data_review = {
            **previous_patient_review,
            **model_patient_review,
            "status": _first_truthy(_prop(model_patient_review, "status"), _prop(previous_patient_review, "status"), "pending"),
            "fields": review_fields,
            "source": _first_truthy(
                _prop(model_patient_review, "source"), _prop(previous_patient_review, "source"),
                ("database" if (patient_record_for_review and db_patient_record_present) else "current_turn"),
            ),
        }
    reset_abandoned_draft_state = abandon_current_booking
    reset_stale_paused_greeting_state = bool(
        (not handoff_required)
        and _js_string(_first_truthy(_dig(decision, "contract", "turn", "intent"), _prop(output, "intent"), _prop(decision, "intent"), "")).lower() in ("small_talk", "greeting")
        and _js_string(_first_truthy(response_code, "")).upper() == "CONVERSATION_ONLY"
        and not _js_truthy(target)
        and _js_string(_first_truthy(_prop(previous, "pending_action"), "")).lower() == "handoff_required"
        and _js_string(_first_truthy(_prop(previous, "operation_state"), _prop(previous, "operation_status"), "")).upper() == "PAUSED"
        and _js_string(_first_truthy(_dig(previous, "superseded_operation", "reason"), "")).upper() == "SUPERSEDED_BY_NEW_REQUEST"
    )
    reset_conversation_session_state = bool(context_session_reset or reset_stale_paused_greeting_state or reset_abandoned_draft_state)
    if reset_conversation_session_state:
        state_previous: Dict[str, Any] = {
            **previous,
            "active_operation": None,
            "operation_action": None,
            "operation_id": None,
            "operation_state": "IDLE",
            "operation_status": "idle",
            "pending_action": None,
            "routing_action": None,
            "conversation_stage": "CONVERSATION",
            "confirmation_state": None,
            "confirmation_target": None,
            "booking_context": {},
            "slot_state": {},
            "superseded_operation": (
                {"reason": "PATIENT_CHANGED_MIND", "superseded_at": _utc_now_iso(), "previous_operation_id": _first_truthy(_prop(previous, "operation_id"), None)}
                if reset_abandoned_draft_state else None
            ),
            "required_next_step": {"type": "answer_current_message"},
        }
    else:
        state_previous = previous
    sv = _prop(previous, "state_version") if _js_truthy(previous) else _UNDEFINED
    next_state_version = _int_if_integral(_js_number_or0(_first_truthy(sv, 0))) + 1
    # P-OFFER v40: unify the presented offer (midturn fresh row vs orchestrator patches).
    fresh_offer_row = _dict_or(inputs.get("read_fresh_offer_midturn"))
    fresh_offer_raw = _prop(fresh_offer_row, "presented_offer")
    fresh_offer = fresh_offer_raw if (isinstance(fresh_offer_raw, dict) and _prop(fresh_offer_raw, "kind") == "presented_offer") else None
    patches_offer_raw = _prop(orch_state_patches, "presented_offer") if _has_key(orch_state_patches, "presented_offer") else _UNDEFINED
    turn_start_ms = _date_parse(_js_string(_first_truthy(_prop(ctx, "received_at"), "")))
    turn_start_ms = turn_start_ms if _is_finite_num(turn_start_ms) else 0.0
    patches_offer_exp = _date_parse(_js_string(_first_truthy(_prop(patches_offer_raw, "expires_at"), ""))) if isinstance(patches_offer_raw, dict) else _NAN
    patches_offer_at = _date_parse(_js_string(_first_truthy(_prop(patches_offer_raw, "offered_at"), ""))) if isinstance(patches_offer_raw, dict) else _NAN
    patches_offer_live = bool(
        isinstance(patches_offer_raw, dict)
        and _prop(patches_offer_raw, "kind") == "presented_offer"
        and _is_finite_num(patches_offer_exp)
        and patches_offer_exp > _now_ms()
        and _is_finite_num(patches_offer_at)
        and patches_offer_at >= turn_start_ms - 2000
    )
    patches_cleared_after_binding = bool(
        patches_offer_raw is None
        and _js_string(_first_truthy((_prop(decision, "response_code") if _js_truthy(decision) else _UNDEFINED), _prop(output, "response_code"), "")).upper()
        in ("CONFIRMATION_REQUIRED", "APPOINTMENT_CREATED", "RESCHEDULE_COMPLETED", "CANCEL_COMPLETED")
    )
    fresh_offer_at = _date_parse(_js_string(_first_truthy(_prop(fresh_offer, "offered_at"), ""))) if fresh_offer is not None else _NAN
    fresh_offer_is_this_turn = bool(
        _js_truthy(fresh_offer) and turn_start_ms
        and _is_finite_num(fresh_offer_at)
        and fresh_offer_at >= turn_start_ms - 2000
    )
    if reset_conversation_session_state:
        unified_offer: Any = None
    elif patches_offer_raw is not _UNDEFINED and (lookup_matches_current_turn or patches_offer_live or patches_cleared_after_binding):
        unified_offer = patches_offer_raw
    elif fresh_offer_is_this_turn:
        unified_offer = fresh_offer
    else:
        unified_offer = (
            patches_offer_raw if patches_offer_raw is not _UNDEFINED
            else _first_truthy(fresh_offer, _prop(previous, "presented_offer"), _prop(previous, "pending_offer"), None)
        )
    oaa = _prop(output, "availability_alternatives")
    froa = _prop(fresh_offer_row, "availability_alternatives")
    booking_context = _json_safe(booking_context)
    slot_state = _json_safe(slot_state)
    facts = _json_safe(facts)
    target = _json_safe(target)
    superseded_operation = _json_safe(superseded_operation)
    state_data: Dict[str, Any] = {
        **state_previous,
        "patient_data_review": merged_patient_data_review,
        "conversation_stage": "CONVERSATION" if reset_conversation_session_state else conversation_stage,
        "required_next_step": {"type": "answer_current_message"} if reset_conversation_session_state else required_next_step,
        "state_schema_version": 1,
        "availability_inquiry": availability_inquiry,
        "availability_lookup_lineage_status": "FRESH_CURRENT_TURN" if lookup_matches_current_turn else "CLEARED_NON_CURRENT_TURN",
        "availability_lineage": _first_truthy(_prop(output, "availability_lineage"), current_lookup_lineage, None) if lookup_matches_current_turn else None,
        "availability_outcome": _first_truthy(_prop(output, "availability_outcome"), None) if lookup_matches_current_turn else None,
        "availability_alternatives": (
            oaa if (lookup_matches_current_turn and isinstance(oaa, list))
            else (froa if (_js_truthy(fresh_offer) and isinstance(froa, list)) else [])
        ),
        "pending_offer": unified_offer,
        "presented_offer": unified_offer,
        "state_machine": None if reset_conversation_session_state else _first_truthy(_prop(orch_state_patches, "state_machine"), _prop(previous, "state_machine"), None),
        "unclear_count": (
            0 if reset_conversation_session_state
            else (
                _prop(orch_state_patches, "unclear_count")
                if isinstance(_prop(orch_state_patches, "unclear_count"), (int, float)) and not isinstance(_prop(orch_state_patches, "unclear_count"), bool)
                else _int_if_integral(_js_number_or0(_first_truthy(_prop(previous, "unclear_count"), 0)))
            )
        ),
        "response_code_history": (
            [] if reset_conversation_session_state
            else (
                list(_prop(orch_state_patches, "response_code_history"))
                if isinstance(_prop(orch_state_patches, "response_code_history"), list)
                else (list(_prop(previous, "response_code_history")) if isinstance(_prop(previous, "response_code_history"), list) else [])
            )
        ),
        "turn_directive": None if reset_conversation_session_state else _first_truthy(_prop(orch_output, "turn_directive"), _prop(orch_state_patches, "turn_directive"), None),
        "availability_requested_time_unavailable": bool(lookup_matches_current_turn and _prop(output, "availability_requested_time_unavailable") is True),
        "deterministic_slot_lookup": _first_truthy(_prop(output, "deterministic_slot_lookup"), None) if lookup_matches_current_turn else None,
        "slot_lookup_ready": lookup_matches_current_turn,
        "next_best_missing_human_field": next_best_missing_human_field,
        "missing_human_fields": missing_human_fields,
        "superseded_operation": (
            None if reset_stale_paused_greeting_state
            else (
                {"reason": "PATIENT_CHANGED_MIND", "superseded_at": _utc_now_iso(), "previous_operation_id": _first_truthy(_prop(previous, "operation_id"), None)}
                if reset_abandoned_draft_state else superseded_operation
            )
        ),
        "last_intent": intent,
        "current_intent": intent,
        "active_operation": None if reset_conversation_session_state else active_operation,
        "operation_status": "idle" if reset_conversation_session_state else operation_state,
        "operation_state": "IDLE" if reset_conversation_session_state else canonical_operation_state,
        "operation_id": None if reset_conversation_session_state else canonical_operation_id,
        "operation_action": None if reset_conversation_session_state else canonical_operation_action,
        "original_response_code": original_response_code,
        "routing_action": None if reset_conversation_session_state else routing_action,
        "resume_eligible": resume_eligible,
        "retryable": retryable,
        "failure_code": failure_code,
        "migration_status": migration_status,
        "migration_issues": list(dict.fromkeys(migration_issues)),
        "pending_action": None if reset_conversation_session_state else pending_action,
        "last_open_question": persisted_last_open_question,
        "waiting_for_reference": bool(
            active_operation in ("cancel_appointment", "reschedule_appointment", "confirm_appointment")
            and not _js_truthy(_prop(output, "appointment_id"))
        ),
        "confirmation_state": None if reset_conversation_session_state else confirmation_state,
        "confirmation_target": None if reset_conversation_session_state else target,
        "confirmation_delivery_status": (
            None if reset_abandoned_draft_state
            else _first_truthy(_prop(target, "confirmation_delivery_status"), _prop(previous, "confirmation_delivery_status"), None)
        ),
        "confirmation_delivery_recorded_at": (
            None if reset_abandoned_draft_state
            else _first_truthy(_prop(target, "confirmation_delivery_recorded_at"), _prop(previous, "confirmation_delivery_recorded_at"), None)
        ),
        "confirmation_ttl_seconds": (
            None if reset_abandoned_draft_state
            else _first_truthy(_prop(target, "confirmation_ttl_seconds"), _prop(previous, "confirmation_ttl_seconds"), 600)
        ),
        "confirmation_expires_at": _first_truthy(_prop(target, "expires_at"), None),
        "confirmation_target_hash": _first_truthy(_prop(target, "context_fingerprint"), None),
        "draft_started_at": draft_started_at,
        "draft_expires_at": draft_expires_at,
        "draft_ttl_seconds": draft_ttl_seconds,
        "business_time_checked": bool(
            _prop(decision, "business_time_checked") is True or _prop(item, "business_time_checked") is True
        ),
        "business_time_status": _first_truthy(
            _prop(decision, "business_time_status"), _prop(item, "business_time_status"),
            _prop(previous, "business_time_status"), "NOT_CHECKED",
        ),
        "business_time_timezone": _first_truthy(
            _prop(decision, "business_time_timezone"), _prop(item, "business_time_timezone"),
            _prop(previous, "business_time_timezone"), None,
        ),
        "business_time_source": _first_truthy(
            _prop(decision, "business_time_source"), _prop(item, "business_time_source"),
            _prop(previous, "business_time_source"), None,
        ),
        "business_time_error_code": _first_truthy(
            _prop(decision, "business_time_error_code"), _prop(item, "business_time_error_code"),
            _prop(previous, "business_time_error_code"), None,
        ),
        "confirmation_target_invalidated": bool(
            new_booking_restart or reset_abandoned_draft_state or faq_pauses_operation
            or (previous_target_invalid and not _js_truthy(decision_target))
        ),
        "confirmation_contract": {
            "state": "DELIVERED" if _js_truthy(target) else ("INVALID_OR_UNDELIVERED" if _js_truthy(previous_target) else "NONE"),
            "target_valid": _js_truthy(target),
            "expires_at": _first_truthy(_prop(target, "expires_at"), None),
        },
        "booking_context": {} if reset_conversation_session_state else booking_context,
        "booking_number": (
            (_first_truthy(execution_booking_number, None))
            if (new_booking_restart or reset_abandoned_draft_state)
            else _first_truthy(execution_booking_number, _prop(previous, "booking_number"), _prop(booking_context, "booking_number"), None)
        ),
        "appointment_id": (
            None if (new_booking_restart or reset_abandoned_draft_state)
            else _first_truthy(execution_appointment_id, _prop(previous, "appointment_id"), None)
        ),
        "response_code": response_code,
        "confidence": _coalesce(_prop(output, "confidence"), _prop(previous, "confidence"), None),
        "escalate": _prop(output, "escalate") is True,
        "slot_state": {} if reset_conversation_session_state else slot_state,
        "facts": facts,
        "conversation_summary": summary_parts,
        "recent_turns": session_recent_turns,
        "last_message_id": _coalesce(_prop(ctx, "message_id"), None),
        "last_idempotency_key": _coalesce(_prop(ctx, "idempotency_key"), None),
        "last_channel": {
            "type": _coalesce(_prop(ctx, "channel_type"), None),
            "id": _coalesce(_prop(ctx, "channel_id"), None),
            "key": _coalesce(_prop(ctx, "channel_key"), None),
        },
        "last_updated": _utc_now_iso(),
    }
    if _boundary:
        state_data["previous_session_summary"] = session_compact.build_session_summary(previous)
    if context_session_reset:
        state_data["active_operation"] = None
        state_data["operation_action"] = None
        state_data["operation_id"] = None
        state_data["operation_state"] = "IDLE"
        state_data["operation_status"] = "idle"
        state_data["pending_action"] = None
        state_data["routing_action"] = None
        state_data["confirmation_state"] = None
        state_data["confirmation_target"] = None
        state_data["confirmation_delivery_status"] = None
        state_data["confirmation_delivery_recorded_at"] = None
        state_data["confirmation_expires_at"] = None
        state_data["confirmation_target_hash"] = None
        state_data["confirmation_contract"] = {"state": "NONE", "target_valid": False, "expires_at": None}
        state_data["booking_number"] = None
        state_data["appointment_id"] = None
        state_data["booking_context"] = {}
        state_data["slot_state"] = {}
        state_data["superseded_operation"] = None
        state_data["last_open_question"] = {"message": None, "requested_fields": [], "type": None, "pending_action": None}
        state_data["patient_data_review"] = None
        state_data["draft_started_at"] = None
        state_data["draft_expires_at"] = None
        state_data["conversation_summary"] = summary_parts
        state_data["recent_turns"] = session_recent_turns
    state_data["state_version"] = next_state_version
    result: Dict[str, Any] = {**(item if isinstance(item, dict) else {}), "state_data": state_data}
    result["booking_context"] = {} if reset_conversation_session_state else booking_context
    result["booking_number"] = None if abandon_current_booking else _first_truthy(execution_booking_number, _prop(booking_context, "booking_number"), None)
    result["slot_state"] = {} if reset_conversation_session_state else slot_state
    result["patient_data_review"] = _first_truthy(state_data.get("patient_data_review"), None)
    result["confirmation_target"] = None if context_session_reset else resolved_ct
    result["confirmation_state"] = None if context_session_reset else confirmation_state
    if new_booking_restart:
        result["operation_id"] = canonical_operation_id
        result["operation_action"] = canonical_operation_action
        result["operation_state"] = canonical_operation_state
        result["operation_status"] = operation_state
        result["active_operation"] = active_operation
        result["slot_id"] = _first_truthy(_prop(booking_context, "slot_id"), None)
        result["booking_context"] = {} if reset_abandoned_draft_state else booking_context
        result["booking_number"] = _first_truthy(execution_booking_number, None)
        result["slot_state"] = {} if reset_abandoned_draft_state else slot_state
        result["appointment_id"] = None
    result["state_version"] = next_state_version
    return result


# ---------------------------------------------------------------------------
# Build Audit Entry
# ---------------------------------------------------------------------------

_AUDIT_KNOWN_MODEL_FALLBACK = "deepseek/deepseek-v3.2"
_AUDIT_BOOKING_VOCAB_RE = _js_re(r"حجز|موعد|دكتور|book")
_GENERIC_ERROR_REPLY_RE = _js_re(r"تعذر صياغة الرد")
_WEAK_LABELS = ("unclear", "other", "small_talk", "clarification")
_BENIGN_LOW_CONFIDENCE_LABELS = ("small_talk", "greeting", "other", "clinic_query")
_DRAFT_LIVE_STATES = ("DRAFT", "AWAITING_CONFIRMATION", "EXECUTING", "COLLECTING_APPOINTMENT_DETAILS", "COLLECTING_PATIENT_DATA")


def build_audit_entry(item: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Build Audit Entry (extracted/code/Build_Audit_Entry.js).

    Input keys (missing nodes read as {}):
      item                                          -> $json (final response item)
      inputs["validate_repaired_contract_deterministic"] -> $(Validate Repaired Contract (Deterministic)).first().json
                                                           (used only when _contract_status === 'VALID')
      inputs["normalize_agent_output_deterministic"] -> $(Normalize Agent Output (Deterministic)).first().json
      inputs["response_policy_deterministic"]       -> $(Response Policy (Deterministic)).first().json
      inputs["normalize_validate"]                  -> $(Normalize & Validate).first().json
      inputs["get_conversation_state"]              -> $(Get Conversation State).first().json (reads .state_data)
      inputs["system_orchestrator_policy"]          -> $(System Orchestrator (Policy)).first().json

    Output: single json dict (one audit row). total_time_ms uses Date.now() —
    wall-clock, exactly like the JS.
    """
    r_vrc = inputs.get("validate_repaired_contract_deterministic")
    if _js_truthy(r_vrc) and _prop(r_vrc, "_contract_status") == "VALID":
        agent_item: Any = r_vrc
    else:
        agent_item = _first_truthy(inputs.get("normalize_agent_output_deterministic"), {})
    response_item = _first_truthy(inputs.get("response_policy_deterministic"), {})
    final_item = item if isinstance(item, dict) else {}
    ro = _prop(response_item, "output")
    ao = _prop(agent_item, "output")
    agent_output = ro if isinstance(ro, (dict, list)) else (ao if isinstance(ao, (dict, list)) else {})
    decision = _first_truthy(_prop(response_item, "system_decision"), _prop(agent_output, "system_decision"), {})
    intermediate_steps = _prop(agent_item, "intermediateSteps")
    intermediate_steps = intermediate_steps if isinstance(intermediate_steps, list) else []
    ctx = _dict_or(inputs.get("normalize_validate"))
    tool_calls: List[Dict[str, Any]] = []
    for step in intermediate_steps:
        s = step if isinstance(step, dict) else {}
        observation = _prop(s, "observation")
        tool_calls.append({
            "tool": _first_truthy(_dig(s, "action", "tool"), None),
            "input": _first_truthy(_dig(s, "action", "toolInput"), None),
            "output": _u16_slice(observation, 0, 500) if isinstance(observation, str) else observation,
        })
    received_at = _prop(ctx, "received_at")
    replayed = bool(_dig(response_item, "replay_gate", "matched") is True)
    response_code = _first_truthy(
        _prop(final_item, "response_code"), _prop(response_item, "response_code"),
        _dig(final_item, "facts", "response_code"), _prop(decision, "response_code"), None,
    )
    operation_status = _first_truthy(
        _prop(final_item, "operation_status"), _prop(response_item, "operation_status"),
        _dig(final_item, "facts", "operation_status"), _prop(agent_output, "operation_status"), None,
    )
    appointment_id = _first_truthy(
        _prop(final_item, "appointment_id"), _prop(response_item, "appointment_id"),
        _dig(final_item, "facts", "appointment_id"), _prop(decision, "appointment_id"),
        _prop(agent_output, "appointment_id"), None,
    )
    booking_number = _first_truthy(
        _prop(final_item, "booking_number"), _prop(response_item, "booking_number"),
        _dig(final_item, "facts", "booking_number"), _prop(decision, "booking_number"),
        _dig(decision, "booking_context", "booking_number"), _prop(agent_output, "booking_number"), None,
    )
    appointment_type = _first_truthy(
        _prop(final_item, "appointment_type"), _prop(response_item, "appointment_type"),
        _dig(final_item, "facts", "appointment_type"), _prop(decision, "appointment_type"),
        _dig(decision, "booking_context", "appointment_type"), _prop(agent_output, "appointment_type"), None,
    )
    intent = _first_truthy(
        _dig(final_item, "agent_contract", "turn", "intent"), _dig(final_item, "turn", "intent"),
        _dig(response_item, "facts", "turn_intent"), _dig(agent_item, "contract", "turn", "intent"),
        ("booking" if replayed else None), None,
    )
    invalid_transition = bool(response_code == "INVALID_STATE_TRANSITION" or _prop(response_item, "audit_event") == "invalid_transition")
    runtime_model_raw = _js_trim(_js_string(_first_truthy(
        _prop(final_item, "model"), _prop(final_item, "model_name"), _prop(final_item, "model_id"),
        _prop(response_item, "model"), _prop(agent_output, "model"), _prop(agent_output, "model_name"), "",
    )))
    runtime_model = runtime_model_raw or _configured_model(_AUDIT_KNOWN_MODEL_FALLBACK)

    # understanding-failure telemetry (zero-token flywheel).
    st_state = _first_truthy(_prop(_dict_or(inputs.get("get_conversation_state")), "state_data"), {})
    st_state = st_state if isinstance(st_state, dict) else {}
    draft_live = _js_string(_first_truthy(_prop(st_state, "operation_state"), "")).upper() in _DRAFT_LIVE_STATES
    model_intent_label = _js_string(_first_truthy(_dig(agent_item, "_normalization", "model_turn_intent"), "")).lower()
    uc = _dig(agent_item, "_normalization", "confidence")
    understanding_confidence = uc if (uc is not _UNDEFINED and uc is not None) else None
    conf_num = _js_number(understanding_confidence)
    failure_types: List[str] = []
    if model_intent_label in _WEAK_LABELS and draft_live:
        failure_types.append("model_unclear_live_draft")
    if model_intent_label in _WEAK_LABELS and _AUDIT_BOOKING_VOCAB_RE.search(_js_string(_first_truthy(_prop(ctx, "message_text"), ""))):
        failure_types.append("booking_vocab_non_booking")
    if _is_finite_num(conf_num) and conf_num < 0.5 and model_intent_label not in _BENIGN_LOW_CONFIDENCE_LABELS:
        failure_types.append("low_confidence_label")
    reply_for_telemetry = _js_trim(_js_string(_first_truthy(
        _prop(final_item, "canonical_reply"), _prop(final_item, "final_reply"), _prop(final_item, "rendered_reply"),
        _prop(response_item, "canonical_reply"), "",
    )))
    if not reply_for_telemetry:
        failure_types.append("empty_reply")
    elif _GENERIC_ERROR_REPLY_RE.search(reply_for_telemetry):
        failure_types.append("generic_error_reply")
    orch_item = _dict_or(inputs.get("system_orchestrator_policy"))
    orch_decision = _obj_or_none(_prop(orch_item, "system_decision")) or {}
    orch_state = _obj_or_none(_prop(orch_decision, "state_machine")) or {}
    received_at_ms = _date_parse(_js_string(_first_truthy(received_at, "")))
    total_time_ms = max(0.0, _now_ms() - received_at_ms) if _is_finite_num(received_at_ms) else None
    errors = _dig(agent_item, "_normalization", "errors")
    normalization_error = "|".join(_js_string(e) for e in errors) if isinstance(errors, list) else None
    return {
        "conversation_id": _coalesce(_prop(ctx, "conversation_id"), None),
        "clinic_id": _coalesce(_prop(ctx, "clinic_id"), None),
        "patient_id": _coalesce(_prop(ctx, "patient_id"), None),
        "message_text": _coalesce(_prop(ctx, "message_text"), None),
        "intent": intent,
        "operation_status": "invalid_transition" if invalid_transition else operation_status,
        "escalate": bool(
            _prop(final_item, "escalate") is True
            or _prop(response_item, "escalate") is True
            or _prop(agent_output, "escalate") is True
        ),
        "appointment_id": appointment_id,
        "booking_number": booking_number,
        "appointment_type": appointment_type,
        "reply_text": _first_truthy(_js_trim(_js_string(_first_truthy(
            _prop(final_item, "canonical_reply"), _prop(final_item, "final_reply"), _prop(final_item, "rendered_reply"),
            _prop(response_item, "canonical_reply"), "",
        ))), None),
        "response_code": response_code,
        "decision_engine": _first_truthy(_prop(orch_item, "decision_engine"), None),
        "engine_version": _first_truthy(_prop(orch_item, "engine_version"), None),
        "decision_engine_source": _first_truthy(_prop(orch_item, "decision_engine_source"), None),
        "decision_rule": _first_truthy(_prop(orch_decision, "decision_rule"), None),
        "state_from": _first_truthy(_prop(orch_state, "previous_state"), None),
        "state_to": _first_truthy(_prop(orch_state, "current_state"), None),
        "model": "deterministic-replay-gate" if replayed else runtime_model,
        "model_source": "deterministic" if replayed else ("runtime" if runtime_model_raw else "known-config"),
        "tool_calls": tool_calls,
        "tool_call_count": len(tool_calls),
        "total_time_ms": _int_if_integral(total_time_ms) if total_time_ms is not None else None,
        "received_at": _coalesce(received_at, None),
        "normalization_valid": True if replayed else _coalesce(_dig(agent_item, "_normalization", "valid"), None),
        "normalization_error": None if replayed else normalization_error,
        "audit_event": "invalid_transition" if invalid_transition else None,
        "understanding_failure_types": failure_types,
        "understanding_draft_live": draft_live,
        "understanding_model_intent": model_intent_label,
        "understanding_confidence": understanding_confidence,
    }


# ---------------------------------------------------------------------------
# Compute AI Request Usage (Deterministic)
# ---------------------------------------------------------------------------

_CHARS_PER_TOKEN = 3.2  # calibrated against observed provider totals
def _configured_model(default: str = "deepseek/deepseek-v3.2") -> str:
    """The model actually configured for this deployment.

    The n8n nodes hardcoded a model string; the port keeps that as a fallback but prefers
    the live setting so audit/usage rows name the model that really answered.
    """
    try:
        from app.core.config import settings
        return str(getattr(settings, "LLM_PRIMARY_MODEL", "") or default) or default
    except Exception:
        return default


_USAGE_MODEL_FALLBACK = "deepseek/deepseek-v3.2"


_TOKEN_PRICE_CACHE: Dict[str, Any] = {}


def _token_prices() -> Dict[str, Any]:
    """Parsed TOKEN_PRICES_JSON: {model: {input_per_1m, output_per_1m}}. Cached."""
    import json as _json
    raw = str(getattr(settings, "TOKEN_PRICES_JSON", "") or "").strip()
    if not raw:
        return {}
    if "prices" in _TOKEN_PRICE_CACHE and _TOKEN_PRICE_CACHE.get("_raw") == raw:
        return _TOKEN_PRICE_CACHE["prices"]
    try:
        parsed = _json.loads(raw)
        ok = all(isinstance(m, dict) and "input_per_1m" in m and "output_per_1m" in m
                 for m in parsed.values()) if isinstance(parsed, dict) else False
        _TOKEN_PRICE_CACHE.clear()
        _TOKEN_PRICE_CACHE["_raw"] = raw
        _TOKEN_PRICE_CACHE["prices"] = parsed if ok else {}
    except Exception:
        _TOKEN_PRICE_CACHE.clear()
        _TOKEN_PRICE_CACHE["prices"] = {}
    return _TOKEN_PRICE_CACHE["prices"]


def _row_cost(model: str, input_tokens: Any, output_tokens: Any) -> Optional[float]:
    """Exact per-call cost from the configured per-1M prices; None when unpriced."""
    prices = _token_prices().get(str(model or ""))
    if not prices or input_tokens is None or output_tokens is None:
        return None
    try:
        cost = (float(input_tokens) / 1_000_000.0) * float(prices["input_per_1m"])              + (float(output_tokens) / 1_000_000.0) * float(prices["output_per_1m"])
        return round(cost, 6)
    except (TypeError, ValueError):
        return None


def _read_model_tokens(rows: Any) -> float:
    """JS readModelTokens: sum provider tokenUsage across all items of a model node."""
    total = 0.0
    for row in rows if isinstance(rows, list) else []:
        d = row if isinstance(row, dict) else {}
        resp = _prop(d, "response")
        u = _first_truthy(
            _prop(d, "tokenUsage"),
            _prop(d, "usage"),
            (_first_truthy(_prop(resp, "tokenUsage"), _prop(resp, "usage")) if _js_truthy(resp) else _UNDEFINED),
            {},
        )
        u = u if isinstance(u, dict) else {}
        t = _js_number(_coalesce(_prop(u, "totalTokens"), _prop(u, "total_tokens"), _prop(u, "total")))
        if _is_finite_num(t) and t > 0:
            total += t
            continue
        p = _js_number_or0(_coalesce(_prop(u, "promptTokens"), _prop(u, "inputTokens"), _prop(u, "prompt_tokens"), _prop(u, "input_tokens")))
        c = _js_number_or0(_coalesce(_prop(u, "completionTokens"), _prop(u, "outputTokens"), _prop(u, "completion_tokens"), _prop(u, "output_tokens")))
        if p > 0 or c > 0:
            total += p + c
    return total


def _estimate_tokens(chars: Any) -> float:
    return float(max(1, _js_round(_js_number(chars) / _CHARS_PER_TOKEN)))


def _read_model_tokens_parts(rows: Any) -> Dict[str, Any]:
    """Split provider tokenUsage into input/output sums (2026-09-18 cost accounting).

    Reads the same key variants as _read_model_tokens but keeps the halves separate so
    ai_requests can price input and output differently. Unsplit totals count as output.
    """
    p_sum = 0.0
    c_sum = 0.0
    for row in rows if isinstance(rows, list) else []:
        d = row if isinstance(row, dict) else {}
        resp = _prop(d, "response")
        u = _first_truthy(
            _prop(d, "tokenUsage"),
            _prop(d, "usage"),
            (_first_truthy(_prop(resp, "tokenUsage"), _prop(resp, "usage")) if _js_truthy(resp) else _UNDEFINED),
            {},
        )
        u = u if isinstance(u, dict) else {}
        p = _js_number_or0(_coalesce(_prop(u, "promptTokens"), _prop(u, "inputTokens"), _prop(u, "prompt_tokens"), _prop(u, "input_tokens")))
        c = _js_number_or0(_coalesce(_prop(u, "completionTokens"), _prop(u, "outputTokens"), _prop(u, "completion_tokens"), _prop(u, "output_tokens")))
        t = _js_number(_coalesce(_prop(u, "totalTokens"), _prop(u, "total_tokens"), _prop(u, "total")))
        if _is_finite_num(p) and p > 0:
            p_sum += p
        if _is_finite_num(c) and c > 0:
            c_sum += c
        if (_is_finite_num(t) and t > 0) and not (_is_finite_num(p) and p > 0) and not (_is_finite_num(c) and c > 0):
            c_sum += t
    return {"input": p_sum if p_sum > 0 else None, "output": c_sum if c_sum > 0 else None}


def compute_ai_request_usage_deterministic(item: Dict[str, Any], inputs: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Source node: Compute AI Request Usage (Deterministic) (extracted/code/Compute_AI_Request_Usage_Deterministic.js).

    Input keys (missing nodes read as {} / []):
      item    -> unused by the JS (kept for signature uniformity)
      inputs["normalize_validate"]                      -> $(Normalize & Validate).first().json
      inputs["build_clinic_persona_context_deterministic"] -> $(Build Clinic Persona Context (Deterministic)).first().json
      inputs["booking_assistant_agent"]                 -> $(Booking Assistant Agent).first().json (reads .output)
      inputs["result_reply_composer"]                   -> $(Result Reply Composer).first().json (reads .output)
      inputs["prepare_single_agent_result_context"]     -> $(Prepare Single Agent Result Context).first().json (JSON-stringified)
      inputs["deepseek_model"]                          -> $(DeepSeek Model).all() json dicts
      inputs["deepseek_result_model"]                   -> $(DeepSeek Result Model).all() json dicts
      inputs["execution_id"]                            -> $execution.id (PORT-TODO(n8n); omitted from
                                                           the metadata JSON string when absent, like
                                                           JSON.stringify dropping undefined)

    Output: list of row dicts (one ai_request_usage row per LLM call that ran).
    """
    now_iso = _utc_now_iso()  # PORT-TODO(n8n): $now.toISO() — UTC instant emitted.
    ctx = _dict_or(inputs.get("normalize_validate"))
    persona = _dict_or(inputs.get("build_clinic_persona_context_deterministic"))
    prompt_chars = _js_number_or0(_prop(persona, "agent_system_prompt_chars"))
    reply1 = _js_string(_first_truthy(_prop(_dict_or(inputs.get("booking_assistant_agent")), "output"), ""))
    reply2 = _js_string(_first_truthy(_prop(_dict_or(inputs.get("result_reply_composer")), "output"), ""))
    ctx2 = _json_stringify(_first_truthy(inputs.get("prepare_single_agent_result_context"), {})) or ""

    rows: List[Dict[str, Any]] = []

    def _push_row(node_name: str, exact_tokens: Any, input_est: Any, output_est: Any,
                  exact_input: Any = None, exact_output: Any = None,
                  model: Any = None) -> None:
        exact_num = _js_number(exact_tokens)
        exact = exact_num if (_is_finite_num(exact_num) and exact_num > 0) else None
        parts_given = (_is_finite_num(_js_number(exact_input)) and _js_number(exact_input) > 0) or                       (_is_finite_num(_js_number(exact_output)) and _js_number(exact_output) > 0)
        if exact is not None and parts_given:
            # Provider reported the split — prefer it over the total-only estimate.
            input_tokens = _int_if_integral(_js_number(exact_input)) if _js_number(exact_input) > 0 else None
            output_tokens = _int_if_integral(_js_number(exact_output)) if _js_number(exact_output) > 0 else None
            total = (input_tokens or 0) + (output_tokens or 0) or exact
        else:
            input_tokens = _estimate_tokens(input_est) if exact is None else None
            output_tokens = _estimate_tokens(output_est) if exact is None else None
            total = exact if exact is not None else ((input_tokens or 0) + (output_tokens or 0))
        if not total:
            return
        meta: Dict[str, Any] = {
            "source": "agent_k2",
            "estimated": exact is None,
            "basis": "char_count_approximation" if exact is None else "provider_tokenUsage",
            "model_node": node_name,
        }
        execution_id = inputs.get("execution_id")
        if execution_id is not None and execution_id is not _UNDEFINED:
            meta["execution_id"] = execution_id
        rows.append({
            "clinic_id": _first_truthy(_prop(ctx, "clinic_id"), None),
            "conversation_id": _first_truthy(_prop(ctx, "conversation_id"), None),
            "provider": "deepseek",
            "model": _configured_model(_USAGE_MODEL_FALLBACK) if model is None else str(model),
            "input_tokens": _int_if_integral(input_tokens) if input_tokens is not None else None,
            "output_tokens": _int_if_integral(output_tokens) if output_tokens is not None else None,
            "total_tokens": _int_if_integral(total),
            "cost": _row_cost(_configured_model(_USAGE_MODEL_FALLBACK) if model is None else str(model),
                              input_tokens, output_tokens),
            "response_received_at": now_iso,
            "metadata": _json_stringify(meta),
            "request_payload": _json_stringify({"model_node": node_name, "estimated": exact is None}),
        })

    if prompt_chars > 0 or reply1:
        _agent_parts = _read_model_tokens_parts(inputs.get("deepseek_model"))
        _push_row("DeepSeek Model", _read_model_tokens(inputs.get("deepseek_model")), prompt_chars + 1400, _u16_len(reply1),
                  exact_input=_agent_parts["input"], exact_output=_agent_parts["output"])
    if reply2:
        _composer_parts = _read_model_tokens_parts(inputs.get("deepseek_result_model"))
        _push_row("Result Reply Composer", _read_model_tokens(inputs.get("deepseek_result_model")), _u16_len(ctx2) + 2400, _u16_len(reply2),
                  exact_input=_composer_parts["input"], exact_output=_composer_parts["output"],
                  model=str(getattr(settings, "LLM_REPAIR_MODEL", "") or "") or None)
    return rows


# ---------------------------------------------------------------------------
# Build Outgoing Message SQL Parameters
# ---------------------------------------------------------------------------

_OUTGOING_MODEL_FALLBACK = "deepseek/deepseek-v3.2"


def _sum_usage_tokens(rows: Any) -> float:
    """JS sumUsageTokens: provider totals, else prompt+completion."""
    total = 0.0
    for row in rows if isinstance(rows, list) else []:
        d = row if isinstance(row, dict) else {}
        resp = _prop(d, "response")
        usage = _first_truthy(
            _prop(d, "tokenUsage"),
            _prop(d, "usage"),
            (_first_truthy(_prop(resp, "tokenUsage"), _prop(resp, "usage")) if _js_truthy(resp) else _UNDEFINED),
            {},
        )
        usage = usage if isinstance(usage, dict) else {}
        reported = _js_number(_coalesce(_prop(usage, "totalTokens"), _prop(usage, "total_tokens")))
        if _is_finite_num(reported) and reported > 0:
            total += reported
        else:
            total += _js_number_or0(_coalesce(_prop(usage, "promptTokens"), _prop(usage, "inputTokens"), _prop(usage, "prompt_tokens"), _prop(usage, "input_tokens")))
            total += _js_number_or0(_coalesce(_prop(usage, "completionTokens"), _prop(usage, "outputTokens"), _prop(usage, "completion_tokens"), _prop(usage, "output_tokens")))
    return total


def _has_transport_error(value: Any) -> bool:
    if not _js_truthy(value):
        return False
    d = value if isinstance(value, dict) else {}
    return bool(
        _js_truthy(_prop(d, "error"))
        or _js_truthy(_prop(d, "errorMessage"))
        or _js_truthy(_prop(d, "errorDetails"))
        or (_is_finite_num(_js_number(_prop(d, "statusCode"))) and _js_number(_prop(d, "statusCode")) >= 400)
        or (_is_finite_num(_js_number(_prop(d, "status"))) and _js_number(_prop(d, "status")) >= 400)
    )


def build_outgoing_message_sql_parameters(item: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Build Outgoing Message SQL Parameters (extracted/code/Build_Outgoing_Message_SQL_Parameters.js).

    Input keys (missing nodes read as {} / []):
      item    -> unused by the JS (kept for signature uniformity)
      inputs["normalize_validate"]               -> $(Normalize & Validate).first().json
      inputs["save_conversation_state"]          -> $(Save Conversation State).first().json
      inputs["save_conversation_state_retry_v18"] -> $(Save Conversation State (retry) (v18)).first().json
      inputs["extract_single_agent_reply"]       -> $(Extract Single Agent Reply).first().json
      inputs["response_policy_deterministic"]    -> $(Response Policy (Deterministic)).first().json
      inputs["deepseek_model"]                   -> $(DeepSeek Model).all() json dicts
      inputs["deepseek_result_model"]            -> $(DeepSeek Result Model).all() json dicts

    Output: {"query_params": [10 positional params for the outgoing-message SQL],
             "outgoing_model", "outgoing_ai_tokens"}.
    """
    normalized = _dict_or(inputs.get("normalize_validate"))
    initial = _first_truthy(inputs.get("save_conversation_state"), {})
    retry = _first_truthy(inputs.get("save_conversation_state_retry_v18"), {})
    retry_ran = len(retry) > 0 if isinstance(retry, (dict, list, str)) else False
    save_transport_failed = bool(_has_transport_error(initial) or _has_transport_error(retry))
    save_failed = bool(
        save_transport_failed
        or (retry_ran and _prop(retry, "saved") is not True)
        or ((not retry_ran) and _prop(initial, "saved") is False and _prop(initial, "rejected_reason") != "CONCURRENT_STATE_STALE")
    )
    extracted = _dict_or(inputs.get("extract_single_agent_reply"))
    extracted_reply = (
        _prop(extracted, "rendered_reply") if _prop(extracted, "render_used") is True
        else _first_truthy(_prop(extracted, "agent_reply"), _prop(extracted, "final_reply"), _prop(extracted, "canonical_reply"), _prop(extracted, "rendered_reply"), "")
    )
    # Reviewer/user directive (2026-09-18): the outgoing row must carry the REAL
    # model-authored reply even when the state save failed — logging a canned
    # infrastructure notice as the reply both reads robotic and poisoned the
    # dedupe gate (a retry then returned the notice instead of re-running).
    reply = _js_string(_first_truthy(extracted_reply, ""))
    response_policy = _dict_or(inputs.get("response_policy_deterministic"))
    agent1_tokens = _sum_usage_tokens(inputs.get("deepseek_model"))
    agent2_tokens = _sum_usage_tokens(inputs.get("deepseek_result_model"))
    total_tokens = agent1_tokens + agent2_tokens
    outgoing_metadata = _json_stringify({
        "intent": _first_truthy(_dig(response_policy, "facts", "turn_intent"), None),
        "response_code": "STATE_SAVE_FAILED" if save_failed else _first_truthy(_prop(response_policy, "response_code"), None),
        "operation_status": _first_truthy(_dig(response_policy, "output", "operation_status"), None),
        "appointment_id": _first_truthy(_dig(response_policy, "output", "appointment_id"), None),
        "idempotency_key": _first_truthy(_prop(normalized, "idempotency_key"), None),
        "state_save_transport_failed": save_transport_failed,
        "agent1_tokens": _int_if_integral(agent1_tokens) if agent1_tokens > 0 else None,
        "agent2_tokens": _int_if_integral(agent2_tokens) if agent2_tokens > 0 else None,
    })
    return {
        "query_params": [
            _first_truthy(_prop(normalized, "idempotency_key"), None),
            _first_truthy(_prop(normalized, "conversation_id"), None),
            _first_truthy(_prop(normalized, "clinic_id"), None),
            _first_truthy(_prop(normalized, "patient_id"), None),
            reply,
            _utc_now_iso(),
            outgoing_metadata,
            _first_truthy(_prop(normalized, "message_id"), None),
            _configured_model(_OUTGOING_MODEL_FALLBACK),
            _int_if_integral(total_tokens) if total_tokens > 0 else None,
        ],
        "outgoing_model": _configured_model(_OUTGOING_MODEL_FALLBACK),
        "outgoing_ai_tokens": _int_if_integral(total_tokens) if total_tokens > 0 else None,
    }


# ---------------------------------------------------------------------------
# Extract K2 Signature Context
# ---------------------------------------------------------------------------


def extract_k2_signature_context(item: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Extract K2 Signature Context (extracted/code/Extract_K2_Signature_Context.js).

    Input keys (missing nodes read as {}):
      item                              -> unused by the JS (kept for signature uniformity)
      inputs["normalize_validate"]      -> $(Normalize & Validate).first().json
      inputs["webhook_incoming_message"] -> $(Webhook - Incoming Message).first().json

    Output: single json dict = `{...normalized, k2_signature, k2_signed_payload}`
    where k2_signed_payload is JSON.stringify of the raw webhook body (None when
    the body is undefined — JSON.stringify(undefined) is undefined).
    """
    normalized = _dict_or(inputs.get("normalize_validate"))
    inbound = _dict_or(inputs.get("webhook_incoming_message"))
    body = _prop(inbound, "body")
    source_body = body if isinstance(body, (dict, list)) else inbound
    headers = _prop(inbound, "headers")
    headers = headers if isinstance(headers, dict) else {}
    signature = _first_truthy(
        _prop(headers, "x-k2-signature"), _prop(headers, "X-K2-Signature"), _prop(headers, "X-K2-SIGNATURE"), None,
    )
    out = dict(normalized)
    out["k2_signature"] = signature
    out["k2_signed_payload"] = _json_stringify(source_body)
    return out


# ---------------------------------------------------------------------------
# Validate Patient Ownership
# ---------------------------------------------------------------------------

_WEEKDAY_MAP = {"Sun": 0, "Mon": 1, "Tue": 2, "Wed": 3, "Thu": 4, "Fri": 5, "Sat": 6}


def _is_valid_timezone(value: str) -> bool:
    # PORT-TODO(n8n): Intl.DateTimeFormat validation approximated with zoneinfo;
    # on Windows the tzdata package must be present for IANA lookups to succeed.
    try:
        ZoneInfo(value)
        return True
    except Exception:
        return False


def validate_patient_ownership(item: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Validate Patient Ownership (extracted/code/Validate_Patient_Ownership.js).

    Input keys:
      item                      -> $input.first().json (the ownership DB row)
      inputs["normalize_validate"] -> $('Normalize & Validate').item.json
            PORT-TODO(n8n): the JS uses paired-item access (.item); the runner must
            supply the paired Normalize & Validate json explicitly.

    Output: single json dict = `{...row, ownership_valid, security_checked,
    security_error, clinic_found, clinic_timezone(_raw/_configured/_error_code),
    canonical_time_context}`. canonical_time_context.timezone resolution uses
    zoneinfo (PORT-TODO(n8n): Intl.DateTimeFormat semantics).
    """
    row = item if isinstance(item, dict) else {}
    ctx = _dict_or(inputs.get("normalize_validate"))
    clinic_found = bool(_prop(row, "clinic_found") is not False and _js_truthy(_prop(row, "clinic_id")))
    ownership_valid = bool(
        clinic_found
        and _prop(row, "ownership_valid") is True
        and _js_string(_first_truthy(_prop(row, "conversation_patient_id"), "")) == _js_string(_first_truthy(_prop(ctx, "patient_id"), ""))
        and _js_string(_first_truthy(_prop(row, "clinic_id"), "")) == _js_string(_first_truthy(_prop(ctx, "clinic_id"), ""))
    )
    raw_timezone = _js_trim(_js_string(_coalesce(_prop(row, "clinic_timezone"), "")))
    timezone_configured = _is_valid_timezone(raw_timezone) if raw_timezone else False
    timezone_value = raw_timezone if timezone_configured else None
    timezone_error_code = (
        (None if timezone_configured else "CLINIC_TIMEZONE_INVALID") if raw_timezone else "CLINIC_TIMEZONE_NOT_CONFIGURED"
    )
    reference_now_iso = _first_truthy(_dig(ctx, "time_context", "now_iso"), _utc_now_iso())
    reference_ms = _date_parse(reference_now_iso)
    parts: Dict[str, Any] = {}
    if timezone_configured and _is_finite_num(reference_ms):
        try:
            local = datetime.fromtimestamp(reference_ms / 1000.0, tz=timezone.utc).astimezone(ZoneInfo(raw_timezone))
            weekday_short = ("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")[(local.weekday() + 1) % 7]
            parts = {
                "year": local.year,
                "month": local.month,
                "day": local.day,
                "hour": local.hour,
                "minute": local.minute,
                "second": local.second,
                "weekday": weekday_short,
            }
        except Exception:
            parts = {}
    local_as_utc: Optional[float] = None
    if (
        timezone_configured and _is_finite_num(reference_ms)
        and all(k in parts for k in ("year", "month", "day", "hour", "minute", "second"))
    ):
        local_as_utc = float(datetime(
            int(parts["year"]), int(parts["month"]), int(parts["day"]),
            int(parts["hour"]), int(parts["minute"]), int(parts["second"]),
            tzinfo=timezone.utc,
        ).timestamp() * 1000.0)
    offset_ms: Optional[float] = None
    if timezone_configured and _is_finite_num(reference_ms) and local_as_utc is not None:
        offset_ms = local_as_utc - math.floor(reference_ms / 1000) * 1000
    sign = "+" if (offset_ms is not None and offset_ms >= 0) else "-"
    abs_minutes: Optional[int] = None if offset_ms is None else _js_round(abs(offset_ms) / 60000)
    utc_offset = (
        sign + str(abs_minutes // 60).zfill(2) + ":" + str(abs_minutes % 60).zfill(2)
        if abs_minutes is not None else None
    )
    weekday_val = _WEEKDAY_MAP.get(parts.get("weekday")) if parts else None
    canonical_time_context = {
        "schema_version": 4,
        "timezone": timezone_value,
        "timezone_configured": timezone_configured,
        "timezone_source": "clinic_configuration" if timezone_configured else "invalid_or_missing_clinic_configuration",
        "timezone_error_code": timezone_error_code,
        "utc_offset": utc_offset,
        "now_iso": reference_now_iso,
        "now_local_date": (
            str(parts["year"]).zfill(4) + "-" + str(parts["month"]).zfill(2) + "-" + str(parts["day"]).zfill(2)
            if (timezone_configured and "year" in parts and "month" in parts and "day" in parts) else None
        ),
        "now_local_time": (
            str(parts["hour"]).zfill(2) + ":" + str(parts["minute"]).zfill(2) + ":" + str(parts["second"]).zfill(2)
            if (timezone_configured and "hour" in parts and "minute" in parts and "second" in parts) else None
        ),
        "now_local_weekday": weekday_val if (timezone_configured and isinstance(weekday_val, int)) else None,
    }
    out = dict(row)
    out.update({
        "ownership_valid": ownership_valid,
        "security_checked": True,
        "security_error": "CLINIC_NOT_FOUND" if not clinic_found else (None if ownership_valid else "PATIENT_CONVERSATION_OWNERSHIP_MISMATCH"),
        "clinic_found": clinic_found,
        "clinic_timezone": timezone_value,
        "clinic_timezone_raw": _first_truthy(raw_timezone, None),
        "clinic_timezone_configured": timezone_configured,
        "clinic_timezone_error_code": timezone_error_code,
        "canonical_time_context": canonical_time_context,
    })
    return out


# ---------------------------------------------------------------------------
# Evaluate Completed Create Replay
# ---------------------------------------------------------------------------

_REPLAY_REPLY_WITH_NUMBER = "تم تأكيد الحجز بالفعل ورقم الحجز "
_REPLAY_REPLY_PLAIN = "تم تأكيد الحجز بالفعل"


def evaluate_completed_create_replay(item: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Evaluate Completed Create Replay (extracted/code/Evaluate_Completed_Create_Replay.js).

    Input keys:
      item                      -> $json (state row; reads .state_data)
      inputs["normalize_validate"] -> $('Normalize & Validate').item.json
            PORT-TODO(n8n): the JS uses paired-item access (.item); the runner must
            supply the paired Normalize & Validate json explicitly.

    Output: single json dict = `{...$json, replay_gate: {matched}}` plus, when the
    replay matched, `system_decision` and `proposal` overrides.
    """
    ctx = _dict_or(inputs.get("normalize_validate"))
    sd = _prop(item, "state_data")
    previous = sd if isinstance(sd, dict) else {}
    requested_operation_id = _js_trim(_js_string(_first_truthy(_prop(ctx, "operation_id"), "")))
    public_booking_number = _js_trim(_js_string(_first_truthy(_prop(previous, "booking_number"), "")))
    is_completed_create_replay = bool(
        requested_operation_id != ""
        and _prop(previous, "operation_action") == "create_appointment"
        and _js_string(_first_truthy(_prop(previous, "operation_status"), "")) in ("success", "completed")
        and _js_string(_first_truthy(_prop(previous, "operation_id"), "")) == requested_operation_id
        and _js_trim(_js_string(_first_truthy(_prop(previous, "appointment_id"), ""))) != ""
    )
    replay_reply = (_REPLAY_REPLY_WITH_NUMBER + public_booking_number) if public_booking_number else _REPLAY_REPLY_PLAIN
    out = dict(item if isinstance(item, dict) else {})
    out["replay_gate"] = {"matched": is_completed_create_replay}
    if is_completed_create_replay:
        out["system_decision"] = {
            "intent": "booking",
            "proposed_action": "none",
            "action": "create_appointment",
            "allowed": False,
            "response_code": "IDEMPOTENT_REPLAY",
            "operation_id": _prop(previous, "operation_id"),
            "appointment_id": _prop(previous, "appointment_id"),
            "replayed": True,
            "confirmation_state": "confirmed",
            "confirmation_target": None,
            "final_reply": replay_reply,
        }
        out["proposal"] = {
            "intent": "booking",
            "proposed_action": "none",
            "confidence": 1,
            "proposed_reply": out["system_decision"]["final_reply"],
            "slot_state": _first_truthy(_prop(previous, "slot_state"), {}),
            "booking_context": _first_truthy(_prop(previous, "booking_context"), {}),
            "confirmation_required": False,
            "user_confirmation_signal": False,
            "confirmation_target": None,
            "appointment_id": _prop(previous, "appointment_id"),
            "cancellation_reason": "user_requested",
            "cancelled_by": None,
            "escalate": False,
        }
    return out


# ---------------------------------------------------------------------------
# Route Single Agent Phase
# ---------------------------------------------------------------------------

_FENCED_JSON_RE = re.compile(r"```(?:json)?" + _JS_WS_CLASS + r"*([\s\S]*?)```", re.IGNORECASE)


def build_save_state_rpc_body(item: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Build Save State RPC Body (extracted/code/Build_Save_State_RPC_Body.js).

    Builds the RPC body for k2_save_conversation_state.
    Ownership contract: p_previous_state_version = current state_version - 1,
    so the SQL upsert rejects stale concurrent writes (fail-closed).

    Input keys:
      build_persistent_conversation_state <- $(Build Persistent Conversation State).first().json
      normalize_validate                  <- $(Normalize & Validate).first().json
    """
    bp = _first_truthy((inputs or {}).get("build_persistent_conversation_state"), {})
    nv = _first_truthy((inputs or {}).get("normalize_validate"), {})
    sd = bp.get("state_data") if isinstance(bp.get("state_data"), dict) else {}
    version = int(_js_number(sd.get("state_version") or 0)) or 1
    return {
        **(item or {}),
        "rpc_body": {
            "p_conversation_id": _js_string(_first_truthy(nv.get("conversation_id"), "")),
            "p_state_data": sd,
            "p_previous_state_version": max(0, version - 1),
        },
    }
