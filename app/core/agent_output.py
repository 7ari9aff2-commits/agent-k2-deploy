"""Normalize Agent Output (Deterministic) — NAO v3 faithful 1:1 port.

Source node: Normalize Agent Output (Deterministic)
(extracted/code/Normalize_Agent_Output_Deterministic.js — 769 lines)

k2.dialogue.v3 parser/validator + deterministic legacy bridge. Rules preserved
from the JS header comment: the deterministic layer validates FORMAT only
(enums, ISO dates, E.164 phones, ages, UUIDs). It NEVER reads message_text for
semantic decisions and never infers meaning from text. Semantic regexes are
forbidden — the only text-touching paths are the observability-only Arabic
hygiene view (``normalized_language_views``) and the P42b offered-slot rescue,
both ported exactly as-is (quirks included: the JS ASCII-only ``\\b`` in the
temporal-claim day-word regex means that regex matches only between ASCII word
characters — effectively never on pure Arabic — and the port reproduces that).

Public API: ``normalize_agent_output(inputs: dict) -> dict`` — returns the inner
json dict the JS emits as ``[{ json: result }][0].json``.

Required ``inputs`` schema
--------------------------
The JS reads six upstream nodes via ``$(NodeName).first().json`` plus the
current item (``$json``). The pipeline runner passes those outputs on the
``inputs`` dict; the key set below is derived from what the JS actually
consumes. Every key is optional: an absent node behaves exactly like the JS
``safeNode`` try/catch (yields ``{}``), except ``repair_prompt`` whose PRESENCE
is the signal (the JS ``Boolean($(node).first().json)`` is true whenever the
node executed, even with an empty ``{}`` output — so the runner includes the
key iff the repair node executed).

- ``current``            ← ``$json`` — the agent item. Fields read: ``text``,
  ``raw_output``, ``output``, ``response`` (the model output, ``??`` chain),
  ``error`` / ``errorMessage`` / ``statusCode`` (transport-failure detection).
  It is also spread into the output (after ``temporal_claim_guard_active``).
- ``normalize_validate`` ← ``$('Normalize & Validate').first().json`` —
  ``message_id``, ``source_event_id``, ``idempotency_key``, ``clinic_id``,
  ``channel_key``, ``channel_type``, ``channel_id``, ``conversation_id``,
  ``message_text``, ``received_at``, ``time_context`` (validator fallback).
- ``conversation_state`` ← ``$('Get Conversation State').first().json`` —
  ``state_data``: ``last_updated``/``updated_at``/``last_message_at``,
  ``slot_state``, ``booking_context``, ``facts.patient``,
  ``active_operation``/``operation_action``, ``operation_state``/``operation_status``,
  ``draft_expires_at``, ``presented_offer``/``pending_offer``,
  ``confirmation_target``.
- ``clinic_context``     ← ``$('Get Clinic Context').first().json`` —
  ``country_code``/``clinic_country_code``, ``doctor_directory``,
  ``doctor_count``, ``single_doctor_id``, ``single_doctor_name``.
- ``patient_ownership``  ← ``$('Validate Patient Ownership').first().json`` —
  ``canonical_time_context`` (``now_local_date``).
- ``persona_builder``    ← ``$('Build Clinic Persona Context (Deterministic)').first().json``
  — ``pre_agent_stage_contract``, ``error_followup_context``, ``clinic_query_type``.
- ``repair_prompt``      ← ``$('Build Repair Prompt (Deterministic)').first().json``
  — presence-only (see above).

Pure function: no I/O, no logging, stdlib only. The JS ``Date.now()`` reads are
snapshot once per call (they were distinct calls microseconds apart in JS).
"""

import calendar
import json
import re
import time
import unicodedata
from datetime import datetime, timezone
from typing import TypedDict
from app.core.js_semantics import cp_to_u16 as _cp_to_u16, dict_or_empty as _dict, first_not_none as _first_not_none, is_finite as _is_finite, iso_from_ms as _iso_from_ms, js_and as _js_and, js_is_integer as _js_is_integer, js_len as _js_len, js_or as _js_or, truthy as _truthy


# ── JS-semantics shims (same semantics as the ones in app/core/orchestrator.py) ──

_UNDEFINED = object()  # distinguishes a missing key (JS undefined) from an explicit null

_JS_WS_INNER = r'\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff'
_JS_S = '[' + _JS_WS_INNER + ']'  # JS \s character class (Python \s differs on \ufeff / \x1c-\x1f \x85)
_WS_PLUS_RE = re.compile(_JS_S + '+')
_WS_2PLUS_RE = re.compile(_JS_S + '{2,}')


def _prop(obj, key):
    """Property read on a value that may not be an object (JS yields undefined, never throws)."""
    return obj.get(key) if isinstance(obj, dict) else None


def _dig(obj, *path):
    """JS ``a?.b?.c`` optional chaining: any non-object link yields None (undefined)."""
    cur = obj
    for key in path:
        if isinstance(cur, dict):
            cur = cur.get(key)
        else:
            return None
    return cur


def _js_string(value):
    """JS String() coercion: arrays join with ','; plain objects → '[object Object]'."""
    if value is None or value is _UNDEFINED:
        return ''
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, float):
        if value != value:  # NaN
            return 'NaN'
        if value in (float('inf'), float('-inf')):
            return 'Infinity' if value > 0 else '-Infinity'
        if value.is_integer() and abs(value) < 1e21:
            return str(int(value))
        r = repr(value)
        if 'e' in r or 'E' in r:
            mant, _, exp = r.lower().partition('e')
            e = int(exp)
            return mant + 'e' + ('+' if e >= 0 else '-') + str(abs(e))
        return r
    if isinstance(value, int):
        return str(value)
    if isinstance(value, list):
        return ','.join(_js_string(v) for v in value)
    if isinstance(value, dict):
        return '[object Object]'
    return str(value)


def _nullish_str(value):
    """JS ``String(value ?? '')`` — null/undefined → '', everything else via String()."""
    return '' if value is None else _js_string(value)


def _js_number(value):
    """JS Number() coercion: bool→1/0, numeric strings parsed, ''→0, None (undefined/null)→NaN."""
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    if value is None or value is _UNDEFINED:
        return float('nan')
    if isinstance(value, str):
        s = value.strip()
        if s == '':
            return 0
        try:
            if s[:2] in ('0x', '0X'):
                n = int(s, 16)
            else:
                n = int(s)
            return n
        except ValueError:
            try:
                f = float(s)
            except ValueError:
                return float('nan')
            return int(f) if f.is_integer() else f
    return float('nan')


_INT_HEAD_RE = re.compile(r'[\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]*([+-]?[0-9]+)')


def _js_parse_int(value):
    """JS parseInt(x, 10): leading JS whitespace + optional sign + ASCII digits; NaN when none."""
    m = _INT_HEAD_RE.match(_js_string(value))
    if not m:
        return float('nan')
    return int(m.group(1))


def _js_trim(s):
    """JS String.prototype.trim() — the JS WhiteSpace + LineTerminator set (differs from Python strip())."""
    return s.strip('\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff')


def _u16_slice(s, start, end=None):
    """JS String.prototype.slice(start, end) — indexes are UTF-16 code units.

    PORT-TODO(n8n): when a cut lands inside an astral (surrogate) pair, JS keeps a
    lone high surrogate; this port stops before the pair instead (a one-code-point
    difference only when an emoji straddles the exact cut index) to avoid emitting
    invalid scalars into downstream JSON/DB layers.
    """
    n = _js_len(s)
    if start < 0:
        start = max(0, n + start)
    else:
        start = min(start, n)
    if end is None:
        end = n
    elif end < 0:
        end = max(0, n + end)
    else:
        end = min(end, n)
    if start >= end:
        return ''
    units = 0
    out = []
    for ch in s:
        w = 2 if ord(ch) > 0xFFFF else 1
        if units >= end or units + w > end:
            break
        if units < start and units + w > start:
            units += w
            continue
        if units >= start:
            out.append(ch)
        units += w
    return ''.join(out)


def _tpl(*parts):
    """JS template literal ``${a}text${b}`` — undefined renders as 'undefined', null as 'null'."""
    out = []
    for p in parts:
        if p is _UNDEFINED:
            out.append('undefined')
        elif p is None:
            out.append('null')
        else:
            out.append(_js_string(p))
    return ''.join(out)


def _date_parse_ms(value):
    """JS Date.parse() for the ISO-8601 shapes this pipeline produces; None when invalid.

    Date-only strings are UTC (like JS); naive date-times are treated as UTC
    deterministically (JS would use the machine's local zone — canonical received_at
    values in this pipeline are always UTC 'Z' strings, so this path is not hit).

    PORT-TODO(n8n): JS Date.parse also accepts vendor-specific formats (RFC 2822,
    'YYYY/MM/DD', …); the port implements the ISO-8601 subset only.
    """
    s = _js_string(value).strip()
    if not s:
        return None
    txt = s[:-1] + '+00:00' if s[-1] in ('Z', 'z') else s
    try:
        dt = datetime.fromisoformat(txt)
    except ValueError:
        try:
            dt = datetime.strptime(txt[:10], '%Y-%m-%d')
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return calendar.timegm(dt.utctimetuple()) * 1000 + dt.microsecond // 1000


def _utc_now_iso():
    """JS new Date().toISOString()."""
    now = datetime.now(timezone.utc)
    return now.strftime('%Y-%m-%dT%H:%M:%S') + '.%03dZ' % (now.microsecond // 1000)


def _utc_now_date():
    """JS new Date().toISOString().slice(0, 10)."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


def _utc_weekday(ms):
    """JS new Date(ms).getUTCDay() — Sunday=0..Saturday=6."""
    dt = datetime.fromtimestamp(ms // 1000, tz=timezone.utc)
    return (dt.weekday() + 1) % 7


def _stable_hash(value):
    """JS stableHash — FNV-1a 32-bit over the string's code points (for...of iteration)."""
    h = 2166136261
    for ch in _nullish_str(value):
        h ^= ord(ch)
        h = (h * 16777619) & 0xFFFFFFFF
    return format(h, '08x')


def _json_stringify(value):
    """JS JSON.stringify — compact separators, non-ASCII kept literal."""
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


# ── Schema constants (copied verbatim from the JS) ──
SCHEMA_VERSION = 'k2.dialogue.v3'

TURN_INTENTS = ['booking_request', 'booking_continuation', 'availability_inquiry',
    'cancellation_request', 'reschedule_request', 'confirmation', 'correction',
    'small_talk', 'clinic_query', 'greeting', 'unclear', 'other']
RELATIONS = ['new_request', 'answer', 'confirmation', 'correction',
    'change_details', 'follow_up', 'none', 'unclear']
CERTAINTIES = ['certain', 'probable', 'uncertain']
CONFIRMATION_INTENTS = ['affirmative', 'negative', 'question', 'conditional', 'none']
SELECTION_KINDS = ['presented_rank', 'presented_match', 'any', 'none']
OPERATION_TYPES = ['create_appointment', 'cancel_appointment', 'reschedule_appointment', 'check_availability', '']
VISIT_TYPES = ['NEW_VISIT', 'FOLLOW_UP']

# ── E.164 phone normalization (retained from P1.7 as a validator) ──
PHONE_RULES = {
    'SA': {'code': '+966', 'lengths': [9], 'prefixes': ['5']},
    'AE': {'code': '+971', 'lengths': [9], 'prefixes': ['5']},
    'KW': {'code': '+965', 'lengths': [8], 'prefixes': ['5', '6', '9']},
    'QA': {'code': '+974', 'lengths': [8], 'prefixes': ['3', '5', '6', '7']},
    'BH': {'code': '+973', 'lengths': [8], 'prefixes': ['3']},
    'OM': {'code': '+968', 'lengths': [8], 'prefixes': ['7', '9']},
    'EG': {'code': '+20', 'lengths': [10], 'prefixes': ['1', '2']},
    'PH': {'code': '+63', 'lengths': [10], 'prefixes': ['9']},
    'IN': {'code': '+91', 'lengths': [10], 'prefixes': ['6', '7', '8', '9']},
    'PK': {'code': '+92', 'lengths': [10], 'prefixes': ['3']},
    'BD': {'code': '+880', 'lengths': [10], 'prefixes': ['1']},
    'ID': {'code': '+62', 'lengths': [9, 10, 11, 12], 'prefixes': ['8']},
    'YE': {'code': '+967', 'lengths': [9], 'prefixes': ['7']},
    'JO': {'code': '+962', 'lengths': [9], 'prefixes': ['7']},
    'SD': {'code': '+249', 'lengths': [9], 'prefixes': ['9']},
    'SY': {'code': '+963', 'lengths': [9], 'prefixes': ['9']},
    'IQ': {'code': '+964', 'lengths': [10], 'prefixes': ['7']},
    'LB': {'code': '+961', 'lengths': [7, 8], 'prefixes': ['3', '7']},
    'TR': {'code': '+90', 'lengths': [10], 'prefixes': ['5']},
    'US': {'code': '+1', 'lengths': [10], 'prefixes': ['2', '3', '4', '5', '6', '7', '8', '9']},
    'GB': {'code': '+44', 'lengths': [10], 'prefixes': ['7']}
}
ARABIC_INDIC = {chr(0x0660 + i): str(i) for i in range(10)}


def _to_english_digits(s):
    """JS toEnglishDigits — Arabic-Indic digits (U+0660–U+0669) → ASCII digits."""
    return ''.join(ARABIC_INDIC.get(ch, ch) for ch in _js_string(s))


def _normalize_phone(raw_input, default_country=None):
    """JS normalizePhone (NAO variant)."""
    if default_country is None:
        default_country = 'SA'
    if raw_input is None or raw_input == '':
        return None
    s = re.sub(r'[^0-9+]', '', _to_english_digits(_js_string(raw_input)))
    if not s:
        return None
    if s[0] == '+':
        for rule in PHONE_RULES.values():
            if s.startswith(rule['code']):
                return s
        return s
    # Reviewer fix: '00' international prefix ('00966501234567') previously fell into
    # the local-0 strip and produced a corrupted +9660966... number.
    if s.startswith('00'):
        return '+' + s[2:]
    for rule in PHONE_RULES.values():
        if len(s) in rule['lengths'] and any(s.startswith(p) for p in rule['prefixes']):
            return rule['code'] + s
    if s.startswith('0'):
        s = s[1:]
    default_rule = PHONE_RULES.get(default_country)
    if default_rule:
        return default_rule['code'] + s
    return '+' + s


# ── Format validators (JS \d is ASCII-only; the port uses [0-9] for the same reason) ──
ISO_DATE = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}')
TIME_24 = re.compile(r'([01][0-9]|2[0-3]):[0-5][0-9](?::[0-5][0-9])?')
UUID_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', re.IGNORECASE)


def _iso_date_valid(date_iso):
    """JS isoDateValid — format + real calendar date (JS invalid dates → NaN → False)."""
    s = _js_string(_js_or(date_iso, ''))
    if not ISO_DATE.fullmatch(s):
        return False
    try:
        datetime.strptime(s + 'T12:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
        return True
    except ValueError:
        return False


def _date_within_horizon(date_iso, now_local_date, horizon_days):
    """JS dateWithinHorizon."""
    if not _iso_date_valid(date_iso) or not _iso_date_valid(now_local_date):
        return False
    a = _date_parse_ms(_js_string(now_local_date) + 'T12:00:00Z')
    b = _date_parse_ms(_js_string(date_iso) + 'T12:00:00Z')
    if a is None or b is None:
        return False
    days = round((b - a) / 86400000)
    return days >= 0 and days <= (horizon_days or 60)


def _normalize_time(value):
    """JS normalizeTime. Reviewer fix: a single-digit hour ("9:00") is a valid patient
    statement — zero-pad instead of dropping the requested time."""
    t = _js_string(_js_or(value, '')).strip()
    if re.fullmatch(r'([0-9]):[0-5][0-9]', t):
        t = '0' + t
    if not TIME_24.fullmatch(t):
        return None
    return t[:5] if len(t) == 8 else (t + ':00' if len(t) == 4 else t)


def _clean_str(value):
    """JS cleanStr — collapse whitespace, trim, empty → null."""
    s = _WS_PLUS_RE.sub(' ', _js_string('' if value is None else value)).strip()
    return s if s else None


# ── Robust JSON extraction (no semantics) ──
_FENCED_RE = re.compile(r'```(?:json)?\s*([\s\S]*?)```', re.IGNORECASE)


def _parse_at(t, start):
    """JS parseAt — string-aware balanced-brace scan; returns {'value', 'end'} or None."""
    depth = 0
    in_string = False
    escaped = False
    i = start
    while i < len(t):
        ch = t[i]
        if in_string:
            if escaped:
                escaped = False
            elif ch == '\\':
                escaped = True
            elif ch == '"':
                in_string = False
            i += 1
            continue
        if ch == '"':
            in_string = True
            i += 1
            continue
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                try:
                    value = json.loads(t[start:i + 1])
                except Exception:
                    return None
                if _truthy(value) and isinstance(value, dict):
                    return {'value': value, 'end': i}
                return None
        i += 1
    return None


def _extract(text):
    """JS extract — fenced-block strip + last balanced JSON object wins."""
    if _truthy(text) and isinstance(text, dict):
        return text
    if text is None:
        return None
    t = _js_string(text).strip()
    m = _FENCED_RE.search(t)
    if m:
        t = m.group(1).strip()
    candidates = []
    i = 0
    while i < len(t):
        if t[i] != '{':
            i += 1
            continue
        parsed_candidate = _parse_at(t, i)
        if parsed_candidate:
            candidates.append(parsed_candidate['value'])
            i = parsed_candidate['end']
        i += 1
    if not candidates:
        return None
    return candidates[-1]


# ── P41 CLAIM-GUARD: an understand-phase reply may never claim a completed booking ──
_CLAIM_RE = re.compile(
    r'(نثبت|ثبت)' + _JS_S + r'*(?:لك)?' + _JS_S + r'*(?:الموعد|الحجز)'
    r'|تم' + _JS_S + r'+(?:الحجز|التثبيت|تأكيد' + _JS_S + r'*الحجز)'
    r'|اتأكد' + _JS_S + r'*(?:الحجز)?'
    r'|اتسجل' + _JS_S + r'*(?:الحجز|لك)'
)
_NEGATION_BEFORE_RE = re.compile(r'(?:مش|ما' + _JS_S + r'|مفيش|لن|لما|غير)(?:[' + _JS_WS_INNER + r'ه]{1,2})?\Z')
_HAL_RE = re.compile(_JS_S + r'*هل')


# ── Validator context: clinic-local calendar + country for format checks ──

def _build_validator_ctx(ctx_node, clinic_row, ownership, now_date):
    """JS timeCtx / nowLocalDate / validatorCtx block."""
    octx = ownership.get('canonical_time_context')
    if _truthy(octx) and isinstance(octx, dict):
        time_ctx = octx
    else:
        tc = ctx_node.get('time_context')
        time_ctx = tc if (_truthy(tc) and isinstance(tc, dict)) else {}
    now_local_date = _nullish_str(time_ctx.get('now_local_date'))
    if not ISO_DATE.fullmatch(now_local_date):
        now_local_date = now_date
    return {
        'now_local_date': now_local_date,
        'clinic_country_code': _js_string(_js_or(clinic_row.get('country_code'), clinic_row.get('clinic_country_code'), 'SA')),
        'date_horizon_days': 60,
    }


# ── Message classification (exclusively from contract fields — never text) ──
def _message_class(contract, state):
    """JS messageClass — the ``state`` argument is unused in the JS body too (kept for fidelity)."""
    del state
    c = _dict(contract)
    turn = _dict(c.get('turn'))
    intent = _js_string(_js_or(turn.get('intent'), 'unclear'))
    conf_intent = _js_string(_js_or(_dict(c.get('confirmation')).get('intent'), 'none'))
    sel_kind = _js_string(_js_or(_dict(c.get('selection')).get('kind'), 'none'))
    op_type = _js_string(_js_or(_dict(c.get('operation_proposal')).get('type'), ''))
    if op_type == 'cancel_appointment' or intent == 'cancellation_request':
        return 'cancel_request'
    if op_type == 'reschedule_appointment' or intent == 'reschedule_request':
        return 'reschedule_request'
    if sel_kind != 'none':
        return 'selection_presented'
    if conf_intent == 'affirmative':
        return 'confirmation_affirm'
    if conf_intent == 'negative':
        return 'confirmation_negative'
    if conf_intent == 'question' or conf_intent == 'conditional':
        return 'confirmation_question'
    if intent == 'confirmation':
        return 'confirmation_affirm'
    if intent == 'booking_request' or intent == 'booking_continuation' or op_type == 'create_appointment':
        return 'booking'
    if intent == 'availability_inquiry' or op_type == 'check_availability':
        return 'availability_inquiry'
    if intent == 'correction':
        return 'correction'
    if intent == 'small_talk' or intent == 'greeting':
        return 'small_talk'
    if intent == 'clinic_query':
        return 'clinic_query'
    return 'unclear'


# ── Deterministic day-word absorption (closed vocabulary → ISO) ──
_DAY_WORD_OFFSETS = [
    ('بعد بكرة', 2), ('بعد بكره', 2), ('بعد غد', 2), ('بعدغد', 2), ('بعدغده', 2),
    ('بعدبكرة', 2), ('بعدبكره', 2), ('بعدغده', 2),
    ('بكرة', 1), ('بكره', 1), ('غدا', 1), ('غدًا', 1),
    ('اليوم', 0)
]
# Same-day deictics directly after a weekday name mean TODAY, not next week.
_SAME_DAY_DEICTIC_RE = re.compile(r'(?:السبت|الأحد|الاحد|الاثنين|الثلاثاء|الأربعاء|الاربعاء|الخميس|الجمعة)\s*(?:هذا|هذي|ده|دا|دهم|الحالي)')
_WEEKDAY_TARGETS = [('السبت', 6), ('الاحد', 0), ('الاثنين', 1), ('الثلاثاء', 2), ('الاربعاء', 3), ('الخميس', 4), ('الجمعه', 5), ('الجمعة', 5), ('الاحد', 0)]


def _absorb_day_word_to_iso(raw, now_iso_date):
    """JS absorbDayWordToIso. Input is folded (NFKC, harakat stripped, spaces collapsed)
    first — glued ('بعدبكرة') and diacritic ('بكِرة') spellings used to slip through to the
    bare 'بكرة' rule and book a day early. Negation is a known limitation (closed vocab)."""
    s = _js_string(_js_or(raw, '')).strip()
    s = unicodedata.normalize('NFKC', s)
    s = _HARAKAT_RE.sub('', s)
    s = _WS_PLUS_RE.sub(' ', s).strip()
    # Reviewer fix: fold letter variants (الجمعه/الإثنين/الاربعه) exactly like
    # normalizeArabicUserText — harakat-only folding left the colloquial spellings
    # unmatched, the date went None, and _keep resurrected the REJECTED prior date.
    s = re.sub('[أإآٱ]', 'ا', s).replace('ة', 'ه')
    s = _WS_PLUS_RE.sub(' ', s).strip()
    if not s or not ISO_DATE.fullmatch(_js_string(_js_or(now_iso_date, ''))):
        return None
    base = _date_parse_ms(_js_string(now_iso_date) + 'T00:00:00Z')
    if base is None:
        return None
    delta = None
    for word, offset in _DAY_WORD_OFFSETS:
        if word in s:
            delta = offset
            break
    if delta is None:
        base_wd = _utc_weekday(base)
        # Reviewer fix: "الجمعة أو السبت" resolves by FIRST MENTION position, not table
        # order (السبت precedes الجمعة in the table and used to win the wrong way).
        hits = [(s.find(word), target) for word, target in _WEEKDAY_TARGETS if word in s]
        hits = [(pos, target) for pos, target in hits if pos >= 0]
        if hits:
            pos, target = min(hits)
            delta = (target - base_wd + 7) % 7
            if delta == 0 and not _SAME_DAY_DEICTIC_RE.search(s):
                # "الخميس" said on a Thursday means NEXT Thursday — a patient naming
                # today's weekday is booking ahead, not asking for a same-day slot
                # that almost certainly no longer exists. An explicit same-day
                # marker ("السبت ده") still means today.
                delta = 7
    if delta is None:
        return None
    iso = _iso_from_ms(base + delta * 86400000)
    return iso[:10] if iso is not None else None


# ── Observability-only Arabic hygiene view (never used for decisions) ──
_HARAKAT_RE = re.compile('[ًٌٍَُِّْـ]')
_ARABIC_DIGITS_RE = re.compile('[٠-٩]')


def _normalize_arabic_user_text(value):
    """JS normalizeArabicUserText — NFKC + letter/harakat folding + digit transliteration."""
    s = unicodedata.normalize('NFKC', _nullish_str(value))
    s = re.sub('[أإآٱ]', 'ا', s)
    s = s.replace('ى', 'ي').replace('ؤ', 'و').replace('ئ', 'ي').replace('ة', 'ه')
    s = _HARAKAT_RE.sub('', s)
    s = _ARABIC_DIGITS_RE.sub(lambda m: str('٠١٢٣٤٥٦٧٨٩'.index(m.group(0))), s)
    s = _WS_PLUS_RE.sub(' ', s)
    return _js_trim(s).lower()


# ── P42b rescue regexes ──
_RESCUE_NEGATED_RE = re.compile(r'(?:ل[آا]?|مش|مفيش|لن|لما|ما' + _JS_S + r'|هل|فيه|يوجد)')
_MTIME_RE = re.compile(r'(?:الساعه|ساعه|الساعة)' + _JS_S + r'*([0-9]{1,2})(?::([0-9]{2}))?' + _JS_S + r'*((?:صباحا|صباح|مساء|ص|م)(?=' + _JS_S + r'|\Z|[.,!؟]))?')
_MRANK_RE = re.compile(r'(?:الاختيار|الخيار)' + _JS_S + r'*(?:رقم)?' + _JS_S + r'*([0-9])')
_NEG_BEFORE_TIME_RE = re.compile(r'(?:مش|غير|بدون|لا)' + _JS_S + r'*\Z')
_PM_RE = re.compile(r'(م|مساء)')
_AM_RE = re.compile(r'(ص|صباح)')


# ── P41 temporal-claim guard regexes ──
# JS \b is ASCII-only (\w = [A-Za-z0-9_]); every word in the day-word alternation is
# non-ASCII, so JS \b only matches when ASCII word characters surround the match on
# both sides. The lookaround pair below reproduces exactly that (the regex is
# effectively dead on pure-Arabic text — the quirk is preserved, not fixed).
_TEMPORAL_DAY_RE = re.compile(
    r'(?<=[A-Za-z0-9_])(?:اليوم|بكره|بكرة|غدا|غدًا|بعد بكره|بعد بكرة|بعد غد|باچر'
    r'|السبت|الاحد|الأحد|الاثنين|الثلاثاء|الاربعاء|الأربعاء|الخميس|الجمعة)(?=[A-Za-z0-9_])',
    re.IGNORECASE,
)
_TEMPORAL_SAA_RE = re.compile(r'الساعة' + _JS_S + r'+[0-9]{1,2}(?::[0-9]{2})?' + _JS_S + r'*(?:صباحا|صباحًا|مساء|مساءً|ص|م)?', re.IGNORECASE)
_TEMPORAL_HHMM_RE = re.compile(r'(?<![A-Za-z0-9_])[0-9]{1,2}:[0-9]{2}(?![A-Za-z0-9_])')
_TEMPORAL_DATE_RE = re.compile(r'(?<![A-Za-z0-9_])[0-9]{1,2}[/-][0-9]{1,2}(?:[/-][0-9]{2,4})?(?![A-Za-z0-9_])')


# ── Legacy v2 bridge projection maps (deterministic; consumed until Phase 3 replaces it) ──
INTENT_TO_V2 = {'booking_request': 'booking_request', 'booking_continuation': 'booking_continuation', 'availability_inquiry': 'availability_inquiry', 'cancellation_request': 'cancellation_request', 'reschedule_request': 'reschedule_request', 'confirmation': 'confirmation', 'correction': 'correction', 'small_talk': 'small_talk', 'greeting': 'small_talk', 'clinic_query': 'faq_inquiry', 'unclear': 'unclear', 'other': 'other'}
RELATION_TO_V2 = {'new_request': 'new_request', 'answer': 'answer', 'confirmation': 'confirmation', 'correction': 'correction', 'change_details': 'correction', 'follow_up': 'continuation', 'none': 'none', 'unclear': 'none'}
CERTAINTY_TO_V2 = {'certain': 'clear', 'probable': 'ambiguous', 'uncertain': 'uncertain'}
ROUTING_BY_CLASS = {'cancel_request': 'cancel', 'reschedule_request': 'reschedule', 'selection_presented': 'booking', 'confirmation_affirm': 'confirmation', 'confirmation_negative': 'confirmation', 'confirmation_question': 'confirmation', 'booking': 'booking', 'availability_inquiry': 'booking', 'correction': 'booking', 'small_talk': 'small_talk', 'clinic_query': 'faq', 'unclear': 'none'}


# ── Validate + deterministically repair a model contract ──
def _validate_contract(raw, ctx):
    """JS validateContract — returns { valid, errors[], warnings[], contract }.

    ``ctx``: { now_local_date, clinic_country_code, date_horizon_days }. Errors are
    structural (trigger the one-shot model self-repair retry); warnings are field repairs.
    """
    ctx = _dict(ctx)
    errors = []
    warnings = []
    c = raw
    if isinstance(c, str):
        try:
            c = json.loads(c)
        except Exception:
            c = None
        if not _truthy(c) or not isinstance(c, (dict, list)):
            errors.append('contract_unparseable')
    if not _truthy(c) or not isinstance(c, (dict, list)):
        return {'valid': False, 'errors': errors if errors else ['contract_missing'], 'warnings': warnings, 'contract': None}
    out = {'schema_version': SCHEMA_VERSION, 'phase': 'understand', 'reply': _clean_str(_prop(c, 'reply'))}

    # P41 CLAIM-GUARD - DETECTION ONLY (changed 2026-09-17).
    # This used to rewrite the model's sentence, replacing the first claim with the
    # literal 'تمام'. Rewriting the model's words is not how this system works: the
    # model is taught up front what it may claim (see the composer prompt), and
    # correctness is enforced by the evidence contract in
    # response_context.validate_composer_output - the composer may only assert a
    # completed operation when a mutation_result fact says so. The detection is kept as
    # a warning so the audit still shows turns where the model over-claimed.
    if isinstance(out['reply'], str) and not _HAL_RE.match(out['reply']) and _CLAIM_RE.search(out['reply']):
        warnings.append('reply_claims_unverified_booking')

    def _in_enum(value, allowed, fallback):
        v = _js_string('' if value is None else value).strip().lower()
        if not v:
            return fallback
        if v in allowed:
            return v
        warnings.append('enum_repaired:' + v)
        return fallback

    turn = _prop(c, 'turn')
    turn = turn if isinstance(turn, dict) else {}
    intent = _in_enum(turn.get('intent'), TURN_INTENTS, 'unclear')
    turn_intent_raw = _js_string(_js_or(turn.get('intent'), '')).strip()
    if intent == 'unclear' and turn_intent_raw != '' and turn_intent_raw.lower() not in TURN_INTENTS:
        warnings.append('turn_intent_repaired')
    n_conf = _js_number(turn.get('confidence'))
    confidence = max(0, min(1, n_conf)) if _is_finite(n_conf) else None
    out['turn'] = {
        'intent': intent,
        'relation_to_previous_turn': _in_enum(turn.get('relation_to_previous_turn'), RELATIONS, 'none'),
        'certainty': _in_enum(turn.get('certainty'), CERTAINTIES, 'uncertain'),
        'confidence': confidence,
    }

    conf = _prop(c, 'confirmation')
    conf = conf if isinstance(conf, dict) else {}
    out['confirmation'] = {'intent': _in_enum(conf.get('intent'), CONFIRMATION_INTENTS, 'none')}

    sel = _prop(c, 'selection')
    sel = sel if isinstance(sel, dict) else {}
    sel_kind = _in_enum(sel.get('kind'), SELECTION_KINDS, 'none')
    rank_num = _js_number(sel.get('rank'))
    sel_rank = rank_num if (_js_is_integer(rank_num) and 1 <= rank_num <= 4) else None
    if _iso_date_valid(sel.get('date')):
        sel_date = _js_string(sel.get('date'))
    else:
        if _truthy(sel.get('date')):
            warnings.append('selection_date_invalid')
        sel_date = None
    sel_time = _normalize_time(sel.get('time'))
    if sel_time is None:
        if _truthy(sel.get('time')):
            warnings.append('selection_time_invalid')
        sel_time = None
    out['selection'] = {'kind': sel_kind, 'rank': sel_rank, 'date': sel_date, 'time': sel_time}
    if (sel_kind != 'none' and sel_kind != 'any' and not sel_rank and not sel_date and not sel_time
            and not _truthy(sel.get('time')) and not _truthy(sel.get('date'))):
        warnings.append('selection_without_anchor')

    ent = _prop(c, 'entities')
    ent = ent if isinstance(ent, dict) else {}
    ent_date = _clean_str(ent.get('date'))
    if ent_date is not None and not _date_within_horizon(ent_date, ctx.get('now_local_date'), _js_or(ctx.get('date_horizon_days'), 60)):
        warnings.append('entities_date_out_of_horizon:' + ent_date)
        ent_date = None
    ent_time = _normalize_time(ent.get('time'))
    if ent_time is None and _truthy(ent.get('time')):
        warnings.append('entities_time_invalid')
    age = ent.get('patient_age')
    if age is not None and age != '':
        raw_age = _to_english_digits(_js_string(age))
        # Reviewer fix: "2.5" (sintin w noss) previously concatenated to 25 — a 12x
        # corruption. Decimal ages floor to the whole year instead.
        dec = re.match(r'\s*(\d{1,3})(?:\.(\d+))?\s*$', raw_age)
        if dec:
            n = int(dec.group(1))
        else:
            n = _js_parse_int(re.sub(r'[^0-9]', '', raw_age))
        if _is_finite(n) and 0 <= n <= 130:
            age = n
        else:
            warnings.append('patient_age_out_of_range')
            age = None
    else:
        age = None
    phone = _clean_str(ent.get('patient_phone'))
    if phone is not None:
        normalized = _normalize_phone(phone, _js_or(ctx.get('clinic_country_code'), 'SA'))
        if normalized is not None:
            phone = normalized
        else:
            warnings.append('patient_phone_invalid')
            phone = None
    vt_raw = _js_string(_js_or(ent.get('visit_type'), ent.get('appointment_type'), '')).strip().upper()
    # Reviewer fix: hyphenated spellings ('FOLLOW-UP') normalize like whitespace forms.
    v = _WS_PLUS_RE.sub('_', vt_raw.replace('-', ' '))
    if not v:
        visit_type = None
    elif v in VISIT_TYPES:
        visit_type = v
    else:
        warnings.append('visit_type_repaired:' + v)
        visit_type = None
    appointment_id = _clean_str(ent.get('appointment_id'))
    if appointment_id is not None and UUID_RE.fullmatch(appointment_id):
        out_appointment_id = appointment_id
    elif appointment_id is not None:
        warnings.append('appointment_id_not_uuid')
        out_appointment_id = None
    else:
        out_appointment_id = None
    out['entities'] = {
        'doctor_name': _clean_str(ent.get('doctor_name')),
        'service_name': _clean_str(ent.get('service_name')),
        'date': ent_date,
        'time': ent_time,
        'visit_type': visit_type,
        'patient_name': _clean_str(ent.get('patient_name')),
        'patient_phone': phone,
        'patient_age': age,
        'patient_address': _clean_str(ent.get('patient_address')),
        'appointment_id': out_appointment_id,
        'booking_number': _clean_str(ent.get('booking_number')),
    }

    op = _prop(c, 'operation_proposal')
    op = op if isinstance(op, dict) else {}
    out['operation_proposal'] = {
        'type': _in_enum(op.get('type'), OPERATION_TYPES, ''),
        'requested': op.get('requested') is True,
    }

    out['references_prior_conversation'] = _prop(c, 'references_prior_conversation') is True
    out['escalate'] = _prop(c, 'escalate') is True
    out['handoff_reason'] = _clean_str(_prop(c, 'handoff_reason'))
    return {'valid': len(errors) == 0, 'errors': errors, 'warnings': warnings, 'contract': out}


# ── Public entry point: the Code-node module body ──
def normalize_agent_output(inputs: dict) -> dict:
    """Source node: Normalize Agent Output (Deterministic)
    (extracted/code/Normalize_Agent_Output_Deterministic.js).

    Mirrors the JS node body and returns the inner json dict (n8n's
    ``[{ json: result }][0].json``). See the module docstring for the ``inputs`` schema.
    """
    inputs = _dict(inputs)
    current = _dict(inputs.get('current'))  # JS $json
    now_ms = int(time.time() * 1000)  # snapshot for the JS Date.now() reads
    now_date = _utc_now_date()

    ctx = _dict(_js_or(inputs.get('normalize_validate'), {}))  # safeNode('Normalize & Validate')
    state = _js_or(_dict(_js_or(inputs.get('conversation_state'), {})).get('state_data'), {})
    clinic_row = _dict(_js_or(inputs.get('clinic_context'), {}))
    ownership = _dict(_js_or(inputs.get('patient_ownership'), {}))
    persona_ctx = _dict(_js_or(inputs.get('persona_builder'), {}))

    raw_output = _js_string(_first_not_none(
        current.get('text'), current.get('raw_output'), current.get('output'), current.get('response'), '')).strip()

    # ── Model-call failure detection (transport errors only) ──
    agent_call_failed = bool(
        _truthy(current.get('error'))
        or _truthy(current.get('errorMessage'))
        or _js_number(current.get('statusCode')) >= 400
    )

    validator_ctx = _build_validator_ctx(ctx, clinic_row, ownership, now_date)

    # ── Context + lineage ──
    current_turn_id = _js_string(_js_or(ctx.get('message_id'), ctx.get('source_event_id'), ctx.get('idempotency_key'), '')).strip()
    channel_key_expr = _js_or(ctx.get('channel_key'), _tpl(
        ctx.get('channel_type') if 'channel_type' in ctx else _UNDEFINED, ':',
        ctx.get('channel_id') if 'channel_id' in ctx else _UNDEFINED))
    current_turn_key = '|'.join(_nullish_str(v).strip() for v in (
        ctx.get('clinic_id'), channel_key_expr, ctx.get('conversation_id'), current_turn_id))
    current_message_fingerprint = _stable_hash('|'.join(_nullish_str(v) for v in (
        ctx.get('message_text'), ctx.get('message_id'), ctx.get('source_event_id'), ctx.get('received_at'))))
    current_turn_lineage = {
        'schema_version': 3,
        'turn_id': current_turn_id or None,
        'turn_key': current_turn_key,
        'message_id': _js_or(ctx.get('message_id'), None),
        'source_event_id': _js_or(ctx.get('source_event_id'), None),
        'conversation_id': _js_or(ctx.get('conversation_id'), None),
        'message_fingerprint': current_message_fingerprint,
        'created_at': _js_or(ctx.get('received_at'), _utc_now_iso()),
    }

    # ── Observability-only Arabic hygiene view (never used for decisions) ──
    normalized_message = _normalize_arabic_user_text(_js_trim(_js_string(_js_or(ctx.get('message_text'), ''))))

    # ── Parse + validate (single validator, one repair attempt upstream) ──
    parsed_doc = _extract(raw_output)
    # Deterministic day-word absorption (closed vocabulary → ISO).
    if (_truthy(parsed_doc) and isinstance(parsed_doc, dict)
            and _truthy(parsed_doc.get('entities')) and isinstance(parsed_doc.get('entities'), dict)
            and _truthy(parsed_doc['entities'].get('date'))
            and not ISO_DATE.fullmatch(_js_string(parsed_doc['entities'].get('date')))):
        absorbed_date = _absorb_day_word_to_iso(parsed_doc['entities'].get('date'), validator_ctx['now_local_date'])
        if absorbed_date is not None:
            parsed_doc['entities']['date'] = absorbed_date

    # P42b DETERMINISTIC OFFERED-SLOT RESCUE (hotfixed per regression team):
    # - null-safe on parsedDoc (RG-1) / intent + negation-question gates (RG-2)
    # - day-aware matching (RG-7). Fires ONLY on an exact time/rank match.
    presented = state.get('presented_offer')
    rescue_offer = presented if (_truthy(presented) and isinstance(presented, dict) and isinstance(presented.get('alternatives'), list)) else None
    rescue_expiry = _date_parse_ms(_js_string(_js_or(rescue_offer.get('expires_at'), ''))) if rescue_offer is not None else None
    rescue_live = bool(rescue_offer is not None and rescue_expiry is not None and rescue_expiry > now_ms)
    rescue_doc_ok = _truthy(parsed_doc) and isinstance(parsed_doc, dict)
    pd_turn = parsed_doc.get('turn') if rescue_doc_ok else None
    rescue_intent = _js_string(_js_or(_dict(pd_turn).get('intent'), 'unclear')) if (rescue_doc_ok and _truthy(pd_turn) and isinstance(pd_turn, dict)) else 'unclear'
    pd_confirmation = parsed_doc.get('confirmation') if rescue_doc_ok else None
    rescue_conf = _js_string(_js_or(_dict(pd_confirmation).get('intent'), 'none')) if (rescue_doc_ok and _truthy(pd_confirmation) and isinstance(pd_confirmation, dict)) else 'none'
    pd_selection = parsed_doc.get('selection') if rescue_doc_ok else None
    rescue_sel = pd_selection if (rescue_doc_ok and _truthy(pd_selection) and isinstance(pd_selection, dict)) else None
    _norm_msg = _js_string(_js_or(normalized_message, '')).strip()
    # Reviewer fix: negation anywhere in the message (not only at its head) must
    # block the slot rescue — 'بصراحة مش عايز الساعه 3' was accepted as a pick.
    rescue_negated = (_RESCUE_NEGATED_RE.match(_norm_msg) is not None
                      or _NEG_BEFORE_TIME_RE.search(_norm_msg) is not None
                      or re.search(r'(?:مش|غير|بدون|لا)', _norm_msg) is not None)
    rescue_needed = (
        rescue_live and rescue_doc_ok
        and rescue_intent in ('unclear', 'booking_continuation')
        and rescue_conf not in ('negative', 'question')
        and not rescue_negated
        and (rescue_sel is None or _js_string(_js_or(rescue_sel.get('kind'), 'none')) == 'none')
    )
    if rescue_needed and normalized_message:
        alts_all = rescue_offer.get('alternatives')
        pd_entities = parsed_doc.get('entities')
        want_date = _u16_slice(_js_string(pd_entities.get('date')), 0, 10) if (
            rescue_doc_ok and _truthy(pd_entities) and isinstance(pd_entities, dict) and _truthy(pd_entities.get('date'))) else None
        if want_date is not None:
            date_filtered = [a for a in alts_all if _u16_slice(_js_string(_js_or(
                _js_and(a, _prop(a, 'local_date')), _js_and(a, _prop(a, 'date')), '')), 0, 10) == want_date]
        else:
            date_filtered = []
        # P42b (RG-7): if the patient named a day that none of the offered slots match,
        # do NOT fall back to binding a different day — no rescue.
        alts = date_filtered if len(date_filtered) else ([] if want_date is not None else alts_all)
        m_time = _MTIME_RE.search(normalized_message)
        m_rank = _MRANK_RE.search(normalized_message)
        rescue_alt = None
        if m_time is not None:
            m_time_idx = _cp_to_u16(normalized_message, m_time.start())
            before = _u16_slice(normalized_message, max(0, m_time_idx - 10), m_time_idx)
            neg_before = _NEG_BEFORE_TIME_RE.search(before) is not None
            if not neg_before:
                r_hh = _js_parse_int(m_time.group(1))
                r_mm = _js_string(m_time.group(2)) if m_time.group(2) is not None else '00'
                is_pm = _PM_RE.match(_js_string(_js_or(m_time.group(3), ''))) is not None
                is_am = _AM_RE.match(_js_string(_js_or(m_time.group(3), ''))) is not None
                if is_pm and r_hh < 12:
                    r_hh += 12
                if is_am and r_hh == 12:
                    r_hh = 0
                if 0 <= r_hh <= 23:
                    r_hhmm = _js_string(r_hh).zfill(2) + ':' + r_mm
                    for a in alts:
                        if _u16_slice(_js_string(_js_or(
                                _js_and(a, _prop(a, 'local_time')), _js_and(a, _prop(a, 'time')), '')), 0, 5) == r_hhmm:
                            rescue_alt = a
                            break
        if rescue_alt is None and m_rank is not None:
            r_rank = _js_parse_int(m_rank.group(1))
            if 1 <= r_rank <= len(alts):
                rescue_alt = _js_or(alts[r_rank - 1], None)
        if rescue_alt is not None and (
                _truthy(_prop(rescue_alt, 'slot_id')) or _truthy(_prop(rescue_alt, 'local_date')) or _truthy(_prop(rescue_alt, 'date'))):
            parsed_doc['selection'] = {
                'kind': 'presented_match',
                'rank': None,
                'date': _u16_slice(_js_string(_js_or(
                    _js_and(rescue_alt, _prop(rescue_alt, 'local_date')), _js_and(rescue_alt, _prop(rescue_alt, 'date')), '')), 0, 10) or None,
                'time': _u16_slice(_js_string(_js_or(
                    _js_and(rescue_alt, _prop(rescue_alt, 'local_time')), _js_and(rescue_alt, _prop(rescue_alt, 'time')), '')), 0, 5) or None,
            }
            pd_turn_chk = parsed_doc.get('turn')
            if (rescue_doc_ok and _truthy(pd_turn_chk) and isinstance(pd_turn_chk, dict)
                    and _js_string(_js_or(pd_turn_chk.get('intent'), 'unclear')) == 'unclear'):
                pd_turn_chk['intent'] = 'booking_continuation'

    if agent_call_failed:
        validation = {'valid': False, 'errors': ['MODEL_CALL_FAILED'], 'warnings': [], 'contract': None}
    else:
        validation = _validate_contract(parsed_doc, validator_ctx)
    # JS: Boolean($('Build Repair Prompt (Deterministic)').first().json) inside try/catch —
    # true whenever the repair node executed (even with an empty {} output).
    repair_ran = isinstance(inputs.get('repair_prompt'), dict)
    structural_failure = (not agent_call_failed) and (not validation['valid'])
    repair_needed = structural_failure and not repair_ran
    contract_v3 = validation['contract']
    model_call_status = ('MODEL_CALL_FAILED' if agent_call_failed
                         else ('VALID' if validation['valid'] else ('INVALID_AFTER_REPAIR' if repair_ran else 'INVALID_OR_INCOMPLETE_CONTRACT')))

    # ── Contract-derived facts (never from message text) ──
    turn_v3 = contract_v3.get('turn') if contract_v3 is not None else None
    turn_intent_v3 = _js_string(_js_or(_dict(turn_v3).get('intent'), 'unclear')) if turn_v3 is not None else 'unclear'
    relation_v3 = _js_string(_js_or(_dict(turn_v3).get('relation_to_previous_turn'), 'none')) if turn_v3 is not None else 'none'
    confirmation_intent_v3 = _js_string(_js_or(_dict(_dict(contract_v3).get('confirmation')).get('intent'), 'none')) if contract_v3 is not None else 'none'
    operation_type_v3 = _js_string(_js_or(_dict(_dict(contract_v3).get('operation_proposal')).get('type'), '')) if contract_v3 is not None else ''
    operation_requested = (_dict(_dict(contract_v3).get('operation_proposal')).get('requested') is True) if contract_v3 is not None else False
    references_prior_conversation = (_dict(contract_v3).get('references_prior_conversation') is True) if contract_v3 is not None else False
    escalation_requested = (_dict(contract_v3).get('escalate') is True) if contract_v3 is not None else False
    handoff_reason = (_js_string(_js_or(_dict(contract_v3).get('handoff_reason'), '')).strip() or None) if contract_v3 is not None else None
    cls = _message_class(contract_v3, state) if contract_v3 is not None else 'unclear'
    availability_inquiry = cls == 'availability_inquiry'

    # ── Temporal-claim guard (patient-safety, stage-driven, not text-driven) ──
    pre_agent_stage = _js_or(persona_ctx.get('pre_agent_stage_contract'), None)
    temporal_claim_guard_active = bool(
        isinstance(pre_agent_stage, dict)
        and pre_agent_stage.get('type') == 'confirm_patient_data'
        and pre_agent_stage.get('date_allowed') is False
    )

    def _sanitize_unsupported_temporal_claims(value):
        raw_s = _nullish_str(value)
        if not temporal_claim_guard_active:
            return _js_trim(raw_s)
        s = _TEMPORAL_DAY_RE.sub('', raw_s)
        s = _TEMPORAL_SAA_RE.sub('', s)
        s = _TEMPORAL_HHMM_RE.sub('', s)
        s = _TEMPORAL_DATE_RE.sub('', s)
        s = _WS_2PLUS_RE.sub(' ', s)
        return _js_trim(s)

    extracted_reply = None
    if contract_v3 is not None and _truthy(contract_v3.get('reply')):
        extracted_reply = _sanitize_unsupported_temporal_claims(_js_string(contract_v3.get('reply'))) or None

    # ── Entities for plumbing (IDs resolve later via Resolve Booking IDs) ──
    ent_v3 = contract_v3.get('entities') if contract_v3 is not None else {}
    ent_v3 = ent_v3 if isinstance(ent_v3, dict) else {}
    clean_entities = {
        'doctor_name': ent_v3.get('doctor_name'),
        'doctor_id': None,
        'service_name': ent_v3.get('service_name'),
        'service_id': None,
        'appointment_type': ent_v3.get('visit_type'),
        'date': ent_v3.get('date'),
        'time': ent_v3.get('time'),
        'slot_id': None,
        'branch_id': None,
        'branch_name': None,
        'appointment_id': ent_v3.get('appointment_id'),
        'booking_number': ent_v3.get('booking_number'),
        'patient_name': ent_v3.get('patient_name'),
        'patient_phone': ent_v3.get('patient_phone'),
        'patient_age': ent_v3.get('patient_age'),
        'patient_address': ent_v3.get('patient_address'),
        'references_prior_conversation': references_prior_conversation,
    }
    invalid_date_input = any(str(w).startswith('entities_date_out_of_horizon') for w in validation['warnings'])
    invalid_time_input = any(str(w) == 'entities_time_invalid' for w in validation['warnings'])

    # ── Booking context management (deterministic merge, kept for the legacy bridge) ──
    state_dict = _dict(state)
    prior_activity_ms0 = _date_parse_ms(_js_or(_js_or(
        state_dict.get('last_updated'), state_dict.get('updated_at')), state_dict.get('last_message_at'), ''))
    received_str = _js_string(_js_or(ctx.get('received_at'), ''))
    current_activity_ms0 = _date_parse_ms(received_str) if received_str else _date_parse_ms(_utc_now_iso())
    session_context_stale = bool(
        prior_activity_ms0 is not None and current_activity_ms0 is not None
        and current_activity_ms0 - prior_activity_ms0 >= 2 * 60 * 60 * 1000
    )
    session_context_usable = (not session_context_stale) or references_prior_conversation is True
    prior_slot_raw = state_dict.get('slot_state')
    prior_slot = prior_slot_raw if (session_context_usable and _truthy(prior_slot_raw) and isinstance(prior_slot_raw, dict)) else {}
    prior_context_raw = state_dict.get('booking_context')
    prior_context = prior_context_raw if (session_context_usable and _truthy(prior_context_raw) and isinstance(prior_context_raw, dict)) else {}
    state_facts = state_dict.get('facts')
    state_patient_facts = (_dict(state_facts).get('patient')
                           if (_truthy(state_facts) and isinstance(state_facts, dict)
                               and _truthy(_dict(state_facts).get('patient'))
                               and isinstance(_dict(state_facts).get('patient'), dict)) else {})

    def _keep(v, old):
        """JS keep(v, old): v !== undefined && v !== null && String(v) !== '' ? v : (old ?? null)."""
        if v is not None and _js_string(v) != '':
            return v
        return old

    prior_operation_action = _js_string(_js_or(_js_or(
        state_dict.get('active_operation'), state_dict.get('operation_action')), '')).strip().lower()
    prior_operation_state = _js_string(_js_or(_js_or(
        state_dict.get('operation_state'), state_dict.get('operation_status')), '')).strip().upper()
    prior_draft_expiry_ms = _date_parse_ms(_js_string(_js_or(state_dict.get('draft_expires_at'), '')))
    prior_draft_expired = bool(
        prior_draft_expiry_ms is not None and current_activity_ms0 is not None
        and prior_draft_expiry_ms <= current_activity_ms0
    )
    prior_create_is_live = (
        prior_operation_action == 'create_appointment'
        and prior_operation_state not in ('COMPLETED', 'CANCELLED', 'FAILED_FINAL', 'IDLE')
        and not (prior_draft_expired and prior_operation_state in ('DRAFT', 'COLLECTING_DETAILS', 'COLLECTING_APPOINTMENT_DETAILS', ''))
    )

    fresh_doctor_name_given = _js_string(_js_or(clean_entities.get('doctor_name'), '')).strip() != ''
    fresh_service_name_given = _js_string(_js_or(clean_entities.get('service_name'), '')).strip() != ''

    def _resolved_id(new_value, fresh_given, prior_value):
        """JS: x !== null && x !== undefined ? x : (freshGiven ? null : priorValue)."""
        if new_value is not None:
            return new_value
        return None if fresh_given else prior_value

    slot_state = {
        'branch_id': _keep(clean_entities.get('branch_id'), prior_slot.get('branch_id')),
        'branch_name': _keep(clean_entities.get('branch_name'), prior_slot.get('branch_name')),
        'doctor_id': _resolved_id(clean_entities.get('doctor_id'), fresh_doctor_name_given, prior_slot.get('doctor_id')),
        'doctor_name': _keep(clean_entities.get('doctor_name'), prior_slot.get('doctor_name')),
        'service_id': _resolved_id(clean_entities.get('service_id'), fresh_service_name_given, prior_slot.get('service_id')),
        'service_name': _keep(clean_entities.get('service_name'), prior_slot.get('service_name')),
        'appointment_type': _resolved_id(clean_entities.get('appointment_type'), False, prior_slot.get('appointment_type')),
        'date': _keep(clean_entities.get('date'), prior_slot.get('date')),
        'time': _keep(clean_entities.get('time'), prior_slot.get('time')),
        'slot_id': _keep(clean_entities.get('slot_id'), prior_slot.get('slot_id')),
    }
    prior_age_old = prior_context['patient_age'] if 'patient_age' in prior_context else (
        state_patient_facts['age'] if 'age' in state_patient_facts else None)
    booking_context = {
        'branch_id': _keep(clean_entities.get('branch_id'), prior_context.get('branch_id')),
        'branch_name': _keep(clean_entities.get('branch_name'), prior_context.get('branch_name')),
        'doctor_id': _resolved_id(clean_entities.get('doctor_id'), fresh_doctor_name_given, prior_context.get('doctor_id')),
        'doctor_name': _keep(clean_entities.get('doctor_name'), prior_context.get('doctor_name')),
        'service_id': _resolved_id(clean_entities.get('service_id'), fresh_service_name_given, prior_context.get('service_id')),
        'service_name': _keep(clean_entities.get('service_name'), prior_context.get('service_name')),
        'appointment_type': _resolved_id(clean_entities.get('appointment_type'), False, prior_context.get('appointment_type')),
        'slot_id': _keep(clean_entities.get('slot_id'), prior_context.get('slot_id')),
        'date': _keep(clean_entities.get('date'), prior_context.get('date')),
        'time': _keep(clean_entities.get('time'), prior_context.get('time')),
        'patient_name': _keep(clean_entities.get('patient_name'), _js_or(prior_context.get('patient_name'), state_patient_facts.get('name'))),
        'patient_phone': _keep(clean_entities.get('patient_phone'), _js_or(_js_or(
            prior_context.get('patient_phone'), state_patient_facts.get('phone')), state_patient_facts.get('mobile'))),
        # JS keep(x !== undefined ? x : null, priorContext.patient_age !== undefined ? priorContext.patient_age : (stateFacts.age !== undefined ? stateFacts.age : null))
        'patient_age': _keep(clean_entities.get('patient_age'), prior_age_old),
        'patient_address': _keep(clean_entities.get('patient_address'), _js_or(prior_context.get('patient_address'), state_patient_facts.get('address'))),
        'references_prior_conversation': references_prior_conversation,
    }

    if invalid_date_input or invalid_time_input:
        slot_state['slot_id'] = None
        booking_context['slot_id'] = None

    # ── Error-followup recovery (state-driven; preserves doctor identity after a failed reply) ──
    error_followup_recovered = False
    recovery_raw = persona_ctx.get('error_followup_context')
    builder_recovery = recovery_raw if (_truthy(recovery_raw) and isinstance(recovery_raw, dict)) else {}
    clinic_doctors = clinic_row.get('doctor_directory')
    normalize_clinic_doctors = clinic_doctors if isinstance(clinic_doctors, list) else []
    recovery_id_str = _js_string(_js_or(builder_recovery.get('doctor_id'), ''))
    normalize_recovered_doctor = None
    for doctor in normalize_clinic_doctors:
        if _js_string(_js_or(_js_or(_prop(doctor, 'doctor_id'), _prop(doctor, 'id')), '')) == recovery_id_str:
            normalize_recovered_doctor = doctor
            break
    if (builder_recovery.get('active') is True and _truthy(builder_recovery.get('doctor_name'))
            and not fresh_doctor_name_given):
        error_followup_recovered = True
        clean_entities['doctor_name'] = _js_string(builder_recovery.get('doctor_name')).strip()
        recovered_dict = _dict(normalize_recovered_doctor)
        clean_entities['doctor_id'] = _js_or(builder_recovery.get('doctor_id'), recovered_dict.get('doctor_id'), recovered_dict.get('id'), None)
        slot_state['doctor_name'] = clean_entities['doctor_name']
        slot_state['doctor_id'] = _js_or(clean_entities['doctor_id'], slot_state.get('doctor_id'), None)
        booking_context['doctor_name'] = clean_entities['doctor_name']
        booking_context['doctor_id'] = _js_or(clean_entities['doctor_id'], booking_context.get('doctor_id'), None)
        if _truthy(builder_recovery.get('appointment_type')) and not _truthy(booking_context.get('appointment_type')):
            clean_entities['appointment_type'] = builder_recovery.get('appointment_type')
            slot_state['appointment_type'] = builder_recovery.get('appointment_type')
            booking_context['appointment_type'] = builder_recovery.get('appointment_type')

    # ── Fresh booking restart (contract-driven R1: intent + relation + live artifacts only) ──
    presented_raw = state_dict.get('presented_offer')
    pending_raw = state_dict.get('pending_offer')
    prior_offer_raw = presented_raw if (_truthy(presented_raw) and isinstance(presented_raw, dict)) else (
        pending_raw if (_truthy(pending_raw) and isinstance(pending_raw, dict)) else None)
    prior_offer_live = False
    if prior_offer_raw is not None:
        exp = _date_parse_ms(_js_string(_js_or(prior_offer_raw.get('expires_at'), '')))
        prior_offer_live = bool(exp is not None and current_activity_ms0 is not None and exp > current_activity_ms0)
    target_raw = state_dict.get('confirmation_target')
    prior_target_raw = target_raw if (_truthy(target_raw) and isinstance(target_raw, dict)) else None
    prior_target_live = False
    if prior_target_raw is not None and prior_target_raw.get('invalidated') is not True:
        status = _js_string(_js_or(_js_or(
            prior_target_raw.get('confirmation_delivery_status'), prior_target_raw.get('delivery')), 'pending')).lower()
        if status in ('pending', 'sent', 'proposed'):
            exp = _date_parse_ms(_js_string(_js_or(prior_target_raw.get('expires_at'), '')))
            prior_target_live = exp is None or (current_activity_ms0 is not None and exp > current_activity_ms0)
    new_booking_restart = (
        turn_intent_v3 == 'booking_request'
        and relation_v3 == 'new_request'
        and not references_prior_conversation
        and not prior_offer_live
        and not prior_target_live
    )

    if new_booking_restart:
        for field in ('date', 'time', 'slot_id'):
            val = _js_or(clean_entities.get(field), None)
            slot_state[field] = val
            booking_context[field] = val
        for field in ('doctor_id', 'doctor_name', 'service_id', 'service_name', 'appointment_type', 'date', 'time', 'slot_id'):
            current_value = _js_or(clean_entities.get(field), None)
            slot_state[field] = current_value
            booking_context[field] = current_value

    # ── Single-doctor lock (runs AFTER the restart wipe) ──
    clinic_doctor_count = _js_number(_js_or(clinic_row.get('doctor_count'), 0))
    single_doctor_id = _js_string(_js_or(clinic_row.get('single_doctor_id'), '')).strip() or None
    single_doctor_name = _js_string(_js_or(clinic_row.get('single_doctor_name'), '')).strip() or None
    doctor_selection_turn = turn_intent_v3 in ('booking_request', 'booking_continuation', 'availability_inquiry')
    explicit_doctor_in_turn = _truthy(clean_entities.get('doctor_id')) or _truthy(clean_entities.get('doctor_name'))
    doctor_context_already_set = _truthy(booking_context.get('doctor_id')) or _truthy(booking_context.get('doctor_name'))
    can_auto_select_single_doctor = (
        clinic_doctor_count == 1
        and bool(single_doctor_id) and bool(single_doctor_name)
        and doctor_selection_turn
        and not explicit_doctor_in_turn
        and not doctor_context_already_set
    )
    if can_auto_select_single_doctor:
        clean_entities['doctor_id'] = single_doctor_id
        clean_entities['doctor_name'] = single_doctor_name
        slot_state['doctor_id'] = single_doctor_id
        slot_state['doctor_name'] = single_doctor_name
        booking_context['doctor_id'] = single_doctor_id
        booking_context['doctor_name'] = single_doctor_name

    # ── Canonical carry for continuation turns (state-driven) ──
    canonical_entities = dict(clean_entities)
    state_activity_ms = _date_parse_ms(_js_or(_js_or(
        state_dict.get('last_updated'), state_dict.get('updated_at')), state_dict.get('last_message_at'), ''))
    current_activity_ms = _date_parse_ms(_js_string(_js_or(ctx.get('received_at'), '')))
    if not current_activity_ms:
        current_activity_ms = now_ms
    state_conversation_fresh = (
        (current_activity_ms - state_activity_ms < 2 * 60 * 60 * 1000)
        if state_activity_ms is not None else bool(prior_create_is_live)
    )
    context_carry_allowed = (
        not new_booking_restart
        and state_conversation_fresh
        and (confirmation_intent_v3 == 'affirmative'
             or turn_intent_v3 in ('small_talk', 'greeting', 'booking_continuation', 'confirmation', 'correction', 'availability_inquiry')
             or operation_requested is False
             or relation_v3 in ('answer', 'confirmation', 'follow_up'))
    )
    if context_carry_allowed:
        for field in ('doctor_id', 'doctor_name', 'service_id', 'service_name', 'appointment_type', 'date', 'time', 'slot_id', 'branch_id', 'branch_name'):
            cv = canonical_entities.get(field)
            if (cv is None or cv == '') and _truthy(booking_context.get(field)):
                canonical_entities[field] = booking_context.get(field)

    # ── Legacy v2 bridge projection (deterministic; consumed until Phase 3 replaces it) ──
    v2_intent = INTENT_TO_V2.get(turn_intent_v3) or 'unclear'
    v2_relation = RELATION_TO_V2.get(relation_v3) or 'none'
    v2_certainty_key = _js_string(_js_or(_dict(turn_v3).get('certainty'), '')) if turn_v3 is not None else ''
    v2_certainty = CERTAINTY_TO_V2.get(v2_certainty_key) or 'uncertain'
    v2_routing = 'escalation' if escalation_requested else (ROUTING_BY_CLASS.get(cls) or 'none')
    confidence_raw = _js_number(_dict(turn_v3).get('confidence') if turn_v3 is not None else float('nan'))
    normalized_confidence = max(0, min(1, confidence_raw)) if _is_finite(confidence_raw) else None

    persona_clinic_query_type = _js_string(_js_or(persona_ctx.get('clinic_query_type'), ''))
    if cls == 'clinic_query':
        projected_query = {
            'type': 'service_price' if persona_clinic_query_type == 'service_price' else (
                'doctor_service' if persona_clinic_query_type in ('doctor_catalog', 'doctor_fact') else 'faq'),
        }
    elif cls == 'availability_inquiry':
        projected_query = {
            'type': 'availability',
            'date': _js_or(canonical_entities.get('date'), None),
            'time': _js_or(canonical_entities.get('time'), None),
        }
    else:
        projected_query = None
    query_scope = {
        'type': _dig(projected_query, 'type'),
        'date': _dig(projected_query, 'date'),
        'time': _dig(projected_query, 'time'),
        'resolution_status': 'resolved' if (cls == 'availability_inquiry' and _truthy(canonical_entities.get('date'))) else 'unresolved',
        'requires_resolution': False,
    }

    if contract_v3 is None:
        projected_next_step = {'type': 'none', 'field': None}
    elif cls == 'confirmation_affirm':
        projected_next_step = {'type': 'confirm_action', 'field': None}
    elif cls == 'availability_inquiry':
        projected_next_step = {'type': 'show_availability', 'field': None if _truthy(canonical_entities.get('date')) else 'date'}
    elif cls == 'clinic_query':
        projected_next_step = {'type': 'provide_answer', 'field': None}
    elif cls == 'small_talk':
        projected_next_step = {'type': 'none', 'field': None}
    elif cls in ('cancel_request', 'reschedule_request'):
        if _truthy(canonical_entities.get('appointment_id')) or _truthy(canonical_entities.get('booking_number')):
            projected_next_step = {'type': 'confirm_action', 'field': None}
        else:
            projected_next_step = {'type': 'ask_for_missing_information', 'field': 'booking_number'}
    elif cls in ('booking', 'selection_presented'):
        if not _truthy(booking_context.get('appointment_type')):
            projected_next_step = {'type': 'ask_for_missing_information', 'field': 'appointment_type'}
        else:
            projected_next_step = None
            for field in ('patient_name', 'patient_age', 'patient_phone', 'patient_address'):
                value = booking_context.get(field)
                if value is None or _js_string(value).strip() == '':
                    projected_next_step = {'type': 'ask_for_missing_information', 'field': field}
                    break
            if projected_next_step is None:
                # JS checks !bookingContext.date but BOTH branches return the same
                # { type: 'show_availability', field: null } — quirk preserved.
                if not _truthy(booking_context.get('date')):
                    projected_next_step = {'type': 'show_availability', 'field': None}
                else:
                    projected_next_step = {'type': 'show_availability', 'field': None}
    else:
        projected_next_step = {'type': 'none', 'field': None}

    fallback_v3 = {
        'schema_version': 'k2.dialogue.v3',
        'phase': 'understand',
        'reply': None,
        'turn': {'intent': 'unclear', 'relation_to_previous_turn': 'none', 'certainty': 'uncertain', 'confidence': None},
        'confirmation': {'intent': 'none'},
        'selection': {'kind': 'none', 'rank': None, 'date': None, 'time': None},
        'entities': {'doctor_name': None, 'service_name': None, 'date': None, 'time': None, 'visit_type': None, 'patient_name': None, 'patient_phone': None, 'patient_age': None, 'patient_address': None, 'appointment_id': None, 'booking_number': None},
        'operation_proposal': {'type': '', 'requested': False},
        'references_prior_conversation': False,
        'escalate': False,
        'handoff_reason': None,
    }

    output_contract = {
        'schema_version': 'k2.dialogue.v2',
        'turn': {'intent': v2_intent, 'relation_to_previous_turn': v2_relation, 'certainty': v2_certainty,
                 'confidence': normalized_confidence, 'answer_to': None},
        'operation_proposal': {'type': operation_type_v3, 'requested': operation_requested},
        'routing': {'target': v2_routing},
        'entities': canonical_entities,
        'query': projected_query,
        'confirmation': {'intent': confirmation_intent_v3, 'target_operation': _js_or(operation_type_v3, None)},
        'next_step': projected_next_step,
    }

    effective_model_call_status = 'ERROR_FOLLOWUP_RECOVERED' if error_followup_recovered else model_call_status

    out = {'temporal_claim_guard_active': temporal_claim_guard_active}
    out.update(current)  # JS ...$json spread
    out['agent_reply'] = extracted_reply
    out['agent_raw_output'] = raw_output
    out['contract'] = output_contract
    out['contract_v3'] = _js_or(contract_v3, fallback_v3)
    out['slot_state'] = slot_state
    out['booking_context'] = booking_context
    out['new_booking_restart'] = new_booking_restart
    out['availability_inquiry'] = availability_inquiry
    out['model_call_failed'] = agent_call_failed
    out['model_call_status'] = effective_model_call_status
    out['error_followup_recovered'] = error_followup_recovered
    out['escalation_requested'] = escalation_requested
    out['handoff_reason'] = handoff_reason
    out['_contract_status'] = ('MODEL_CALL_FAILED' if agent_call_failed else (
        'VALID' if validation['valid'] else ('INVALID_AFTER_REPAIR' if repair_ran else 'REPAIR_NEEDED')))
    out['_contract_errors'] = validation['errors']
    out['_contract_warnings'] = validation['warnings']
    out['_contract_repair_needed'] = repair_needed
    out['_repair_source'] = 'self_repair' if repair_ran else 'primary'
    out['_normalization'] = {
        'schema_version': 'k2.dialogue.v3',
        'valid': validation['valid'],
        'errors': validation['errors'],
        'warnings': validation['warnings'],
        'certainty': v2_certainty,
        'confidence': normalized_confidence,
        'turn_intent': v2_intent,
        'relation_to_previous_turn': v2_relation,
        'confirmation_intent': confirmation_intent_v3,
        'operation_type': operation_type_v3,
        'operation_requested': operation_requested,
        'routing_target': v2_routing,
        'next_step_type': _dig(projected_next_step, 'type'),
        'next_step_field': _dig(projected_next_step, 'field'),
        'message_class': cls,
        'model_turn_intent': turn_intent_v3,
        'model_relation': relation_v3,
        'invalid_temporal_input': invalid_date_input or invalid_time_input,
        'invalid_temporal_fields': [f for f in ('date' if invalid_date_input else None, 'time' if invalid_time_input else None) if f],
        'new_booking_restart': new_booking_restart,
        'escalation': escalation_requested,
        'booking_signal': turn_intent_v3 in ('booking_request', 'booking_continuation'),
        'query_is_faq': _dig(projected_query, 'type') == 'faq',
        'query_is_price': _dig(projected_query, 'type') == 'service_price',
        'query_is_availability': availability_inquiry,
        'query_is_doctor_service': _dig(projected_query, 'type') == 'doctor_service',
        'query_scope': query_scope,
        'turn_lineage': current_turn_lineage,
        'normalized_language_views': {'arabic': normalized_message},
        'model_call_failed': agent_call_failed,
        'model_call_status': effective_model_call_status,
        'repair_attempted': repair_ran,
        'repair_needed': repair_needed,
    }
    return out
