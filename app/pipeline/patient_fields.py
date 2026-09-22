"""P1.7 Patient Field Normalization — faithful 1:1 port.

Source node: P1.7 Patient Field Normalization
(extracted/code/P1_7_Patient_Field_Normalization.js)

Extracts patient contact facts from the patient's own words only — never from an
agent-output blob: the canonical message text comes from the Normalize & Validate
item, with a "looks plain" fallback to the item's own ``message_text``. Ports the
node's own 21-country E.164 phone rule table, phone/age/address extractors
(including the JS ASCII-only ``\\b`` semantics, reproduced with explicit
``[A-Za-z0-9_]`` lookarounds), and the ``p17_extraction_meta`` block. The old
``app/utils/phone.py`` is deliberately NOT imported — its logic is not this node's.

Public API: ``normalize_patient_fields(item: dict) -> dict`` — returns the inner
json dict the JS emits for that item (``{ json: result }``).

Required ``item`` schema
------------------------
``item`` is the current item json (JS ``item.json``) plus ONE reserved node-input
key the pipeline runner attaches:

- ``normalize_validate`` ← ``$('Normalize & Validate').first().json`` —
  ``message_text`` (canonical patient words) and ``clinic_country_code`` (default
  country fallback). Absent key behaves like the JS try/catch (yields ``{}``).

The JS result copies every key of the item data (``for (const k in data)``);
the reserved ``normalize_validate`` key is runner plumbing, not item data, so it
is consumed as context and excluded from the copied result — the emitted keys
stay exactly the JS ones.

Pure function: no I/O, no logging, stdlib only.
"""

import re
from app.core.js_semantics import dict_or_empty as _dict, js_len as _js_len, js_or as _js_or, truthy as _truthy, u16_index_of as _u16_index_of


# ── JS-semantics shims (same semantics as the ones in app/core/orchestrator.py) ──

_JS_WS_INNER = r'\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff'
_JS_S = '[' + _JS_WS_INNER + ']'  # JS \s character class (Python \s differs on \ufeff / \x1c-\x1f \x85)


def _js_string(value):
    """JS String() coercion: arrays join with ','; plain objects → '[object Object]'."""
    if value is None:
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


def _js_trim(s):
    """JS String.prototype.trim() — the JS WhiteSpace + LineTerminator set (differs from Python strip())."""
    return s.strip('\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff')


def _u16_slice(s, start, end=None):
    """JS String.prototype.slice(start, end) / substring(start, end) — UTF-16 code-unit indexes.

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


_INT_HEAD_RE = re.compile('[' + _JS_WS_INNER + r']*([+-]?[0-9]+)')


def _js_parse_int(value):
    """JS parseInt(x, 10): leading JS whitespace + optional sign + ASCII digits; NaN when none."""
    m = _INT_HEAD_RE.match(_js_string(value))
    if not m:
        return float('nan')
    return int(m.group(1))


# ── E.164 phone normalization (the node's own 21-country rule table, verbatim) ──
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
PHONE_KEYWORDS = re.compile(r'(?:هاتف|تلفون|جوال|موبايل|واتس|whatsapp|phone|tel|mobile|cell|call)', re.IGNORECASE)


def _to_english_digits(s):
    """JS toEnglishDigits — Arabic-Indic digits (U+0660–U+0669) → ASCII digits."""
    return ''.join(ARABIC_INDIC.get(ch, ch) for ch in _js_string(s))


def _is_mostly_digits(s):
    """JS isMostlyDigits."""
    if not _truthy(s):
        return False
    no_space = re.sub('[' + _JS_WS_INNER + r'\-./,:]', '', _js_string(s))
    if len(no_space) == 0:
        return False
    digits = len(re.findall(r'[0-9]', no_space))
    return digits >= 5 and digits / len(no_space) > 0.6


def _normalize_phone(raw_input, default_country=None):
    """JS normalizePhone (P1.7 variant)."""
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
        if len(s) in rule['lengths']:
            starts_with_valid_prefix = any(s.startswith(p) for p in rule['prefixes'])
            if starts_with_valid_prefix:
                return rule['code'] + s
    if s.startswith('0'):
        s = s[1:]
    default_rule = PHONE_RULES.get(default_country)
    if default_rule:
        return default_rule['code'] + s
    return '+' + s


_E164_RE = re.compile(r'\+[0-9]{7,15}')
_LOCAL_PHONE_RE = re.compile(r'[0-9](?:[0-9' + _JS_WS_INNER + r'\-.]{7,18})[0-9]')
_PHONE_SEP_RE = re.compile('[' + _JS_WS_INNER + r'\-.]')


def _extract_phone_from_text(text):
    """JS extractPhoneFromText — E.164 first, then keyword-context-scored local numbers."""
    if not _truthy(text):
        return None
    s = _to_english_digits(_js_string(text))
    e164_match = _E164_RE.search(s)
    if e164_match:
        return _normalize_phone(e164_match.group(0))
    local_matches = _LOCAL_PHONE_RE.findall(s)
    if local_matches:
        best = None
        for lm in local_matches:
            m = _PHONE_SEP_RE.sub('', lm)
            if len(m) < 9 or len(m) > 15:
                continue
            pos = _u16_index_of(s, lm)
            idx = max(0, pos - 20)
            context_before = _u16_slice(s, idx, pos)
            has_phone_context = PHONE_KEYWORDS.search(context_before) is not None
            normalized = _normalize_phone(m)
            if normalized is not None:
                if best is None or (has_phone_context and not best['has_phone_context']) or len(m) > best['length']:
                    best = {'normalized': normalized, 'has_phone_context': has_phone_context, 'length': len(m)}
        return best['normalized'] if best is not None else None
    return None


# JS \b is ASCII-only (\w = [A-Za-z0-9_]); reproduced with explicit lookarounds.
_AGE_PATTERNS = (
    re.compile(r"(?:i\s*am|i'm|عمري|عمرها|عمره|عمر)" + _JS_S + r'*([0-9]{1,3})', re.IGNORECASE),
    re.compile(r'([0-9]{1,3})' + _JS_S + r'*(?:years?' + _JS_S + r'*old|سنة|سنوات|عام|اعوام|أعوام)', re.IGNORECASE),
    re.compile(r'(?:(?<![A-Za-z0-9_])age|(?<=[A-Za-z0-9_])العمر)' + _JS_S + r'*[:=]?' + _JS_S + r'*([0-9]{1,3})(?![A-Za-z0-9_])', re.IGNORECASE),
)


def _extract_age(raw_input):
    """JS extractAge — first matching pattern wins; age must be 0..130."""
    if raw_input is None:
        return None
    s = _to_english_digits(_js_string(raw_input))
    for pattern in _AGE_PATTERNS:
        m = pattern.search(s)
        if m:
            n = _js_parse_int(m.group(1))
            if 0 <= n <= 130:
                return n
    return None


_ENGLISH_ADDRESS_KEYWORDS = re.compile(
    r'(?<![A-Za-z0-9_])(?:street|st|avenue|ave|road|rd|boulevard|blvd|drive|dr|lane|ln|court|ct'
    r'|building|bldg|floor|fl|apartment|apt|suite|ste|house|home|address)(?![A-Za-z0-9_])',
    re.IGNORECASE,
)
# Arabic keywords are word-delimited: حي matches only when followed by a space
# ('حي الروضة'), never inside 'حياك' / 'يحيك'.
_ARABIC_ADDRESS_KEYWORDS = re.compile(
    # Reviewer fix: bare دور/رقم matched substrings ('الدورة الشهرية', 'رقم حجزي...')
    # and the complaint text was saved as the patient's home address. دور/شقة/طابق now
    # require the following space, and bare رقم requires digits after it ('رقم 12').
    r'(?:شارع|طريق|مبنى|بناية|عمارة|طابق|دور|شقة|حي' + _JS_S + r'+|منطقة|مدينة|عنوان|بجوار|بالقرب|ميدان|محلة|رقم' + _JS_S + r'+[0-9])'
)
_POSTAL_RE = re.compile(r'(?<![A-Za-z0-9_])[0-9]{4,6}(?![A-Za-z0-9_])')
_NEWLINE_TAB_RE = re.compile(r'[\r\n\t]')


def _extract_address(raw_input):
    """JS extractAddress — plain single-line address heuristics, in the JS check order."""
    if raw_input is None:
        return None
    s = _js_trim(_to_english_digits(_js_string(raw_input)))
    if _js_len(s) < 5:
        return None
    if _js_len(s) > 300:
        return None
    if '{' in s or '[' in s:
        return None
    if '\\' in s:
        return None
    if _is_mostly_digits(s):
        return None
    if 'schema_version' in s or 'k2.dialogue' in s or 'phase' in s or 'reply' in s:
        return None
    if _NEWLINE_TAB_RE.search(s):
        return None
    has_postal = _POSTAL_RE.search(s) is not None
    english_match = _ENGLISH_ADDRESS_KEYWORDS.search(s) is not None
    arabic_match = _ARABIC_ADDRESS_KEYWORDS.search(s) is not None
    if has_postal or english_match or arabic_match:
        return s
    return None


# ── Public entry point: the Code-node module body (per item) ──
def normalize_patient_fields(item: dict) -> dict:
    """Source node: P1.7 Patient Field Normalization
    (extracted/code/P1_7_Patient_Field_Normalization.js).

    Mirrors the JS per-item body and returns the inner json dict (n8n's
    ``{ json: result }``). See the module docstring for the ``item`` schema and
    the reserved ``normalize_validate`` key.
    """
    data = _dict(item)
    canonical_inbound = _dict(_js_or(data.get('normalize_validate'), {}))
    # Patient data comes from the PATIENT words only — never from an agent-output blob.
    canonical_user_text = _js_trim(_js_string(_js_or(canonical_inbound.get('message_text'), '')))
    text = canonical_user_text
    if not text:
        candidate = _js_trim(_js_string(_js_or(data.get('message_text'), '')))
        looks_plain = (
            _js_len(candidate) > 0
            and _js_len(candidate) <= 500
            and '{' not in candidate
            and 'schema_version' not in candidate
            and 'k2.dialogue' not in candidate
            and re.search('[A-Za-z\u0600-\u06FF]', candidate) is not None
        )
        text = candidate if looks_plain else ''
    clinic_country = _js_or(data.get('clinic_country_code'), canonical_inbound.get('clinic_country_code'), 'SA')
    extracted_phone = _extract_phone_from_text(text)
    extracted_age = _extract_age(text)
    extracted_address = _extract_address(text)
    # JS: for (const k in data) result[k] = data[k] — the reserved node-input key is
    # runner plumbing (not JS item data) and is excluded to keep output keys identical.
    result = {k: v for k, v in data.items() if k != 'normalize_validate'}
    if extracted_phone is not None:
        result['p17_extracted_phone'] = extracted_phone
    if extracted_age is not None:
        result['p17_extracted_age'] = extracted_age
    if extracted_address is not None:
        result['p17_extracted_address'] = extracted_address
    result['p17_extraction_meta'] = {
        'phone_found': extracted_phone is not None,
        'age_found': extracted_age is not None,
        'address_found': extracted_address is not None,
        'clinic_country': clinic_country,
        'text_length': _js_len(text),
    }
    return result
