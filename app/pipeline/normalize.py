"""Normalize & Validate + Handle Normalize Error (Deterministic) — faithful 1:1 ports.

Source nodes: Normalize & Validate (extracted/code/Normalize_Validate.js)
              Handle Normalize Error (Deterministic) (extracted/code/Handle_Normalize_Error_Deterministic.js)

Two node bodies live here:

- ``normalize_and_validate`` — the inbound webhook body becomes the canonical
  pipeline item: clinic/channel/patient/conversation ids, the idempotency key
  (``channel_type:channel_id:message_id``), smart 8000-char message truncation at
  a sentence/word boundary, the deterministic P1.2/P0.3 reference-time context
  (source timestamp preferred over runtime now), language detection, and the
  ``metadata`` echo block. A payload missing any required field — including the
  mandatory ``source_event_id`` (never manufacture a random id) — returns the
  ``INVALID_INBOUND_PAYLOAD`` envelope with the exact Arabic error message.
- ``handle_normalize_error`` — re-checks the (possibly error-tagged) item after
  the flow, recomputing the missing-field list from the raw body aliases and
  stamping ``normalization_error*`` fields.

Pure functions: no I/O, no logging, stdlib only.
"""

import calendar
import re
import time
import unicodedata
from datetime import datetime, timezone
from app.core.js_semantics import cp_to_u16 as _cp_to_u16, is_finite as _is_finite, iso_from_ms as _iso_from_ms, js_len as _js_len, js_or as _js_or, truthy as _truthy


# ── JS-semantics shims (same semantics as the ones in app/core/orchestrator.py) ──

_JS_WS_INNER = r'\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff'
_JS_S = '[' + _JS_WS_INNER + ']'  # JS \s character class (Python \s differs on \ufeff / \x1c-\x1f \x85)
_WS_PLUS_RE = re.compile(_JS_S + '+')


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


def _u16_last_index_of(s, sub):
    """JS String.prototype.lastIndexOf — UTF-16 code-unit index, -1 when absent."""
    pos = s.rfind(sub)
    return -1 if pos < 0 else _cp_to_u16(s, pos)


def _js_number(value):
    """JS Number() coercion: bool→1/0, numeric strings parsed, ''→0, None (undefined/null)→NaN."""
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    if value is None:
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


def _date_parse_ms(value):
    """JS Date.parse() for the ISO-8601 shapes this pipeline produces; None when invalid.

    Date-only strings are UTC (like JS); naive date-times are treated as UTC
    deterministically (JS would use the machine's local zone — webhook timestamps in
    this pipeline are UTC 'Z' strings or epoch numbers, so this path is not hit).

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


# ── Node 1: Normalize & Validate ──
def normalize_and_validate(payload: dict, headers: dict) -> dict:
    """Source node: Normalize & Validate (extracted/code/Normalize_Validate.js).

    Mirrors the JS node body and returns the inner json dict (n8n's
    ``[{ json: out }][0].json``).

    ``payload`` is the RAW webhook body dict — the JS ``$input.item.json.body ??
    $input.item.json`` unwrap is resolved by the runner, which passes the body
    directly (when the trigger item carries no body envelope, the item itself is
    the body). ``headers`` is the lowercased webhook headers dict; the JS node
    never reads it — accepted to mirror the runner contract.
    """
    del headers  # the JS node reads no header fields; kept for the runner's call signature
    now_ms = int(time.time() * 1000)  # JS Date.now() / new Date()
    raw = payload
    body = raw if (_truthy(raw) and isinstance(raw, dict)) else {}

    def _pick(*values):
        """JS pick(...): first value that is not undefined/null and whose String(v).trim() is non-empty."""
        for v in values:
            if v is not None and _js_trim(_js_string(v)) != '':
                return v
        return None

    def _clean(t):
        """JS clean(t): String(t ?? '').trim().replace(/\\s+/g, ' ').normalize('NFC')."""
        s = _js_trim(_js_string('' if t is None else t))
        s = _WS_PLUS_RE.sub(' ', s)
        return unicodedata.normalize('NFC', s)

    clinic_id = _clean(_pick(_prop(body, 'clinic_id'), _prop(body, 'clinicId')))
    channel_type = _WS_PLUS_RE.sub('_', _clean(_pick(
        _prop(body, 'channel_type'), _prop(body, 'channel'), _prop(body, 'platform'), _prop(body, 'source'))).lower())
    channel_id = _clean(_pick(
        _prop(body, 'channel_id'), _prop(body, 'channelId'), _prop(body, 'chat_id'),
        _prop(body, 'thread_id'), _prop(body, 'sender_id'), _prop(body, 'user_id'), _prop(body, 'from_id')))
    patient_id = _clean(_pick(_prop(body, 'patient_id'), _prop(body, 'patientId')))
    conversation_id = _clean(_pick(
        _prop(body, 'conversation_id'), _prop(body, 'conversationId'), _prop(body, 'thread_id'), _prop(body, 'chat_id')))
    chat_id_raw = _pick(
        _dig(body, 'metadata', 'telegram_chat_id'), _dig(body, 'metadata', 'whatsapp_chat_id'),
        _dig(body, 'metadata', 'chat_id'), _prop(body, 'chat_id'))
    chat_id = '' if chat_id_raw is None else _clean(chat_id_raw)
    raw_message_text = _clean(_pick(
        _prop(body, 'message_text'), _prop(body, 'messageText'), _prop(body, 'text'),
        _dig(body, 'message', 'text'), _dig(body, 'message', 'body'), _prop(body, 'content')))

    # P1.7: Increased MAX_MESSAGE_CHARS to 8000 to handle long patient descriptions / complaints.
    # Smart truncation at sentence/word boundary preserves readability when over limit.
    max_message_chars = 8000
    message_too_long = _js_len(raw_message_text) > max_message_chars
    text = raw_message_text
    if message_too_long:
        text_slice = _u16_slice(raw_message_text, 0, max_message_chars)
        # Try sentence boundary (Arabic + English + Urdu) to keep meaning intact.
        best_boundary = -1
        for marker in ('.\n', '.\r\n', '؟\n', '۔ ', '. ', '! ', '? ', '؟ ', '\n\n'):
            idx = _u16_last_index_of(text_slice, marker)
            if idx > max_message_chars * 0.7:
                best_boundary = max(best_boundary, idx + _js_len(marker) - 1)
        if best_boundary > 0:
            text = _js_trim(_u16_slice(text_slice, 0, best_boundary + 1))
        else:
            # Fall back to word/line boundary.
            last_space = max(_u16_last_index_of(text_slice, ' '), _u16_last_index_of(text_slice, '\n'))
            if last_space > max_message_chars * 0.85:
                text = _js_trim(_u16_slice(text_slice, 0, last_space))
            else:
                text = text_slice

    source_event_id = _clean(_pick(
        _prop(body, 'source_event_id'), _prop(body, 'sourceEventId'), _prop(body, 'wamid'),
        _prop(body, 'message_id'), _prop(body, 'messageId'), _prop(body, 'event_id'), _prop(body, 'eventId'),
        _prop(body, 'update_id'), _prop(body, 'updateId')))
    # Reviewer fix: bare 'id' removed from the alias chain — a flattened provider
    # body whose only id is a constant WABA/entry id made every message share one
    # idempotency key, silently suppressing all follow-up messages as duplicates.
    source_timestamp = _pick(
        _prop(body, 'received_at'), _prop(body, 'receivedAt'), _prop(body, 'timestamp'),
        _prop(body, 'created_at'), _prop(body, 'createdAt'), _prop(body, 'sent_at'))
    run_id = _clean(_pick(_prop(body, 'run_id'), _prop(body, 'runId')))
    test_id = _clean(_pick(_prop(body, 'test_id'), _prop(body, 'testId')))
    operation_id = _clean(_pick(_prop(body, 'operation_id'), _prop(body, 'operationId'), _dig(body, 'metadata', 'operation_id')))
    md_k2 = _prop(_prop(body, 'metadata'), 'k2_deferred_replay')
    deferred_replay = (md_k2 is True) or (md_k2 == 'true') or (_prop(body, 'k2_deferred_replay') is True)

    # P1.2/P0.3: deterministic reference time. Prefer the inbound event timestamp;
    # fall back to runtime time only when the source timestamp is absent/invalid.
    # The clinic timezone is intentionally unresolved here: Get Clinic Context is authoritative.
    def _parse_source_timestamp(value):
        if value is None or _js_string(value).strip() == '':
            return None
        s = _js_string(value).strip()
        numeric = _js_number(s)
        if _is_finite(numeric):
            ms = numeric * 1000 if _js_len(s) == 10 else numeric
        else:
            parsed = _date_parse_ms(s)
            ms = float('nan') if parsed is None else parsed
        # JS: Number.isFinite(ms) && !Number.isNaN(new Date(ms).getTime())
        return ms if (_is_finite(ms) and abs(ms) <= 8.64e15) else None

    source_timestamp_ms = _parse_source_timestamp(source_timestamp)
    reference_now_ms = source_timestamp_ms if source_timestamp_ms is not None else now_ms
    reference_now_iso = _iso_from_ms(reference_now_ms)
    time_context = {
        'schema_version': 2,
        'timezone': None,
        'utc_offset': None,
        'timezone_source': 'pending_clinic_configuration',
        'source': 'runtime_now' if source_timestamp_ms is None else 'received_at',
        'source_timestamp': None if source_timestamp_ms is None else reference_now_iso,
        'now_iso': reference_now_iso,
        'now_local_date': None,
        'now_local_time': None,
        'now_local_weekday': None,
    }
    idempotency_degraded = not source_event_id
    # A source event id is mandatory. Never manufacture a random id because retries
    # without a stable provider id must be rejected instead of processed twice.
    message_id = source_event_id if source_event_id else None

    missing = []
    for key, value in (
        ('clinic_id', clinic_id),
        ('channel_type', channel_type),
        ('channel_id', channel_id),
        ('patient_id', patient_id),
        ('conversation_id', conversation_id),
        ('message_text', text),
    ):
        if not value:
            missing.append(key)
    normalization_missing = missing + ([] if source_event_id else ['source_event_id'])
    if normalization_missing:
        return {
            'clinic_id': clinic_id or None,
            'channel_type': channel_type or None,
            'channel_id': channel_id or None,
            'patient_id': patient_id or None,
            'conversation_id': conversation_id or None,
            'message_text': text or None,
            'message_id': message_id,
            'source_event_id': source_event_id or None,
            'idempotency_degraded': idempotency_degraded,
            'deferred_replay': deferred_replay,
            'normalization_error': True,
            'normalization_error_code': 'INVALID_INBOUND_PAYLOAD',
            'normalization_missing_fields': normalization_missing,
            'normalization_error_message': 'تعذر معالجة الطلب بسبب نقص معرف الرسالة الثابت أو بيانات الرسالة الأساسية',
        }

    channel_key = f'{channel_type}:{channel_id}'
    idempotency_key = f'{channel_key}:{message_id}'
    correlation_id = _clean(_pick(
        _prop(body, 'correlation_id'), _prop(body, 'correlationId'), _dig(body, 'metadata', 'correlation_id'), idempotency_key))
    has_arabic = re.search('[\u0600-\u06FF]', text) is not None
    has_english = re.search('[a-zA-Z]', text) is not None
    language = 'mixed' if (has_arabic and has_english) else ('arabic' if has_arabic else ('english' if has_english else 'unknown'))
    return {
        'clinic_id': clinic_id,
        'channel_type': channel_type,
        'channel_id': channel_id,
        'channel_key': channel_key,
        'patient_id': patient_id,
        'conversation_id': conversation_id,
        'chat_id': chat_id or None,
        'message_text': text,
        'message_truncated': message_too_long is True,
        'message_id': message_id,
        'source_event_id': source_event_id or None,
        'idempotency_degraded': idempotency_degraded,
        'deferred_replay': deferred_replay,
        'wamid': _js_or(_prop(body, 'wamid'), None),
        'received_at': reference_now_iso,
        'idempotency_key': idempotency_key,
        'operation_id': operation_id or None,
        'correlation_id': correlation_id or None,
        'run_id': run_id or None,
        'test_id': test_id or None,
        'time_context': time_context,
        'metadata': {
            'chat_id': chat_id or None,
            'content_length': _js_len(text),
            'original_content_length': _js_len(raw_message_text),
            'message_length_limit': max_message_chars,
            'language': language,
            'source_event_id': source_event_id or None,
            'idempotency_degraded': idempotency_degraded,
            'wamid': _js_or(_prop(body, 'wamid'), None),
            'channel_type': channel_type,
            'channel_id': channel_id,
            'message_id': message_id,
            'idempotency_key': idempotency_key,
            'operation_id': operation_id or None,
            'correlation_id': correlation_id or None,
            'deferred_replay': deferred_replay,
            'k2_deferred_replay': deferred_replay,
            'time_context': time_context,
        },
    }


# ── Node 2: Handle Normalize Error (Deterministic) ──
_HANDLE_ERROR_ALIASES = {
    'clinic_id': ('clinic_id', 'clinicId'),
    'channel_type': ('channel_type', 'channel', 'platform', 'source'),
    'channel_id': ('channel_id', 'channelId', 'chat_id', 'thread_id', 'sender_id', 'user_id', 'from_id'),
    'patient_id': ('patient_id', 'patientId'),
    'conversation_id': ('conversation_id', 'conversationId', 'thread_id', 'chat_id'),
    'message_text': ('message_text', 'messageText', 'text', 'content'),
    'source_event_id': ('source_event_id', 'wamid', 'message_id', 'messageId', 'event_id', 'eventId', 'update_id', 'updateId', 'id'),
}


def handle_normalize_error(inputs: dict) -> dict:
    """Source node: Handle Normalize Error (Deterministic)
    (extracted/code/Handle_Normalize_Error_Deterministic.js).

    ``inputs`` is the current item json (JS ``$input.item.json || {}``). When the
    item carries a ``body`` object (webhook shape) the missing-field re-check reads
    the raw body aliases from it, exactly like the JS. Returns the item with the
    ``normalization_error`` / ``normalization_error_code`` /
    ``normalization_missing_fields`` / ``normalization_error_message`` fields stamped.
    """
    input_item = inputs if isinstance(inputs, dict) else {}
    body_raw = input_item.get('body')
    body = body_raw if _truthy(body_raw) and isinstance(body_raw, (dict, list)) else input_item

    missing = []
    for key in ('clinic_id', 'channel_type', 'channel_id', 'patient_id', 'conversation_id', 'message_text', 'source_event_id'):
        found = False
        for alias in _HANDLE_ERROR_ALIASES[key]:
            v = _prop(body, alias)
            if v is not None and _js_string(v).strip() != '':
                found = True
                break
        if not found:
            missing.append(key)

    error_like = bool(
        _truthy(input_item.get('error'))
        or _truthy(input_item.get('errorResponse'))
        or _truthy(input_item.get('normalization_error'))
    )
    invalid = error_like or len(missing) > 0 or not _truthy(input_item.get('idempotency_key')) or not _truthy(input_item.get('message_id'))
    existing_code = _js_string(_js_or(input_item.get('normalization_error_code'), '')).strip()
    error_code = existing_code if existing_code == 'MESSAGE_TOO_LONG' else ('INVALID_INBOUND_PAYLOAD' if invalid else None)
    error_message = (
        _js_or(input_item.get('normalization_error_message'), 'الرسالة أطول من الحد المسموح لمعالجتها')
        if existing_code == 'MESSAGE_TOO_LONG'
        else ('تعذر معالجة الطلب بسبب نقص بيانات الرسالة الأساسية' if invalid else None)
    )
    out = dict(input_item)
    out['normalization_error'] = invalid
    out['normalization_error_code'] = error_code
    out['normalization_missing_fields'] = missing
    out['normalization_error_message'] = error_message
    return out
