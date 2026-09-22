"""Reply Guard (Deterministic) — faithful 1:1 port.

Source node: Reply Guard (Deterministic) (extracted/code/Reply_Guard_Deterministic.js)

The node reads the Response Policy envelope and, for a closed set of
``response_code`` / ``decision_rule`` outcomes, replaces the model-facing reply
with a deterministic Arabic template (including the APPOINTMENT_CREATED success
template with branch/location/queue facts). When no override fires the item
passes through untouched (the very same dict object, like the JS returns
``[{ json: item }]`` with the same reference).

Public API: ``apply_reply_guard(inputs: dict) -> dict`` — returns the inner json
dict the JS emits as ``[{ json: out }][0].json`` (the n8n item wrapper is dropped
per docs/port_conventions.md).

Required ``inputs`` schema
--------------------------
The JS reads exactly one upstream node via ``$(NodeName).first().json``. The
pipeline runner passes that node output on the ``inputs`` dict; the key set below
is derived from what the JS actually consumes — nothing more.

- ``response_policy``  ← ``$('Response Policy (Deterministic)').first().json`` —
  required (JS ``item``). Fields read:
  - ``system_decision`` (object): ``decision_rule``, ``response_code``,
    ``action``, ``booking_number``, ``appointment_id``, ``booking_context``
    (``patient_name``/``patient_phone``/``date``/``time``/``booking_number``),
    ``confirmation_target`` (same patient/slot fields + ``booking_number``).
  - ``response_code`` (fallback when ``system_decision.response_code`` is empty).
  - ``facts``: ``branch_name``, ``branch_location`` (``address``,
    ``maps_url``, ``location_config`` (``maps_url``, ``latitude``, ``longitude``)),
    ``clinic_location.queue_base_url``.
  - ``booking_number``, ``appointment_id``, ``queue_number``, ``queue_path``,
    ``queue_url`` (item-level overrides, first non-empty wins via ``pick``).
  - ``output`` / ``text`` / ``agent_raw_output``: when a string containing a JSON
    object with a string ``reply`` field, that ``reply`` is rewritten to the
    override (JSON round-trip like the JS).

Output keys (exactly as the JS emits): the input item keys, plus
``agent_reply`` (the override text) and the ``_reply_guard`` sub-object
``{ triggered, rule, code, override, read_from }``. When no override fires the
item is returned unchanged and no keys are added.

Pure function: no I/O, no logging, stdlib only.
"""

import json
import re
from typing import TypedDict
from app.core.js_semantics import dict_or_empty as _dict, js_and as _js_and, js_or as _js_or, truthy as _truthy


# ── JS-semantics shims (same semantics as the ones in app/core/orchestrator.py) ──


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
    if isinstance(value, list):
        return ','.join(_js_string(v) for v in value)
    if isinstance(value, dict):
        return '[object Object]'
    return str(value)


def _js_cat(*parts):
    """JS ``+`` string concatenation coercion (None renders as 'null', as in JS)."""
    out = []
    for p in parts:
        if p is None:
            out.append('null')
        elif isinstance(p, bool):
            out.append('true' if p else 'false')
        else:
            out.append(_js_string(p))
    return ''.join(out)


def _js_trim(s):
    """JS String.prototype.trim() — the JS WhiteSpace + LineTerminator set (differs from Python strip())."""
    return s.strip('\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff')


def _pick(*vals):
    """JS ``pick(...)``: first value that is not undefined/null and whose String(v).trim() is non-empty; else null."""
    for v in vals:
        if v is not None and _js_trim(_js_string(v)) != '':
            return v
    return None


def _json_stringify(value):
    """JS ``JSON.stringify`` — compact separators, non-ASCII kept literal."""
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


_WEEKDAYS_AR = ['الأحد', 'الإثنين', 'الثلاثاء', 'الأربعاء', 'الخميس', 'الجمعة', 'السبت']
_ISO_DATE_RE = re.compile(r'\d{4}-\d{2}-\d{2}')


# ── Public entry point: the Code-node module body ──
def apply_reply_guard(inputs: dict) -> dict:
    """Source node: Reply Guard (Deterministic) (extracted/code/Reply_Guard_Deterministic.js).

    Mirrors the JS node body and returns the inner json dict (n8n's
    ``[{ json: out }][0].json``). See the module docstring for the ``inputs`` schema.
    """
    inputs = _dict(inputs)
    item = _dict(_js_or(inputs.get('response_policy'), {}))  # JS $("Response Policy (Deterministic)").first().json || {}

    decision = item.get('system_decision') if (_truthy(item.get('system_decision')) and isinstance(item.get('system_decision'), dict)) else {}
    rule = _js_or(decision.get('decision_rule'), None)
    code = _js_or(decision.get('response_code'), item.get('response_code'), None)

    # ── No patient-facing text is produced here (changed 2026-09-17) ──────────────
    # This layer used to write a prewritten Arabic sentence per terminal response code
    # (APPOINTMENT_CREATED, CANCEL_COMPLETED, RESCHEDULE_COMPLETED, IDEMPOTENT_REPLAY,
    # CONFIRMATION_EXPIRED, AVAILABILITY_SOURCE_ERROR, confirm_without_target). That let
    # a rigid template win over the model's own words — exactly what this system must
    # never do. The model authors every reply from the fact catalog, and correctness is
    # enforced by the evidence contract in response_context.validate_composer_output
    # (every cited fact must exist and the model must report no unsupported claims).
    #
    # Detection is kept so the audit trail and the fact catalog can mark a turn as
    # terminal, but `override` is always None: nothing here can replace the reply.
    _TERMINAL_CODES = {
        'APPOINTMENT_CREATED', 'CANCEL_COMPLETED', 'RESCHEDULE_COMPLETED',
        'IDEMPOTENT_REPLAY', 'CONFIRMATION_EXPIRED', 'AVAILABILITY_SOURCE_ERROR',
    }
    terminal_detected = bool(code) and (
        code in _TERMINAL_CODES
        or (code == 'CONVERSATION_ONLY' and rule == 'confirm_without_target')
    )

    out = dict(item)
    out['_reply_guard'] = {
        'triggered': terminal_detected,
        'rule': rule,
        'code': code,
        'override': None,
        'read_from': 'Response Policy (Deterministic)',
        'text_generation': 'removed 2026-09-17 - the model authors every patient reply',
    }
    return out
