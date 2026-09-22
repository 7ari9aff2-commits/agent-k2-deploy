"""Contract Adapter v4→v3 + Derive Actions — faithful 1:1 port.

Source nodes: K2 Contract Adapter v4 to v3 (extracted/code/K2_Contract_Adapter_v4_to_v3.js)
              Derive Actions (Deterministic) (extracted/code/Derive_Actions_Deterministic.js)

Two node bodies live here:

- ``contract_v4_to_v3`` — finds the agent's v4 dialogue contract (``k2.dialogue.v4``)
  serialized inside one of the item's candidate string fields, parses it, projects
  it to the legacy ``k2.dialogue.v3`` shape, and re-serializes it back into the
  item (``output``/``text`` + the originating key). Derives ``certainty`` from the
  numeric ``confidence`` and ``references_prior_conversation`` from the
  reference/``follow_up`` signal (the 2026-09-07 FIX comments in the JS).
  Non-v4 / unparseable payloads pass through untouched with an ``_adapter``
  marker — the JS quirk that a payload which parses to a JS-falsy value
  (``0``/``''``/``false``/``null``) reports ``unparseable`` while any other
  non-v4 document reports ``not_v4`` is preserved.
- ``derive_actions`` — maps the authoritative ``response_code`` to the concrete
  action list consumed by the execution phase, choosing between the upstream
  System Orchestrator decision and the local envelope exactly like the JS.

Pure functions: no I/O, no logging, stdlib only.
"""

import json
import re
from typing import TypedDict
from app.core.js_semantics import dict_or_empty as _dict, first_not_none as _first_not_none, js_and as _js_and, js_or as _js_or, truthy as _truthy


# ── JS-semantics shims (same semantics as the ones in app/core/orchestrator.py) ──


def _prop(obj, key):
    """Property read on a value that may not be an object (JS yields undefined, never throws)."""
    return obj.get(key) if isinstance(obj, dict) else None


def _json_stringify(value):
    """JS ``JSON.stringify`` — compact separators, non-ASCII kept literal."""
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


_UUID_V4ISH_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{1,4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE)  # version-agnostic: UUIDv7 ids exist

_CANDIDATE_KEYS = ('output', 'text', 'agent_raw_output', 'raw', 'data')


# ── Node 1: K2 Contract Adapter v4 to v3 ──

def _extract_balanced_json_object(text: str):
    """First balanced {...} object in the text that parses as JSON (reviewer fix)."""
    import json as _json

    start = text.find("{")
    while start != -1:
        depth = 0
        in_str = False
        esc = False
        for i in range(start, len(text)):
            ch = text[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
            elif ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        obj = _json.loads(text[start:i + 1])
                        if isinstance(obj, dict):
                            return obj
                    except Exception:
                        pass
                    break
        start = text.find("{", start + 1)
    return None

def contract_v4_to_v3(contract_v4: dict, extra: dict | None = None) -> dict:
    """Source node: K2 Contract Adapter v4 to v3 (extracted/code/K2_Contract_Adapter_v4_to_v3.js).

    Mirrors the JS node body and returns the inner json dict (n8n's
    ``[{ json: out }][0].json``).

    ``contract_v4`` is the inbound item json (JS ``$input.item.json || {}``); the
    v4 document lives serialized inside one of the candidate string fields
    (``output``/``text``/``agent_raw_output``/``raw``/``data``) — exactly like the
    JS. ``extra`` is accepted for pipeline-runner signature compatibility but is
    unused: the JS node reads no other node context.
    """
    del extra  # the JS node reads no external context; kept for the runner's call signature
    item = _dict(_js_or(contract_v4, {}))

    key = None
    for k in _CANDIDATE_KEYS:
        v = item.get(k)
        if isinstance(v, str) and v.strip().startswith('{'):
            key = k
            break

    parsed = None
    if key is not None:
        try:
            parsed = json.loads(item[key])
        except Exception:
            parsed = None

    if not _truthy(parsed) or not isinstance(parsed, dict) or parsed.get('schema_version') != 'k2.dialogue.v4':
        if isinstance(parsed, dict) and parsed.get('reply'):
            parsed.setdefault('schema_version', 'k2.dialogue.v4')
        else:
            raw_text = None
            embedded = None
            for k in _CANDIDATE_KEYS:
                cand = item.get(k)
                if isinstance(cand, str) and cand.strip() and not cand.strip().startswith('{'):
                    if embedded is None:
                        # Reviewer fix (P1): prose before the JSON used to swallow the
                        # whole contract into `reply` — every entity and the operation
                        # proposal were dropped. Extract the embedded object instead.
                        embedded = _extract_balanced_json_object(cand)
                    if raw_text is None:
                        raw_text = cand.strip()
                    break
            if isinstance(embedded, dict) and (embedded.get('reply') or embedded.get('entities')):
                embedded.setdefault('schema_version', 'k2.dialogue.v4')
                parsed = embedded
            elif raw_text and not _truthy(parsed):
                parsed = {
                    'schema_version': 'k2.dialogue.v4',
                    'reply': raw_text,
                    'turn': {'intent': 'other', 'relation_to_previous_turn': 'none'},
                    'confidence': 0.9,
                    'ambiguous': [],
                    'confirmation': {'intent': 'none'},
                    'selection': {'kind': 'none', 'rank': None},
                    'entities': {},
                    'operation_proposal': {'type': 'none', 'requested': False},
                    'escalate': None
                }
            else:
                reason = 'not_v4' if _truthy(parsed) else 'unparseable'
                out = dict(item)
                out['_adapter'] = {'action': 'passthrough', 'reason': reason}
                return out

    e = _dict(parsed.get('entities'))
    ref = e.get('reference').strip() if isinstance(e.get('reference'), str) else ''
    is_uuid = bool(_UUID_V4ISH_RE.fullmatch(ref))

    # FIX (2026-09-07): v4 has no `certainty` enum, only numeric `confidence`.
    # Derive it from the model's own confidence score instead of discarding it.
    confidence = parsed.get('confidence')
    confidence_num = confidence if (isinstance(confidence, (int, float)) and not isinstance(confidence, bool)) else None
    derived_certainty = None if confidence_num is None else (
        'certain' if confidence_num >= 0.75 else ('probable' if confidence_num >= 0.4 else 'uncertain'))

    # FIX (2026-09-07): v4 has no direct references_prior_conversation signal.
    # Derive a proxy: explicit reference/booking number, or a follow_up turn.
    turn_obj = _js_and(parsed.get('turn'), _prop(parsed.get('turn'), 'relation_to_previous_turn'))
    relation = _js_or(turn_obj, 'none')
    derived_prior_reference = len(ref) > 0 or relation == 'follow_up'

    sel_obj = parsed.get('selection')
    conf_obj = parsed.get('confirmation')
    op = parsed.get('operation_proposal')
    op_type = _prop(op, 'type')
    v3 = {
        'schema_version': 'k2.dialogue.v3',
        'phase': 'understand',
        'reply': _js_or(parsed.get('reply'), ''),
        'turn': {
            'intent': _js_or(_js_and(parsed.get('turn'), _prop(parsed.get('turn'), 'intent')), 'other'),
            'relation_to_previous_turn': relation,
            'certainty': derived_certainty,
            'confidence': confidence_num,
        },
        'confirmation': {'intent': _js_or(_js_and(conf_obj, _prop(conf_obj, 'intent')), 'none')},
        'selection': {
            'kind': _js_or(_js_and(sel_obj, _prop(sel_obj, 'kind')), 'none'),
            'rank': _first_not_none(_js_and(sel_obj, _prop(sel_obj, 'rank'))),
            'date': _js_or(e.get('date'), None),
            'time': _js_or(e.get('time'), None),
        },
        'entities': {
            'doctor_name': _js_or(e.get('doctor_name'), None),
            'service_name': _js_or(e.get('service_name'), None),
            'date': _js_or(e.get('date'), None),
            'time': _js_or(e.get('time'), None),
            'visit_type': _js_or(e.get('visit_type'), None),
            'patient_name': _js_or(e.get('patient_name'), None),
            'patient_phone': _js_or(e.get('patient_phone'), None),
            'patient_age': e.get('patient_age') if (isinstance(e.get('patient_age'), (int, float)) and not isinstance(e.get('patient_age'), bool)) else None,
            'patient_address': _js_or(e.get('patient_address'), None),
            'appointment_id': ref if is_uuid else None,
            'booking_number': ref if (ref and not is_uuid) else None,
        },
        'operation_proposal': {
            'type': '' if (not _truthy(op) or not _truthy(op_type) or op_type == 'none') else op_type,
            'requested': bool(_truthy(op) and isinstance(op, dict) and op.get('requested') is True),
        },
        'references_prior_conversation': derived_prior_reference,
        'escalate': bool(_truthy(parsed.get('escalate'))),
        'handoff_reason': _js_or(parsed.get('escalate'), None),
        'ambiguous': parsed.get('ambiguous') if isinstance(parsed.get('ambiguous'), list) else [],
    }

    out = dict(item)
    if key is not None:
        out[key] = _json_stringify(v3)
    out['output'] = _json_stringify(v3)
    out['text'] = _json_stringify(v3)
    out['_adapter'] = {
        'action': 'v4_to_v3',
        'escalated_reason': v3['handoff_reason'],
        'ambiguous': v3['ambiguous'],
        'derived_certainty': derived_certainty,
        'derived_prior_reference': derived_prior_reference,
    }
    return out


# ── Node 2: Derive Actions (Deterministic) — null-safe version ──
def derive_actions(inputs: dict) -> dict:
    """Source node: Derive Actions (Deterministic) (extracted/code/Derive_Actions_Deterministic.js).

    Mirrors the JS node body and returns the inner json dict (n8n's
    ``[{ json: out }][0].json``). See the module docstring for the ``inputs`` schema.
    """
    inputs = _dict(inputs)
    current = _dict(inputs.get('current'))  # JS $json

    output = _js_or(current.get('output'), {})
    # JS: try { upstreamDecision = $('System Orchestrator (Policy)').first().json.system_decision || {}; } catch {}
    upstream_decision = _js_or(_dict(_dict(_js_or(inputs.get('system_orchestrator'), {})).get('system_decision')), {})

    local_sd = current.get('system_decision')
    local_decision = local_sd if isinstance(local_sd, dict) else {}

    # Claim-blocked/replay paths must use the current local Response Policy envelope,
    # not the stale upstream Orchestrator decision.
    has_claim_outcome = bool(
        _truthy(current.get('operation_claim_decision'))
        or _truthy(current.get('operation_claim_blocked'))
        or _truthy(current.get('operation_replay'))
    )
    if has_claim_outcome:
        decision = local_decision if len(local_decision) else upstream_decision
    else:
        decision = upstream_decision if len(upstream_decision) else local_decision

    # Null-safe inbound context. Normalize & Validate always returns 1 item in normal flow,
    # but a missing item is treated as an empty context to avoid throwing.
    ctx = _dict(_js_or(inputs.get('normalize_validate'), {}))

    actions = []
    response_code = _js_or(decision.get('response_code'), current.get('response_code'), _dict(output).get('response_code'), 'CONVERSATION_ONLY')
    booking_number = _js_or(
        _dict(output).get('booking_number'),
        decision.get('booking_number'),
        _js_and(decision.get('booking_context'), _prop(decision.get('booking_context'), 'booking_number')),
        None,
    )
    decision_ct = decision.get('confirmation_target')
    confirmation_target_expires = _js_or(_js_and(decision_ct, _prop(decision_ct, 'expires_at')), None)

    if response_code == 'HANDOFF_REQUIRED':
        actions.append({'type': 'handoff', 'reason': 'agent_escalation', 'priority': 'high'})
    elif response_code == 'APPOINTMENT_CREATED':
        actions.append({'type': 'send_confirmation', 'appointment_id': _dict(output).get('appointment_id'), 'booking_number': booking_number})
    elif response_code == 'IDEMPOTENT_REPLAY':
        if decision.get('action') == 'create_appointment':
            actions.append({'type': 'send_confirmation', 'appointment_id': _dict(output).get('appointment_id'), 'booking_number': booking_number, 'replayed': True})
        elif decision.get('action') == 'reschedule_appointment':
            actions.append({'type': 'send_reschedule_confirmation', 'appointment_id': _dict(output).get('appointment_id'), 'booking_number': booking_number, 'replayed': True})
        elif decision.get('action') == 'cancel_appointment':
            actions.append({'type': 'send_cancellation_confirmation', 'appointment_id': _dict(output).get('appointment_id'), 'booking_number': booking_number, 'replayed': True})
        else:
            # Reviewer fix: a replay whose action is unknown/none previously fabricated
            # a CANCELLATION confirmation — telling the patient something was cancelled
            # that never was. Neutral reply instead.
            actions.append({'type': 'send_conversation_reply', 'reason': 'replay_without_action'})
    elif response_code == 'CANCEL_COMPLETED':
        actions.append({'type': 'send_cancellation_confirmation', 'appointment_id': _dict(output).get('appointment_id'), 'booking_number': booking_number})
    elif response_code == 'RESCHEDULE_COMPLETED':
        actions.append({'type': 'send_reschedule_confirmation', 'appointment_id': _dict(output).get('appointment_id'), 'booking_number': booking_number,
                        'new_slot_id': _js_or(_js_and(decision_ct, _prop(decision_ct, 'new_slot_id')), None)})
    elif response_code == 'AVAILABILITY_RESULTS':
        actions.append({'type': 'send_conversation_reply', 'reason': 'availability_results'})
    elif response_code == 'CONVERSATION_ONLY':
        actions.append({'type': 'send_conversation_reply'})
    elif response_code == 'CONFIRMATION_REQUIRED':
        actions.append({'type': 'request_confirmation', 'target': _js_or(decision_ct, None), 'expires_at': confirmation_target_expires})
    elif response_code == 'CONFIRMATION_EXPIRED':
        actions.append({'type': 'refresh_booking_confirmation', 'reason': 'confirmation_target_expired_or_changed'})
    elif response_code == 'MISSING_REQUIRED_FIELDS':
        actions.append({'type': 'collect_required_fields', 'fields': _js_or(decision.get('missing_fields'), [])})
    elif response_code in ('APPOINTMENT_CREATION_FAILED', 'CANCEL_FAILED', 'CANCELLATION_NOT_ALLOWED', 'APPOINTMENT_NOT_FOUND_OR_NOT_OWNED', 'CANCEL_RETRYABLE'):
        rc_str = _js_string_safe(response_code)
        operation = 'cancel_appointment' if (rc_str.startswith('CANCEL') or 'CANCELLATION' in rc_str or 'APPOINTMENT_NOT_FOUND' in rc_str) else 'create_appointment'
        actions.append({'type': 'send_operation_failure', 'operation': operation})
    elif response_code in ('RESCHEDULE_NOT_ALLOWED', 'RESCHEDULE_CONFLICT', 'RESCHEDULE_RETRYABLE'):
        actions.append({'type': 'send_operation_failure', 'operation': 'reschedule_appointment'})
    elif response_code == 'SLOT_UNAVAILABLE':
        availability_alternatives = _dict(output).get('availability_alternatives')
        slot_lookup_alts = _prop(_dict(output).get('deterministic_slot_lookup'), 'alternatives')
        alternatives = availability_alternatives if isinstance(availability_alternatives, list) else (slot_lookup_alts if isinstance(slot_lookup_alts, list) else [])
        actions.append({
            'type': 'offer_available_slots',
            'operation': 'create_appointment' if decision.get('action') == 'create_appointment' else 'reschedule_appointment',
            'requested_time_unavailable': _dict(output).get('availability_requested_time_unavailable') is True,
            'alternatives': alternatives,
        })
    elif response_code == 'AVAILABILITY_SOURCE_ERROR':
        actions.append({'type': 'send_availability_source_error'})
    else:
        actions.append({'type': 'send_policy_reply', 'response_code': response_code})

    out = dict(current)
    out['system_decision'] = decision
    out['response_code'] = response_code
    out['escalation_requested'] = decision.get('response_code') == 'HANDOFF_REQUIRED' and decision.get('escalation_requested') is True
    out['actions'] = actions
    out['audit_context'] = {
        'clinic_id': ctx.get('clinic_id'),
        'conversation_id': ctx.get('conversation_id'),
        'patient_id': ctx.get('patient_id'),
        'response_code': decision.get('response_code'),
        'confirmation_expires_at': confirmation_target_expires,
    }
    return out


def _js_string_safe(value):
    """String coercion for str-method access; JS would throw on non-strings here, the port coerces."""
    if isinstance(value, str):
        return value
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return str(value)
