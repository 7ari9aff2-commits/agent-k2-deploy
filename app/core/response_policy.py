"""Response Policy v3 — Facts only (DeepSeek #2 generates the reply).

Faithful 1:1 Python port of the n8n Code node ``Response Policy (Deterministic)``.

Source node: Response Policy (Deterministic) (extracted/code/Response_Policy_Deterministic.js)

This node produces safe execution facts for DeepSeek #2. NO conversational reply
generation here; only a deterministic result-based fallback reply for the DeepSeek #2
failure path. It reads the result from the operation that actually ran;
claim-blocked/replay decisions must use the local decision envelope, not the stale
upstream decision.

Public API: ``build_response(ctx: dict) -> dict`` — returns the inner json dict the JS
emits as ``[{ json: {...} }][0].json`` (the n8n item wrapper is dropped per
docs/port_conventions.md). Like the JS, it mutates the ``confirmation_target`` object
in place when stamping delivery metadata (the same shared reference ends up in the
returned ``system_decision`` / ``confirmation_target``).

Required ``ctx`` schema
-----------------------
``ctx`` carries the n8n node outputs the JS reads via ``$(NodeName).first().json``
plus the current item (JS ``$json``). The key set below is derived from what the JS
actually consumes — nothing more. Every key is optional: an absent node behaves
exactly like the JS ``readNode`` try/catch (yields ``{}``). Typed as
``ResponsePolicyCtx``.

- ``current``                 ← ``$json`` — the current item flowing into this node.
  Fields read: ``system_decision`` (local decision), ``operation_claim_decision``,
  ``operation_claim_blocked``, ``operation_replay``, ``model_call_failed``,
  ``_normalization``, ``contract``, ``availability_inquiry``, ``booking_context``,
  ``booking_number``, ``active_operation``, ``operation_action``, ``operation_state``,
  ``operation_status``, ``deterministic_identity_resolution``,
  ``deterministic_slot_lookup``, ``operation_finalize_response``,
  ``availability_outcome``, ``patient_data_review``, ``agent_reply``. It is also the
  execution fallback (JS ``execution = $json``) and is spread at the top of the output.
- ``system_orchestrator``     ← ``$('System Orchestrator (Policy)').first().json`` —
  ``system_decision`` (upstream decision, also ``handoff_reason`` fallback).
- ``persona_builder``         ← ``$('Build Clinic Persona Context (Deterministic)').first().json``
  — ``error_followup_context`` (recovery gating + BOOKING_RECOVERY_EXPLANATION fallback).
- ``merge_completion``        ← ``$('Merge Operation Completion').first().json`` —
  preferred execution source when non-empty.
- ``execute_create``          ← ``$('Execute Approved Create Appointment').first().json``
- ``execute_cancel``          ← ``$('Execute Approved Cancel Appointment').first().json``
- ``execute_reschedule``      ← ``$('Execute Approved Reschedule Appointment').first().json``
  (the one matching the derived ``action`` becomes the execution source when
  ``Merge Operation Completion`` is empty).
- ``validate_repaired``       ← ``$('Validate Repaired Contract (Deterministic)').first().json``
  — used when its ``_contract_status`` is ``'VALID'`` (recovery + reply source).
- ``normalize_agent_output``  ← ``$('Normalize Agent Output (Deterministic)').first().json``
  — fallback for the above.
- ``normalize_validate``      ← ``$('Normalize & Validate').first().json`` —
  ``message_text`` (standalone-greeting detection + ``facts.patient_message``).
- ``clinic_context``          ← ``$('Get Clinic Context').first().json`` —
  ``clinic_location_config`` + ``branch_directory`` (tenant-scoped location facts).
- ``conversation_state``      ← ``$('Get Conversation State').first().json`` —
  ``state_data`` (clinic_name fallback + prior suspended booking draft).

Pure function: no I/O, no logging, stdlib only.
"""

import json
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any, TypedDict
from app.core.js_semantics import dict_or_empty as _dict, first_not_none as _first_not_none, js_or as _js_or
from app.core.js_semantics import truthy as _truthy


# ── JS-semantics shims (same semantics as the ones in app/core/orchestrator.py) ──


def _js_string(value):
    """Close JS String() coercion for the value shapes this decision path produces."""
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _js_cat(*parts):
    """JS ``+`` string concatenation coercion."""
    out = []
    for p in parts:
        if p is None:
            out.append('null')
        elif isinstance(p, bool):
            out.append('true' if p else 'false')
        else:
            out.append(_js_string(p))
    return ''.join(out)


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


def _has_data(value):
    """JS ``value && typeof value === 'object' && Object.keys(value).length > 0``."""
    if isinstance(value, (dict, list)):
        return len(value) > 0
    return False


def _dig(obj, *path):
    """JS ``a?.b?.c`` optional chaining: any non-object link yields None (undefined)."""
    cur = obj
    for key in path:
        if isinstance(cur, dict):
            cur = cur.get(key)
        else:
            return None
    return cur


def _read_node(ctx, key):
    """JS ``readNode(name)``: the node output json, or ``{}`` when absent (try/catch)."""
    value = ctx.get(key)
    return value if isinstance(value, dict) else {}


def _utc_now_iso():
    """JS ``new Date().toISOString()`` — UTC, millisecond precision, 'Z' suffix."""
    now = datetime.now(timezone.utc)
    return now.strftime('%Y-%m-%dT%H:%M:%S') + '.%03dZ' % (now.microsecond // 1000)


def _utc_shift_iso(seconds):
    """JS ``new Date(Date.now() + seconds * 1000).toISOString()``."""
    dt = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    return dt.strftime('%Y-%m-%dT%H:%M:%S') + '.%03dZ' % (dt.microsecond // 1000)


def _json_stringify(value):
    """JS ``JSON.stringify`` — compact separators, non-ASCII kept literal."""
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


# ── Regexes copied byte-for-byte from the JS (JS ``$`` anchor → Python ``\Z``) ──

_SLOT_UNAVAILABLE_RE = re.compile(
    r'P0001|slot unavailable|already booked|الفتحة غير متاحة|تم حجزها بالفعل|الموعد غير متاح|الموعد محجوز',
    re.IGNORECASE,
)
_RECOVERY_GENERIC_REPLY_RE = re.compile(
    r'^(?:حياك الله|اهلا|أهلاً|أهلًا|مرحبا|مرحباً|مرحبًا|هلا)(?:[،،.!؟? ]|\Z)',
    re.IGNORECASE,
)
_STANDALONE_GREETING_RE = re.compile(
    r'^(?:السلام عليكم(?: ورحمة الله وبركاته)?|وعليكم السلام(?: ورحمة الله وبركاته)?|هلا(?: ومرحبا)?|مرحبا|مرحباً|مرحبًا|اهلا|أهلا|أهلًا|صباح الخير|مساء الخير)[\s.!؟،,]*\Z',
    re.IGNORECASE,
)
_QUEUE_TOKEN_RE = re.compile(r'/queue/([^/?#]+)\Z', re.IGNORECASE)
_QUEUE_BASE_RE = re.compile(r'/queue\Z', re.IGNORECASE)
_TRAILING_SLASHES_RE = re.compile(r'/+\Z')

_UNAVAILABLE_SLOTS_CODES = (
    'NO_AVAILABLE_SLOTS',
    'FULLY_BOOKED',
    'DOCTOR_NOT_WORKING_THAT_DAY',
    'SLOT_UNAVAILABLE',
    'REQUESTED_TIME_NOT_AVAILABLE',
    'NO_AVAILABLE_SLOTS_IN_WINDOW',
)

_BOOKING_ENTITY_KEYS = (
    'doctor_name', 'doctor_id', 'service_name', 'service_id', 'appointment_type',
    'date', 'time', 'branch_name', 'branch_id', 'slot_id', 'appointment_id',
    'expected_old_slot_id', 'new_slot_id',
)
_PATIENT_ENTITY_KEYS = ('patient_name', 'patient_phone', 'patient_age', 'patient_address')


def _build_queue_url(base, path):
    """JS ``buildQueueUrl`` — public queue URL only from tenant config + opaque path."""
    if not base or not path:
        return None
    token_match = _QUEUE_TOKEN_RE.search(_js_string(path))
    if _QUEUE_BASE_RE.search(_js_string(base)) and token_match:
        return _js_cat(base, '/', token_match.group(1))
    return _js_cat(base, '' if _js_string(path).startswith('/') else '/', path)


# ── Public entry point: the Code-node module body ──
def build_response(ctx: dict) -> dict:
    """Source node: Response Policy (Deterministic) (extracted/code/Response_Policy_Deterministic.js).

    Mirrors the JS node body and returns the inner json dict (n8n's
    ``[{ json: {...} }][0].json``). See the module docstring for the ``ctx`` schema.
    """
    ctx = _dict(ctx)
    current = _dict(ctx.get('current'))  # JS $json

    # Read the result from the operation that actually ran.
    # Claim-blocked/replay decisions must use the local decision envelope, not the stale upstream decision.
    local_decision_raw = current.get('system_decision')
    local_decision = local_decision_raw if isinstance(local_decision_raw, dict) else {}
    system_orchestrator_output = _read_node(ctx, 'system_orchestrator')
    upstream_decision = _dict(_js_or(system_orchestrator_output.get('system_decision'), {}))
    has_claim_outcome = bool(_js_or(
        current.get('operation_claim_decision'),
        current.get('operation_claim_blocked'),
        current.get('operation_replay'),
    ))
    if has_claim_outcome:
        decision = local_decision if len(local_decision) > 0 else upstream_decision
    else:
        decision = upstream_decision if len(upstream_decision) > 0 else local_decision

    builder_context_for_recovery = _read_node(ctx, 'persona_builder')
    _efc = builder_context_for_recovery.get('error_followup_context')
    error_followup_context = _efc if isinstance(_efc, dict) else {}
    error_followup_active = error_followup_context.get('active') is True

    finalized = _read_node(ctx, 'merge_completion')
    action = _js_string(_js_or(decision.get('action'), _dig(decision, 'confirmation_target', 'action'), '')).lower()
    if action == 'cancel_appointment':
        action_execution = _read_node(ctx, 'execute_cancel')
    elif action == 'reschedule_appointment':
        action_execution = _read_node(ctx, 'execute_reschedule')
    elif action == 'create_appointment':
        action_execution = _read_node(ctx, 'execute_create')
    else:
        action_execution = {}
    execution = finalized if _has_data(finalized) else action_execution
    if not _has_data(execution):
        execution = current

    execution_error = _js_or(
        execution.get('error'),
        execution.get('errorResponse'),
        _dig(execution, 'data', 'error'),
        None,
    )
    error_text = _json_stringify(_js_or(execution_error, execution))
    slot_unavailable = bool(_SLOT_UNAVAILABLE_RE.search(error_text))
    raw_success = (
        execution.get('success') is True
        or execution.get('ok') is True
        or _dig(execution, 'data', 'success') is True
    )
    raw_appointment_id = _js_or(
        execution.get('appointment_id'),
        _dig(execution, 'data', 'appointment_id'),
        execution.get('id'),
        decision.get('appointment_id'),
        None,
    )

    model_call_failed = (
        current.get('model_call_failed') is True
        or _dig(current, '_normalization', 'model_call_failed') is True
        or decision.get('model_call_failed') is True
        or _dig(decision, 'contract', 'model_call_failed') is True
    )
    response_code = 'PROVIDER_UNAVAILABLE' if model_call_failed else _js_or(decision.get('response_code'), 'CONVERSATION_ONLY')
    operation_status = 'failed_retryable' if model_call_failed else 'pending'
    retryable = model_call_failed

    # ── Read contract from Orchestrator ──
    contract = _js_or(decision.get('contract'), current.get('contract'), {})
    contract_dict = _dict(contract)
    turn = _dict(contract_dict.get('turn'))
    entities = _dict(contract_dict.get('entities'))
    next_step = _dict(contract_dict.get('next_step'))
    query = _dict(contract_dict.get('query'))
    operation_proposal = _dict(contract_dict.get('operation_proposal'))
    operation_type = _js_string(_js_or(operation_proposal.get('type'), '')).lower()

    turn_intent = _js_string(_js_or(turn.get('intent'), 'other')).lower()
    certainty = _js_string(_js_or(turn.get('certainty'), 'uncertain')).lower()
    relation_to_previous = _js_string(_js_or(turn.get('relation_to_previous_turn'), 'none')).lower()
    next_step_type = _js_string(_js_or(next_step.get('type'), 'none')).lower()
    next_step_field = _js_or(next_step.get('field'), None)

    validate_repaired_output = _read_node(ctx, 'validate_repaired')
    if validate_repaired_output.get('_contract_status') == 'VALID':
        normalized_for_recovery = validate_repaired_output
    else:
        normalized_for_recovery = _read_node(ctx, 'normalize_agent_output')
    recovery_restored_context = (
        normalized_for_recovery.get('error_followup_recovered') is True
        or normalized_for_recovery.get('model_call_status') == 'ERROR_FOLLOWUP_RECOVERED'
    )
    recovery_agent_reply = _js_string(_js_or(
        normalized_for_recovery.get('agent_reply'),
        _dig(normalized_for_recovery, 'contract', 'reply'),
        '',
    )).strip()
    recovery_generic_reply = bool(_RECOVERY_GENERIC_REPLY_RE.search(recovery_agent_reply))
    recovery_scheduling_evidence = bool(
        _truthy(entities.get('date'))
        or _truthy(entities.get('time'))
        or _truthy(entities.get('service_name'))
        or ((not recovery_restored_context) and _truthy(entities.get('doctor_name')))
        or next_step_field == 'date'
        or next_step_field == 'time'
        or _js_string(_js_or(turn.get('intent'), '')).lower() in (
            'booking_request', 'booking_continuation', 'availability_inquiry', 'correction',
        )
    )
    error_followup_needs_fallback = (
        error_followup_active
        and not model_call_failed
        and not recovery_scheduling_evidence
        and (
            not recovery_agent_reply
            or recovery_generic_reply
            or _js_string(_js_or(turn.get('intent'), '')).lower() in ('unclear', 'other', 'small_talk')
        )
    )
    normalize_validate_output = _read_node(ctx, 'normalize_validate')
    current_patient_message = _js_string(_js_or(normalize_validate_output.get('message_text'), '')).strip()
    is_standalone_greeting = bool(_STANDALONE_GREETING_RE.search(current_patient_message))

    # ── Query info ──
    query_type = _js_string(_js_or(query.get('type'), '')).lower()
    query_is_availability = (
        query_type == 'availability'
        or operation_type == 'check_availability'
        or next_step_type == 'show_availability'
        or decision.get('availability_inquiry') is True
        or current.get('availability_inquiry') is True
    )
    query_scope = _js_or(query.get('scope'), None)

    # ── Booking context (needed before safeEntities) ──
    booking_context = _js_or(decision.get('booking_context'), current.get('booking_context'), {})
    if not isinstance(booking_context, dict):
        booking_context = {}
    booking_number = _js_string(_js_or(
        execution.get('booking_number'),
        _dig(execution, 'data', 'booking_number'),
        _dig(execution, 'operation_finalize_response', 'booking_number'),
        decision.get('booking_number'),
        _dig(decision, 'booking_context', 'booking_number'),
        current.get('booking_number'),
        '',
    )).strip() or None
    if booking_number and not booking_context.get('booking_number'):
        booking_context['booking_number'] = booking_number
    policy_prior_reference = (
        entities.get('references_prior_conversation') is True
        or _js_string(_js_or(entities.get('references_prior_conversation'), '')).strip().lower() == 'true'
        or entities.get('references_prior_conversation') == 1
    )
    policy_operation_name = _js_string(_js_or(
        current.get('active_operation'),
        current.get('operation_action'),
        decision.get('active_operation'),
        execution.get('active_operation'),
        '',
    )).strip().lower()
    policy_operation_state = _js_string(_js_or(
        current.get('operation_state'),
        current.get('operation_status'),
        decision.get('operation_state'),
        execution.get('operation_state'),
        '',
    )).strip().upper()
    policy_has_live_operation = (
        policy_operation_name in ('create_appointment', 'cancel_appointment', 'reschedule_appointment')
        and policy_operation_state not in ('COMPLETED', 'CANCELLED', 'FAILED_FINAL', 'IDLE', '')
    )
    suppress_historical_booking_context = (
        turn_intent == 'small_talk'
        and not policy_prior_reference
        and not policy_has_live_operation
        and not _truthy(decision.get('confirmation_target'))
    )
    if suppress_historical_booking_context:
        for _field in ('doctor_id', 'doctor_name', 'service_id', 'service_name', 'slot_id', 'date', 'time'):
            booking_context[_field] = None
    # ── FIX v12 (2026-08-19, root of the repeated-questions loop): the raw booking_context carried
    # stale names ('احمد', old service IDs) from previous turns, while the clinic catalog's fully
    # resolved canonical names (e.g. 'د. أحمد الحنكشلاوي') sit in deterministic_identity_resolution
    # (doctor_matches/service_matches). Promote the canonical names into booking_context so every
    # downstream node (BRP, Build Persistent Conversation State, retry merge) stores the REAL name.
    # Works for ALL clinics and ALL doctors — catalog-driven, nothing hardcoded.
    try:
        idr = _js_or(execution.get('deterministic_identity_resolution'), current.get('deterministic_identity_resolution'))
        if isinstance(idr, dict):
            dm = idr.get('doctor_matches') if isinstance(idr.get('doctor_matches'), list) else []
            if dm and _truthy(dm[0]):
                first_dm = _dict(dm[0])
                if _truthy(first_dm.get('id')) and not booking_context.get('doctor_id'):
                    booking_context['doctor_id'] = first_dm.get('id')
                dn = _js_string(_js_or(first_dm.get('doctor_name'), first_dm.get('name'), '')).strip()
                if dn and not booking_context.get('doctor_name'):
                    booking_context['doctor_name'] = dn
            elif idr.get('doctor_resolved') is True:
                requested = _js_string(_js_or(idr.get('requested_doctor_name'), '')).strip()
                if requested and not booking_context.get('doctor_name'):
                    booking_context['doctor_name'] = requested
            sm = idr.get('service_matches') if isinstance(idr.get('service_matches'), list) else []
            if sm and _truthy(sm[0]):
                first_sm = _dict(sm[0])
                if _truthy(first_sm.get('id')) and not booking_context.get('service_id'):
                    booking_context['service_id'] = first_sm.get('id')
                sn = _js_string(_js_or(first_sm.get('service_name'), first_sm.get('name'), '')).strip()
                if sn and not booking_context.get('service_name'):
                    booking_context['service_name'] = sn
    except Exception:
        pass

    # ── Entities (safe facts) — from contract entities OR bookingContext ──
    safe_entities = {
        'doctor_name': _js_or(entities.get('doctor_name'), booking_context.get('doctor_name'), None),
        'service_name': _js_or(entities.get('service_name'), booking_context.get('service_name'), None),
        'date': _js_or(entities.get('date'), booking_context.get('date'), None),
        'time': _js_or(entities.get('time'), booking_context.get('time'), None),
        'branch_id': _js_or(entities.get('branch_id'), booking_context.get('branch_id'), None),
        'branch_name': _js_or(entities.get('branch_name'), booking_context.get('branch_name'), None),
    }

    # ── Missing fields (for DeepSeek #2 to know what to ask) ──
    # FIX: Never rewrite Orchestrator's missing_fields with an invented field (booking_type does not exist
    # in the render label dictionary and causes DeepSeek #2 to ask about the WRONG field, e.g. "time" instead of doctor).
    # Read missing fields from the Orchestrator only — single source of truth.
    _mh = decision.get('missing_human_fields')
    _mf = decision.get('missing_fields')
    missing_human = _mh if isinstance(_mh, list) else (_mf if isinstance(_mf, list) else [])
    next_best_missing = _js_or(
        decision.get('next_best_missing_human_field'),
        missing_human[0] if missing_human else None,
        None,
    )

    # ── FIX: Transparent expired-booking handling ──
    # When the Orchestrator detected a greeting during/after an abandoned booking, it attaches
    # prior_expired_booking (doctor/service/date/time of the old abandoned booking). Pass it through
    # verbatim so DeepSeek #2 can produce the transparent "old booking expired, let's start fresh" reply.
    _peb = decision.get('prior_expired_booking')
    prior_expired_booking = _peb if isinstance(_peb, dict) else None
    greeting_restart = (not is_standalone_greeting) and (
        decision.get('greeting_restart') is True
        or (
            decision.get('new_booking_restart') is True
            and decision.get('response_code') == 'NEW_BOOKING_STARTED'
            and prior_expired_booking is not None
        )
    )
    references_prior_conversation = (
        entities.get('references_prior_conversation') is True
        or _js_string(_js_or(entities.get('references_prior_conversation'), '')).strip().lower() == 'true'
    )
    suppress_prior_draft_on_fresh_booking = decision.get('new_booking_restart') is True and not references_prior_conversation

    # ── Confirmation target ──
    _ct = decision.get('confirmation_target')
    confirmation_target = _ct if isinstance(_ct, dict) else None
    confirmation_action = _js_string(_js_or(_dig(confirmation_target, 'action'), decision.get('action'), '')).lower()
    confirmation_state = _js_or(decision.get('confirmation_state'), None)
    destructive_target_present = bool(
        confirmation_target is not None
        and confirmation_action in ('create_appointment', 'cancel_appointment', 'reschedule_appointment')
    )

    # Tenant-scoped location facts. Prefer the branch attached to the current booking
    # and never guess a branch when multiple active branches exist.
    clinic_context = _read_node(ctx, 'clinic_context')
    _clc = clinic_context.get('clinic_location_config')
    clinic_location = _clc if isinstance(_clc, dict) else {}
    branch_directory = clinic_context.get('branch_directory') if isinstance(clinic_context.get('branch_directory'), list) else []
    branch_id_candidate = _js_or(
        entities.get('branch_id'),
        booking_context.get('branch_id'),
        _dig(confirmation_target, 'branch_id'),
        execution.get('branch_id'),
        _dig(execution, 'data', 'branch_id'),
        None,
    )
    selected_branch = None
    for _branch in branch_directory:
        if _js_string(_js_or(_dig(_branch, 'branch_id'), '')) == _js_string(_js_or(branch_id_candidate, '')):
            selected_branch = _branch
            break
    if selected_branch is None and len(branch_directory) == 1:
        selected_branch = branch_directory[0]
    branch_location = None
    if isinstance(selected_branch, dict):
        _slc = selected_branch.get('location_config')
        branch_location = {
            'branch_id': _js_or(selected_branch.get('branch_id'), None),
            'branch_name': _js_or(selected_branch.get('branch_name'), selected_branch.get('name'), None),
            'address': _js_or(selected_branch.get('address'), None),
            'phone': _js_or(selected_branch.get('phone'), None),
            'location_config': _slc if isinstance(_slc, dict) else {},
        }

    # Queue link fields are returned by create_appointment_with_queue_link. Build the
    # public URL only from the tenant-scoped clinic configuration and the opaque path.
    queue_path = _js_string(_js_or(
        execution.get('queue_path'),
        _dig(execution, 'data', 'queue_path'),
        _dig(execution, 'operation_finalize_response', 'queue_path'),
        '',
    )).strip() or None
    queue_expires_at = _js_or(
        execution.get('queue_expires_at'),
        _dig(execution, 'data', 'queue_expires_at'),
        _dig(execution, 'operation_finalize_response', 'queue_expires_at'),
        None,
    )
    queue_number = _first_not_none(
        execution.get('queue_number'),
        _dig(execution, 'data', 'queue_number'),
        _dig(execution, 'operation_finalize_response', 'queue_number'),
    )
    queue_base_url = _TRAILING_SLASHES_RE.sub('', _js_string(_js_or(clinic_location.get('queue_base_url'), '')).strip())
    queue_url = _build_queue_url(queue_base_url, queue_path)

    # ── Deterministic slot lookup ──
    deterministic_lookup = execution.get('deterministic_slot_lookup')
    if not isinstance(deterministic_lookup, dict):
        deterministic_lookup = current.get('deterministic_slot_lookup')
    if not isinstance(deterministic_lookup, dict):
        deterministic_lookup = decision.get('deterministic_slot_lookup')
    if not isinstance(deterministic_lookup, dict):
        deterministic_lookup = {}
    deterministic_availability_outcome = _js_string(_js_or(deterministic_lookup.get('availability_outcome'), '')).lower()
    deterministic_code = _js_string(_js_or(deterministic_lookup.get('result_code'), '')).upper()
    deterministic_unavailable = (
        deterministic_availability_outcome == 'verified_unavailable'
        or deterministic_code in _UNAVAILABLE_SLOTS_CODES
    )
    deterministic_authority_error = (
        deterministic_availability_outcome == 'authority_error'
        or deterministic_code == 'AUTHORITY_ERROR'
        or deterministic_code == 'AVAILABILITY_SOURCE_ERROR'
    )

    if isinstance(deterministic_lookup.get('alternatives'), list):
        availability_alternatives = list(deterministic_lookup['alternatives'])[:4]
    elif isinstance(deterministic_lookup.get('nearest_slots'), list):
        availability_alternatives = list(deterministic_lookup['nearest_slots'])[:4]
    else:
        availability_alternatives = []
    alternative_labels = []
    for _slot in availability_alternatives:
        _label = _dig(_slot, 'label')
        if isinstance(_label, str) and _label.strip():
            alternative_labels.append(_label.strip())
        else:
            _parts = [
                _js_string(_js_or(_dig(_slot, 'local_date'), '')),
                _js_string(_js_or(_dig(_slot, 'local_time'), '')),
            ]
            alternative_labels.append(' '.join(p for p in _parts if p))
    alternative_labels = [lbl for lbl in alternative_labels if lbl]

    # ── Child execution results ──
    child_execution_required = (
        decision.get('allowed') is True
        and confirmation_action in ('create_appointment', 'cancel_appointment', 'reschedule_appointment')
    )
    execution_has_no_error = not execution_error
    _fe = execution.get('operation_finalize_response')
    if isinstance(_fe, dict):
        finalize_envelope = _fe
    elif isinstance(current.get('operation_finalize_response'), dict):
        finalize_envelope = current['operation_finalize_response']
    else:
        finalize_envelope = None
    # A child contract is valid only when the validator explicitly says so. The
    # absence of a SQL error is not proof that the child returned the required schema.
    if child_execution_required:
        child_contract_checked = (
            execution.get('child_contract_checked') is True
            or (isinstance(finalize_envelope, dict) and finalize_envelope.get('child_contract_checked') is True)
        )
        child_contract_valid = (
            (
                execution.get('child_contract_valid') is True
                or (isinstance(finalize_envelope, dict) and finalize_envelope.get('child_contract_valid') is True)
            )
            and not (isinstance(finalize_envelope, dict) and finalize_envelope.get('child_contract_valid') is False)
        )
    else:
        child_contract_checked = True
        child_contract_valid = True
    success = (child_contract_checked and child_contract_valid and execution_has_no_error) if child_execution_required else raw_success
    appointment_id = (
        None
        if (child_execution_required and (not child_contract_checked or not child_contract_valid))
        else raw_appointment_id
    )

    # ── Business time ──
    business_time_blocked = execution.get('business_time_checked') is True and execution.get('business_time_allowed') is False
    business_time_code = _js_string(_js_or(execution.get('business_time_code'), execution.get('business_time_error_code'), '')).upper()

    # ── Time context ──
    canonical_time_context = _js_or(execution.get('canonical_time_context'), execution.get('time_context'), {})
    canonical_timezone = _js_string(_js_or(_dig(canonical_time_context, 'timezone'), ''))

    # ── Escalation ──
    escalation_requested = decision.get('response_code') == 'HANDOFF_REQUIRED' and decision.get('escalation_requested') is True
    handoff_reason = _js_or(decision.get('handoff_reason'), upstream_decision.get('handoff_reason'), None)

    # ── Non-scheduling turn detection ──
    _has_booking_entity = any(_truthy(entities.get(_key)) for _key in _BOOKING_ENTITY_KEYS)
    _has_patient_entity = any(_truthy(entities.get(_key)) for _key in _PATIENT_ENTITY_KEYS)
    # Computed in the JS and never read — kept 1:1.
    _all_booking_entities_empty = not _has_booking_entity and not _has_patient_entity  # noqa: F841
    # A scheduling turn that contains appointment type or patient data is still part
    # of the booking contract even if no doctor/service/date entity is present.
    # Do not let the generic non-scheduling classifier override missing-field policy.
    is_non_scheduling_turn = (
        decision.get('non_scheduling_turn') is True
        and not _has_booking_entity
        and not _has_patient_entity
        and len(missing_human) == 0
    )
    # DLG-12: empty booking_request / cancellation_request / reschedule_request keep the Orchestrator's
    # MISSING_REQUIRED_FIELDS decision; only small_talk and other/unclear are conversation-only here.
    inferred_non_scheduling_turn = turn_intent == 'small_talk' or (
        turn_intent in ('other', 'unclear', '') and confirmation_target is None
    )

    # ── Decision logic ──
    # No reply generation — only facts + response_code for DeepSeek #2

    # Escalation
    if model_call_failed:
        response_code = 'PROVIDER_UNAVAILABLE'
        operation_status = 'failed_retryable'
        retryable = True
    # Escalation
    elif escalation_requested:
        response_code = 'HANDOFF_REQUIRED'
        operation_status = 'pending'
    # New booking restart is only a status when the deterministic decision itself
    # selected NEW_BOOKING_STARTED. Never let this metadata overwrite a fresh
    # availability result, confirmation target, or approved execution decision.
    elif decision.get('new_booking_restart') is True and response_code == 'NEW_BOOKING_STARTED':
        response_code = 'NEW_BOOKING_STARTED'
        operation_status = 'collecting_details'
        retryable = False
    # Availability inquiry
    elif query_is_availability and not destructive_target_present:
        if deterministic_authority_error:
            response_code = 'AVAILABILITY_SOURCE_ERROR'
            operation_status = 'failed_retryable'
            retryable = True
        elif deterministic_unavailable:
            availability_code = deterministic_code if deterministic_code in _UNAVAILABLE_SLOTS_CODES else 'SLOT_UNAVAILABLE'
            response_code = availability_code
            operation_status = 'collecting_details'
            retryable = False
        elif decision.get('allowed') is True or decision.get('response_code') == 'AVAILABILITY_RESULTS':
            response_code = 'AVAILABILITY_RESULTS'
            operation_status = 'collecting_details'
        elif decision.get('response_code') == 'MISSING_REQUIRED_FIELDS':
            response_code = 'MISSING_REQUIRED_FIELDS'
            operation_status = 'collecting_details'
        else:
            response_code = 'AVAILABILITY_LOOKUP_REQUIRED'
            operation_status = 'collecting_details'
    # Patient data review has priority over the generic non-scheduling classifier.
    # A model may call the turn booking_continuation while the deterministic layer still
    # needs name/phone/age/address. Never downgrade this state to CONVERSATION_ONLY.
    elif response_code == 'PATIENT_DATA_CONFIRMATION_REQUIRED' or (
        decision.get('action') == 'create_appointment'
        and decision.get('patient_data_complete') is False
        and not child_execution_required
    ):
        response_code = 'PATIENT_DATA_CONFIRMATION_REQUIRED'
        operation_status = 'collecting_details'
        retryable = False
    # A deterministic booking confirmation target has priority over the generic
    # small-talk classifier. The target is only a pending confirmation request here;
    # execution remains blocked until the patient sends the affirmative turn.
    elif (
        confirmation_target is not None
        and _truthy(confirmation_target.get('confirmation_id'))
        and confirmation_action in ('create_appointment', 'cancel_appointment', 'reschedule_appointment')
        and not child_execution_required
    ):
        response_code = 'CONFIRMATION_REQUIRED'
        operation_status = 'awaiting_confirmation'
        retryable = False
    # Narrow technical recovery: only override an empty, generic, or unrelated Agent 1 result.
    # P0 guard (reviewer-verified): an APPROVED turn whose model contract said
    # small_talk/other must NOT be downgraded here — the mutation already committed.
    elif error_followup_needs_fallback and not child_execution_required:
        response_code = 'BOOKING_RECOVERY_EXPLANATION'
        operation_status = 'collecting_details'
        retryable = False
    # FAQ / Doctor Service / Price / Small talk — conversation only (DeepSeek #2 generates reply)
    elif (is_non_scheduling_turn or inferred_non_scheduling_turn) and not child_execution_required:
        response_code = 'CONVERSATION_ONLY'
        operation_status = 'idle'
    # Child execution invalid
    elif child_execution_required and (not child_contract_checked or not child_contract_valid):
        # If there are missing human fields, try to collect them instead of giving up
        if len(missing_human) > 0:
            response_code = 'MISSING_REQUIRED_FIELDS'
            operation_status = 'collecting_details'
            retryable = True
        else:
            response_code = 'CHILD_CONTRACT_INVALID'
            operation_status = 'failed_final'
            retryable = False
    # Business time blocked
    elif business_time_blocked:
        response_code = (
            business_time_code
            if business_time_code in ('BUSINESS_HOURS_VIOLATION', 'BUSINESS_HOURS_UNAVAILABLE')
            else 'BUSINESS_HOURS_VIOLATION'
        )
        operation_status = 'failed_final' if business_time_code == 'BUSINESS_HOURS_UNAVAILABLE' else 'collecting_details'
        retryable = False
    # Execution success/failure
    elif _truthy(decision.get('allowed')) and decision.get('action') == 'create_appointment':
        if success:
            response_code = 'APPOINTMENT_CREATED'
            operation_status = 'success'
        else:
            response_code = 'SLOT_UNAVAILABLE' if slot_unavailable else _js_or(
                execution.get('error_code'),
                _dig(execution, 'data', 'error_code'),
                'APPOINTMENT_CREATION_FAILED',
            )
            retryable = response_code != 'SLOT_UNAVAILABLE'
            operation_status = 'failed'
    elif _truthy(decision.get('allowed')) and decision.get('action') == 'reschedule_appointment':
        code = _js_or(
            execution.get('response_code'),
            _dig(execution, 'data', 'response_code'),
            execution.get('error_code'),
            _dig(execution, 'data', 'error_code'),
            None,
        )
        replay_or_success = success and _js_or(code, 'RESCHEDULE_COMPLETED') in ('RESCHEDULE_COMPLETED', 'IDEMPOTENT_REPLAY')
        if replay_or_success:
            response_code = 'IDEMPOTENT_REPLAY' if code == 'IDEMPOTENT_REPLAY' else 'RESCHEDULE_COMPLETED'
            operation_status = 'success'
        else:
            response_code = _js_or(code, 'RESCHEDULE_RETRYABLE')
            retryable = execution.get('retryable') is True or response_code == 'RESCHEDULE_RETRYABLE'
            operation_status = 'failed_retryable' if _truthy(retryable) else 'failed'
    elif _truthy(decision.get('allowed')) and decision.get('action') == 'cancel_appointment':
        cancel_response_code = _js_or(
            execution.get('response_code'),
            _dig(execution, 'data', 'response_code'),
            None,
        )
        cancel_success = success and _js_or(cancel_response_code, 'CANCEL_COMPLETED') in ('CANCEL_COMPLETED', 'IDEMPOTENT_REPLAY')
        if cancel_success:
            response_code = 'IDEMPOTENT_REPLAY' if cancel_response_code == 'IDEMPOTENT_REPLAY' else 'CANCEL_COMPLETED'
            operation_status = 'success'
        else:
            response_code = _js_or(
                cancel_response_code,
                execution.get('error_code'),
                _dig(execution, 'data', 'error_code'),
                'CANCEL_RETRYABLE',
            )
            retryable = execution.get('retryable') is True or response_code == 'CANCEL_RETRYABLE'
            operation_status = 'failed_retryable' if _truthy(retryable) else 'failed'
    # Patient data review is a collecting-details state, not a generic conversation.
    elif response_code == 'PATIENT_DATA_CONFIRMATION_REQUIRED':
        operation_status = 'collecting_details'
    # Confirmation required
    elif response_code == 'CONFIRMATION_REQUIRED':
        if confirmation_target is not None:
            operation_status = 'awaiting_confirmation'
            # Stamp delivery metadata
            now_iso = _utc_now_iso()
            ttl_number = _js_number(_js_or(confirmation_target.get('confirmation_ttl_seconds'), 600))
            ttl = max(60, ttl_number) if math.isfinite(ttl_number) else 600
            expires = _utc_shift_iso(ttl)
            confirmation_target['confirmation_delivery_status'] = 'sent'
            confirmation_target['confirmation_delivery_recorded_at'] = (
                confirmation_target.get('confirmation_delivery_recorded_at') or now_iso
            )
            confirmation_target['expires_at'] = expires
        else:
            response_code = 'MISSING_REQUIRED_FIELDS'
            operation_status = 'collecting_details'
    # Confirmation approved (will execute)
    elif response_code in ('EXECUTE_APPROVED', 'CANCEL_APPROVED', 'RESCHEDULE_APPROVED'):
        operation_status = 'executing'
    # Missing fields / confidence review
    elif response_code in ('MISSING_REQUIRED_FIELDS', 'CONFIDENCE_REVIEW_REQUIRED', 'CONFIRMATION_INCOMPLETE'):
        operation_status = 'collecting_details'
    # Handoff
    elif response_code == 'HANDOFF_REQUIRED':
        operation_status = 'pending'
    # Default — conversation only
    else:
        response_code = 'CONVERSATION_ONLY'
        operation_status = 'idle'

    # Defensive current-message guard: the renderer receives conversation-only facts for a standalone greeting.
    if is_standalone_greeting and not model_call_failed:
        response_code = 'CONVERSATION_ONLY'
        operation_status = 'idle'
        retryable = False

    # ── Build safe facts for DeepSeek #2 ──

    def _clinic_name_from_state():
        try:
            ws = _read_node(ctx, 'conversation_state')
            state_data = ws.get('state_data')
            sd = json.loads(state_data) if isinstance(state_data, str) else state_data
            if isinstance(sd, dict):
                return _js_string(_js_or(_dig(sd, 'facts', 'clinic', 'name'), sd.get('clinic_name'), '')).strip() or None
        except Exception:
            pass
        return None

    def _identity_resolution_fact():
        try:
            r = _js_or(execution.get('deterministic_identity_resolution'), current.get('deterministic_identity_resolution'))
            if not isinstance(r, dict):
                return {}
            matches = r.get('doctor_matches') if isinstance(r.get('doctor_matches'), list) else []
            requested = _js_string(_js_or(r.get('requested_doctor_name'), '')).strip()
            first_name = _js_string(_js_or(_dig(matches[0], 'doctor_name') if matches else None, '')).strip()
            return {
                'doctor_resolved': r.get('doctor_resolved') is True,
                'requested_doctor_name': requested or first_name,
                'doctor_matches': matches[:5],
                '_asked': requested,
            }
        except Exception:
            return {}

    facts_clinic_name = (
        _js_string(_js_or(execution.get('clinic_name'), _dig(execution, 'clinic', 'name'), '')).strip()
        or _clinic_name_from_state()
        or None
    )
    identity_resolution_fact = _identity_resolution_fact()

    safe_facts = {
        'turn_intent': turn_intent,
        'certainty': certainty,
        'relation_to_previous_turn': relation_to_previous,
        'response_code': response_code,
        'operation_status': operation_status,
        'conversation_stage': _js_or(decision.get('conversation_stage'), None),
        'required_next_step': _js_or(decision.get('required_next_step'), None),
        'next_step_type': next_step_type,
        'next_step_field': next_step_field,
        'missing_human_fields': missing_human,
        'next_best_missing_human_field': next_best_missing,
        'entities': safe_entities,
        'branch_id': _js_or(_dig(branch_location, 'branch_id'), safe_entities.get('branch_id'), None),
        'branch_name': _js_or(_dig(branch_location, 'branch_name'), safe_entities.get('branch_name'), None),
        'branch_location': branch_location,
        'clinic_location': clinic_location,
        'booking_context': booking_context,
        'confirmation_target': confirmation_target,
        'confirmation_state': confirmation_state,
        'confirmation_action': confirmation_action,
        # Internal appointment UUID is withheld from the Composer facts envelope.
        'appointment_id': None,
        'booking_number': booking_number,
        'queue_number': queue_number,
        'queue_path': queue_path,
        'queue_url': queue_url,
        'queue_expires_at': queue_expires_at,
        'success': success,
        'retryable': retryable,
        'escalation_requested': escalation_requested,
        'handoff_reason': handoff_reason,
        'query_type': query_type,
        'query_scope': query_scope,
        'availability_outcome': (
            'unavailable' if deterministic_unavailable
            else 'error' if deterministic_authority_error
            else _js_or(
                deterministic_lookup.get('availability_outcome'),
                current.get('availability_outcome'),
                'matched' if _truthy(deterministic_lookup.get('matched')) else None,
            )
        ),
        'availability_alternatives': alternative_labels,
        'timezone': canonical_timezone,
        'clinic_name': facts_clinic_name,
        'patient_name': _js_string(_js_or(execution.get('patient_name'), '')).strip() or None,
        'patient_phone': _js_string(_js_or(execution.get('patient_phone'), '')).strip() or None,
        'patient_age': execution.get('patient_age'),
        'patient_address': _js_string(_js_or(execution.get('patient_address'), '')).strip() or None,
        'patient_data_review': _js_or(execution.get('patient_data_review'), current.get('patient_data_review'), None),
        'non_scheduling_turn': is_non_scheduling_turn,
        'patient_message': _js_string(_js_or(normalize_validate_output.get('message_text'), '')).strip() or None,
        # FIX: transparent expired-booking facts for DeepSeek #2
        'prior_expired_booking': prior_expired_booking,
        'greeting_restart': greeting_restart,
        # FIX v7: deterministic doctor resolution info (needed by Extract Single Agent Reply post-check)
        'deterministic_identity_resolution': identity_resolution_fact,
    }

    def _current_agent_reply():
        try:
            vr = _read_node(ctx, 'validate_repaired')
            normalized = vr if vr.get('_contract_status') == 'VALID' else _read_node(ctx, 'normalize_agent_output')
            reply = _js_string(_js_or(normalized.get('agent_reply'), _dig(normalized, 'contract', 'reply'), '')).strip()
            if error_followup_needs_fallback and recovery_generic_reply:
                return None
            return reply or None
        except Exception:
            return None

    # ── Deterministic fallback replies: REMOVED 2026-09-17 ────────────────────
    # This block held one prewritten Arabic sentence per response code, consumed
    # only by Extract Single Agent Reply (since deleted). In the n8n graph it was
    # a safety net handed to the reply composer; the composer now receives the
    # fact catalog and writes the reply itself, so a canned sentence here would be
    # a rigid reply waiting to leak into production. response_code is still
    # returned below for the audit trail.

    current_agent_reply = _current_agent_reply()
    current_reply_trimmed = _js_string(_js_or(current.get('agent_reply'), '')).strip()

    # ── Return facts (no reply — DeepSeek #2 generates it) ──
    return {
        **current,
        'system_decision': {**decision, 'response_code': response_code, 'escalation_requested': escalation_requested},
        'escalation_requested': escalation_requested,
        'agent_reply': None if error_followup_needs_fallback else _js_or(current_agent_reply, current_reply_trimmed, None),
        'facts': safe_facts,
        'response_code': response_code,
        'operation_status': operation_status,
        'success': success,
        'retryable': retryable,
        'appointment_id': _js_or(appointment_id, None),
        'booking_number': booking_number,
        'queue_number': queue_number,
        'queue_path': queue_path,
        'queue_url': queue_url,
        'queue_expires_at': queue_expires_at,
        'patient_name': _js_string(_js_or(execution.get('patient_name'), '')).strip() or None,
        'patient_phone': _js_string(_js_or(execution.get('patient_phone'), '')).strip() or None,
        'missing_human_fields': missing_human,
        'next_best_missing_human_field': next_best_missing,
        'confirmation_target': confirmation_target,
        'contract': contract,
    }
