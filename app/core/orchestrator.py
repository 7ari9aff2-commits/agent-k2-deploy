"""Decision Core v3 — faithful 1:1 Python port of the System Orchestrator (Policy) n8n Code node.

Source node: System Orchestrator (Policy) (extracted/code/System_Orchestrator_Policy.js)

ONE state-table engine produces every scheduling transition. No message_text reads.
No regex semantics. Contract-only decisions. Emits the legacy-compatible envelope the
downstream consumers expect: ``system_decision`` plus top-level ``booking_context`` /
``slot_state`` / ``confirmation_target`` / ``state_patches`` — same key names and the
same nesting as the JS ``resultJson``. n8n's ``[{ json: resultJson }]`` wrapper is
dropped per port conventions: the inner json dict is returned directly.

Public API
----------
``decide(contract_v3, state_data, clinic_context, now_ts=None, **node_outputs) -> dict``

Positional parameters (mirroring the JS runtime inputs):

- ``contract_v3``: the primary model contract v3 (what
  ``Normalize Agent Output (Deterministic)`` emits as ``contract_v3``).
- ``state_data``: persisted conversation state (the ``state_data`` field of the
  ``Get Conversation State`` node output).
- ``clinic_context``: merged clinic context. Each key of the JS ``clinic`` object
  (``clinic_id``, ``patient_id``, ``conversation_id``, ``timezone``,
  ``timezone_configured``, ``now_local_date``, ``now_iso``, ``utc_offset``,
  ``doctor_count``, ``single_doctor_id``, ``single_doctor_name``) is taken from here
  first and falls back to the node outputs below (JS ``||`` semantics).
- ``now_ts``: epoch seconds used as the deterministic replacement for JS
  ``Date.now()`` whenever ``clinic.now_iso`` is missing or unparseable. ``None``
  (default) falls back to the wall clock, exactly like the JS.

Optional keyword-only parameters — node outputs read by the JS via
``$(NodeName).first().json`` / ``$json``, all defaulting to ``{}``:

- ``normalize_agent_output``: output of ``Normalize Agent Output (Deterministic)`` —
  supplies the companion v2 ``contract`` and ``_normalization`` for the primary v3.
- ``validate_repaired_contract``: output of ``Validate Repaired Contract
  (Deterministic)`` — when its ``_contract_status`` is ``'VALID'`` its
  ``contract_v3`` / ``contract`` / ``_normalization`` win and
  ``decision_engine_source`` becomes ``'self_repair'``.
- ``apply_resolved_booking_ids``: output of ``Apply Resolved Booking IDs
  (Deterministic)`` — supplies ``resolved`` (doctor/service/appointment ids,
  ``resolver_result``) and, when its ``contract_v3.entities.appointment_id`` is set,
  overrides the contract v3 (BUGFIX 2026-09-09 preserved).
- ``normalize_validate``: output of ``Normalize & Validate`` — supplies
  ``clinic_id`` / ``patient_id`` / ``conversation_id`` / ``message_id`` /
  ``idempotency_key`` / ``operation_id``.
- ``get_clinic_context``: output of ``Get Clinic Context`` — supplies
  ``doctor_count``, ``single_doctor_id``, ``single_doctor_name``.
- ``validate_patient_ownership``: output of ``Validate Patient Ownership`` — supplies
  ``canonical_time_context`` (``timezone`` / ``now_local_date`` / ``now_iso`` /
  ``utc_offset`` / ``timezone_configured`` / ``timezone_error_code``).
- ``p1_7_patient_field_normalization``: output of ``P1.7 Patient Field
  Normalization`` — supplies ``p17_extracted_phone`` / ``p17_extracted_age`` /
  ``p17_extracted_address`` which are merged over the contract entities.
- ``current_item``: the current n8n item json (JS ``$json``) — supplies
  ``deterministic_slot_lookup``, ``operation_id``, ``availability_outcome``,
  ``availability_alternatives``, ``_normalization``, and is spread at the top of the
  output envelope (the JS ``...$json``).

Internal note: the JS file has a single ``decide(input)`` state-table function; here
that engine is ``_decide_state_table`` (the only forced rename, because the public
entry point is named ``decide``), and the module-level orchestration around it is the
public ``decide``.

Pure function: no I/O, no logging, stdlib only.
"""

import calendar
import math
import re
import time
from datetime import datetime, timezone
from app.core.js_semantics import dict_or_empty as _dict, first_not_none as _first_not_none, iso_from_ms as _iso_from_ms, js_is_integer as _js_is_integer, js_or as _js_or
from app.core.js_semantics import truthy as _truthy

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

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


def to_english_digits(s):
    """Source node: System Orchestrator (Policy) — toEnglishDigits helper."""
    return ''.join(ARABIC_INDIC.get(ch, ch) for ch in _js_string(s))


ISO_DATE = re.compile(r'\d{4}-\d{2}-\d{2}')
TIME_24 = re.compile(r'([01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?')
UUID_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', re.IGNORECASE)


def iso_date_valid(date_iso):
    """Source node: System Orchestrator (Policy) — isoDateValid helper."""
    s = _js_string(_js_or(date_iso, ''))
    if not ISO_DATE.fullmatch(s):
        return False
    try:
        datetime.strptime(s + 'T12:00:00Z', '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
        return True
    except ValueError:
        return False


def normalize_time(value):
    """Source node: System Orchestrator (Policy) — normalizeTime helper."""
    t = _js_string(_js_or(value, '')).strip()
    if not TIME_24.fullmatch(t):
        return None
    return t[:5] if len(t) == 8 else (t + ':00' if len(t) == 4 else t)


# Message classification comes exclusively from contract fields — never text.
def message_class(contract, state):
    """Source node: System Orchestrator (Policy) — messageClass helper."""
    c = _dict(contract)
    turn = _dict(c.get('turn'))
    intent = str(_js_or(turn.get('intent'), 'unclear'))
    conf_intent = str(_js_or(_dict(c.get('confirmation')).get('intent'), 'none'))
    sel_kind = str(_js_or(_dict(c.get('selection')).get('kind'), 'none'))
    op_type = str(_js_or(_dict(c.get('operation_proposal')).get('type'), ''))
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


STATES = {
    'IDLE': 'IDLE', 'ASK_DOCTOR': 'ASK_DOCTOR', 'ASK_VISIT_TYPE': 'ASK_VISIT_TYPE',
    'COLLECT_PATIENT_DATA': 'COLLECT_PATIENT_DATA', 'CONFIRM_PATIENT_DATA': 'CONFIRM_PATIENT_DATA',
    'ASK_DATE': 'ASK_DATE', 'AWAIT_SLOT_CHOICE': 'AWAIT_SLOT_CHOICE',
    'AWAIT_CONFIRMATION': 'AWAIT_CONFIRMATION', 'EXECUTING': 'EXECUTING', 'COMPLETED': 'COMPLETED'
}
_STATE_VALUES = frozenset(STATES.values())
OFFER_TTL_SECONDS = 600
CONFIRM_TTL_SECONDS = 600


def uuid_valid(v):
    """Source node: System Orchestrator (Policy) — uuidValid helper."""
    return UUID_RE.fullmatch(_js_string(_js_or(v, ''))) is not None


def now_ms(clinic, now_ts=None):
    """Source node: System Orchestrator (Policy) — nowMs helper.

    ``now_ts`` (epoch seconds) replaces the JS ``Date.now()`` fallback so callers can
    drive the engine deterministically; ``None`` falls back to the wall clock.
    """
    t = _date_parse_ms(_dict(clinic).get('now_iso'))
    if t is not None:
        return t
    if now_ts is not None:
        return int(now_ts * 1000)
    return int(time.time() * 1000)


# Deterministic UUID mint (pure): FNV hash of a seed, formatted 8-4-4-4-12
# with version 4 / variant bits so it passes strict UUID validation.
def mint_uuid(seed):
    """Source node: System Orchestrator (Policy) — mintUuid helper (FNV-1a + murmur finalizer, bit-exact)."""
    h = 2166136261
    s = '' if seed is None else _js_string(seed)
    for cp in _utf16_code_units(s):  # mirrors s.codePointAt(i) over the JS UTF-16 loop
        h ^= cp
        h = (h * 16777619) & 0xFFFFFFFF

    def hex_digits(n):
        nonlocal h
        out = []
        for _ in range(n):
            h = ((h ^ (h >> 13)) * 0x5BD1E995) & 0xFFFFFFFF
            out.append(format(h & 15, 'x'))
        return ''.join(out)

    return (hex_digits(8) + '-' + hex_digits(4) + '-4' + hex_digits(3) + '-'
            + format(8 + (h % 4), 'x') + hex_digits(3) + '-' + hex_digits(12))


# ── Live-artifact validity ──
def offer_live(offer, clinic, state, now_ts=None):
    """Source node: System Orchestrator (Policy) — offerLive helper (``state`` unused, kept for fidelity)."""
    if not isinstance(offer, dict):
        return False
    clinic = _dict(clinic)
    if _truthy(clinic.get('clinic_id')) and _truthy(offer.get('clinic_id')) \
            and str(offer.get('clinic_id')) != str(clinic.get('clinic_id')):
        return False
    if _truthy(clinic.get('conversation_id')) and _truthy(offer.get('conversation_id')) \
            and str(offer.get('conversation_id')) != str(clinic.get('conversation_id')):
        return False
    exp = _date_parse_ms(_js_string(_js_or(offer.get('expires_at'), '')))
    if exp is None:
        return False
    return exp > now_ms(clinic, now_ts)


def target_live(target, clinic, now_ts=None):
    """Source node: System Orchestrator (Policy) — targetLive helper."""
    if not isinstance(target, dict):
        return False
    exp = _date_parse_ms(_js_string(_js_or(target.get('expires_at'), '')))
    if exp is not None and exp <= now_ms(_dict(clinic), now_ts):
        return False
    return True


def target_binding_valid(target, state, clinic, now_ts=None):
    """Source node: System Orchestrator (Policy) — targetBindingValid helper (``state`` unused, kept for fidelity)."""
    if not isinstance(target, dict):
        return False
    action = str(target.get('action') or '')
    clinic = _dict(clinic)
    if action in ('create_appointment',):
        return (uuid_valid(target.get('doctor_id')) and uuid_valid(target.get('slot_id'))
            and iso_date_valid(target.get('date')) and bool(normalize_time(target.get('time')))
            and str(_js_or(target.get('appointment_type'), '')) in ('NEW_VISIT', 'FOLLOW_UP')
            and (not _truthy(clinic) or not _truthy(clinic.get('clinic_id'))
                 or str(_js_or(target.get('clinic_id'), clinic.get('clinic_id'))) == str(clinic.get('clinic_id')))
            and (not _truthy(clinic) or not _truthy(clinic.get('patient_id'))
                 or str(_js_or(target.get('patient_id'), clinic.get('patient_id'))) == str(clinic.get('patient_id')))
            and (not _truthy(clinic) or not _truthy(clinic.get('conversation_id'))
                 or str(_js_or(target.get('conversation_id'), clinic.get('conversation_id'))) == str(clinic.get('conversation_id'))))
    if action == 'cancel_appointment':
        return uuid_valid(target.get('appointment_id'))
    if action == 'reschedule_appointment':
        return uuid_valid(target.get('appointment_id')) and uuid_valid(target.get('new_slot_id'))
    return False


# ── Patient data ──
def patient_fields_from(state, contract):
    """Source node: System Orchestrator (Policy) — patientFieldsFrom helper."""
    state = _dict(state)
    bc = _dict(_js_or(state.get('booking_context'), {}))
    pdr = state.get('patient_data_review')
    review = _dict(_js_or(_dict(pdr).get('fields') if isinstance(pdr, dict) else None, {}))
    facts_root = state.get('facts')
    facts = _dict(_js_or(_dict(facts_root).get('patient') if isinstance(facts_root, dict) else None, {}))
    ent = _dict(_js_or(_dict(contract).get('entities') if isinstance(contract, dict) else None, {}))

    def pick(*vals):
        for v in vals:
            if v is not None and str(v).strip() != '':
                return v
        return None

    return {
        'patient_name': pick(ent.get('patient_name'), bc.get('patient_name'), review.get('name'), facts.get('name')),
        'patient_phone': pick(ent.get('patient_phone'), bc.get('patient_phone'), review.get('phone'), facts.get('phone')),
        'patient_age': _first_not_none(ent.get('patient_age'), bc.get('patient_age'), review.get('age'), facts.get('age')),
        'patient_address': pick(ent.get('patient_address'), bc.get('patient_address'), review.get('address'), facts.get('address'))
    }


def patient_data_complete(fields, appointment_type):
    """Source node: System Orchestrator (Policy) — patientDataComplete helper.
    Outpatient clinical policy: patient_name and patient_phone are the essential required identity fields.
    """
    fields = _dict(fields)
    return bool(fields.get('patient_name') and fields.get('patient_phone')
        and str(_js_or(appointment_type, '')))


# ── Legacy shim: map pre-v3 persisted state to a v3 state ──
def shim_state(state, clinic, now_ts=None):
    """Source node: System Orchestrator (Policy) — shimState helper."""
    if not isinstance(state, dict):
        return STATES['IDLE']
    legacy = str(state.get('operation_state') or '').upper()
    target = state.get('confirmation_target') or None
    if legacy == 'AWAITING_CONFIRMATION' or (
            _truthy(target) and target_live(target, clinic, now_ts)
            and str(_js_or(_dict(target).get('confirmation_delivery_status'),
                            _dict(target).get('delivery'), 'pending')) in ('proposed', 'sent', 'pending')):
        return STATES['AWAIT_CONFIRMATION']
    if offer_live(_js_or(state.get('presented_offer'), state.get('pending_offer')), clinic, state, now_ts):
        return STATES['AWAIT_SLOT_CHOICE']
    if legacy in ('COMPLETED',):
        return STATES['COMPLETED']
    if legacy in ('CANCELLED', 'FAILED_FINAL'):
        return STATES['IDLE']
    stage = str(state.get('conversation_stage') or '')
    if stage == 'WAITING_PATIENT_DATA_CONFIRMATION':
        return STATES['CONFIRM_PATIENT_DATA']
    bc = _dict(_js_or(state.get('booking_context'), {}))
    has_doctor = bool(bc.get('doctor_id') or bc.get('doctor_name'))
    if not has_doctor and not bc.get('date'):
        return STATES['ASK_DOCTOR'] if legacy else STATES['IDLE']
    if has_doctor and not bc.get('appointment_type'):
        return STATES['ASK_VISIT_TYPE']
    fields = patient_fields_from(state, None)
    if not patient_data_complete(fields, bc.get('appointment_type')):
        return STATES['COLLECT_PATIENT_DATA']
    if not bc.get('date'):
        return STATES['ASK_DATE']
    return STATES['ASK_DATE']


def current_state_of(state, clinic, now_ts=None):
    """Source node: System Orchestrator (Policy) — currentStateOf helper."""
    state = _dict(state)
    sm = state.get('state_machine')
    cs = str(_js_or(sm.get('current_state'), '')) if isinstance(sm, dict) else ''
    if cs and cs in _STATE_VALUES:
        return cs
    return shim_state(state, clinic, now_ts)


# ── Selection binding against a live presented_offer ──
def match_offered_alternative(offer, contract):
    """Source node: System Orchestrator (Policy) — matchOfferedAlternative helper."""
    alts = offer.get('alternatives') if isinstance(offer, dict) and isinstance(offer.get('alternatives'), list) else []
    if not alts:
        return None
    contract = _dict(contract)
    sel = _dict(_js_or(contract.get('selection'), {}))
    if sel.get('kind') == 'presented_rank' and _js_is_integer(sel.get('rank')):
        for a in alts:
            if _js_number(_dict(a).get('rank')) == _js_number(sel.get('rank')):
                return a
        return None
    if sel.get('kind') == 'any' or _dict(contract.get('confirmation')).get('intent') == 'affirmative':
        def _rank_key(a):
            n = _js_number(_dict(a).get('rank'))
            if math.isnan(n) or n == 0:  # JS: (Number(a.rank) || 99)
                return 99
            return n
        return _js_or(sorted(alts, key=_rank_key)[0], None)
    if sel.get('kind') == 'presented_match':
        def strip_hamza(v):  # defined in the JS source but never applied there — kept for fidelity
            return re.sub('[أإآ]', 'ا', _js_string(v))
        for a in alts:
            a = _dict(a)
            date_ok = True
            time_ok = True
            if sel.get('date'):
                date_ok = str(_js_or(a.get('local_date'), a.get('date'), '')) == str(sel.get('date'))
            if sel.get('time'):
                time_ok = str(_js_or(a.get('local_time'), a.get('time'), ''))[:5] == str(sel.get('time'))[:5]
            if date_ok and time_ok:
                return a
        return None
    return None


# ── Confirmation target builders ──
def build_create_target(inp, slot, verification_status, now_ts=None):
    """Source node: System Orchestrator (Policy) — buildCreateTarget helper."""
    slot = _dict(slot)
    state = _dict(inp.get('state'))
    clinic = _dict(inp.get('clinic'))
    lineage = _dict(inp.get('lineage'))
    resolved = _dict(inp.get('resolved'))
    contract = _dict(inp.get('contract'))
    bc = _dict(_js_or(state.get('booking_context'), {}))
    offer = _js_or(state.get('presented_offer'), state.get('pending_offer'), None)
    fields = patient_fields_from(state, contract)
    ent = _dict(contract.get('entities'))
    # P42b (RG-5): default to NEW_VISIT at build time — a null type could never pass
    # the strict binding check, producing a deterministic confirm-stall.
    appointment_type = _js_or(bc.get('appointment_type'),
                              _dict(offer).get('appointment_type') if _truthy(offer) else None,
                              ent.get('visit_type'), 'NEW_VISIT')
    if _truthy(lineage.get('operation_id')):
        operation_id = lineage.get('operation_id')
    elif _truthy(lineage.get('idempotency_key')):
        operation_id = _js_cat(lineage.get('idempotency_key'), ':create_appointment')
    else:
        operation_id = mint_uuid(_js_cat('op:', clinic.get('conversation_id'), ':', _js_or(lineage.get('message_id'), '')))
    return {
        'schema_version': 3,
        'action': 'create_appointment',
        'clinic_id': _js_or(clinic.get('clinic_id'), None),
        'patient_id': _js_or(clinic.get('patient_id'), None),
        'conversation_id': _js_or(clinic.get('conversation_id'), None),
        'doctor_id': _js_or(slot.get('doctor_id'), bc.get('doctor_id'), resolved.get('doctor_id'), None),
        'doctor_name': _js_or(slot.get('doctor_name'), bc.get('doctor_name'), resolved.get('doctor_name'), None),
        'service_id': _js_or(slot.get('service_id'), bc.get('service_id'),
                             _dict(offer).get('service_id') if _truthy(offer) else None, None),
        'service_name': _js_or(slot.get('service_name'), bc.get('service_name'), None),
        'slot_id': _js_or(slot.get('slot_id'), None),
        'date': _js_or(slot.get('local_date'), slot.get('date'), None),
        'time': _js_string(_js_or(slot.get('local_time'), slot.get('time'), ''))[:5] or None,
        'appointment_type': appointment_type if appointment_type in ('NEW_VISIT', 'FOLLOW_UP') else None,
        'patient_name': fields.get('patient_name'), 'patient_phone': fields.get('patient_phone'),
        'patient_age': fields.get('patient_age'), 'patient_address': fields.get('patient_address'),
        'operation_id': operation_id,
        'last_user_message_id_at_request': _js_or(lineage.get('message_id'), None),
        'confirmation_id': mint_uuid(_js_cat('confirm:', operation_id)),
        'confirmation_ttl_seconds': CONFIRM_TTL_SECONDS,
        'expires_at': _iso_from_ms(now_ms(clinic, now_ts) + CONFIRM_TTL_SECONDS * 1000),
        'verification_status': verification_status,
        'delivery': 'pending',
        'source': 'state_table_v3'
    }


# ── Turn directive (model guidance facts, no patient-facing strings) ──
def directive_for(row_id, ctx):
    """Source node: System Orchestrator (Policy) — directiveFor helper (ctx keys keep the JS camelCase names)."""
    d = {'rule': row_id}
    if row_id == 'ask_doctor':
        d['must_ask'] = ['doctor']
    elif row_id == 'ask_visit_type':
        d['must_ask'] = ['visit_type']
    elif row_id == 'collect_patient_data':
        d['must_ask'] = _js_or(ctx.get('missingPatientFields'), ['patient_name', 'patient_phone'])
    elif row_id == 'confirm_patient_data':
        d['must_show_review'] = _js_or(ctx.get('reviewFields'), None)
    elif row_id == 'ask_date':
        d['must_ask'] = ['date']
    elif row_id == 'present_alternatives':
        d['present_alternatives'] = {'max': 4}
    elif row_id == 'propose_confirm':
        d['must_propose_confirm'] = True
        d['confirm_facts'] = _js_or(ctx.get('confirmFacts'), None)
    locked = ctx.get('lockedFields')
    if isinstance(locked, (list, str)) and len(locked):  # JS: ctx.lockedFields && ctx.lockedFields.length
        d['locked_fields'] = locked
    return d


def locked_fields_of(state, contract):
    """Source node: System Orchestrator (Policy) — lockedFieldsOf helper."""
    bc = _dict(_js_or(_dict(state).get('booking_context'), {}))
    ent = _dict(_js_or(_dict(contract).get('entities'), {}))
    locked = []
    if bc.get('doctor_id') or bc.get('doctor_name') or ent.get('doctor_name'):
        locked.append('doctor')
    if bc.get('appointment_type') or ent.get('visit_type'):
        locked.append('visit_type')
    if bc.get('patient_name') or ent.get('patient_name'):
        locked.append('patient_name')
    if bc.get('patient_phone') or ent.get('patient_phone'):
        locked.append('patient_phone')
    merged_age = bc.get('patient_age') if bc.get('patient_age') is not None else ent.get('patient_age')
    if merged_age is not None:  # JS: (bc.patient_age ?? ent.patient_age) !== undefined && ... !== null
        locked.append('patient_age')
    if bc.get('patient_address') or ent.get('patient_address'):
        locked.append('patient_address')
    if bc.get('date'):
        locked.append('date')
    return locked


# v39: true only when a complete patient profile exists but NO review record
# exists yet (never reviewed). A pending review is owned by the
# CONFIRM_PATIENT_DATA row; a confirmed review must never re-fire.
def saved_record_pending_review(state, contract):
    """Source node: System Orchestrator (Policy) — savedRecordPendingReview helper (``contract`` unused, kept for fidelity)."""
    if not isinstance(state, dict):
        return False
    review = state.get('patient_data_review') if isinstance(state.get('patient_data_review'), dict) else None
    review_status = str(_js_or(review.get('status'), '')).strip().lower() if review else ''
    if review_status:
        return False
    return True


def missing_patient_fields(state_like, contract):
    """Source node: System Orchestrator (Policy) — missingPatientFields helper."""
    f = patient_fields_from(state_like, contract)
    missing = []
    if not f.get('patient_name'):
        missing.append('patient_name')
    if not f.get('patient_phone'):
        missing.append('patient_phone')
    return missing


def review_from(state_like, source):
    """Source node: System Orchestrator (Policy) — reviewFrom helper."""
    bc = _dict(_js_or(_dict(state_like).get('booking_context'), {}))
    return {
        'fields': {'name': _js_or(bc.get('patient_name'), None),
                   'age': bc.get('patient_age') if bc.get('patient_age') is not None else None,
                   'phone': _js_or(bc.get('patient_phone'), None),
                   'address': _js_or(bc.get('patient_address'), None)},
        'status': 'pending_confirmation', 'source': source
    }


def build_offer(inp, alternatives, now_ts=None):
    """Source node: System Orchestrator (Policy) — buildOffer helper."""
    clinic = _dict(inp.get('clinic'))
    state = _dict(inp.get('state'))
    resolved = _dict(inp.get('resolved'))
    bc = _dict(_js_or(state.get('booking_context'), {}))
    contract = _dict(inp.get('contract'))
    now = now_ms(clinic, now_ts)
    first = alternatives[0] if alternatives and isinstance(alternatives[0], dict) else {}
    mapped = []
    for i, raw_slot in enumerate(alternatives[:4]):
        slot = raw_slot if isinstance(raw_slot, dict) else {}
        mapped.append({
            'rank': _js_number(_js_or(slot.get('rank'), i + 1)),
            'slot_id': _js_or(slot.get('slot_id'), None),
            'start_time': _js_or(slot.get('start_time'), None),
            'local_date': _js_or(slot.get('local_date'), slot.get('date'), None),
            'local_time': _js_string(_js_or(slot.get('local_time'), slot.get('time'), ''))[:5] or None,
            'label': _js_or(slot.get('label'), None),
            'doctor_id': _js_or(slot.get('doctor_id'), None),
            'service_id': _js_or(slot.get('service_id'), None),
            'clinic_id': _js_or(slot.get('clinic_id'), clinic.get('clinic_id'), None),
            'slot_status': _js_or(slot.get('slot_status'), 'available')
        })
    return {
        'schema_version': 2,
        'kind': 'presented_offer',
        'offered_at': _iso_from_ms(now),
        'expires_at': _iso_from_ms(now + OFFER_TTL_SECONDS * 1000),
        'clinic_id': _js_or(clinic.get('clinic_id'), None),
        'patient_id': _js_or(clinic.get('patient_id'), None),
        'conversation_id': _js_or(clinic.get('conversation_id'), None),
        'doctor_id': _js_or(first.get('doctor_id'), bc.get('doctor_id'), resolved.get('doctor_id'), None),
        'doctor_name': _js_or(first.get('doctor_name'), bc.get('doctor_name'), resolved.get('doctor_name'), None),
        'service_id': _js_or(first.get('service_id'), bc.get('service_id'), None),
        'appointment_type': _js_or(bc.get('appointment_type'), _dict(contract.get('entities')).get('visit_type'), None),
        'alternatives': mapped
    }


# ── The state-table engine (JS ``decide(input)``) ──
def _decide_state_table(inp, now_ts=None):
    """Source node: System Orchestrator (Policy) — the main decide(input) entry point.

    ``inp`` mirrors the JS input object: {state, contract, clinic, resolved, lookup, lineage}.
    First-match order of the rows below is load-bearing — keep it exactly.
    """
    state = _dict(inp.get('state'))
    contract = _dict(inp.get('contract'))
    clinic = _dict(inp.get('clinic'))
    resolved = _dict(inp.get('resolved'))
    lookup_in = inp.get('lookup')
    lookup = lookup_in if isinstance(lookup_in, dict) and lookup_in.get('executed') is True else None
    lineage = _dict(inp.get('lineage'))

    cs = current_state_of(state, clinic, now_ts)
    cls = message_class(contract, state)
    veto_reasons = []
    bc0 = _dict(_js_or(state.get('booking_context'), {}))
    raw_offer = _js_or(state.get('presented_offer'), state.get('pending_offer'))
    offer = raw_offer if offer_live(raw_offer, clinic, state, now_ts) else None
    prior_target = state.get('confirmation_target') if target_live(state.get('confirmation_target'), clinic, now_ts) else None
    patches = {
        'state_machine': {'current_state': cs, 'previous_state': cs, 'last_decision_rule': None,
                          'updated_at': _js_or(clinic.get('now_iso'), None)},
        'presented_offer': _js_or(state.get('presented_offer'), state.get('pending_offer'), None),
        'confirmation_target': _js_or(state.get('confirmation_target'), None),
        'turn_directive': None,
        'booking_context': dict(bc0),
        'patient_data_review': _js_or(state.get('patient_data_review'), None),
        'unclear_count': _number_or_zero(state.get('unclear_count')),
        'response_code_history': list(state['response_code_history'])[-4:]
            if isinstance(state.get('response_code_history'), list) else [],
        'progress_this_turn': False
    }
    if cls != 'unclear':
        patches['unclear_count'] = 0

    def finish(rule_id, next_state, response_code, extra=None):
        extra = extra or {}
        patches['state_machine'] = {'current_state': next_state, 'previous_state': cs,
                                    'last_decision_rule': rule_id, 'updated_at': _js_or(clinic.get('now_iso'), None)}
        mf = _js_or(extra.get('missing_fields'), [])
        decision = {
            'allowed': extra.get('allowed') is True,
            'action': _js_or(extra.get('action'), None),
            'response_code': response_code,
            'decision_rule': rule_id,
            'booking_context': patches['booking_context'],
            'confirmation_target': patches['confirmation_target'],
            'confirmation_state': _js_or(extra.get('confirmation_state'), None),
            'patient_data_complete': extra.get('patient_data_complete') is True,
            'patient_data_gate_satisfied': extra.get('patient_data_gate_satisfied') is True,
            'missing_fields': _js_or(extra.get('missing_fields'), []),
            'missing_human_fields': _js_or(extra.get('missing_fields'), []),
            'next_best_missing_human_field': mf[0] if isinstance(mf, list) and mf and _truthy(mf[0]) else None,
            'escalation_requested': extra.get('escalation_requested') is True,
            'handoff_reason': _js_or(extra.get('handoff_reason'), None),
            'new_booking_restart': extra.get('new_booking_restart') is True,
            'non_scheduling_turn': extra.get('non_scheduling_turn') is True,
            'availability_inquiry': cls == 'availability_inquiry',
            'deterministic_slot_lookup': _js_or(lookup, None),
            'contract': contract,
            'turn_directive': patches['turn_directive'],
            'state_machine': patches['state_machine'],
            'veto_reasons': veto_reasons
        }
        # MODEL-FIRST 2026-09-03: loop breaker removed. The model manages the
        # dialogue; repetition never escalates to a human. Only contract.escalate does.
        hist = patches['response_code_history']
        patches['response_code_history'] = (hist + [
            {'code': response_code, 'state': cs, 'rule': rule_id, 'at': _js_or(clinic.get('now_iso'), None)}
        ])[-4:]
        return {'next_state': next_state, 'decision_rule': rule_id, 'veto_reasons': veto_reasons,
                'system_decision': decision, 'state_patches': patches, 'lookup_request': None}

    # ── Global rows ──
    if contract.get('escalate') is True:
        return finish('escalate_requested', cs, 'HANDOFF_REQUIRED',
                      {'escalation_requested': True, 'handoff_reason': _js_or(contract.get('handoff_reason'), 'model_escalation')})

    # R1 — fresh booking restart: only intent+relation, never text, and never while
    # a live offer/target is being confirmed.
    relation = str(_js_or(_dict(contract.get('turn')).get('relation_to_previous_turn'), 'none'))
    fresh_booking_restart = (cls == 'booking'
        and str(_js_or(_dict(contract.get('turn')).get('intent'), '')) == 'booking_request'
        and relation == 'new_request' and not _truthy(offer) and not _truthy(prior_target))
    if fresh_booking_restart:
        # v32: a restart replaces the doctor/service/slot under discussion, but
        # facts already collected for the SAME patient (visit type, patient
        # identity) stay valid. Only wipe them when the message introduces a
        # DIFFERENT patient identity. Without this, a mid-booking doctor switch
        # re-asked visit_type (exec 8260) while the sent reply asked for the date,
        # and the next turn (8273) got contradictory inputs and burned its whole
        # 4000-token budget on an empty reply.
        prev_bc = _dict(_js_or(state.get('booking_context'), {}))
        ent_c = _dict(contract.get('entities'))
        prev_name = str(_js_or(prev_bc.get('patient_name'), '')).strip()
        prev_phone = str(_js_or(prev_bc.get('patient_phone'), '')).strip()
        ent_name = str(_js_or(ent_c.get('patient_name'), '')).strip()
        ent_phone = str(_js_or(ent_c.get('patient_phone'), '')).strip()
        patient_identity_changed = (ent_name and prev_name and ent_name != prev_name) \
            or (ent_phone and prev_phone and ent_phone != prev_phone)
        patches['booking_context'] = {}
        patches['presented_offer'] = None
        patches['confirmation_target'] = None
        patches['patient_data_review'] = None
        patches['progress_this_turn'] = True
        if not patient_identity_changed:
            slot_state_v = state.get('slot_state')
            known_visit_type = _js_or(prev_bc.get('appointment_type'),
                                      _dict(slot_state_v).get('appointment_type') if isinstance(slot_state_v, dict) else None,
                                      None)
            if known_visit_type and str(known_visit_type).strip().upper() in ('NEW_VISIT', 'FOLLOW_UP'):
                patches['booking_context']['appointment_type'] = str(known_visit_type).strip().upper()

    if cls == 'unclear':
        # MODEL-FIRST 2026-09-03: unclear turns keep asking for clarification.
        # Auto-handoff removed; only the model may request a human via contract.escalate.
        patches['unclear_count'] = _number_or_zero(state.get('unclear_count')) + 1
        return finish('unclear_clarify', cs, 'CONVERSATION_ONLY', {'non_scheduling_turn': True})

    if cls == 'small_talk' or cls == 'clinic_query':
        # No reset, no state change; a live operation simply pauses.
        return finish('small_talk_hold' if cls == 'small_talk' else 'clinic_query_hold', cs, 'CONVERSATION_ONLY',
                      {'non_scheduling_turn': True})

    # ── Lookup result rows (a deterministic lookup ran this turn) ──
    if _truthy(lookup):
        alts = lookup.get('alternatives') if isinstance(lookup.get('alternatives'), list) else []
        outcome = str(_js_or(lookup.get('availability_outcome'), '')).lower()
        code = str(_js_or(lookup.get('result_code'), '')).upper()
        if outcome == 'authority_error' or code == 'AVAILABILITY_SOURCE_ERROR':
            return finish('lookup_authority_error', cs, 'AVAILABILITY_SOURCE_ERROR', {})
        if lookup.get('search_mode') == 'exact_slot':
            slot = alts[0] if alts else None
            if lookup.get('slot_found') is True and _truthy(slot) and uuid_valid(_dict(slot).get('slot_id')):
                target = build_create_target(inp, slot, 'exact_verified', now_ts)
                if target_binding_valid(target, state, clinic, now_ts):
                    patches['confirmation_target'] = target
                    complete = patient_data_complete(patient_fields_from(state, contract), target.get('appointment_type'))
                    if not complete:
                        missing = missing_patient_fields(state, contract)
                        patches['turn_directive'] = directive_for('collect_patient_data', {
                            'missingPatientFields': missing, 'lockedFields': locked_fields_of(patches, contract)})
                        return finish('slot_verified_need_patient_data', STATES['COLLECT_PATIENT_DATA'],
                                      'MISSING_REQUIRED_FIELDS', {'missing_fields': missing,
                                                                   'patient_data_complete': False,
                                                                   'patient_data_gate_satisfied': False})
                    patches['turn_directive'] = directive_for('propose_confirm', {
                        'confirmFacts': {'doctor_name': target.get('doctor_name'), 'date': target.get('date'),
                                         'time': target.get('time'), 'appointment_type': target.get('appointment_type')},
                        'lockedFields': locked_fields_of(state, contract)})
                    return finish('selection_bound_verified', STATES['AWAIT_CONFIRMATION'], 'CONFIRMATION_REQUIRED',
                                  {'confirmation_state': 'proposed', 'patient_data_complete': complete,
                                   'patient_data_gate_satisfied': complete})
                veto_reasons.append('target_invariants_failed_after_exact_verification')
                return finish('selection_bind_rejected', STATES['ASK_DATE'], 'MISSING_REQUIRED_FIELDS',
                              {'missing_fields': ['date']})
            if alts:
                patches['presented_offer'] = build_offer(inp, alts, now_ts)
                patches['turn_directive'] = directive_for('present_alternatives', {'lockedFields': locked_fields_of(state, contract)})
                if isinstance(patches.get('booking_context'), dict):  # V37-NO-PHANTOM-DATE: verified offer/outcome supersedes the raw requested date
                    patches['booking_context']['date'] = None
                    patches['booking_context']['time'] = None
                return finish('exact_unavailable_nearest_offered', STATES['AWAIT_SLOT_CHOICE'], 'SLOT_UNAVAILABLE', {})
            patches['presented_offer'] = None
            patches['turn_directive'] = directive_for('ask_date', {'lockedFields': locked_fields_of(state, contract)})
            if isinstance(patches.get('booking_context'), dict):  # V37-NO-PHANTOM-DATE: verified offer/outcome supersedes the raw requested date
                patches['booking_context']['date'] = None
                patches['booking_context']['time'] = None
            return finish('exact_unavailable_ask_again', STATES['ASK_DATE'], _js_or(code, 'SLOT_UNAVAILABLE'), {})
        # requested_window / nearby_alternatives
        if alts:
            patches['presented_offer'] = build_offer(inp, alts, now_ts)
            patches['progress_this_turn'] = True
            patches['turn_directive'] = directive_for('present_alternatives', {'lockedFields': locked_fields_of(state, contract)})
            if isinstance(patches.get('booking_context'), dict):  # V37-NO-PHANTOM-DATE: verified offer/outcome supersedes the raw requested date
                patches['booking_context']['date'] = None
                patches['booking_context']['time'] = None
            return finish('alternatives_presented', STATES['AWAIT_SLOT_CHOICE'], 'AVAILABILITY_RESULTS', {})
        patches['presented_offer'] = None
        patches['turn_directive'] = directive_for('ask_date', {'lockedFields': locked_fields_of(state, contract)})
        if isinstance(patches.get('booking_context'), dict):  # V37-NO-PHANTOM-DATE: verified offer/outcome supersedes the raw requested date
            patches['booking_context']['date'] = None
            patches['booking_context']['time'] = None
        return finish('window_unavailable', STATES['ASK_DATE'], _js_or(code, 'NO_AVAILABLE_SLOTS'), {})

    # ── Cancel / reschedule rows (own targets, own confirmations) ──
    if cls == 'cancel_request' or cls == 'reschedule_request':
        facts_root = state.get('facts')
        booking_facts = _dict(_dict(facts_root).get('booking') if isinstance(facts_root, dict) else None)
        appt_id = _js_or(_dict(contract.get('entities')).get('appointment_id'),
                         resolved.get('appointment_id'),
                         state.get('appointment_id'),
                         booking_facts.get('appointment_id'),
                         _dict(state.get('booking_context')).get('appointment_id'),
                         _dict(state.get('slot_state')).get('appointment_id'),
                         None)
        if not uuid_valid(appt_id):
            return finish('cancel_need_appointment' if cls == 'cancel_request' else 'reschedule_need_appointment',
                          cs, 'MISSING_REQUIRED_FIELDS', {'missing_fields': ['appointment_id']})
        # RESCHEDULE FIX: a reschedule requires a resolved NEW slot plus the appointment's
        # real old slot (from the resolver), not the conversation's slot_state. When the new
        # slot isn't resolved yet, ask for the new date instead of proposing a confirmation.
        is_reschedule = cls == 'reschedule_request'
        slot_state_v = state.get('slot_state')
        slot_state = _dict(slot_state_v) if isinstance(slot_state_v, dict) else {}
        old_slot_id = _js_or(resolved.get('expected_old_slot_id'), slot_state.get('slot_id'), None) if is_reschedule \
            else _js_or(slot_state.get('slot_id'), None)
        new_slot_id = _js_or(resolved.get('new_slot_id'), None) if is_reschedule else None
        if is_reschedule and not uuid_valid(new_slot_id):
            patches['turn_directive'] = directive_for('ask_date', {'lockedFields': locked_fields_of(state, contract)})
            return finish('reschedule_need_new_slot', STATES['ASK_DATE'], 'MISSING_REQUIRED_FIELDS',
                          {'missing_fields': ['date']})
        target = {
            'schema_version': 3,
            'action': 'cancel_appointment' if cls == 'cancel_request' else 'reschedule_appointment',
            'clinic_id': _js_or(clinic.get('clinic_id'), None), 'patient_id': _js_or(clinic.get('patient_id'), None),
            'conversation_id': _js_or(clinic.get('conversation_id'), None),
            'appointment_id': appt_id,
            'booking_number': _js_or(_dict(contract.get('entities')).get('booking_number'), state.get('booking_number'), None),
            'expected_old_slot_id': old_slot_id,
            'new_slot_id': new_slot_id,
            'operation_id': _js_or(lineage.get('operation_id'),
                                   _js_cat(lineage.get('idempotency_key'), ':',
                                           'cancel_appointment' if cls == 'cancel_request' else 'reschedule_appointment')
                                   if _truthy(lineage.get('idempotency_key')) else None,
                                   mint_uuid(_js_cat('opx:', _js_or(lineage.get('message_id'), appt_id)))),
            'last_user_message_id_at_request': _js_or(lineage.get('message_id'), None),
            'confirmation_id': mint_uuid(_js_cat('confirmx:', appt_id, ':', _js_or(lineage.get('message_id'), ''))),
            'confirmation_ttl_seconds': CONFIRM_TTL_SECONDS,
            'expires_at': _iso_from_ms(now_ms(clinic, now_ts) + CONFIRM_TTL_SECONDS * 1000),
            'delivery': 'pending', 'source': 'state_table_v3'
        }
        if not target_binding_valid(target, state, clinic, now_ts):
            return finish('op_target_invalid', cs, 'MISSING_REQUIRED_FIELDS', {'missing_fields': ['appointment_id']})
        patches['confirmation_target'] = target
        patches['turn_directive'] = directive_for('propose_confirm', {'confirmFacts': (
            {'action': target.get('action'), 'appointment_id': target.get('appointment_id'),
             'new_slot_id': target.get('new_slot_id')} if is_reschedule
            else {'action': target.get('action'), 'appointment_id': target.get('appointment_id')})})
        return finish('cancel_proposed' if cls == 'cancel_request' else 'reschedule_proposed',
                      STATES['AWAIT_CONFIRMATION'], 'CONFIRMATION_REQUIRED', {'confirmation_state': 'proposed'})

    # ── Confirmation rows (state-independent C1 gate over a live target) ──
    if (cls in ('confirmation_affirm', 'confirmation_negative', 'confirmation_question')) and cs != STATES['CONFIRM_PATIENT_DATA']:
        if _truthy(prior_target) and target_binding_valid(prior_target, state, clinic, now_ts) and cls == 'confirmation_affirm':
            act = str(_js_or(prior_target.get('action'), ''))
            approved_code = 'EXECUTE_APPROVED' if act == 'create_appointment' \
                else ('CANCEL_APPROVED' if act == 'cancel_appointment' else 'RESCHEDULE_APPROVED')
            fields = patient_fields_from(state, contract)
            complete = act != 'create_appointment' or patient_data_complete(
                fields, _js_or(prior_target.get('appointment_type'), bc0.get('appointment_type')))
            if act == 'create_appointment' and not complete:
                missing = missing_patient_fields(state, contract)
                veto_reasons.append('patient_data_gate_failed')
                return finish('confirm_blocked_missing_data', cs, 'MISSING_REQUIRED_FIELDS', {'missing_fields': missing})
            patches['confirmation_target'] = {**prior_target, 'delivery': 'confirmed'}
            return finish('c1_confirm_execute', STATES['EXECUTING'], approved_code,
                          {'allowed': True, 'action': act, 'confirmation_state': 'confirmed',
                           'patient_data_complete': complete, 'patient_data_gate_satisfied': complete})
        if _truthy(prior_target) and cls == 'confirmation_negative':
            patches['confirmation_target'] = None
            patches['progress_this_turn'] = True
            if _truthy(offer):
                patches['turn_directive'] = directive_for('present_alternatives', {'lockedFields': locked_fields_of(state, contract)})
                return finish('confirm_rejected_offer_open', STATES['AWAIT_SLOT_CHOICE'], 'AVAILABILITY_RESULTS', {})
            patches['turn_directive'] = directive_for('ask_date', {'lockedFields': locked_fields_of(state, contract)})
            return finish('confirm_rejected_ask_date', STATES['ASK_DATE'], 'CONVERSATION_ONLY', {})
        if _truthy(prior_target) and cls == 'confirmation_question':
            return finish('confirm_question_hold', cs, 'CONVERSATION_ONLY', {})
        if _truthy(offer) and cls == 'confirmation_affirm' and cs == STATES['AWAIT_SLOT_CHOICE']:
            # Acceptance of an offered alternative without an exact-verification lookup:
            # bind from the verified offered alternative itself.
            alt = match_offered_alternative(offer, contract)
            if _truthy(alt) and uuid_valid(_dict(alt).get('slot_id')):
                target = build_create_target(inp, alt, 'offered_alternative', now_ts)
                if target_binding_valid(target, state, clinic, now_ts):
                    patches['confirmation_target'] = target
                    complete = patient_data_complete(patient_fields_from(state, contract), target.get('appointment_type'))
                    if not complete:
                        missing = missing_patient_fields(state, contract)
                        patches['turn_directive'] = directive_for('collect_patient_data', {
                            'missingPatientFields': missing, 'lockedFields': locked_fields_of(patches, contract)})
                        return finish('offer_affirm_need_patient_data', STATES['COLLECT_PATIENT_DATA'],
                                      'MISSING_REQUIRED_FIELDS', {'missing_fields': missing,
                                                                   'patient_data_complete': False,
                                                                   'patient_data_gate_satisfied': False})
                    patches['turn_directive'] = directive_for('propose_confirm', {
                        'confirmFacts': {'doctor_name': target.get('doctor_name'), 'date': target.get('date'),
                                         'time': target.get('time'), 'appointment_type': target.get('appointment_type')},
                        'lockedFields': locked_fields_of(state, contract)})
                    return finish('selection_bound_from_offer', STATES['AWAIT_CONFIRMATION'], 'CONFIRMATION_REQUIRED',
                                  {'confirmation_state': 'proposed', 'patient_data_complete': complete,
                                   'patient_data_gate_satisfied': complete})
                veto_reasons.append('offer_bind_invariants_failed')
            return finish('offer_affirm_no_match', cs, 'CONVERSATION_ONLY', {})
        if (_truthy(prior_target) and not target_binding_valid(prior_target, state, clinic, now_ts)) \
                or (not _truthy(prior_target) and _truthy(state.get('confirmation_target'))):
            patches['confirmation_target'] = None
            return finish('stale_target_refresh', STATES['ASK_DATE'], 'CONFIRMATION_EXPIRED', {})
        return finish('confirm_without_target', cs, 'CONVERSATION_ONLY', {'non_scheduling_turn': True})

    # ── Booking rows (state table) ──
    ent = _dict(_js_or(contract.get('entities'), {}))
    # BUGFIX (2026-09-09): unknown-doctor guard — if the patient named a doctor THIS turn and
    # the resolver matched zero doctors, that name is not a real clinic doctor. Never treat it
    # as a given doctor; the agent replies that the doctor is unavailable and lists the directory.
    doctor_unknown = ent.get('doctor_name') is not None and str(ent.get('doctor_name')).strip() != '' \
        and resolved.get('doctor_match_count') == 0
    if doctor_unknown:
        ent['doctor_name'] = None  # mutates contract.entities when present, exactly like the JS
    doctor_given = bool(resolved.get('doctor_id') or ent.get('doctor_name')
                        or patches['booking_context'].get('doctor_id') or patches['booking_context'].get('doctor_name'))
    doctor_count = _number_or_zero(clinic.get('doctor_count'))
    visit_type = _js_or(patches['booking_context'].get('appointment_type'), ent.get('visit_type'), None)

    # Merge patient/visit entities collected this turn into the booking context.
    if cls == 'booking' or cls == 'correction':
        changed = False
        bc = patches['booking_context']
        if resolved.get('doctor_id') and bc.get('doctor_id') != resolved.get('doctor_id'):
            bc['doctor_id'] = resolved.get('doctor_id')
            bc['doctor_name'] = _js_or(resolved.get('doctor_name'), bc.get('doctor_name'))
            changed = True
        elif ent.get('doctor_name') and (not bc.get('doctor_name') or cls == 'correction'):
            bc['doctor_name'] = ent.get('doctor_name')
            bc['doctor_id'] = None
            changed = True
        if resolved.get('service_id') and bc.get('service_id') != resolved.get('service_id'):
            bc['service_id'] = resolved.get('service_id')
            bc['service_name'] = _js_or(resolved.get('service_name'), bc.get('service_name'))
            changed = True
        if ent.get('visit_type') and bc.get('appointment_type') != ent.get('visit_type'):
            bc['appointment_type'] = ent.get('visit_type')
            changed = True
        if ent.get('patient_name') and bc.get('patient_name') != ent.get('patient_name'):
            bc['patient_name'] = ent.get('patient_name')
            changed = True
        if ent.get('patient_phone') and bc.get('patient_phone') != ent.get('patient_phone'):
            bc['patient_phone'] = ent.get('patient_phone')
            changed = True
        if ent.get('patient_age') is not None and bc.get('patient_age') != ent.get('patient_age'):
            bc['patient_age'] = ent.get('patient_age')
            changed = True
        if ent.get('patient_address') and bc.get('patient_address') != ent.get('patient_address'):
            bc['patient_address'] = ent.get('patient_address')
            changed = True
        if iso_date_valid(ent.get('date')) and bc.get('date') != str(ent.get('date')):
            bc['date'] = str(ent.get('date'))
            changed = True
        ent_time = normalize_time(ent.get('time'))
        if _truthy(ent_time) and bc.get('time') != ent_time:
            bc['time'] = ent_time
            changed = True
        if changed:
            patches['progress_this_turn'] = True
    known_date_iso = str(ent.get('date')) if iso_date_valid(ent.get('date')) \
        else (str(patches['booking_context'].get('date')) if iso_date_valid(patches['booking_context'].get('date')) else None)

    if not doctor_given and (cls == 'booking' or cls == 'availability_inquiry'):
        if doctor_count == 1 and _truthy(clinic.get('single_doctor_id')) and uuid_valid(clinic.get('single_doctor_id')):
            patches['booking_context']['doctor_id'] = clinic.get('single_doctor_id')
            patches['booking_context']['doctor_name'] = _js_or(clinic.get('single_doctor_name'), None)
            patches['progress_this_turn'] = True
            patches['turn_directive'] = directive_for('ask_visit_type', {'lockedFields': locked_fields_of(patches, contract)})
            return finish('restart_single_doctor_visit_type' if fresh_booking_restart else 'single_doctor_visit_type',
                          STATES['ASK_VISIT_TYPE'], 'CONVERSATION_ONLY' if visit_type else 'MISSING_REQUIRED_FIELDS',
                          {'missing_fields': [] if visit_type else ['visit_type'],
                           'new_booking_restart': fresh_booking_restart})
        patches['turn_directive'] = directive_for('ask_doctor', {})
        return finish('restart_ask_doctor' if fresh_booking_restart else 'ask_doctor', STATES['ASK_DOCTOR'],
                      'MISSING_REQUIRED_FIELDS', {'missing_fields': ['doctor'], 'new_booking_restart': fresh_booking_restart})

    if cs == STATES['IDLE'] or cs == STATES['ASK_DOCTOR'] or fresh_booking_restart:
        if cls == 'booking' and doctor_given and not visit_type:
            patches['turn_directive'] = directive_for('ask_visit_type', {'lockedFields': locked_fields_of(patches, contract)})
            return finish('ask_visit_type', STATES['ASK_VISIT_TYPE'], 'MISSING_REQUIRED_FIELDS',
                          {'missing_fields': ['visit_type'], 'new_booking_restart': fresh_booking_restart})
        if cls == 'availability_inquiry':
            patches['turn_directive'] = directive_for('ask_date', {'lockedFields': locked_fields_of(patches, contract)})
            return finish('availability_needs_date', STATES['ASK_DATE'] if doctor_given else STATES['ASK_DOCTOR'],
                          'MISSING_REQUIRED_FIELDS', {'missing_fields': ['date'] if doctor_given else ['doctor']})

    if cs == STATES['ASK_VISIT_TYPE'] or (cs != STATES['ASK_DOCTOR'] and doctor_given and cls == 'booking'
            and (not visit_type
                 or (cs == STATES['IDLE'] and not patient_data_complete(patient_fields_from(state, contract), visit_type))
                 or (cs == STATES['IDLE'] and saved_record_pending_review(state, contract)))):  # v39: IDLE dead-zone fix — when the visit type is already known, IDLE turns with an incomplete or never-reviewed profile are owned here instead of falling to default_conversation
        if visit_type:
            patches['booking_context']['appointment_type'] = visit_type
            facts_root = state.get('facts')
            saved = _dict(facts_root).get('patient') if isinstance(facts_root, dict) else None
            saved_complete = bool(_truthy(saved) and _dict(saved).get('name') and _dict(saved).get('phone')
                                  and (_dict(saved).get('age') if _dict(saved).get('age') is not None else None) is not None
                                  and _dict(saved).get('address'))
            if saved_complete and not _truthy(patches['patient_data_review']):
                patches['patient_data_review'] = {'fields': {'name': saved.get('name'), 'age': saved.get('age'),
                                                             'phone': saved.get('phone'), 'address': saved.get('address')},
                                                  'status': 'pending_confirmation', 'source': 'saved_record'}
                patches['turn_directive'] = directive_for('confirm_patient_data', {
                    'reviewFields': patches['patient_data_review']['fields'],
                    'lockedFields': locked_fields_of(patches, contract)})
                return finish('saved_record_review', STATES['CONFIRM_PATIENT_DATA'], 'PATIENT_DATA_CONFIRMATION_REQUIRED', {})
            missing = missing_patient_fields(state, contract)
            if not missing:
                patient_changed_now = bool(ent.get('patient_name') or ent.get('patient_phone')
                                           or ent.get('patient_age') is not None or ent.get('patient_address'))
                pdr_now = patches['patient_data_review']
                if _truthy(pdr_now) and str(_js_or(_dict(pdr_now).get('status'), '')).lower() == 'confirmed' and not patient_changed_now:
                    patches['turn_directive'] = directive_for('ask_date', {'lockedFields': locked_fields_of(patches, contract)})
                    return finish('review_confirmed_skip_reconfirm', STATES['ASK_DATE'], 'CONVERSATION_ONLY', {})
                patches['patient_data_review'] = review_from(patches, 'collected')
                patches['turn_directive'] = directive_for('confirm_patient_data', {
                    'reviewFields': patches['patient_data_review']['fields'],
                    'lockedFields': locked_fields_of(patches, contract)})
                return finish('collect_complete_inline', STATES['CONFIRM_PATIENT_DATA'], 'PATIENT_DATA_CONFIRMATION_REQUIRED', {})
            patches['turn_directive'] = directive_for('collect_patient_data', {
                'missingPatientFields': missing, 'lockedFields': locked_fields_of(patches, contract)})
            return finish('collect_patient_data', STATES['COLLECT_PATIENT_DATA'], 'MISSING_REQUIRED_FIELDS',
                          {'missing_fields': missing})
        patches['turn_directive'] = directive_for('ask_visit_type', {'lockedFields': locked_fields_of(patches, contract)})
        return finish('ask_visit_type_repeat', STATES['ASK_VISIT_TYPE'], 'MISSING_REQUIRED_FIELDS',
                      {'missing_fields': ['visit_type']})

    if cs == STATES['COLLECT_PATIENT_DATA']:
        missing = missing_patient_fields(patches, contract)
        if not missing:
            if _truthy(prior_target) and prior_target.get('action') == 'create_appointment' \
                    and target_binding_valid(prior_target, state, clinic, now_ts):
                patches['turn_directive'] = directive_for('propose_confirm', {
                    'confirmFacts': {'doctor_name': prior_target.get('doctor_name'), 'date': prior_target.get('date'),
                                     'time': prior_target.get('time'), 'appointment_type': prior_target.get('appointment_type')},
                    'lockedFields': locked_fields_of(patches, contract)})
                return finish('collect_complete_slot_bound_propose', STATES['AWAIT_CONFIRMATION'], 'CONFIRMATION_REQUIRED',
                              {'confirmation_state': 'proposed', 'patient_data_complete': True, 'patient_data_gate_satisfied': True})
            patches['patient_data_review'] = review_from(patches, 'collected')
            patches['turn_directive'] = directive_for('confirm_patient_data', {
                'reviewFields': patches['patient_data_review']['fields'],
                'lockedFields': locked_fields_of(patches, contract)})
            return finish('collect_complete', STATES['CONFIRM_PATIENT_DATA'], 'PATIENT_DATA_CONFIRMATION_REQUIRED', {})
        patches['turn_directive'] = directive_for('collect_patient_data', {
            'missingPatientFields': missing, 'lockedFields': locked_fields_of(patches, contract)})
        return finish('collect_continue', STATES['COLLECT_PATIENT_DATA'], 'MISSING_REQUIRED_FIELDS',
                      {'missing_fields': missing})

    if cs == STATES['CONFIRM_PATIENT_DATA']:
        if cls == 'confirmation_question':
            pdr_now = patches['patient_data_review']
            patches['turn_directive'] = directive_for('confirm_patient_data', {
                'reviewFields': _js_or(_dict(pdr_now).get('fields') if isinstance(pdr_now, dict) else None, None),
                'lockedFields': locked_fields_of(patches, contract)})
            return finish('review_question_hold', STATES['CONFIRM_PATIENT_DATA'], 'PATIENT_DATA_CONFIRMATION_REQUIRED', {})
        if cls == 'correction' or (cls == 'booking' and (ent.get('patient_name') or ent.get('patient_phone')
                or ent.get('patient_age') is not None or ent.get('patient_address'))):
            patches['patient_data_review'] = review_from(patches, 'corrected')
            patches['turn_directive'] = directive_for('confirm_patient_data', {
                'reviewFields': patches['patient_data_review']['fields'],
                'lockedFields': locked_fields_of(patches, contract)})
            return finish('review_corrected', STATES['CONFIRM_PATIENT_DATA'], 'PATIENT_DATA_CONFIRMATION_REQUIRED', {})
        if isinstance(patches['patient_data_review'], dict):
            patches['patient_data_review']['status'] = 'confirmed'  # shared reference with state, like the JS
        # FIX (owner-reported test failure 2026-09-07): a verified appointment slot bound
        # BEFORE the patient-data review (confirmation_target from the date/time step) was
        # being discarded here and the patient re-asked for the date, because this branch
        # never consulted priorTarget — only the C1 gate above did, and that gate is
        # explicitly skipped while cs === CONFIRM_PATIENT_DATA. Execute the already-bound
        # target now, mirroring the C1 confirm-execute gate, instead of resetting to ASK_DATE.
        if _truthy(prior_target) and prior_target.get('action') == 'create_appointment' \
                and target_binding_valid(prior_target, state, clinic, now_ts):
            fields = patient_fields_from(state, contract)
            complete = patient_data_complete(fields, _js_or(prior_target.get('appointment_type'), bc0.get('appointment_type')))
            if complete:
                patches['confirmation_target'] = {**prior_target, 'delivery': 'confirmed'}
                return finish('review_confirmed_execute_bound_target', STATES['EXECUTING'], 'EXECUTE_APPROVED',
                              {'allowed': True, 'action': 'create_appointment', 'confirmation_state': 'confirmed',
                               'patient_data_complete': True, 'patient_data_gate_satisfied': True})
            missing = missing_patient_fields(state, contract)
            veto_reasons.append('patient_data_gate_failed')
            return finish('review_confirmed_missing_data', STATES['CONFIRM_PATIENT_DATA'], 'MISSING_REQUIRED_FIELDS',
                          {'missing_fields': missing})
        # Journey finding (2026-09-18): after the data-confirm there may be NO prior
        # target but a LIVE presented_offer the patient already engaged with — bind it
        # now (CONFIRMATION_REQUIRED) instead of asking the date again and dropping to
        # CONVERSATION (a full wasted turn for the patient).
        bound_alt = match_offered_alternative(offer, contract) if _truthy(offer) else None
        if _truthy(bound_alt) and uuid_valid(_dict(bound_alt).get('slot_id')):
            target = build_create_target(inp, bound_alt, 'offered_alternative', now_ts)
            if target_binding_valid(target, state, clinic, now_ts):
                complete = patient_data_complete(patient_fields_from(state, contract), target.get('appointment_type'))
                if complete:
                    patches['confirmation_target'] = target
                    # Mirror the P42 row: the slot identity must land in booking_context
                    # and slot_state too — the transition guard's create data_valid reads
                    # it from there (its absence blocked the executor in the journey).
                    patches['slot_state'] = {
                        **(patches.get('slot_state') if isinstance(patches.get('slot_state'), dict) else {}),
                        'slot_id': target.get('slot_id'), 'date': target.get('date'),
                        'time': target.get('time'), 'doctor_id': target.get('doctor_id'),
                        'doctor_name': target.get('doctor_name'),
                        'appointment_type': target.get('appointment_type'),
                    }
                    patches['booking_context'] = {
                        **(patches.get('booking_context') if isinstance(patches.get('booking_context'), dict) else {}),
                        'slot_id': target.get('slot_id'), 'date': target.get('date'),
                        'time': target.get('time'), 'doctor_id': target.get('doctor_id'),
                        'doctor_name': target.get('doctor_name'),
                        'appointment_type': target.get('appointment_type'),
                    }
                    patches['turn_directive'] = directive_for('propose_confirm', {
                        'confirmFacts': {'doctor_name': target.get('doctor_name'), 'date': target.get('date'),
                                         'time': target.get('time'), 'appointment_type': target.get('appointment_type')},
                        'lockedFields': locked_fields_of(state, contract)})
                    return finish('review_confirmed_bind_offer', STATES['AWAIT_CONFIRMATION'], 'CONFIRMATION_REQUIRED',
                                  {'confirmation_state': 'proposed', 'patient_data_complete': complete,
                                   'patient_data_gate_satisfied': complete})
        patches['turn_directive'] = directive_for('ask_date', {'lockedFields': locked_fields_of(patches, contract)})
        return finish('review_confirmed_ask_date', STATES['ASK_DATE'], 'CONVERSATION_ONLY', {})

    # P47 TOOL-PATH BINDING BRIDGE (owner directive: no field may ever drop).
    # A live presented_offer (written by the availability tool) plus a selection in
    # the contract binds immediately, regardless of the current state row.
    # Binding is keyed on the CONTRACT's selection, not the message class: a patient
    # confirming ('أيوه حجز' → booking_continuation) with a selection against a live
    # offer is a slot pick, whatever the classifier called the turn.
    binding_class = (
        cls == 'selection_presented'
        or _js_string(_dig(contract, 'selection', 'kind')) in ('presented_match', 'presented_rank')
    )
    if _truthy(offer) and binding_class:
        bridge_alt = match_offered_alternative(offer, contract)
        if _truthy(bridge_alt) and uuid_valid(_dict(bridge_alt).get('slot_id')) \
                and (not _truthy(prior_target)
                     or str(_js_or(_dict(prior_target).get('slot_id'), '')) != str(_js_or(_dict(bridge_alt).get('slot_id'), ''))):
            bridge_target = build_create_target(inp, bridge_alt, 'offered_alternative', now_ts)
            if target_binding_valid(bridge_target, state, clinic, now_ts):
                patches['confirmation_target'] = bridge_target
                bridge_complete = patient_data_complete(patient_fields_from(state, contract), bridge_target.get('appointment_type'))
                if not bridge_complete:
                    missing = missing_patient_fields(state, contract)
                    patches['turn_directive'] = directive_for('collect_patient_data', {
                        'missingPatientFields': missing, 'lockedFields': locked_fields_of(patches, contract)})
                    return finish('selection_bound_from_offer_bridge_need_data', STATES['COLLECT_PATIENT_DATA'],
                                  'MISSING_REQUIRED_FIELDS', {'missing_fields': missing,
                                                               'patient_data_complete': False,
                                                               'patient_data_gate_satisfied': False})
                patches['turn_directive'] = directive_for('propose_confirm', {
                    'confirmFacts': {'doctor_name': bridge_target.get('doctor_name'), 'date': bridge_target.get('date'),
                                     'time': bridge_target.get('time'), 'appointment_type': bridge_target.get('appointment_type')},
                    'lockedFields': locked_fields_of(state, contract)})
                return finish('selection_bound_from_offer_bridge', STATES['AWAIT_CONFIRMATION'], 'CONFIRMATION_REQUIRED',
                              {'confirmation_state': 'proposed', 'patient_data_complete': bridge_complete,
                               'patient_data_gate_satisfied': bridge_complete})
    # P47b PATIENT-FIELD REFRESH (owner directive: changing a field must work):
    # a pending target keeps the fields captured at bind time; if the patient
    # corrects name/phone/age/address afterwards, refresh the pending target.
    if _truthy(prior_target) and prior_target.get('action') == 'create_appointment':
        ref_ent = _dict(contract.get('entities')) if isinstance(contract.get('entities'), dict) else {}
        ref_patch = {}
        if ref_ent.get('patient_name'):
            ref_patch['patient_name'] = str(ref_ent['patient_name']).strip()
        if ref_ent.get('patient_phone'):
            ref_patch['patient_phone'] = str(ref_ent['patient_phone']).strip()
        if ref_ent.get('patient_age') is not None and ref_ent.get('patient_age') != '':
            ref_patch['patient_age'] = ref_ent.get('patient_age')
        if ref_ent.get('patient_address'):
            ref_patch['patient_address'] = str(ref_ent['patient_address']).strip()
        if ref_patch:
            patches['confirmation_target'] = {**prior_target, **ref_patch}
    if cs in (STATES['ASK_DATE'], STATES['AWAIT_SLOT_CHOICE'], STATES['AWAIT_CONFIRMATION']):
        # Journey finding (2026-09-18): the patient confirming their data with an
        # affirmative + a requested create proposal IS the slot pick — the offer is
        # live and the confirm turn references it ('أيوه صح' to 'أأكد الخميس 10:30؟').
        # At AWAIT_CONFIRMATION the C1 confirm-execute row above owns the affirmative:
        # binding again here would loop CONFIRMATION_REQUIRED forever (journey T6).
        if _truthy(offer) and (binding_class
                or (cls == 'confirmation_affirm' and _dig(contract, 'operation_proposal', 'requested') is True
                    and cs != STATES['AWAIT_CONFIRMATION'])):  # P42: bind from any of ASK_DATE/AWAIT_SLOT_CHOICE/AWAIT_CONFIRMATION — interleaved turns legally move cs off AWAIT_SLOT_CHOICE while the offer stays open
            alt = match_offered_alternative(offer, contract)
            if _truthy(alt) and uuid_valid(_dict(alt).get('slot_id')) \
                    and _truthy(prior_target) and prior_target.get('action') == 'create_appointment' \
                    and target_binding_valid(prior_target, state, clinic, now_ts) \
                    and str(_js_or(_dict(prior_target).get('slot_id'), '')) == str(_dict(alt).get('slot_id')) \
                    and cs == STATES['AWAIT_CONFIRMATION'] \
                    and str(_js_or(_dict(contract.get('confirmation')).get('intent'), 'none')) in ('affirmative', 'negative'):
                # P42c (journey root-cause 2026-09-19): the affirm/negative turn on the
                # already-bound slot belongs to the C1 confirm-execute row. The classifier
                # ranks selection_presented above confirmation_affirm, so an affirm that
                # echoes the offered list landed here and re-bound CONFIRMATION_REQUIRED
                # forever (journey T6). Execute / reject the bound target instead of
                # re-binding it — same gates as the C1 row.
                conf_intent_echo = str(_js_or(_dict(contract.get('confirmation')).get('intent'), 'none'))
                if conf_intent_echo == 'affirmative':
                    fields = patient_fields_from(state, contract)
                    complete = patient_data_complete(
                        fields, _js_or(prior_target.get('appointment_type'), bc0.get('appointment_type')))
                    if not complete:
                        missing = missing_patient_fields(state, contract)
                        veto_reasons.append('patient_data_gate_failed')
                        return finish('confirm_blocked_missing_data', cs, 'MISSING_REQUIRED_FIELDS',
                                      {'missing_fields': missing})
                    patches['confirmation_target'] = {**prior_target, 'delivery': 'confirmed'}
                    return finish('c1_confirm_execute', STATES['EXECUTING'], 'EXECUTE_APPROVED',
                                  {'allowed': True, 'action': 'create_appointment',
                                   'confirmation_state': 'confirmed',
                                   'patient_data_complete': complete,
                                   'patient_data_gate_satisfied': complete})
                patches['confirmation_target'] = None
                patches['progress_this_turn'] = True
                patches['turn_directive'] = directive_for('present_alternatives', {'lockedFields': locked_fields_of(state, contract)})
                return finish('confirm_rejected_offer_open', STATES['AWAIT_SLOT_CHOICE'], 'AVAILABILITY_RESULTS', {})
            if _truthy(alt) and uuid_valid(_dict(alt).get('slot_id')):
                target = build_create_target(inp, alt, 'offered_alternative', now_ts)
                if target_binding_valid(target, state, clinic, now_ts):
                    patches['confirmation_target'] = target
                    complete = patient_data_complete(patient_fields_from(state, contract), target.get('appointment_type'))
                    if not complete:
                        missing = missing_patient_fields(state, contract)
                        patches['turn_directive'] = directive_for('collect_patient_data', {
                            'missingPatientFields': missing, 'lockedFields': locked_fields_of(patches, contract)})
                        return finish('selection_bound_from_offer_need_data', STATES['COLLECT_PATIENT_DATA'],
                                      'MISSING_REQUIRED_FIELDS', {'missing_fields': missing,
                                                                   'patient_data_complete': False,
                                                                   'patient_data_gate_satisfied': False})
                    patches['turn_directive'] = directive_for('propose_confirm', {
                        'confirmFacts': {'doctor_name': target.get('doctor_name'), 'date': target.get('date'),
                                         'time': target.get('time'), 'appointment_type': target.get('appointment_type')},
                        'lockedFields': locked_fields_of(state, contract)})
                    return finish('selection_bound_from_offer', STATES['AWAIT_CONFIRMATION'], 'CONFIRMATION_REQUIRED',
                                  {'confirmation_state': 'proposed', 'patient_data_complete': complete,
                                   'patient_data_gate_satisfied': complete})
                veto_reasons.append('offer_bind_invariants_failed')
            return finish('selection_no_match', cs, 'CONVERSATION_ONLY', {})
        if known_date_iso and cls == 'booking' and not _truthy(offer):
            # Date given but the lookup did not run this turn (preconditions failed).
            missing = missing_patient_fields(state, contract)
            if not visit_type or missing:
                patches['turn_directive'] = directive_for('ask_visit_type' if not visit_type else 'collect_patient_data',
                                                          {'missingPatientFields': missing,
                                                           'lockedFields': locked_fields_of(patches, contract)})
                return finish('date_given_profile_incomplete',
                              STATES['ASK_VISIT_TYPE'] if not visit_type else STATES['COLLECT_PATIENT_DATA'],
                              'MISSING_REQUIRED_FIELDS', {'missing_fields': ['visit_type'] if not visit_type else missing})
            patches['turn_directive'] = directive_for('ask_date', {'lockedFields': locked_fields_of(patches, contract)})
            return finish('date_given_lookup_gate_failed', STATES['ASK_DATE'], 'AVAILABILITY_SOURCE_ERROR', {})
        # v41 DATE-CONTRADICTION FIX (owner-approved 2026-09-07): the catch-all below
        # used to return ask_date / missing_fields:['date'] for ANY message reaching
        # these states without a match above — even when booking_context.date was
        # already collected (exec 64124 'اكد': 6795 completion tokens; exec 64143
        # 'صح': 8000 tokens, finish_reason=length, empty output). Enforced rule: never
        # request a field the booking context already holds.
        ent_date_given = str(ent.get('date')) if iso_date_valid(ent.get('date')) else None
        offer_first_date = None
        if _truthy(offer) and isinstance(offer.get('alternatives'), list) and offer.get('alternatives'):
            first_alt = offer['alternatives'][0]
            if isinstance(first_alt, dict):
                offer_first_date = str(_js_or(first_alt.get('local_date'), first_alt.get('date'), ''))[:10]
        cur_doc_id = patches['booking_context'].get('doctor_id') or resolved.get('doctor_id')
        cur_doc_name = patches['booking_context'].get('doctor_name') or ent.get('doctor_name')
        offer_doc_id = _dict(offer).get('doctor_id') if _truthy(offer) else None
        offer_doc_name = _dict(offer).get('doctor_name') if _truthy(offer) else None
        target_doc_id = _dict(prior_target).get('doctor_id') if _truthy(prior_target) else None
        target_doc_name = _dict(prior_target).get('doctor_name') if _truthy(prior_target) else None

        doctor_switched_offer = bool(_truthy(offer) and (
            (cur_doc_id and offer_doc_id and str(cur_doc_id) != str(offer_doc_id))
            or (cur_doc_name and offer_doc_name and str(cur_doc_name).strip() != str(offer_doc_name).strip())
        ))
        doctor_switched_target = bool(_truthy(prior_target) and (
            (cur_doc_id and target_doc_id and str(cur_doc_id) != str(target_doc_id))
            or (cur_doc_name and target_doc_name and str(cur_doc_name).strip() != str(target_doc_name).strip())
        ))

        offer_contradicted = bool((_truthy(offer) and ent_date_given and offer_first_date
                                  and offer_first_date != ent_date_given) or doctor_switched_offer)
        target_contradicted = bool((_truthy(prior_target) and (
            (_dict(prior_target).get('date') and str(_dict(prior_target).get('date')) != str(known_date_iso))
            or (_dict(prior_target).get('time') and patches['booking_context'].get('time')
                and normalize_time(_dict(prior_target).get('time')) != normalize_time(patches['booking_context'].get('time')))))
            or doctor_switched_target)
        if offer_contradicted or target_contradicted:
            # The patient steered to a different day/time this turn — the pending offer /
            # confirmation target is stale and must not be re-presented or executed.
            patches['presented_offer'] = None
            patches['confirmation_target'] = None
        if not known_date_iso:
            # date genuinely missing → keep the original ask_date behavior.
            patches['turn_directive'] = directive_for('ask_date', {'lockedFields': locked_fields_of(patches, contract)})
            return finish('ask_date', STATES['ASK_DATE'], 'MISSING_REQUIRED_FIELDS', {'missing_fields': ['date']})
        if _truthy(offer) and not offer_contradicted:
            # date collected + live offer → re-present the open alternatives.
            patches['turn_directive'] = directive_for('present_alternatives', {'lockedFields': locked_fields_of(state, contract)})
            return finish('date_known_offer_represent', STATES['AWAIT_SLOT_CHOICE'], 'AVAILABILITY_RESULTS', {})
        if _truthy(prior_target) and not target_contradicted:
            # date collected + live confirmation target → re-propose the confirmation.
            patches['turn_directive'] = directive_for('propose_confirm', {
                'confirmFacts': {'doctor_name': _js_or(prior_target.get('doctor_name'), None),
                                 'date': _js_or(prior_target.get('date'), None),
                                 'time': _js_or(prior_target.get('time'), None),
                                 'appointment_type': _js_or(prior_target.get('appointment_type'), None)},
                'lockedFields': locked_fields_of(state, contract)})
            return finish('date_known_repropose_confirm', STATES['AWAIT_CONFIRMATION'], 'CONFIRMATION_REQUIRED',
                          {'confirmation_state': 'proposed'})
        # Date already collected with no live offer/target: hold — the deterministic
        # layer never re-asks for a collected field (v35 MODEL-FIRST: the model sees
        # booking_context.date in its facts and decides what to ask next).
        return finish('date_known_hold', cs, 'CONVERSATION_ONLY', {'non_scheduling_turn': True})

    if cs == STATES['COMPLETED']:
        if cls == 'booking':
            patches['booking_context'] = {}
            patches['presented_offer'] = None
            patches['confirmation_target'] = None
            patches['turn_directive'] = directive_for('ask_doctor', {})
            return finish('completed_new_booking', STATES['ASK_DOCTOR'], 'MISSING_REQUIRED_FIELDS',
                          {'missing_fields': ['doctor'], 'new_booking_restart': True})
        return finish('completed_conversation', STATES['COMPLETED'], 'CONVERSATION_ONLY', {'non_scheduling_turn': True})

    return finish('default_conversation', cs, 'CONVERSATION_ONLY', {'non_scheduling_turn': True})


# ── JS semantics shims (String() / Number() / truthiness / || / Date.parse) ──


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
    """Close JS String() coercion for the value shapes this decision path produces."""
    if value is None:
        return ''
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _js_cat(*parts):
    """JS ``+`` string concatenation coercion (null/None renders as 'null', as in the JS seeds)."""
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


def _number_or_zero(value):
    """JS ``Number(x || 0)``: NaN-producing inputs collapse to 0."""
    n = _js_number(value)
    return 0 if math.isnan(n) else n


def _utf16_code_units(s):
    """Code points as s.codePointAt(i) yields them while i walks UTF-16 units (JS loop semantics)."""
    units = []
    for ch in s:
        cp = ord(ch)
        if cp > 0xFFFF:
            units.append(0xD800 + ((cp - 0x10000) >> 10))
            units.append(0xDC00 + ((cp - 0x10000) & 0x3FF))
        else:
            units.append(cp)
    return units


def _date_parse_ms(value):
    """JS Date.parse() for the ISO-8601 shapes this pipeline produces; None when invalid.

    Date-only strings are UTC (like JS); naive date-times are treated as UTC
    deterministically (JS would use the machine's local zone — canonical now_iso
    values in this pipeline are always UTC 'Z' strings, so this path is not hit).
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


def _is_valid_timezone(value):
    """PORT-TODO(n8n): JS validates via Intl.DateTimeFormat(timeZone); closest stdlib equivalent is
    zoneinfo. On hosts without the tzdata/IANA database (bare Windows) valid IANA names may be
    rejected. Only feeds clinic.timezone_configured, which no rule in this file reads."""
    timezone_name = _js_string(value).strip()
    if not timezone_name:
        return False
    if ZoneInfo is None:
        return False
    try:
        ZoneInfo(timezone_name)
        return True
    except Exception:
        return False


# ── Public entry point: the Code-node module body ──
def decide(contract_v3: dict, state_data: dict, clinic_context: dict, now_ts: float | None = None, *,
           normalize_agent_output: dict | None = None,
           validate_repaired_contract: dict | None = None,
           apply_resolved_booking_ids: dict | None = None,
           normalize_validate: dict | None = None,
           get_clinic_context: dict | None = None,
           validate_patient_ownership: dict | None = None,
           p1_7_patient_field_normalization: dict | None = None,
           current_item: dict | None = None) -> dict:
    """Source node: System Orchestrator (Policy) (extracted/code/System_Orchestrator_Policy.js).

    Runs the full node body: contract source selection (repaired / resolved / primary),
    P1.7 field merge, clinic + lineage assembly, the state-table decision, and the
    legacy-compatible envelope derivation. Returns the inner json dict (n8n's
    ``[{ json: resultJson }][0].json``). See the module docstring for parameters.
    """
    item = _dict(current_item)

    # ── Inputs ──
    # Prefer the repaired contract when the one-shot self-repair branch ran and
    # produced a VALID contract; otherwise use the primary NAO output.
    repaired_contract_source = _dict(validate_repaired_contract)
    repaired = repaired_contract_source if repaired_contract_source.get('_contract_status') == 'VALID' else None
    nao = _dict(normalize_agent_output)
    primary_v2 = _dict(_js_or(nao.get('contract'), {}))
    primary_normalization = _dict(_js_or(nao.get('_normalization'), {}))
    primary_contract_v3 = _dict(contract_v3)
    if repaired is not None:
        contract_source_final = repaired
        repair_source = 'self_repair'
    else:
        # BUGFIX (2026-09-09): prefer the post-resolver contract_v3 from Apply Resolved Booking IDs.
        # The resolver converts textual booking references into real UUIDs, but its results were only
        # mirrored into the v2 contract and top-level fields; contractV3 (read by decide()) still had
        # appointment_id=null when the patient gave a booking number, breaking cancel/reschedule.
        ar = _dict(apply_resolved_booking_ids)
        ar_v3 = ar.get('contract_v3')
        ar_v3 = ar_v3 if isinstance(ar_v3, dict) else None
        if ar_v3 is not None and _truthy(_dict(ar_v3.get('entities')).get('appointment_id')):
            contract_source_final = {'contract_v3': ar_v3, 'contract': primary_v2, '_normalization': primary_normalization}
        else:
            contract_source_final = {'contract_v3': primary_contract_v3, 'contract': primary_v2, '_normalization': primary_normalization}
        repair_source = 'primary'
    contract_v3_final = _dict(_js_or(contract_source_final.get('contract_v3'), {}))
    contract_v2 = _dict(_js_or(contract_source_final.get('contract'), {}))

    ctx = _dict(normalize_validate)
    clinic_row = _dict(get_clinic_context)
    ownership = _dict(validate_patient_ownership)
    canonical = _dict(_js_or(ownership.get('canonical_time_context'), {}))
    state = _dict(_js_or(state_data, {}))
    apply_resolved = _dict(apply_resolved_booking_ids)
    resolver_result = _dict(_js_or(apply_resolved.get('resolver_result'), {}))
    resolved = {
        'doctor_id': _js_or(apply_resolved.get('doctor_id'), None),
        'doctor_name': _js_or(apply_resolved.get('doctor_name'), None),
        'service_id': _js_or(apply_resolved.get('service_id'), None),
        'service_name': _js_or(apply_resolved.get('service_name'), None),
        'appointment_id': _js_or(apply_resolved.get('appointment_id'), None),
        'booking_number': _js_or(apply_resolved.get('booking_number'), None),
        'expected_old_slot_id': _js_or(apply_resolved.get('expected_old_slot_id'), None),
        'new_slot_id': _js_or(apply_resolved.get('new_slot_id'), None),
        'branch_id': _js_or(apply_resolved.get('branch_id'), None),
        'doctor_match_count': resolver_result.get('doctor_match_count') if resolver_result.get('doctor_match_count') is not None else None
    }
    dsl = item.get('deterministic_slot_lookup')
    lookup = dsl if isinstance(dsl, dict) and dsl.get('executed') is True else None

    timezone_ok = (canonical.get('timezone_configured') is True
        and not _truthy(canonical.get('timezone_error_code'))
        and _is_valid_timezone(canonical.get('timezone'))
        and isinstance(canonical.get('now_iso'), str)
        and _date_parse_ms(canonical.get('now_iso')) is not None)

    normalization = _dict(_js_or(contract_source_final.get('_normalization'),
                                 item.get('_normalization'), {}))
    turn_lineage = _dict(_js_or(normalization.get('turn_lineage'), {}))

    clinic_context = _dict(clinic_context)
    clinic = {
        'clinic_id': _js_or(clinic_context.get('clinic_id'), ctx.get('clinic_id'), None),
        'patient_id': _js_or(clinic_context.get('patient_id'), ctx.get('patient_id'), None),
        'conversation_id': _js_or(clinic_context.get('conversation_id'), ctx.get('conversation_id'), None),
        'timezone': _js_or(clinic_context.get('timezone'), canonical.get('timezone'), None),
        'timezone_configured': clinic_context.get('timezone_configured')
            if isinstance(clinic_context.get('timezone_configured'), bool) else timezone_ok,
        'now_local_date': _js_or(clinic_context.get('now_local_date'), canonical.get('now_local_date'), None),
        'now_iso': _js_or(clinic_context.get('now_iso'), canonical.get('now_iso'), None),
        'utc_offset': _js_or(clinic_context.get('utc_offset'), canonical.get('utc_offset'), None),
        'doctor_count': _number_or_zero(_js_or(clinic_context.get('doctor_count'), clinic_row.get('doctor_count'))),
        'single_doctor_id': _js_or(clinic_context.get('single_doctor_id'), clinic_row.get('single_doctor_id'), None),
        'single_doctor_name': _js_or(clinic_context.get('single_doctor_name'), clinic_row.get('single_doctor_name'), None)
    }
    lineage = {
        'message_id': _js_or(turn_lineage.get('message_id'), ctx.get('message_id'), None),
        'idempotency_key': _js_or(ctx.get('idempotency_key'), None),
        'operation_id': _js_or(ctx.get('operation_id'), item.get('operation_id'), None),
        'turn_key': _js_or(turn_lineage.get('turn_key'), None),
        'turn_id': _js_or(turn_lineage.get('turn_id'), None),
        'message_fingerprint': _js_or(turn_lineage.get('message_fingerprint'), None)
    }

    # ── Deterministic patient fields (P1.7 format extraction) override model entities ──
    p17 = _dict(p1_7_patient_field_normalization)
    p17_det = {}
    if _truthy(p17.get('p17_extracted_phone')):
        p17_det['patient_phone'] = str(p17['p17_extracted_phone'])
    if p17.get('p17_extracted_age') is not None:
        p17_det['patient_age'] = p17.get('p17_extracted_age')
    if _truthy(p17.get('p17_extracted_address')):
        p17_det['patient_address'] = str(p17['p17_extracted_address'])
    if p17_det:
        contract_v3_eff = {**contract_v3_final,
                           'entities': {**_dict(_js_or(contract_v3_final.get('entities'), {})), **p17_det}}
    else:
        contract_v3_eff = contract_v3_final

    # ── The ONE decision point ──
    inp = {'state': state, 'contract': contract_v3_eff, 'clinic': clinic,
           'resolved': resolved, 'lookup': lookup, 'lineage': lineage}
    decision_result = _decide_state_table(inp, now_ts)
    decision = decision_result['system_decision']
    patches = decision_result['state_patches']

    # ── Legacy-compatible derivation (consumers: persistence, response policy) ──
    action = decision.get('action') if _truthy(decision.get('action')) and decision.get('action') != 'none' else None
    missing = decision.get('missing_fields') if isinstance(decision.get('missing_fields'), list) else []
    next_best_missing_human_field = _js_or(decision.get('next_best_missing_human_field'), None)
    create_flow_evidence = (action == 'create_appointment'
        or str(_js_or(_dict(_js_or(contract_v3_final.get('operation_proposal'), {})).get('type'), '')) == 'create_appointment'
        or str(_js_or(_dict(_js_or(contract_v2.get('operation_proposal'), {})).get('type'), '')) == 'create_appointment'
        or str(_js_or(state.get('active_operation'), state.get('operation_action'), '')) == 'create_appointment'
        or any(str(f) in ('doctor', 'visit_type', 'patient_name', 'patient_age', 'patient_phone', 'patient_address', 'date')
               for f in missing))
    effective_action = _js_or(action, 'create_appointment' if create_flow_evidence else None)

    code = str(_js_or(decision.get('response_code'), '')).upper()
    if code == 'HANDOFF_REQUIRED':
        conversation_stage = 'HANDOFF_REQUIRED'
    elif code in ('PROVIDER_UNAVAILABLE', 'INVALID_JSON'):
        conversation_stage = 'ERROR_RETRYABLE'
    elif code == 'PATIENT_DATA_CONFIRMATION_REQUIRED':
        conversation_stage = 'WAITING_PATIENT_DATA_CONFIRMATION'
    elif code in ('CONFIRMATION_REQUIRED', 'CONFIDENCE_REVIEW_REQUIRED') or decision.get('confirmation_state') == 'required':
        conversation_stage = 'WAITING_BOOKING_CONFIRMATION'
    elif code in ('AVAILABILITY_LOOKUP_REQUIRED', 'SLOT_LOOKUP_REQUIRED'):
        conversation_stage = 'WAITING_AVAILABILITY'
    elif code == 'MISSING_REQUIRED_FIELDS':
        conversation_stage = ('COLLECTING_PATIENT_DATA' if effective_action == 'create_appointment'
                              and any(str(f).startswith('patient_') for f in missing)
                              else ('COLLECTING_APPOINTMENT_DETAILS' if effective_action == 'create_appointment'
                                    else 'COLLECTING_REQUIRED_FIELDS'))
    elif code in ('EXECUTE_APPROVED', 'CANCEL_APPROVED', 'RESCHEDULE_APPROVED'):
        conversation_stage = 'EXECUTING'
    elif code in ('APPOINTMENT_CREATED', 'CANCEL_COMPLETED', 'RESCHEDULE_COMPLETED', 'IDEMPOTENT_REPLAY'):
        conversation_stage = 'COMPLETED'
    elif decision.get('new_booking_restart') is True:
        conversation_stage = 'COLLECTING_APPOINTMENT_DETAILS'
    else:
        conversation_stage = 'CONVERSATION'

    ct = decision.get('confirmation_target')
    if conversation_stage == 'WAITING_PATIENT_DATA_CONFIRMATION':
        required_next_step = {'type': 'confirm_patient_data', 'fields': ['name', 'phone', 'age', 'address']}
    elif conversation_stage == 'WAITING_BOOKING_CONFIRMATION':
        required_next_step = {'type': 'confirm_booking',
                              'action': _js_or(_dict(ct).get('action') if isinstance(ct, dict) else None, action, 'create_appointment')}
    elif conversation_stage == 'WAITING_AVAILABILITY':
        required_next_step = {'type': 'check_availability'}
    elif conversation_stage == 'COLLECTING_PATIENT_DATA':
        required_next_step = {'type': 'collect_patient_data', 'fields': ['name', 'phone', 'age', 'address'],
                              'field': next_best_missing_human_field}
    elif conversation_stage == 'COLLECTING_APPOINTMENT_DETAILS':
        required_next_step = {'type': 'collect_appointment_details', 'field': next_best_missing_human_field}
    elif conversation_stage == 'HANDOFF_REQUIRED':
        required_next_step = {'type': 'escalate'}
    else:
        required_next_step = {'type': 'answer_current_message'}
    decision['conversation_stage'] = conversation_stage
    decision['required_next_step'] = required_next_step

    v3_intent = str(_js_or(_dict(_js_or(contract_v3_final.get('turn'), {})).get('intent'), '')).lower()
    decision['intent'] = 'faq' if v3_intent == 'clinic_query' else (v3_intent or 'other')
    decision['proposed_action'] = _js_or(action, 'none')
    decision['contract'] = contract_v2
    decision['query'] = _js_or(contract_v2.get('query'), None)
    decision['availability_inquiry'] = decision.get('availability_inquiry') is True
    pdr_final = patches.get('patient_data_review')
    decision['review_pending'] = bool(_truthy(pdr_final)
        and str(_js_or(_dict(pdr_final).get('status'), '')).lower() == 'pending_confirmation')
    decision['target_veto_reasons'] = decision_result['veto_reasons']
    decision['operation'] = _js_or(action, None)

    # Operation lifecycle compatibility fields.
    prior_operation_action = str(_js_or(state.get('active_operation'), state.get('operation_action'), '')).strip().lower() or None
    code = str(_js_or(decision.get('response_code'), '')).upper()
    decision['active_operation'] = _js_or(action, prior_operation_action
                                          if code in ('EXECUTE_APPROVED', 'CANCEL_APPROVED', 'RESCHEDULE_APPROVED') else None)
    decision['operation_action'] = decision.get('active_operation')
    if code == 'CONFIRMATION_REQUIRED':
        decision['operation_status'] = 'awaiting_confirmation'
    elif code in ('EXECUTE_APPROVED', 'CANCEL_APPROVED', 'RESCHEDULE_APPROVED'):
        decision['operation_status'] = 'executing'
    elif code in ('APPOINTMENT_CREATED', 'RESCHEDULE_COMPLETED'):
        decision['operation_status'] = 'completed'
    elif code == 'CANCEL_COMPLETED':
        decision['operation_status'] = 'cancelled'
    else:
        decision['operation_status'] = _js_or(state.get('operation_status'), None)

    # ── Canonical booking context + slot state ──
    bc = _dict(_js_or(patches.get('booking_context'), {}))
    ent3 = _dict(_js_or(contract_v3_final.get('entities'), {}))

    def keep(v, old):
        return v if (v is not None and str(v).strip() != '') else (old if old is not None else None)

    target = decision.get('confirmation_target') if isinstance(decision.get('confirmation_target'), dict) else None
    booking_context = {
        'doctor_id': _js_or(bc.get('doctor_id'), target.get('doctor_id') if target else None, resolved.get('doctor_id'), None),
        'doctor_name': keep(bc.get('doctor_name'),
                            _js_or(target.get('doctor_name') if target else None, resolved.get('doctor_name'), ent3.get('doctor_name'))),
        'service_id': _js_or(bc.get('service_id'), target.get('service_id') if target else None, resolved.get('service_id'), None),
        'service_name': keep(bc.get('service_name'),
                             _js_or(target.get('service_name') if target else None, resolved.get('service_name'), ent3.get('service_name'))),
        'appointment_type': _js_or(bc.get('appointment_type'), target.get('appointment_type') if target else None,
                                   ent3.get('visit_type'), None),
        'slot_id': _js_or(target.get('slot_id') if target else None, bc.get('slot_id'), None),
        'date': _js_or(target.get('date') if target else None, bc.get('date'), None),
        'time': _js_or(target.get('time') if target else None, bc.get('time'), None),
        'booking_number': _js_or(bc.get('booking_number'), resolved.get('booking_number'), None),
        'patient_name': keep(bc.get('patient_name'), ent3.get('patient_name')),
        'patient_phone': keep(bc.get('patient_phone'), ent3.get('patient_phone')),
        'patient_age': bc.get('patient_age') if bc.get('patient_age') is not None else ent3.get('patient_age'),
        'patient_address': keep(bc.get('patient_address'), ent3.get('patient_address')),
        'references_prior_conversation': contract_v3_final.get('references_prior_conversation') is True
    }
    slot_state = {
        'doctor_id': booking_context['doctor_id'],
        'doctor_name': booking_context['doctor_name'],
        'service_id': booking_context['service_id'],
        'service_name': booking_context['service_name'],
        'appointment_type': booking_context['appointment_type'],
        'date': booking_context['date'],
        'time': booking_context['time'],
        'slot_id': booking_context['slot_id'],
        'booking_number': booking_context['booking_number']
    }

    # ── Output envelope ──
    item_availability_alternatives = item.get('availability_alternatives')
    availability_alternatives = item_availability_alternatives if isinstance(item_availability_alternatives, list) \
        else (lookup.get('alternatives') if isinstance(lookup, dict) and isinstance(lookup.get('alternatives'), list) else [])
    result_json = dict(item)  # ...$json
    result_json.update({
        'contract': contract_v2,
        'contract_v3': contract_v3_final,
        'system_decision': decision,
        'booking_context': booking_context,
        'slot_state': slot_state,
        'presented_offer': _js_or(patches.get('presented_offer'), None),
        'pending_offer': _js_or(patches.get('presented_offer'), None),
        'confirmation_target': _js_or(decision.get('confirmation_target'), None),
        'confirmation_state': _js_or(decision.get('confirmation_state'), None),
        'active_operation': _js_or(decision.get('active_operation'), None),
        'operation_action': _js_or(decision.get('operation_action'), None),
        'operation_status': _js_or(decision.get('operation_status'), None),
        'operation_id': _js_or(target.get('operation_id') if target else None, lineage.get('operation_id'), None),
        'availability_outcome': _js_or(item.get('availability_outcome'),
                                       lookup.get('availability_outcome') if isinstance(lookup, dict) else None, None),
        'availability_alternatives': availability_alternatives,
        'deterministic_slot_lookup': _js_or(decision.get('deterministic_slot_lookup'), None),
        'patient_data_review': _js_or(patches.get('patient_data_review'), None),
        'turn_directive': _js_or(decision.get('turn_directive'), None),
        'state_machine': _js_or(decision.get('state_machine'), None),
        'state_patches': patches,
        'new_booking_restart': decision.get('new_booking_restart') is True,
        'escalation_requested': decision.get('escalation_requested') is True,
        'handoff_reason': _js_or(decision.get('handoff_reason'), None),
        'response_code': decision.get('response_code'),
        'decision_engine': 'k2.state_table.v3',
        'engine_version': 'k2.v3',
        'decision_engine_source': repair_source,
        'agent_context': {
            'active_operation': _js_or(decision.get('active_operation'), None),
            'operation_status': _js_or(decision.get('operation_status'), None),
            'confirmation_state': _js_or(decision.get('confirmation_state'), None),
            'confirmation_target': _js_or(decision.get('confirmation_target'), None),
            'conversation_state': conversation_stage,
            'pending_action': required_next_step['type'] if required_next_step['type'] != 'answer_current_message' else None,
            'last_open_question': {
                'message': None,
                'requested_fields': list(missing),
                'type': 'missing_field' if code == 'MISSING_REQUIRED_FIELDS'
                        else ('confirmation' if code == 'CONFIRMATION_REQUIRED' else None),
                'pending_action': required_next_step['type'] if required_next_step else None
            },
            'conversation_stage': conversation_stage,
            'required_next_step': required_next_step,
            'booking_context': booking_context
        }
    })
    return result_json
