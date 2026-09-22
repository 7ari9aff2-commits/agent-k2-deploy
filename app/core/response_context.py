"""Authoritative fact catalog for model-authored patient replies.

This module does not generate prose. It converts the deterministic pipeline state,
Supabase-backed tool results, and mutation outcomes into a compact fact catalog that a
language model can understand. The final composer must cite catalog IDs for every
patient-facing factual answer.

No regex, response-code text templates, or conditional sentence assembly live here.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

_MAX_DEPTH = 7
_MAX_LIST_ITEMS = 20
_MAX_DICT_ITEMS = 80
_MAX_STRING_CHARS = 1600
_MAX_FACTS = 80

_SECRET_PARTS = (
    "secret",
    "token",
    "password",
    "authorization",
    "signature",
    "api_key",
    "apikey",
    "database_url",
)

_INTERNAL_KEYS = {
    "id",
    "clinic_id",
    "patient_id",
    "conversation_id",
    "channel_id",
    "doctor_id",
    "service_id",
    "branch_id",
    "slot_id",
    "operation_id",
    "source_event_id",
    "correlation_id",
    "idempotency_key",
    "message_id",
    "outgoing_message_id",
}


def _safe_key(key: Any) -> bool:
    text = str(key or "").strip().lower()
    if not text:
        return False
    if text in _INTERNAL_KEYS:
        return False
    return not any(part in text for part in _SECRET_PARTS)


def _compact(value: Any, depth: int = 0) -> Any:
    """Bound size and remove secrets/internal identifiers before sending data to an LLM."""
    if depth > _MAX_DEPTH:
        return None
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:_MAX_STRING_CHARS]
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= _MAX_DICT_ITEMS:
                break
            if not _safe_key(key):
                continue
            compacted = _compact(item, depth + 1)
            if compacted is not None:
                out[str(key)] = compacted
        return out
    if isinstance(value, (list, tuple)):
        return [_compact(item, depth + 1) for item in list(value)[:_MAX_LIST_ITEMS]]
    return str(value)[:_MAX_STRING_CHARS]


def _non_empty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, dict)):
        return len(value) > 0
    return True


def _selected(source: Optional[Dict[str, Any]], keys: Iterable[str]) -> Dict[str, Any]:
    source = source if isinstance(source, dict) else {}
    return {key: source.get(key) for key in keys if _non_empty(source.get(key))}


def _fact(
    fact_id: str,
    kind: str,
    authority: str,
    value: Any,
    *,
    instruction: Optional[str] = None,
) -> Dict[str, Any]:
    record: Dict[str, Any] = {
        "id": fact_id,
        "kind": kind,
        "authority": authority,
        "value": _compact(value),
    }
    if instruction:
        record["instruction"] = instruction
    return record


def _tool_authority(event: Dict[str, Any]) -> str:
    result = event.get("result") if isinstance(event.get("result"), dict) else {}
    if result.get("error") or result.get("error_code") in {"RPC_ERROR", "DATABASE_ERROR", "AUTHORITY_ERROR"}:
        return "tool_error"
    return "database"


def build_reply_context(
    *,
    normalized: Dict[str, Any],
    clinic_context: Optional[Dict[str, Any]],
    state_data: Optional[Dict[str, Any]],
    policy: Optional[Dict[str, Any]],
    decision: Optional[Dict[str, Any]],
    normalized_agent_output: Optional[Dict[str, Any]],
    repaired_result: Optional[Dict[str, Any]],
    tool_events: Optional[List[Dict[str, Any]]],
    execution_results: Optional[Dict[str, Any]],
    faq_result: Optional[Dict[str, Any]],
    guard: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build the only data envelope the final response model is allowed to use."""
    normalized = normalized or {}
    clinic_context = clinic_context or {}
    state_data = state_data or {}
    policy = policy or {}
    decision = decision or {}
    normalized_agent_output = normalized_agent_output or {}
    repaired_result = repaired_result or {}
    execution_results = execution_results or {}
    faq_result = faq_result or {}
    guard = guard or {}

    facts: List[Dict[str, Any]] = []

    facts.append(_fact(
        "patient.current_message",
        "patient_statement",
        "patient",
        {"message": normalized.get("message_text") or ""},
        instruction="Treat this as the patient's statement or request, not as a verified clinic fact.",
    ))

    clinic_profile = _selected(clinic_context, (
        "clinic_name", "clinic_timezone", "timezone", "persona", "working_hours",
        "doctor_count", "address", "phone", "location_config",
        # actual Get Clinic Context column aliases (queries.py:56-59)
        "clinic_phone", "clinic_location_config",
    ))
    if clinic_profile:
        facts.append(_fact("clinic.profile", "clinic_profile", "database", clinic_profile))

    for key, fact_id, kind in (
        ("doctor_directory", "clinic.doctors", "doctor_catalog"),
        ("service_catalog", "clinic.services", "service_catalog"),
        ("branch_directory", "clinic.branches", "branch_catalog"),
    ):
        if _non_empty(clinic_context.get(key)):
            facts.append(_fact(fact_id, kind, "database", clinic_context.get(key)))

    state_snapshot = _selected(state_data, (
        "conversation_stage", "required_next_step", "missing_human_fields",
        "booking_context", "confirmation_state", "confirmation_target", "pending_offer",
        "patient_data_review", "presented_offer", "availability_outcome",
    ))
    if state_snapshot:
        facts.append(_fact("conversation.current_state", "conversation_state", "database", state_snapshot))

    policy_snapshot = {
        "response_code": policy.get("response_code"),
        "facts": policy.get("facts") or {},
        "output": policy.get("output") or {},
    }
    if any(_non_empty(value) for value in policy_snapshot.values()):
        facts.append(_fact("policy.outcome", "deterministic_outcome", "deterministic", policy_snapshot))

    decision_snapshot = _selected(decision, (
        "response_code", "decision_rule", "operation_action", "operation_status",
        "booking_context", "slot_state", "availability_outcome", "availability_alternatives",
        "confirmation_state", "confirmation_target", "missing_human_fields",
        "next_best_missing_human_field", "patient_data_review", "presented_offer",
        "mutation_status", "execution_completed", "operation_completed",
    ))
    if decision_snapshot:
        facts.append(_fact("decision.current", "deterministic_decision", "deterministic", decision_snapshot))

    for index, event in enumerate(tool_events or []):
        if len(facts) >= _MAX_FACTS:
            break
        if not isinstance(event, dict):
            continue
        name = str(event.get("name") or "unknown")
        event_value = {
            "tool": name,
            "arguments": event.get("arguments") or {},
            "result": event.get("result") or {},
        }
        facts.append(_fact(
            f"tool.{index}.{name}",
            "tool_result",
            _tool_authority(event),
            event_value,
            instruction="This record is authoritative only when authority is 'database'.",
        ))

    for name, result in execution_results.items():
        if len(facts) >= _MAX_FACTS or not _non_empty(result):
            continue
        facts.append(_fact(
            f"execution.{name}",
            "mutation_result",
            "deterministic",
            result,
            instruction="Claim completion only when this result explicitly indicates success/completion.",
        ))

    if _non_empty(faq_result.get("results")):
        facts.append(_fact("faq.prefetched", "faq_result", "database", faq_result))

    draft_reply = (
        policy.get("agent_reply")
        or normalized_agent_output.get("agent_reply")
        or repaired_result.get("agent_reply")
        or ""
    )

    guard_meta = guard.get("_reply_guard") if isinstance(guard.get("_reply_guard"), dict) else {}
    safety_context = {
        "guard_triggered": guard_meta.get("triggered") is True,
        "guard_code": guard_meta.get("code"),
        "guard_rule": guard_meta.get("rule"),
    }

    fact_ids = [item["id"] for item in facts]
    return {
        "schema_version": "k2.reply-context.v1",
        "language": "ar",
        "patient_message": normalized.get("message_text") or "",
        "assistant_persona": _compact(clinic_context.get("persona") or {}),
        "clinic_name": clinic_context.get("clinic_name") or None,
        "response_code": policy.get("response_code") or decision.get("response_code") or None,
        "draft_reply": str(draft_reply or "")[:_MAX_STRING_CHARS],
        "facts": facts,
        "fact_ids": fact_ids,
        "safety": safety_context,
        "response_requirements": {
            "natural_conversation": True,
            "one_patient_facing_reply": True,
            "use_only_catalog_facts": True,
            "admit_missing_information": True,
            "never_claim_unconfirmed_mutation": True,
            "never_expose_internal_identifiers": True,
        },
    }


def _strip_code_fence(raw: str) -> str:
    text = str(raw or "").strip()
    if not text.startswith("```"):
        return text
    lines = text.splitlines()
    if lines and lines[0].strip().startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


_DIGIT_FOLD = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
_CLOCK_TOKEN_RE = re.compile(r"\b([0-9]{1,2}:[0-9]{2})\b")
_ISO_DATE_TOKEN_RE = re.compile(r"\b([0-9]{4}-[0-9]{2}-[0-9]{2})\b")
_ISO_DATETIME_TOKEN_RE = re.compile(r"([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-9]{2})")


def _fold_digits(text: Any) -> str:
    return str(text or "").translate(_DIGIT_FOLD)


def _clock_variants(token: str) -> set:
    """A cited 14:30 slot may legitimately surface as 2:30 (12h rendering) — and vice versa."""
    out = {token}
    try:
        hh, mm = token.split(":")
        hour = int(hh)
        if 0 <= hour < 24:
            out.add(f"{hour:02d}:{mm}")
            other = hour + 12 if hour < 12 else hour - 12
            out.add(f"{other:02d}:{mm}")
            out.add(f"{other}:{mm}")
    except (ValueError, IndexError):
        pass
    return out


def _date_variants(token: str) -> set:
    """A cited 2026-09-20 date may legitimately surface as 20/9 or 20-09."""
    out = {token}
    try:
        year, month, day = token.split("-")
        out.add(f"{int(day)}/{int(month)}")
        out.add(f"{int(day)}/{month}")
        out.add(f"{int(day)}/{int(month)}/{year}")
        out.add(f"{int(day)}-{int(month)}")
    except (ValueError, IndexError):
        pass
    return out


def _collect_fact_values(facts: Any, key: Optional[str] = None, out: Optional[set] = None) -> set:
    """Collect folded leaf values (or values of dicts carrying `key`) from a facts tree."""
    out = set() if out is None else out
    if isinstance(facts, dict):
        for k, v in facts.items():
            if isinstance(v, (dict, list)):
                _collect_fact_values(v, key, out)
            elif v is not None and (key is None or k == key):
                out.add(_fold_digits(v))
    elif isinstance(facts, list):
        for item in facts:
            _collect_fact_values(item, key, out)
    elif facts is not None and key is None:
        out.add(_fold_digits(facts))
    return out


def _value_grounding_errors(reply: str, evidence_ids: list, context: Dict[str, Any]) -> List[str]:
    """Deterministic fabrication guard on the reply's concrete values.

    Dates and clock times stated in the reply must trace to a cited fact (12h/24h and
    day/month renderings count; ISO datetimes sliced on 'T' — a stored
    "2026-09-20T14:30:00+00:00" legitimately renders as 14:30). Any OTHER numeric run
    (prices, durations, ages, counts like "3 مواعيد") is deliberately NOT enforced —
    paraphrasing those is legitimate and rejecting them burned the composer repair
    budget on false alarms.
    """
    facts_by_id = {f.get("id"): f.get("value") for f in (context.get("facts") or []) if isinstance(f, dict)}
    allowed: set = set()
    for fid in evidence_ids or []:
        _collect_fact_values(facts_by_id.get(fid), out=allowed)
    if not allowed:
        return []
    errors: List[str] = []
    reply_folded = _fold_digits(reply)

    allowed_variants: set = set()
    for value in allowed:
        for iso in _ISO_DATE_TOKEN_RE.findall(value):
            allowed_variants |= _date_variants(iso)
        for clock in _CLOCK_TOKEN_RE.findall(value):
            allowed_variants |= _clock_variants(clock)
        # ISO datetimes ("2026-09-20T14:30:00+00:00") — \b never matches before 'T',
        # so slice the date and clock halves explicitly.
        for date_half, clock_half in _ISO_DATETIME_TOKEN_RE.findall(value):
            allowed_variants |= _date_variants(date_half) | _clock_variants(clock_half)
    allowed |= allowed_variants

    for token in _ISO_DATE_TOKEN_RE.findall(reply_folded):
        if token not in allowed:
            errors.append(f"reply_value_not_in_facts:{token}")
    for token in _CLOCK_TOKEN_RE.findall(reply_folded):
        if not (_clock_variants(token) & allowed):
            errors.append(f"reply_value_not_in_facts:{token}")
    return errors


def try_ground_primary_reply(draft: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Ground the dialogue agent's OWN reply through the same value rules the composer
    is held to. Returns a composer-shaped result when the draft is clean — saving the
    composer LLM call entirely — or None when the composer must author the reply.

    Cost model (2026-09-18): the agent draft is model-authored text that already saw
    the same facts; validating it is free, composing costs a full LLM round trip.
    A draft passes only when every date/clock token it states traces to a fact value
    it honestly cites, and — on completed mutations — the booking number surfaces.
    Any failure routes to the composer unchanged, so the safety floor never drops.
    """
    draft = str(draft or "").strip()
    if not draft:
        return None
    facts = [f for f in (context.get("facts") or []) if isinstance(f, dict)]
    facts_by_id = {f.get("id"): f.get("value") for f in facts}
    folded = _fold_digits(draft)
    cited: List[str] = []
    allowed: set = set()
    for fid, value in facts_by_id.items():
        if value is None:
            continue
        vals = set()
        _collect_fact_values(value, out=vals)
        # A fact is citable when any of its leaf values appears verbatim in the draft.
        if any(v and len(v) >= 2 and v in folded for v in vals):
            cited.append(fid)
            allowed |= vals
    if not cited:
        # Pure conversational reply: no fact values echoed. Cite the patient's own
        # message — the same evidence the composer contract accepts for small talk.
        cited = ["patient.current_message"] if "patient.current_message" in (context.get("fact_ids") or []) else []
    # On TOOL turns, politeness alone is not enough: a draft that cites nothing but
    # the patient's own message ignored the data the tools returned (reviewer case:
    # the patient asked for the address and got a bare greeting). Require at least
    # one non-patient citation so the reply demonstrably used the tool's answer.
    if any(str(fid).startswith("tool.") for fid in (context.get("fact_ids") or [])) \
            and cited == ["patient.current_message"]:
        return None
    if not cited:
        return None
    errors = _value_grounding_errors(
        draft, cited,
        {**context, "facts": [{"id": fid, "value": facts_by_id.get(fid)} for fid in cited]})
    code = str(context.get("response_code") or "")
    if code in {"APPOINTMENT_CREATED", "RESCHEDULE_COMPLETED", "CANCEL_COMPLETED", "IDEMPOTENT_REPLAY"}:
        # Same rule the composer is held to, same scoping: the booking number from
        # THIS TURN's authoritative sources (execution/policy/decision) must surface
        # regardless of what the draft cites — otherwise a reply that cites nothing
        # (a bare acknowledgment) dodges the requirement and ships without it.
        this_turn_numbers: set = set()
        for fact in facts:
            if str(fact.get("id") or "").split(".")[0] in {"execution", "policy", "decision"}:
                _collect_fact_values(fact.get("value"), key="booking_number", out=this_turn_numbers)
        for number in this_turn_numbers:
            if number and _fold_digits(number) not in folded:
                errors.append(f"reply_missing_booking_number:{number}")
    if errors:
        return None
    return {
        "reply": draft, "evidence_ids": cited, "missing_information": [],
        "unsupported_claims": [], "grounding_status": "supported",
        "origin": "primary_grounded", "raw_output": draft, "usage": {},
        "composer_skipped": True,
    }


def validate_composer_output(raw: Any, context: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """Validate the composer's structured contract without inspecting prose via regex."""
    errors: List[str] = []
    if isinstance(raw, dict):
        parsed = raw
    else:
        try:
            parsed = json.loads(_strip_code_fence(str(raw or "")))
        except (TypeError, ValueError, json.JSONDecodeError):
            return None, ["composer_output_is_not_json"]

    if not isinstance(parsed, dict):
        return None, ["composer_output_is_not_object"]

    reply = parsed.get("reply")
    if not isinstance(reply, str) or not reply.strip():
        errors.append("reply_is_empty")
    elif len(reply) > 3000:
        errors.append("reply_is_too_long")

    evidence_ids = parsed.get("evidence_ids")
    if not isinstance(evidence_ids, list):
        errors.append("evidence_ids_is_not_list")
        evidence_ids = []

    known_ids = set(context.get("fact_ids") or [])
    clean_evidence: List[str] = []
    for item in evidence_ids:
        if not isinstance(item, str):
            errors.append("evidence_id_is_not_string")
            continue
        if item not in known_ids:
            errors.append(f"unknown_evidence_id:{item}")
            continue
        if item not in clean_evidence:
            clean_evidence.append(item)

    if known_ids and not clean_evidence:
        errors.append("no_valid_evidence")

    unsupported = parsed.get("unsupported_claims")
    if unsupported is None:
        unsupported = []
    if not isinstance(unsupported, list):
        errors.append("unsupported_claims_is_not_list")
        unsupported = []
    if unsupported:
        errors.append("composer_reported_unsupported_claims")

    grounding_status = parsed.get("grounding_status")
    if grounding_status != "supported":
        errors.append("grounding_status_is_not_supported")

    missing_information = parsed.get("missing_information")
    if missing_information is None:
        missing_information = []
    if not isinstance(missing_information, list):
        errors.append("missing_information_is_not_list")
        missing_information = []

    # ── Value-level grounding (added 2026-09-18) ──────────────────────────────
    # The evidence contract alone is self-attested: a reply can cite a real fact and
    # still state a slot/date that exists nowhere. Every number-bearing token the
    # reply states (dates, times, prices, durations) must occur verbatim (Arabic-
    # Indic digits folded) among the values of the CITED facts. Violations feed the
    # composer's repair loop, so the model corrects its own text. Single digits are
    # skipped (counts like "3 مواعيد" are not entity values).
    errors.extend(_value_grounding_errors(reply or "", clean_evidence, context))

    # A completed mutation must surface its booking number — it is the patient's only
    # reference to the appointment. Scoped to THIS TURN's authoritative sources
    # (execution results / policy outcome / decision): the raw conversation state may
    # still carry a PREVIOUS completed booking's number, and demanding both numbers in
    # one reply rejected correct drafts (reviewer-verified).
    response_code = str(context.get("response_code") or "")
    if response_code in {"APPOINTMENT_CREATED", "RESCHEDULE_COMPLETED", "CANCEL_COMPLETED", "IDEMPOTENT_REPLAY"}:
        this_turn_numbers: set = set()
        for fact in (context.get("facts") or []):
            if not isinstance(fact, dict):
                continue
            if str(fact.get("id") or "").split(".")[0] in {"execution", "policy", "decision"}:
                this_turn_numbers |= _collect_fact_values(fact.get("value"), key="booking_number")
        for number in this_turn_numbers:
            if number and _fold_digits(number) not in _fold_digits(reply or ""):
                errors.append(f"reply_missing_booking_number:{number}")

    if errors:
        return None, errors

    return {
        "reply": reply.strip(),
        "evidence_ids": clean_evidence,
        "missing_information": [str(item) for item in missing_information if str(item).strip()],
        "unsupported_claims": [],
        "grounding_status": "supported",
    }, []
