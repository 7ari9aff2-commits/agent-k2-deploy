"""Faithful port of the n8n `Booking Assistant Agent` LLM layer (agent k2).

Covers:
  - Booking Assistant Agent user-message assembly (agent_user_message_template.js, byte-faithful
    including the production mojibake fallbacks — kept on purpose for parity).
  - DeepSeek Model node options: temperature 0.3, max_tokens 4000, response_format json_object.
  - DeepSeek Repair Model options: temperature 0, max_tokens 800.
  - Reasoning: the n8n node also sent {"reasoning": {"enabled": false, "max_tokens": 2048}}.
    That is now OPT-IN (LLM_SEND_REASONING_PARAM) because reasoning models served by other
    gateways (e.g. Novita's zai-org/glm-5.3-flash) ignore the flag and still consume the
    completion budget on hidden reasoning — with a small max_tokens the visible content
    comes back empty. See _reasoning_options().
The R1/R2/R3 safety layers and repair-prompt/validation live in app.core.llm_safety (separate ports)
and are composed by the pipeline runner, exactly as the n8n graph wires them.
"""
from __future__ import annotations

from datetime import datetime
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import httpx

from app.core.config import settings
from app.core import session_compact

logger = logging.getLogger(__name__)

_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"
SYSTEM_MESSAGE_PATH = _PROMPTS_DIR / "agent_system_message.txt"
RESPONSE_COMPOSER_SYSTEM_MESSAGE_PATH = _PROMPTS_DIR / "response_composer_system_message.txt"


def load_system_message() -> str:
    """System message for the dialogue analyzer and tool-using agent."""
    return SYSTEM_MESSAGE_PATH.read_text(encoding="utf-8")


def load_response_composer_system_message() -> str:
    """Instructions for the model-authored final patient reply."""
    return RESPONSE_COMPOSER_SYSTEM_MESSAGE_PATH.read_text(encoding="utf-8")


def _provider_options() -> Dict[str, Any]:
    """Provider knobs for every model call, tuned for reasoning-capable gateways.

    Two independent concerns:

    * ``reasoning`` block — only when the provider honours it. Reasoning models such as
      zai-org/glm-5.3-flash IGNORE ``enabled: false`` and still spend the completion
      budget on hidden reasoning, so with a small ``max_tokens`` the visible ``content``
      comes back empty. Opt-in via ``LLM_SEND_REASONING_PARAM``.
    * ``reasoning_effort`` — no flag disables thinking on GLM, but this throttles it.
      Measured on Novita: ``low`` cut reasoning tokens 283 -> 83 and call latency
      7.7s -> 5.4s. ``LLM_REASONING_EFFORT=""`` omits the parameter.
    """
    options: Dict[str, Any] = {}
    effort = str(getattr(settings, "LLM_REASONING_EFFORT", "") or "").strip()
    if effort:
        options["reasoning_effort"] = effort
    if getattr(settings, "LLM_SEND_REASONING_PARAM", False):
        options["reasoning"] = {
            "enabled": False,
            "max_tokens": int(getattr(settings, "LLM_REASONING_MAX_TOKENS", 2048)),
        }
    return options


def _extract_message_content(message: Dict[str, Any]) -> str:
    """Visible assistant text, tolerating reasoning-only responses.

    Some gateways return ``content`` plus a separate ``reasoning_content``. When the
    completion budget is consumed by reasoning the visible content is empty and the
    finish reason is ``length``; callers must treat that as an empty reply rather than
    leaking internal reasoning to the patient.
    """
    content = message.get("content")
    if isinstance(content, list):
        content = "".join(
            part.get("text", "") for part in content if isinstance(part, dict)
        )
    return str(content or "")


# ── User message assembly (Booking Assistant Agent node `text` expression) ─────
def build_user_message(
    clinic_context: Dict[str, Any],
    canonical_time_context: Dict[str, Any],
    normalized: Dict[str, Any],
    persona_context: Dict[str, Any],
    state_data: Dict[str, Any],
    faq_result: Optional[Dict[str, Any]],
) -> str:
    """Source node: Booking Assistant Agent (text expression). Returns the JSON.stringify'd payload."""
    c = clinic_context or {}
    t = (canonical_time_context or {})
    x = normalized or {}
    b = persona_context or {}
    st = state_data or {}

    persona = {
        "clinic": c.get("clinic_name") or "",
        "assistant": (c.get("persona") or {}).get("name") or "نور",
        "role": (c.get("persona") or {}).get("role") or "مساعدة استقبال وحجوزات",
        "tone": (c.get("persona") or {}).get("tone") or "warm_professional",
        "dialect": (c.get("persona") or {}).get("dialect") or "saudi",
    }
    faq = faq_result if (faq_result and len(faq_result) > 0) else None
    include_faq_facts = bool(faq) and isinstance(faq.get("results"), list) and len(faq["results"]) > 0
    service = b.get("service_facts") or {}
    # Computed in the source template but never placed into the payload — kept for fidelity.
    include_service = service.get("is_service_fact_inquiry") is True or service.get("is_price_inquiry") is True or service.get("is_service_catalog_inquiry") is True  # noqa: F841
    bc = st.get("booking_context") or {}
    conf = st.get("confirmation_target") if (st.get("confirmation_target") and st.get("confirmation_state") == "required") else None
    offered_raw = st.get("pending_offer", {}).get("alternatives", []) if isinstance(st.get("pending_offer"), dict) and isinstance(st.get("pending_offer", {}).get("alternatives"), list) else []
    # P42c payload guard (journey root-cause 2026-09-19): while a booking confirmation
    # is pending, the offered list must NOT reach the model. The prompt precedence puts
    # "offered list + accepts → selection set" ABOVE "pending confirmation", so the
    # affirm turn classified as a slot pick and the decision table re-bound
    # CONFIRMATION_REQUIRED forever (journey T6). The offer stays in state — the
    # deterministic rows read it from there; only the payload is cleared.
    if conf:
        offered_raw = []
    review = st.get("patient_data_review") or None
    collecting = not conf
    appt = bc.get("appointment_type") or None
    must_ask = st.get("turn_directive", {}).get("must_ask", []) if isinstance(st.get("turn_directive"), dict) and isinstance(st.get("turn_directive", {}).get("must_ask"), list) else []
    missing = st.get("missing_human_fields") if isinstance(st.get("missing_human_fields"), list) else []
    asked = st.get("last_open_question", {}).get("requested_fields", []) if isinstance(st.get("last_open_question"), dict) and isinstance(st.get("last_open_question", {}).get("requested_fields"), list) else []
    want_ref = (isinstance(must_ask, list) and "appointment_id" in must_ask) or (isinstance(asked, list) and "appointment_id" in asked) or (isinstance(missing, list) and "appointment_id" in missing)

    next_ask = None
    if conf:
        if review and review.get("status") == "pending":
            next_ask = "patient_data_confirm"
    elif want_ref:
        next_ask = "appointment_reference"
    elif collecting:
        if not appt:
            next_ask = "visit_type"
        elif len(asked) == 1:
            next_ask = asked[0]
        elif len(must_ask) == 1:
            next_ask = must_ask[0]
        elif missing:
            order = ["date", "time", "reference", "patient_name", "patient_phone", "patient_age", "patient_address"]
            next_ask = next((f for f in order if f in missing), missing[0])

    local_tz_str = t.get("timezone")
    local_date = t.get("now_local_date")
    local_time = t.get("now_local_time")
    now_iso = t.get("now_iso")
    if (not local_date or not local_time) and now_iso and local_tz_str:
        try:
            dt = datetime.fromisoformat(str(now_iso).replace("Z", "+00:00"))
            local_dt = dt.astimezone(ZoneInfo(str(local_tz_str)))
            if not local_date:
                local_date = local_dt.strftime("%Y-%m-%d")
            if not local_time:
                local_time = local_dt.strftime("%H:%M:%S")
        except Exception:
            pass

    payload = {
        "assistant_persona": persona,
        "clinic_name": c.get("clinic_name") or None,
        "context": {
            "clinic_name": c.get("clinic_name") or None,
            "local_time": {
                "timezone": local_tz_str or None,
                "date": local_date or None,
                "time": local_time or None,
                "offset": t.get("utc_offset") or None,
            },
            "doctors": {"count": c.get("doctor_count") or 0, "directory": b.get("clinic_doctor_directory") or []},
        },
        "situation": {
            "today": local_date or None,
            "current_booking": ({"doctor_name": bc.get("doctor_name") or None, "doctor_id": bc.get("doctor_id") or None,
                                 "date": bc.get("date") or None, "time": bc.get("time") or None}
                                if (bc.get("doctor_name") or bc.get("doctor_id") or bc.get("date")) else None),
            "pending_confirmation": ({"action": conf.get("action") or None, "doctor_name": conf.get("doctor_name") or None,
                                      "date": conf.get("date") or None, "time": conf.get("time") or None,
                                      "expires_at": conf.get("expires_at") or None} if conf else None),
            "offered": [{"rank": s.get("rank") or None, "date": s.get("local_date") or None, "time": s.get("local_time") or None} for s in offered_raw],
            "patient_review": ({"status": "pending", "fields": review.get("fields") or None} if review and review.get("status") == "pending" else None),
            "next_ask": next_ask,
        },
        "faq_facts": faq if include_faq_facts else None,
        "current_message": x.get("message_text") or "",
    }
    # Recent dialogue (added 2026-09-18, naturalness): the model never saw what was
    # actually SAID — only structured snapshots — so pronouns like 'معاه' (him) broke
    # and it re-asked answered questions. The last few real exchanges go into the
    # payload; a large model resolves references and mirrors tone from them itself.
    recent = st.get("recent_turns") if isinstance(st.get("recent_turns"), list) else []
    # Session compaction (owner directive 2026-09-25): past the 2h gap the turn is a
    # FRESH conversation — no raw history, no auto-injected summary. The patient who
    # says 'السلام عليكم' after hours gets a natural reception greeting; the summary
    # stays in state and is served ONLY when the agent calls Recall_Session_History
    # (the patient explicitly referenced the past).
    recent_dialogue = []
    if not session_compact.session_boundary(st):
        for t in recent[-6:]:
            if not isinstance(t, dict):
                continue
            role = str(t.get("role") or "user")
            text = str(t.get("content") or t.get("text") or "")[:400]
            if text.strip():
                recent_dialogue.append({"role": role, "text": text})
    if recent_dialogue:
        payload["recent_dialogue"] = recent_dialogue
    return json.dumps(payload, ensure_ascii=False, separators=(", ", ": "))


# ── Check Doctor Availability tool (n8n toolWorkflow node, verbatim description) ──
AVAILABILITY_TOOL_DESCRIPTION = (
    "Check available appointment slots for a doctor on a date in the clinic local calendar. "
    "CALL whenever the patient explicitly asks about available times/days or states a specific day "
    "or wants to book/reschedule an appointment. doctor_id accepts the doctor name exactly as the "
    "patient wrote it. Only call with a requested_date the patient actually stated — if no day was "
    "given, ask which day first. Returns real available slots from the clinic database. "
    "Never invent dates, times, or slots."
)

AVAILABILITY_TOOL = {
    "type": "function",
    "function": {
        "name": "Check_Doctor_Availability",
        "description": AVAILABILITY_TOOL_DESCRIPTION,
        "parameters": {
            "type": "object",
            "properties": {
                "doctor_id": {
                    "type": "string",
                    "description": "Doctor name exactly as the patient wrote it, or a doctor id already known",
                },
                "requested_date": {
                    "type": "string",
                    "description": "ISO YYYY-MM-DD date in the clinic local calendar",
                },
                "service_id": {
                    "type": "string",
                    "description": "Service UUID if known, or null",
                },
            },
            "required": ["doctor_id", "requested_date"],
        },
    },
}

FAQ_TOOL = {
    "type": "function",
    "function": {
        "name": "Search_Clinic_FAQ",
        "description": (
            "Search the clinic knowledge base and FAQ for verified answers regarding "
            "clinic working hours, address, location, prices, accepted insurances, "
            "appointment policies, preparation instructions, and doctor credentials. "
            "Always use this tool when answering patient questions about the clinic."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The specific question or search query in Arabic",
                },
            },
            "required": ["query"],
        },
    },
}

SERVICES_DOCTORS_TOOL = {
    "type": "function",
    "function": {
        "name": "Get_Clinic_Services_And_Doctors",
        "description": (
            "Get the verified list of active doctors, their specialties, branches, "
            "and the clinic service catalog with official prices. Call this when the "
            "patient asks who the doctors are, what specialties exist, or what services and prices are offered."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Optional category filter, or null for all",
                },
            },
        },
    },
}

PATIENT_APPOINTMENTS_TOOL = {
    "type": "function",
    "function": {
        "name": "Get_My_Appointments",
        "description": (
            "Retrieve the patient's existing or upcoming appointments at this clinic. "
            "Call this when the patient asks to view, reschedule, or cancel their appointment."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "booking_number": {
                    "type": "string",
                    "description": "Optional booking number or appointment ID if mentioned by the patient",
                },
            },
        },
    },
}

def recall_session_history(state_data: Dict[str, Any]) -> Dict[str, Any]:
    """Recall_Session_History executor — server-owned history only.

    Returns the stored previous-session summary when one exists; otherwise an
    honest not-found signal the agent must relay verbatim (never fabricated
    history)."""
    st = state_data if isinstance(state_data, dict) else {}
    summary = st.get("previous_session_summary")
    if isinstance(summary, dict) and summary:
        return {"found": True, "previous_session_summary": summary}
    return {"found": False,
            "note": "No previous-session summary is stored for this conversation. "
                    "Tell the patient you cannot find that part of the history "
                    "and ask them to restate what they need."}


# B4 cheap-turn short-circuit (2026-09-25): an affirmation/negation turn inside
# AWAIT_CONFIRMATION needs NO model call — the contract is synthesized from the
# bound target. Vocabulary is deliberately conservative: every token must match,
# nothing else may appear in the message.
def _norm_dialect_token(t: str) -> str:
    return (t.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا")
             .replace("ى", "ي").strip())


_AFFIRM_TOKENS = {_norm_dialect_token(t) for t in {
    "ايوه", "اه", "اها", "نعم", "تمام", "اكد", "اتفقنا",
    "ماشي", "اوكي", "ok", "yes", "طب", "احفظ", "سجل", "ثبت", "ايوا",
}}
_NEGATE_TOKENS = {_norm_dialect_token(t) for t in {
    "لا", "مش", "ملغي", "لغيت", "لغاء", "no", "cancel", "استني", "مستني",
}}


def try_synthesize_confirmation_turn(state_data: Any, user_message: str) -> Optional[str]:
    """B4: when the state holds a live confirmation target and the message is a
    short pure affirm/negation, synthesize the exact contract the model would emit
    (skipping the primary LLM call). Returns None when the turn is not a clean
    confirm/negate — the normal LLM path handles everything else."""
    import re as _re
    st = state_data if isinstance(state_data, dict) else {}
    target = st.get("confirmation_target")
    if not (isinstance(target, dict) and target.get("confirmation_id") and target.get("action")):
        return None
    text = str(user_message or "").strip()
    if not text or len(text) > 40:
        return None
    tokens = [_norm_dialect_token(t) for t in _re.findall(r"[\w]+", text.lower()) if t]
    if not (1 <= len(tokens) <= 4):
        return None
    if all(t in _AFFIRM_TOKENS for t in tokens):
        intent = "affirmative"
    elif all(t in _NEGATE_TOKENS for t in tokens):
        intent = "negative"
    else:
        return None
    selection = ({"kind": "presented_match", "rank": 1}
                 if target.get("action") == "create_appointment" else {"kind": "none", "rank": None})
    return json.dumps({
        "schema_version": "k2.dialogue.v4", "reply": text,
        "turn": {"intent": "confirmation", "relation_to_previous_turn": "confirmation"},
        "confidence": 1.0, "ambiguous": [], "confirmation": {"intent": intent},
        "selection": selection, "entities": {},
        "operation_proposal": {"type": "none", "requested": False},
        "escalate": None,
    }, ensure_ascii=False, separators=(",", ":"))


CLINIC_INFO_TOOL = {
    "type": "function",
    "function": {
        "name": "Get_Clinic_Info",
        "description": (
            "Retrieve the clinic's working hours, address, and active branches. "
            "Call this when the patient asks about working hours, opening/closing times, "
            "the clinic location or address, or available branches."
        ),
        "parameters": {"type": "object", "properties": {}},
    },
}

SESSION_RECALL_TOOL = {
    "type": "function",
    "function": {
        "name": "Recall_Session_History",
        "description": (
            "Retrieve the structured summary of this patient's PREVIOUS conversation session "
            "(before the last 2-hour inactivity gap). Call it ONLY when the patient explicitly "
            "references the past — e.g. mentions an old booking ('I booked a few days ago'), "
            "asks about something discussed earlier, or says something like 'remember when I...'. "
            "Do NOT call it for ordinary greetings or new requests; those start fresh."
        ),
        "parameters": {
            "type": "object",
            "properties": {},
        },
    },
}

RECEPTIONIST_TOOLS = [
    AVAILABILITY_TOOL,
    FAQ_TOOL,
    SERVICES_DOCTORS_TOOL,
    PATIENT_APPOINTMENTS_TOOL,
    SESSION_RECALL_TOOL,
    CLINIC_INFO_TOOL,
]


# ── Agent turn (the only model entry point) ─────────────────────────────────────
def recover_entities_from_tool_events(raw_text: str, tool_events: Optional[list] = None) -> str:
    """Back-fill contract entities the model dropped after a tool round.

    The tool-call arguments are the model's own extraction. When the final JSON omits a
    value that an earlier tool call carried — seen live on 2026-09-17: the model called
    Check_Doctor_Availability with the doctor name, then emitted a contract with
    entities.doctor_name null, so the saved state lost the doctor and the next turn
    re-asked the patient — this restores it. Rules: only SUCCESSFUL tool calls count
    (a failed call's arguments were guesses the tool could not serve — an assumed date
    must never harden into entities.date); the LAST matching call wins (a corrected
    second call outranks a reflexive first one); only EMPTY contract values are filled;
    non-JSON text is untouched (the repair chain owns that path).
    """
    import re

    raw = str(raw_text or "").strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    try:
        parsed = json.loads(raw)
    except Exception:
        return str(raw_text or "")
    if not isinstance(parsed, dict):
        return str(raw_text or "")
    entities = parsed.get("entities")
    if not isinstance(entities, dict):
        return str(raw_text or "")
    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.I)
    changed = False
    # Last matching event wins: the model may call the tool reflexively with the OLD
    # doctor (from situation.current_booking) before the corrected call — the newest
    # call is the one it settled on (reviewer-verified switch-away resurrection).
    # Failed calls are skipped: their arguments were guesses the tool could not serve,
    # and an assumed date must never harden into entities.date.
    candidates: Dict[str, Any] = {}
    for event in tool_events or []:
        if not isinstance(event, dict):
            continue
        result = event.get("result") if isinstance(event.get("result"), dict) else {}
        if result.get("error") or result.get("error_code") or result.get("input_error"):
            continue
        args = event.get("arguments") if isinstance(event.get("arguments"), dict) else {}
        name = str(event.get("name") or "")
        if name == "Check_Doctor_Availability":
            doctor = str(args.get("doctor_id") or "").strip()
            if doctor and not uuid_re.match(doctor):
                candidates["doctor_name"] = doctor
            service_id = str(args.get("service_id") or "").strip()
            if service_id:
                candidates["service_id"] = service_id
        if name == "Get_My_Appointments":
            reference = str(args.get("booking_number") or "").strip()
            if reference:
                candidates["reference"] = reference
    for field, value in candidates.items():
        if value and not str(entities.get(field) or "").strip():
            entities[field] = value
            changed = True
    if not changed:
        return str(raw_text or "")
    parsed["entities"] = entities
    return json.dumps(parsed, ensure_ascii=False)


class AgentTurnText(str):
    """String-compatible agent output carrying the authoritative tool trace.

    Keeping this as a ``str`` preserves the existing LLM safety/contract adapters and
    external tests while making every Supabase result available to the final composer.
    """

    tool_events: list[Dict[str, Any]]
    llm_calls: int
    usage: list[Dict[str, Any]]

    def __new__(cls, content: str, *, tool_events: Optional[list[Dict[str, Any]]] = None,
                llm_calls: int = 0, usage: Optional[list[Dict[str, Any]]] = None) -> "AgentTurnText":
        obj = str.__new__(cls, content or "")
        obj.tool_events = list(tool_events or [])
        obj.llm_calls = int(llm_calls)
        obj.usage = list(usage or [])
        return obj


async def call_primary_model_with_tool(user_message: str, context: Dict[str, Any]) -> AgentTurnText:
    """Analyze the turn and use reception tools when the request needs real data.

    Returns a string-compatible result plus ``tool_events``. The tool trace is retained
    for the final response composer instead of disappearing inside the chat loop.
    """
    from app.services.availability import check_available_slots
    from app.services import faq as faq_service
    from app.db import repository

    messages: list = [
        {"role": "system", "content": load_system_message()},
        {"role": "user", "content": user_message},
    ]
    final_content: Optional[str] = None
    tool_events: list[Dict[str, Any]] = []
    tool_cache: Dict[str, Any] = {}
    usage_events: list[Dict[str, Any]] = []
    llm_calls = 0
    total_tool_calls = 0
    max_turns = max(2, int(getattr(settings, "LLM_TOOL_MAX_TURNS", 3)))
    max_tool_calls = max(1, int(getattr(settings, "LLM_TOOL_MAX_CALLS", 4)))
    for turn in range(max_turns):
        allow_tools = (turn < max_turns - 1)
        message = await _chat_messages(messages, with_tools=allow_tools, force_json=(not allow_tools))
        llm_calls += 1
        if message.get("_usage"):
            usage_events.append(message["_usage"])
        final_content = message.get("content")
        tool_calls = message.get("tool_calls") or []
        if not tool_calls or not allow_tools:
            break
        messages.append(message)
        for tool_call in tool_calls:
            fn = (tool_call.get("function") or {})
            fn_name = fn.get("name")
            try:
                args = json.loads(fn.get("arguments") or "{}")
            except json.JSONDecodeError:
                args = {}
            if not isinstance(args, dict):
                args = {}

            cache_key = json.dumps(
                {"name": fn_name, "arguments": args},
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )
            cached = cache_key in tool_cache
            # Cache hits must NOT consume the budget (reviewer-verified): telling the
            # model that an already-answered call "hit the limit" invites invention.
            if not cached:
                total_tool_calls += 1
            if cached:
                tool_result = tool_cache[cache_key]
            elif total_tool_calls > max_tool_calls:
                tool_result = {
                    "error": "TOOL_CALL_LIMIT_REACHED",
                    "message": "No additional tools may run in this turn",
                }
            elif fn_name == "Check_Doctor_Availability":
                # Resolve doctor_id with fallback to context/state/clinic
                doctor_id = args.get("doctor_id") or context.get("doctor_id")
                if not doctor_id:
                    st = context.get("state_data") or {}
                    bc = st.get("booking_context") or {}
                    doctor_id = bc.get("doctor_id") or (context.get("clinic_context") or {}).get("single_doctor_id")

                requested_date = args.get("requested_date")
                service_id = args.get("service_id") or context.get("service_id")
                if not service_id:
                    st = context.get("state_data") or {}
                    bc = st.get("booking_context") or {}
                    service_id = bc.get("service_id")

                if not doctor_id or not requested_date:
                    tool_result = {
                        "error": "MISSING_REQUIRED_PARAMS",
                        "message": "doctor_id and requested_date are required for availability check",
                    }
                else:
                    tool_input = {
                        "clinic_id": context.get("clinic_id"),
                        "conversation_id": context.get("conversation_id"),
                        "patient_id": context.get("patient_id"),
                        "doctor_id": doctor_id,
                        "requested_date": requested_date,
                        "service_id": service_id,
                    }
                    try:
                        tool_result = await check_available_slots(tool_input)
                    except Exception as exc:
                        tool_result = {"error": str(exc)}

            elif fn_name == "Search_Clinic_FAQ":
                q = args.get("query") or args.get("question") or ""
                try:
                    faq_res = await faq_service.search_clinic_faq(
                        faq_service.FaqSearchInput(clinic_id=context.get("clinic_id"), question=q)
                    )
                    tool_result = faq_res or {"results": [], "count": 0}
                except Exception as exc:
                    tool_result = {"error": str(exc), "results": []}

            elif fn_name == "Get_Clinic_Services_And_Doctors":
                c = context.get("clinic_context") or {}
                b = context.get("persona_context") or {}
                # Reviewer fixes: (a) the raw clinic row leaked internal doctor ids and
                # ignored the persona gate — serve the gated directory the model is
                # allowed to see; (b) services fell back to [] on non-catalog turns —
                # serve the live catalog so mid-booking price/scope questions work.
                tool_result = {
                    "clinic_name": c.get("clinic_name"),
                    "doctors": (b.get("clinic_doctor_directory") or c.get("doctor_directory") or []),
                    "services": ((b.get("service_facts") or {}).get("catalog")
                                 or (c.get("service_catalog") or b.get("service_catalog") or [])),
                    "branches": c.get("branch_directory") or [],
                }

            elif fn_name == "Get_My_Appointments":
                try:
                    appts = await repository.get_patient_appointments(
                        context.get("clinic_id"),
                        context.get("patient_id"),
                        args.get("booking_number")
                    )
                    tool_result = {"appointments": appts, "count": len(appts)}
                except Exception as exc:
                    tool_result = {"error": str(exc), "appointments": []}

            elif fn_name == "Get_Clinic_Info":
                try:
                    tool_result = await repository.get_clinic_info(
                        {"clinic_id": context.get("clinic_id")})
                except Exception as exc:
                    tool_result = {"error": str(exc), "hours": [], "branches": []}

            elif fn_name == "Recall_Session_History":
                # Server-owned data: the stored summary + the compacted raw turns.
                # The agent cannot fabricate history — only quote what was saved.
                tool_result = recall_session_history(
                    context.get("state_data") if isinstance(context.get("state_data"), dict) else {})
            else:
                tool_result = {"error": "UNKNOWN_TOOL"}

            if not cached and total_tool_calls <= max_tool_calls:
                tool_cache[cache_key] = tool_result
            tool_events.append({
                "name": fn_name,
                "arguments": args,
                "result": tool_result,
                "cache_hit": cached,
            })
            messages.append({
                "role": "tool",
                "tool_call_id": tool_call.get("id"),
                "content": json.dumps(tool_result, ensure_ascii=False),
            })
    return AgentTurnText(final_content or "", tool_events=tool_events, llm_calls=llm_calls,
                         usage=usage_events)


async def _chat_messages(messages: list, *, with_tools: bool = True, force_json: bool = False) -> Any:
    """Continuation call with accumulated messages (same model options).
    
    When with_tools is True: tools are attached and response_format json_object is omitted
    to avoid conflicts on OpenAI/DeepSeek endpoints during function calling turns.
    When with_tools is False: response_format is set to json_object to guarantee structured JSON output.
    """
    body: Dict[str, Any] = {
        "model": settings.LLM_PRIMARY_MODEL,
        "messages": messages,
        "temperature": 0.3,
        "max_tokens": 4000,
        **_provider_options(),
    }
    if with_tools:
        body["tools"] = RECEPTIONIST_TOOLS
        body["tool_choice"] = "auto"
    if force_json or not with_tools:
        body["response_format"] = {"type": "json_object"}

    headers = {"Authorization": f"Bearer {settings.LLM_PRIMARY_API_KEY}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=settings.LLM_TIMEOUT_SECONDS) as client:
        resp = await client.post(f"{settings.LLM_PRIMARY_BASE_URL.rstrip('/')}/chat/completions", headers=headers, json=body)
        if resp.status_code != 200:
            raise RuntimeError(f"primary LLM HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
    message = data["choices"][0]["message"]
    # Carry the provider's real token accounting back to the caller so the usage rows
    # stop being char-count estimates.
    message["_usage"] = data.get("usage") or {}
    return message


async def compose_patient_reply(reply_context: Dict[str, Any]) -> Dict[str, Any]:
    """Ask the model to write the final patient reply from authoritative facts only.

    The model receives no reply template and no response-code phrase table. It receives
    a compact fact catalog, analyzes it, writes a natural Arabic response, and cites the
    fact IDs it used. The structured evidence contract is validated without regex.
    """
    from app.core.response_context import validate_composer_output

    messages: list[Dict[str, Any]] = [
        {"role": "system", "content": load_response_composer_system_message()},
        {
            "role": "user",
            "content": json.dumps(reply_context, ensure_ascii=False, separators=(",", ":"), default=str),
        },
    ]
    max_attempts = max(1, int(getattr(settings, "LLM_COMPOSER_MAX_ATTEMPTS", 2)))
    validation_errors: list[str] = []

    for attempt in range(1, max_attempts + 1):
        body: Dict[str, Any] = {
            "model": settings.LLM_PRIMARY_MODEL,
            "messages": messages,
            "temperature": float(getattr(settings, "LLM_COMPOSER_TEMPERATURE", 0.35)),
            "max_tokens": int(getattr(settings, "LLM_COMPOSER_MAX_TOKENS", 2000)),
            "response_format": {"type": "json_object"},
            **_provider_options(),
        }
        headers = {
            "Authorization": f"Bearer {settings.LLM_PRIMARY_API_KEY}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=settings.LLM_TIMEOUT_SECONDS) as client:
            resp = await client.post(
                f"{settings.LLM_PRIMARY_BASE_URL.rstrip('/')}/chat/completions",
                headers=headers,
                json=body,
            )
        if resp.status_code != 200:
            raise RuntimeError(f"response composer HTTP {resp.status_code}: {resp.text[:300]}")

        data = resp.json()
        message = ((data.get("choices") or [{}])[0].get("message") or {})
        raw = _extract_message_content(message)
        parsed, validation_errors = validate_composer_output(raw, reply_context)
        if parsed is not None:
            parsed["raw_output"] = raw
            parsed["attempts"] = attempt
            parsed["usage"] = data.get("usage") or {}
            parsed["input_chars"] = len(messages[1]["content"])
            return parsed

        messages.extend([
            {"role": "assistant", "content": raw},
            {
                "role": "user",
                "content": json.dumps({
                    "instruction": "صحح المخرج السابق فقط. لا تضف حقائق جديدة.",
                    "validation_errors": validation_errors,
                    "valid_fact_ids": reply_context.get("fact_ids") or [],
                }, ensure_ascii=False, separators=(",", ":")),
            },
        ])

    raise ValueError("response composer contract invalid: " + ",".join(validation_errors))


# ── DeepSeek Repair Chain (chainLlm + DeepSeek Repair Model) ────────────────────
async def call_repair_model(prompt: str) -> str:
    """Source node: DeepSeek Repair Chain (text = $json.prompt) + DeepSeek Repair Model options."""
    body = {
        "model": settings.LLM_REPAIR_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
        "max_tokens": 800,
        **_provider_options(),
    }
    headers = {"Authorization": f"Bearer {settings.LLM_REPAIR_API_KEY}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=settings.LLM_TIMEOUT_SECONDS) as client:
        resp = await client.post(f"{settings.LLM_REPAIR_BASE_URL.rstrip('/')}/chat/completions", headers=headers, json=body)
        if resp.status_code != 200:
            raise RuntimeError(f"repair LLM HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
    return data["choices"][0]["message"]["content"]


