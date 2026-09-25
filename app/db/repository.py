"""Asyncpg repository — one async function per n8n Postgres node (faithful 1:1 port).

Every function binds parameters positionally in the exact order of the node's
``queryReplacement`` expression and executes the verbatim SQL from
``app/db/queries.py`` (source of truth: n8n_reference/sql_queries_reference.json).

Port conventions (docs/port_conventions.md):
- Returns are plain ``dict``s (``dict(row)``); jsonb/json columns are decoded
  with ``json.loads`` when asyncpg hands them back as ``str`` (decoded by the
  statement's column type OID, so text columns are never corrupted).
- uuid-shaped string parameters are converted to ``uuid.UUID`` before binding
  (asyncpg is strict about parameter types); parameters the SQL itself casts via
  ``$N::text`` stay strings. ISO-8601 timestamp strings are parsed to
  ``datetime`` for ``timestamptz`` parameters.
- ``from app.db.pool import get_pool`` is imported inside each function so this
  module stays importable without a live database.
- Where the n8n queryReplacement reads another node's output, this port receives
  it on the ``ctx`` dict under a documented key; those wirings are marked with
  ``# PORT-TODO(n8n): param mapping needs runtime validation``.
"""

import datetime
import json
import math
from decimal import Decimal
import re
import uuid
from typing import TYPE_CHECKING, Any, Optional, Sequence

from app.db import queries

if TYPE_CHECKING:  # pragma: no cover - typing only, keeps the module import-time dependency free
    import asyncpg

__all__ = [
    "log_incoming_message",
    "get_clinic_context",
    "get_conversation_state",
    "verify_k2_inbound_signature",
    "k2_inbound_burst_rate_gate",
    "log_k2_rate_decision",
    "mark_k2_burst_message_deferred",
    "get_recent_window_2h",
    "get_active_handoff_request",
    "resolve_booking_ids",
    "resolve_doctor_inquiry",
    "resolve_service_fact",
    "resolve_branch_inquiry",
    "persist_pending_confirmation",
    "read_fresh_offer_midturn",
    "lookup_business_time_context",
    "claim_operation",
    "finalize_operation",
    "execute_approved_create_appointment",
    "execute_approved_cancel_appointment",
    "execute_approved_reschedule_appointment",
    "log_agent_audit_entry",
    "insert_ai_request_usage",
    "get_clinic_info",
    "log_outgoing_message",
    "save_conversation_state_with_retry",
    "get_patient_appointments",
]

# ---------------------------------------------------------------------------
# Binding helpers (n8n/JS semantics preserved)
# ---------------------------------------------------------------------------

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_JSON_TYPE_OIDS = frozenset({114, 3802})  # pg_type OIDs: json, jsonb
_ZERO_UUID = "00000000-0000-0000-0000-000000000000"

# Build_Retry_RPC_Body_v18.js — ORCH-SEC terminal operation states.
TERMINAL_OP_STATES = ("COMPLETED", "CANCELLED", "FAILED_FINAL")

# Build_Retry_RPC_Body_v18.js — current-turn state keys replayed on retry.
CURRENT_TURN_KEYS = (
    "patient_data_review", "conversation_stage", "required_next_step", "state_schema_version",
    "availability_inquiry", "availability_lookup_lineage_status", "availability_lineage", "availability_outcome",
    "availability_alternatives", "availability_requested_time_unavailable", "deterministic_slot_lookup", "slot_lookup_ready",
    "next_best_missing_human_field", "missing_human_fields", "superseded_operation", "last_intent", "current_intent",
    "active_operation", "operation_status", "operation_state", "operation_id", "operation_action", "original_response_code",
    "routing_action", "resume_eligible", "retryable", "failure_code", "migration_status", "migration_issues", "pending_action",
    "last_open_question", "waiting_for_reference", "confirmation_state", "confirmation_target", "confirmation_delivery_status",
    "confirmation_delivery_recorded_at", "confirmation_ttl_seconds", "confirmation_expires_at", "confirmation_target_hash",
    "draft_started_at", "draft_expires_at", "draft_ttl_seconds", "business_time_checked", "business_time_status",
    "business_time_timezone", "business_time_source", "business_time_error_code", "confirmation_target_invalidated",
    "confirmation_contract", "appointment_id", "response_code", "confidence", "escalate", "conversation_summary",
    "recent_turns", "last_message_id", "last_idempotency_key", "last_channel", "last_updated",
)


def _js_str(value: Any) -> str:
    """JavaScript String(value) coercion (None -> '')."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _text(value: Any) -> Optional[str]:
    """Bind as a text parameter: SQL NULL preserved, scalars JS-String-coerced."""
    if value is None:
        return None
    return _js_str(value)


def _uuid(value: Any) -> Any:
    """Convert uuid-shaped strings to uuid.UUID before binding.

    Non-uuid-shaped values are passed through so PostgreSQL raises the same
    invalid-input error the n8n node would have produced.
    """
    if value is None or isinstance(value, uuid.UUID):
        return value
    text = value if isinstance(value, str) else str(value)
    if _UUID_RE.match(text):
        return uuid.UUID(text)
    return value


def _ts(value: Any) -> Any:
    """Parse ISO-8601 strings to datetime for timestamptz parameters."""
    if value is None or isinstance(value, datetime.datetime):
        return value
    text = str(value).strip()
    if text.endswith("Z") or text.endswith("z"):
        text = text[:-1] + "+00:00"
    return datetime.datetime.fromisoformat(text)


def _jsonb(value: Any) -> Any:
    """Bind as a jsonb parameter: JSON strings pass through, objects are dumped."""
    if value is None or isinstance(value, (str, bytes)):
        return value
    return json.dumps(value)


def _int_or(value: Any, default: int) -> int:
    """JS ``x || default`` bound as an integer parameter."""
    if not value:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _strict_true(value: Any) -> bool:
    """JS strict ``value === true``."""
    return value is True


def _truthy(value: Any) -> bool:
    """JavaScript truthiness ({} and [] are truthy, unlike Python)."""
    if value is None or value is False:
        return False
    if isinstance(value, str) and value == "":
        return False
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)) and value == 0:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    return True


def _js_or(*values: Any) -> Any:
    """JS ``a || b || ...`` chain (returns the last value when all are falsy)."""
    for value in values:
        if _truthy(value):
            return value
    return values[-1] if values else None


def _first(*values: Any) -> Any:
    """JS ``a ?? b ?? ...`` nullish coalescing chain."""
    for value in values:
        if value is not None:
            return value
    return None


def _obj(value: Any) -> dict:
    """JS ``x && typeof x === 'object' ? x : {}`` guard for object-valued keys."""
    return value if isinstance(value, dict) else {}


def _nv(ctx: dict) -> dict:
    """n8n ``$('Normalize & Validate').first().json`` — ctx['normalized'] when present, else ctx itself."""
    if not isinstance(ctx, dict):
        return {}
    nv = ctx.get("normalized")
    return nv if isinstance(nv, dict) else ctx


# ---------------------------------------------------------------------------
# Result decoding helpers (jsonb/json -> python objects by column type OID)
# ---------------------------------------------------------------------------


def _decode_record(row: Sequence[Any], attributes: Sequence[Any]) -> dict:
    decoded: dict = {}
    for attribute, value in zip(attributes, row):
        if isinstance(value, str) and attribute.type.oid in _JSON_TYPE_OIDS:
            try:
                decoded[attribute.name] = json.loads(value)
            except ValueError:
                decoded[attribute.name] = value
        elif isinstance(value, uuid.UUID):
            # n8n's Postgres node hands JS strings to downstream code nodes; the
            # deterministic ports compare ids with JS string equality, so uuids
            # must surface as strings (js String(uuid) would be "[object Object]").
            decoded[attribute.name] = str(value)
        elif isinstance(value, Decimal):
            decoded[attribute.name] = float(value)
        else:
            decoded[attribute.name] = value
    return decoded


async def _fetchrow(conn: "asyncpg.Connection", sql: str, *args: Any) -> Optional[dict]:
    statement = await conn.prepare(sql)
    rows = await statement.fetch(*args)
    if not rows:
        return None
    return _decode_record(rows[0], statement.get_attributes())


# ---------------------------------------------------------------------------
# Inbound security gate + message logging
# ---------------------------------------------------------------------------


async def log_incoming_message(normalized: dict) -> dict:
    """Source node: Log Incoming Message (extracted/sql/Log_Incoming_Message.json)."""
    nv = normalized or {}
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_LOG_INCOMING_MESSAGE,
            _uuid(nv.get("conversation_id")),
            _uuid(nv.get("clinic_id")),
            _uuid(nv.get("patient_id")),
            nv.get("message_text"),
            _ts(nv.get("received_at")),
            _jsonb(nv.get("metadata")),
            nv.get("idempotency_key"),
            _uuid(nv.get("channel_id")),
            nv.get("channel_type"),
            nv.get("chat_id"),
        )
    return dict(row) if row else {}


async def verify_k2_inbound_signature(signature_ctx: dict) -> dict:
    """Source node: Verify K2 Inbound Signature (extracted/sql/Verify_K2_Inbound_Signature.json).

    ``signature_ctx`` mirrors the output of the n8n node Extract K2 Signature Context.
    """
    ctx = signature_ctx or {}
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_VERIFY_K2_INBOUND_SIGNATURE,
            _uuid(ctx.get("clinic_id")),
            _text(ctx.get("channel_id")),
            _text(ctx.get("channel_type")),
            _text(ctx.get("k2_signed_payload")),
            _text(ctx.get("k2_signature")),
            _strict_true(ctx.get("deferred_replay")),
        )
    return dict(row) if row else {}


async def k2_inbound_burst_rate_gate(ctx: dict) -> dict:
    """Source node: K2 Inbound Burst Rate Gate (extracted/sql/K2_Inbound_Burst_Rate_Gate.json)."""
    nv = _nv(ctx)
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_K2_INBOUND_BURST_RATE_GATE,
            _uuid(nv.get("clinic_id")),
            _uuid(nv.get("patient_id")),
            _uuid(nv.get("conversation_id")),
            nv.get("message_text"),
            _strict_true(nv.get("deferred_replay")),
        )
    return dict(row) if row else {}


async def log_k2_rate_decision(ctx: dict) -> dict:
    """Source node: Log K2 Rate Decision (extracted/sql/Log_K2_Rate_Decision.json).

    ``ctx`` carries the Normalize & Validate ids (top level or under "normalized")
    plus ``gate``: the K2 Inbound Burst Rate Gate result row.
    # PORT-TODO(n8n): param mapping needs runtime validation — the "gate" key
    # name is this port's wiring of $('K2 Inbound Burst Rate Gate').first().json.
    """
    nv = _nv(ctx)
    gate = _obj(ctx.get("gate")) if isinstance(ctx, dict) else {}
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_LOG_K2_RATE_DECISION,
            _uuid(nv.get("clinic_id")),
            _uuid(nv.get("patient_id")),
            _uuid(nv.get("conversation_id")),
            _strict_true(gate.get("allowed")),
            _strict_true(gate.get("priority_allow")),
            _int_or(gate.get("recent_conversation_count"), 0),
            _int_or(gate.get("recent_clinic_count"), 0),
            _int_or(gate.get("conversation_limit"), 8),
            _int_or(gate.get("clinic_limit"), 300),
            _int_or(gate.get("window_seconds"), 15),
        )
    return dict(row) if row else {}


async def mark_k2_burst_message_deferred(ctx: dict) -> dict:
    """Source node: Mark K2 Burst Message Deferred (extracted/sql/Mark_K2_Burst_Message_Deferred.json).

    ``ctx`` carries the Normalize & Validate fields (top level or under
    "normalized"), plus ``log_incoming_message`` (the Log Incoming Message result
    row, for the persisted message id) and ``gate`` (the K2 Inbound Burst Rate
    Gate result row, for priority_allow).
    # PORT-TODO(n8n): param mapping needs runtime validation — the
    # "log_incoming_message" and "gate" key names are this port's wiring of
    # $('Log Incoming Message').first().json / $('K2 Inbound Burst Rate Gate').first().json.
    """
    nv = _nv(ctx)
    ctx = ctx if isinstance(ctx, dict) else {}
    incoming = _obj(ctx.get("log_incoming_message"))
    gate = _obj(ctx.get("gate"))
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_MARK_K2_BURST_MESSAGE_DEFERRED,
            _uuid(nv.get("clinic_id")),
            _uuid(nv.get("patient_id")),
            _uuid(nv.get("conversation_id")),
            _text(nv.get("channel_type")),
            _text(nv.get("channel_id")),
            _uuid(incoming.get("id")),
            nv.get("message_text"),
            _ts(nv.get("received_at")),
            _text(nv.get("source_event_id")),
            _strict_true(gate.get("priority_allow")),
        )
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Context / state reads
# ---------------------------------------------------------------------------


async def get_clinic_context(normalized: dict) -> dict:
    """Source node: Get Clinic Context (extracted/sql/Get_Clinic_Context.json)."""
    nv = normalized or {}
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_GET_CLINIC_CONTEXT,
            _uuid(nv.get("conversation_id")),
            _uuid(nv.get("clinic_id")),
            _uuid(nv.get("patient_id")),
        )
    return dict(row) if row else {}


async def get_conversation_state(normalized: dict) -> dict:
    """Source node: Get Conversation State (extracted/sql/Get_Conversation_State.json)."""
    nv = normalized or {}
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_GET_CONVERSATION_STATE,
            _uuid(nv.get("conversation_id")),
            _uuid(nv.get("clinic_id")),
            _uuid(nv.get("patient_id")),
        )
    return dict(row) if row else {}


async def get_recent_window_2h(ctx: dict) -> dict:
    """Source node: Get Recent Window 2h (extracted/sql/Get_Recent_Window_2h.json)."""
    nv = _nv(ctx)
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_GET_RECENT_WINDOW_2H,
            _uuid(nv.get("conversation_id")),
            _uuid(nv.get("clinic_id")),
            _uuid(nv.get("patient_id")),
            _ts(nv.get("received_at")),
            nv.get("message_text"),
        )
    return dict(row) if row else {}


async def get_active_handoff_request(normalized: dict) -> dict:
    """Source node: Get Active Handoff Request (extracted/sql/Get_Active_Handoff_Request.json)."""
    nv = normalized or {}
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_GET_ACTIVE_HANDOFF_REQUEST,
            _uuid(nv.get("conversation_id")),
            _uuid(nv.get("clinic_id")),
            _uuid(nv.get("patient_id")),
        )
    return dict(row) if row else {}


async def get_clinic_usage_summary(ctx: dict) -> dict:
    """Per-clinic token/cost accounting over ai_requests (added 2026-09-18).

    ctx keys: clinic_id (required), days (default 30), include_recent (default False).
    Returns {totals: {calls, input_tokens, output_tokens, total_tokens, cost},
             by_model: [...], by_day: [...], recent: [...]}.
    """
    import json as _json

    clinic_id = _js_str(_js_or((ctx or {}).get("clinic_id"), ""))
    if not clinic_id:
        return {}
    try:
        days = max(1, min(int((ctx or {}).get("days") or 30), 365))
    except (TypeError, ValueError):
        days = 30
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        totals = await conn.fetchrow(
            """SELECT count(*)::int AS calls,
                      COALESCE(sum(input_tokens), 0)::bigint AS input_tokens,
                      COALESCE(sum(output_tokens), 0)::bigint AS output_tokens,
                      COALESCE(sum(total_tokens), 0)::bigint AS total_tokens,
                      COALESCE(sum(cost), 0)::numeric AS cost
               FROM ai_requests
               WHERE clinic_id = $1::uuid AND created_at >= now() - ($2::text || ' days')::interval""",
            clinic_id, str(days))
        by_model = await conn.fetch(
            """SELECT model, count(*)::int AS calls,
                      COALESCE(sum(input_tokens), 0)::bigint AS input_tokens,
                      COALESCE(sum(output_tokens), 0)::bigint AS output_tokens,
                      COALESCE(sum(total_tokens), 0)::bigint AS total_tokens,
                      COALESCE(sum(cost), 0)::numeric AS cost
               FROM ai_requests
               WHERE clinic_id = $1::uuid AND created_at >= now() - ($2::text || ' days')::interval
               GROUP BY model ORDER BY total_tokens DESC""",
            clinic_id, str(days))
        by_day = await conn.fetch(
            """SELECT date_trunc('day', created_at)::date AS day, count(*)::int AS calls,
                      COALESCE(sum(input_tokens), 0)::bigint AS input_tokens,
                      COALESCE(sum(output_tokens), 0)::bigint AS output_tokens,
                      COALESCE(sum(total_tokens), 0)::bigint AS total_tokens,
                      COALESCE(sum(cost), 0)::numeric AS cost
               FROM ai_requests
               WHERE clinic_id = $1::uuid AND created_at >= now() - ($2::text || ' days')::interval
               GROUP BY 1 ORDER BY 1 DESC""",
            clinic_id, str(days))
        recent = await conn.fetch(
            """SELECT conversation_id, provider, model, input_tokens, output_tokens,
                      total_tokens, cost, latency_ms, created_at
               FROM ai_requests
               WHERE clinic_id = $1::uuid
               ORDER BY created_at DESC LIMIT 20""",
            clinic_id)
    return {
        "clinic_id": clinic_id, "days": days,
        "totals": dict(totals) if totals else {},
        "by_model": [dict(r) for r in by_model],
        "by_day": [dict(r) for r in by_day],
        "recent": [dict(r) for r in recent],
    }


async def get_outgoing_reply(ctx: dict) -> Optional[str]:
    """The delivered reply text for this idempotency key, or None.

    The outgoing Log Outgoing Message row's message_id is md5(<idempotency_key> || ':outgoing')
    (queries.py Log_Outgoing_Message). Non-empty content is REQUIRED: an empty outgoing row
    (a turn that logged nothing) must not suppress a retry — the patient never received
    anything. With content present, a channel retry gets the SAME reply back instead of
    silence, even though the turn itself will not re-run (the claim ledger keeps
    mutations safe).
    """
    import hashlib

    key = _js_str(_js_or((ctx or {}).get("idempotency_key"), ""))
    if not key:
        return None
    from app.db.pool import get_pool

    outgoing_id = hashlib.md5((key + ":outgoing").encode("utf-8")).hexdigest()
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT content FROM messages WHERE id = $1::uuid AND NULLIF(content, '') IS NOT NULL",
            outgoing_id,
        )
    return (str(row["content"]) if row and row["content"] else None) or None


async def read_fresh_offer_midturn(ctx: dict) -> dict:
    """Source node: Read Fresh Offer (Midturn) (extracted/sql/Read_Fresh_Offer_Midturn.json)."""
    nv = _nv(ctx)
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_READ_FRESH_OFFER_MIDTURN,
            _uuid(nv.get("conversation_id")),
            _uuid(nv.get("clinic_id")),
            _uuid(nv.get("patient_id")),
        )
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Deterministic resolvers
# ---------------------------------------------------------------------------


async def resolve_booking_ids(ctx: dict) -> dict:
    """Source node: Resolve Booking IDs (Deterministic) (extracted/sql/Resolve_Booking_IDs_Deterministic.json).

    ``ctx`` mirrors the queryReplacement IIFE: the Normalize & Validate ids (top
    level or under "normalized") plus the agent output item —
    ``repaired_contract`` (Validate Repaired Contract output, used only when
    ``_contract_status == 'VALID'``) or ``agent_output`` (Normalize Agent Output
    output).
    # PORT-TODO(n8n): param mapping needs runtime validation — the
    # "repaired_contract" / "agent_output" key names are this port's wiring of
    # $('Validate Repaired Contract (Deterministic)') / $('Normalize Agent Output (Deterministic)').
    """
    nv = _nv(ctx)
    ctx = ctx if isinstance(ctx, dict) else {}
    # JS: try repaired contract first when VALID, else Normalize Agent Output item.
    agent_item = ctx.get("repaired_contract")
    if not (isinstance(agent_item, dict) and agent_item.get("_contract_status") == "VALID"):
        agent_item = _obj(ctx.get("agent_output"))
    contract = _js_or(agent_item.get("contract"), agent_item.get("agent_contract"), {})
    contract = contract if isinstance(contract, dict) else {}
    entities = _obj(contract.get("entities"))
    booking = _js_or(agent_item.get("booking_context"), agent_item.get("slot_state"), {})
    booking = booking if isinstance(booking, dict) else {}
    operation_proposal = _obj(contract.get("operation_proposal"))
    normalization = _obj(agent_item.get("_normalization"))

    def _v(value: Any) -> str:
        # JS: const v = (a, d = '') => a === null || a === undefined ? d : String(a);
        return "" if value is None else _js_str(value)

    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_RESOLVE_BOOKING_IDS_DETERMINISTIC,
            _uuid(nv.get("clinic_id")),
            _uuid(nv.get("patient_id")),
            _uuid(nv.get("conversation_id")),
            _v(_js_or(entities.get("appointment_id"), agent_item.get("appointment_id"))),
            _v(_js_or(entities.get("expected_old_slot_id"), agent_item.get("expected_old_slot_id"))),
            _v(_js_or(entities.get("new_slot_id"), agent_item.get("new_slot_id"))),
            _v(_js_or(entities.get("doctor_id"), booking.get("doctor_id"))),
            _v(_js_or(entities.get("service_id"), booking.get("service_id"))),
            _v(_js_or(entities.get("date"), booking.get("date"))),
            _v(_js_or(entities.get("time"), booking.get("time"))),
            _v(_js_or(entities.get("doctor_name"), booking.get("doctor_name"))),
            _v(_js_or(entities.get("service_name"), booking.get("service_name"))),
            _v(_js_or(operation_proposal.get("type"), normalization.get("operation_type"), normalization.get("turn_intent"))),
            _v(_js_or(entities.get("booking_number"), booking.get("booking_number"))),
        )
    return dict(row) if row else {}


async def resolve_doctor_inquiry(ctx: dict) -> dict:
    """Source node: Resolve Doctor Inquiry (Deterministic) (extracted/sql/Resolve_Doctor_Inquiry_Deterministic.json)."""
    nv = _nv(ctx)
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_RESOLVE_DOCTOR_INQUIRY_DETERMINISTIC,
            _uuid(nv.get("clinic_id")),
            nv.get("message_text"),
        )
    return dict(row) if row else {}


async def resolve_service_fact(ctx: dict) -> dict:
    """Source node: Resolve Service Fact (Deterministic) (extracted/sql/Resolve_Service_Fact_Deterministic.json)."""
    nv = _nv(ctx)
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_RESOLVE_SERVICE_FACT_DETERMINISTIC,
            nv.get("message_text"),
            _uuid(nv.get("clinic_id")),
        )
    return dict(row) if row else {}


async def resolve_branch_inquiry(ctx: dict) -> dict:
    """Source node: Resolve Branch Inquiry (Deterministic) (extracted/sql/Resolve_Branch_Inquiry_Deterministic.json)."""
    nv = _nv(ctx)
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_RESOLVE_BRANCH_INQUIRY_DETERMINISTIC,
            _uuid(nv.get("clinic_id")),
            nv.get("message_text"),
        )
    return dict(row) if row else {}


async def lookup_business_time_context(ctx: dict) -> dict:
    """Source node: Lookup Business Time Context (extracted/sql/Lookup_Business_Time_Context.json).

    ``ctx`` carries the Normalize & Validate ids (top level or under "normalized")
    plus ``guard``: the Execution Transition Guard (Deterministic) item carrying
    ``system_decision`` and ``slot_state``.
    # PORT-TODO(n8n): param mapping needs runtime validation — the "guard" key
    # name is this port's wiring of $('Execution Transition Guard (Deterministic)').item.json.
    """
    nv = _nv(ctx)
    ctx = ctx if isinstance(ctx, dict) else {}
    guard = _obj(ctx.get("guard"))
    system_decision = _obj(guard.get("system_decision"))
    confirmation_target = _obj(system_decision.get("confirmation_target"))
    slot_state = _obj(guard.get("slot_state"))
    slot_id = _js_or(
        confirmation_target.get("slot_id"),
        confirmation_target.get("new_slot_id"),
        slot_state.get("slot_id"),
        _ZERO_UUID,
    )
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_LOOKUP_BUSINESS_TIME_CONTEXT,
            _uuid(nv.get("clinic_id")),
            _text(slot_id),
        )
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Operation lifecycle (claim / confirm / execute / finalize)
# ---------------------------------------------------------------------------


async def persist_pending_confirmation(ctx: dict) -> dict:
    """Source node: Persist Pending Confirmation (Deterministic) (extracted/sql/Persist_Pending_Confirmation_Deterministic.json).

    ``ctx`` is the deterministic response item carrying ``response_code``,
    ``confirmation_target``, ``claim_conversation_id``, ``booking_context``,
    ``system_decision``, ``current_turn_lineage`` and ``turn_lineage``.
    """
    item = ctx if isinstance(ctx, dict) else {}
    confirmation_target = _obj(item.get("confirmation_target"))
    booking_context = _obj(item.get("booking_context"))
    system_decision = _obj(item.get("system_decision"))
    decision_lineage = _obj(system_decision.get("current_turn_lineage"))
    current_turn_lineage = _obj(item.get("current_turn_lineage"))
    turn_lineage = _obj(item.get("turn_lineage"))
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_PERSIST_PENDING_CONFIRMATION_DETERMINISTIC,
            _text(_js_or(item.get("response_code"), "")),
            _text(_js_or(confirmation_target.get("confirmation_id"), "")),
            _text(_js_or(confirmation_target.get("clinic_id"), item.get("clinic_id"), booking_context.get("clinic_id"), "")),
            _text(_js_or(confirmation_target.get("patient_id"), item.get("patient_id"), "")),
            _text(_js_or(
                confirmation_target.get("conversation_id"),
                item.get("claim_conversation_id"),
                decision_lineage.get("conversation_id"),
                item.get("conversation_id"),
                current_turn_lineage.get("conversation_id"),
                turn_lineage.get("conversation_id"),
                "",
            )),
            _text(_js_or(confirmation_target.get("action"), "")),
            _text(_js_or(confirmation_target.get("context_fingerprint"), "")),
            _jsonb(item.get("confirmation_target") if _truthy(item.get("confirmation_target")) else {}),
            _int_or(confirmation_target.get("confirmation_ttl_seconds"), 600),
        )
    return dict(row) if row else {}


async def claim_operation(ctx: dict) -> dict:
    """Source node: Claim Operation (Atomic) (extracted/sql/Claim_Operation_Atomic.json).

    ``ctx`` mirrors the Prepare Operation Claim Input item: ``claim_clinic_id``,
    ``claim_patient_id``, ``claim_conversation_id``, ``claim_action``,
    ``claim_confirmation_id``, ``claim_target_fingerprint``.
    """
    item = ctx if isinstance(ctx, dict) else {}
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_CLAIM_OPERATION_ATOMIC,
            _uuid(_js_str(item.get("claim_clinic_id"))) if _truthy(item.get("claim_clinic_id")) else "",
            _text(item.get("claim_patient_id")) if _truthy(item.get("claim_patient_id")) else "",
            _text(item.get("claim_conversation_id")) if _truthy(item.get("claim_conversation_id")) else "",
            _text(item.get("claim_action")) if _truthy(item.get("claim_action")) else "__NULL__",
            _text(item.get("claim_confirmation_id")) if _truthy(item.get("claim_confirmation_id")) else "",
            _text(item.get("claim_target_fingerprint")) if _truthy(item.get("claim_target_fingerprint")) else "__NULL__",
        )
    return dict(row) if row else {}


async def finalize_operation(ctx: dict) -> dict:
    """Source node: Finalize Operation (Atomic) (extracted/sql/Finalize_Operation_Atomic.json).

    ``ctx`` mirrors the Prepare Operation Finalize Input item: ``finalize_*`` keys.
    """
    item = ctx if isinstance(ctx, dict) else {}
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_FINALIZE_OPERATION_ATOMIC,
            _uuid(_js_str(_js_or(item.get("finalize_clinic_id"), _ZERO_UUID))),
            _text(_js_or(item.get("finalize_operation_id"), "__NULL__")),
            _text(_js_or(item.get("finalize_status"), "INCONCLUSIVE")),
            _text(_js_or(item.get("finalize_mutation_status"), "UNKNOWN")),
            _text(_js_or(item.get("finalize_response_b64"), "__NULL__")),
            _text(_js_or(item.get("finalize_child_execution_id"), "__NULL__")),
            _text(_js_or(item.get("finalize_last_error_b64"), "__NULL__")),
        )
    return dict(row) if row else {}


async def find_active_appointment_for_slot(ctx: dict) -> dict:
    """Port-added double-booking guard (2026-09-19, QUERY_FIND_ACTIVE_APPOINTMENT_FOR_SLOT).

    A create that committed but whose process died before the state save leaves the
    conversation at AWAIT_CONFIRMATION; the patient's re-affirm mints a NEW
    operation_id the claim ledger never saw, so the ledger alone cannot refuse the
    second booking. Returns the existing active ('scheduled', not deleted) appointment
    for this patient on this slot, or {} when the slot is free.
    """
    item = ctx if isinstance(ctx, dict) else {}
    slot_id = _text(_js_or(item.get("slot_id"), ""))
    if not slot_id:
        return {}
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_FIND_ACTIVE_APPOINTMENT_FOR_SLOT,
            _uuid(_js_str(_js_or(item.get("clinic_id"), ""))),
            _uuid(_js_str(_js_or(item.get("patient_id"), ""))),
            slot_id,
        )
    return dict(row) if row else {}


async def execute_approved_create_appointment(ctx: dict) -> dict:
    """Source node: Execute Approved Create Appointment (extracted/sql/Execute_Approved_Create_Appointment.json).

    ``ctx`` mirrors the item feeding the node (``slot_id`` / ``slot_state`` /
    ``booking_context`` / ``system_decision`` / ``notes`` / ``appointment_type`` /
    ``conversation_id`` / ``patient_*`` at the top level) plus ``normalized``
    (Normalize & Validate item) and ``claim`` (Apply Operation Claim output).
    # PORT-TODO(n8n): param mapping needs runtime validation — the "claim" and
    # "normalized" key names are this port's wiring of the n8n node references
    # $('Apply Operation Claim (Deterministic)') / $('Normalize & Validate').
    """
    item = ctx if isinstance(ctx, dict) else {}
    nv = _nv(item)
    system_decision = _obj(item.get("system_decision"))
    confirmation_target = _obj(system_decision.get("confirmation_target"))
    decision_booking = _obj(system_decision.get("booking_context"))
    booking_context = _obj(item.get("booking_context"))
    slot_state = _obj(item.get("slot_state"))
    claim = _obj(item.get("claim"))
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_EXECUTE_APPROVED_CREATE_APPOINTMENT,
            _uuid(_js_str(_js_or(nv.get("clinic_id"), ""))),
            _uuid(_js_str(_js_or(nv.get("patient_id"), ""))),
            _text(_js_or(item.get("slot_id"), slot_state.get("slot_id"), confirmation_target.get("slot_id"), "")),
            "scheduled",
            _text(_js_or(item.get("notes"), "")),
            _text(_js_or(item.get("appointment_type"), booking_context.get("appointment_type"), decision_booking.get("appointment_type"), "NEW_VISIT")),
            _text(_js_or(nv.get("correlation_id"), "")),
            _text(_js_or(claim.get("operation_id"), confirmation_target.get("operation_id"), "")),
            _uuid(_js_str(_js_or(item.get("conversation_id"), nv.get("conversation_id"), ""))),
            _text(_js_or(item.get("patient_name"), booking_context.get("patient_name"), decision_booking.get("patient_name"), "")),
            _text(_js_or(item.get("patient_phone"), booking_context.get("patient_phone"), decision_booking.get("patient_phone"), "")),
            _text(_first(item.get("patient_age"), booking_context.get("patient_age"), decision_booking.get("patient_age"), "")),
            _text(_js_or(item.get("patient_address"), booking_context.get("patient_address"), decision_booking.get("patient_address"), "")),
        )
    return dict(row) if row else {}


async def execute_approved_cancel_appointment(ctx: dict) -> dict:
    """Source node: Execute Approved Cancel Appointment (extracted/sql/Execute_Approved_Cancel_Appointment.json).

    ``ctx`` mirrors the item feeding the node (``operation_id`` /
    ``operation_context`` / ``system_decision`` at the top level) plus
    ``normalized`` (Normalize & Validate item).
    """
    item = ctx if isinstance(ctx, dict) else {}
    nv = _nv(item)
    system_decision = _obj(item.get("system_decision"))
    confirmation_target = _obj(system_decision.get("confirmation_target"))
    operation_context = _obj(item.get("operation_context"))
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_EXECUTE_APPROVED_CANCEL_APPOINTMENT,
            _uuid(_js_str(_js_or(nv.get("clinic_id"), ""))),
            _uuid(_js_str(_js_or(nv.get("patient_id"), ""))),
            _text(_js_or(system_decision.get("appointment_id"), confirmation_target.get("appointment_id"), "")),
            _text(_js_or(system_decision.get("cancellation_reason"), confirmation_target.get("cancellation_reason"), "user_requested", "")),
            _text(_js_or(
                item.get("operation_id"),
                operation_context.get("operation_id"),
                system_decision.get("operation_id"),
                confirmation_target.get("operation_id"),
                nv.get("operation_id"),
                nv.get("idempotency_key"),
                "",
            )),
            _text(_js_or(nv.get("correlation_id"), nv.get("message_id"), "")),
        )
    return dict(row) if row else {}


async def execute_approved_reschedule_appointment(ctx: dict) -> dict:
    """Source node: Execute Approved Reschedule Appointment (extracted/sql/Execute_Approved_Reschedule_Appointment.json).

    ``ctx`` mirrors the item feeding the node (``operation_id`` /
    ``operation_context`` / ``system_decision`` at the top level) plus
    ``normalized`` (Normalize & Validate item).
    """
    item = ctx if isinstance(ctx, dict) else {}
    nv = _nv(item)
    system_decision = _obj(item.get("system_decision"))
    confirmation_target = _obj(system_decision.get("confirmation_target"))
    operation_context = _obj(item.get("operation_context"))
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_EXECUTE_APPROVED_RESCHEDULE_APPOINTMENT,
            _uuid(nv.get("clinic_id")),
            _uuid(nv.get("patient_id")),
            _uuid(nv.get("conversation_id")),
            _text(_js_or(system_decision.get("appointment_id"), confirmation_target.get("appointment_id"), "")),
            _text(_js_or(system_decision.get("expected_old_slot_id"), confirmation_target.get("expected_old_slot_id"), "")),
            _text(_js_or(system_decision.get("new_slot_id"), confirmation_target.get("new_slot_id"), "")),
            _text(_js_or(
                item.get("operation_id"),
                operation_context.get("operation_id"),
                system_decision.get("operation_id"),
                confirmation_target.get("operation_id"),
                nv.get("operation_id"),
                nv.get("idempotency_key"),
                "",
            )),
            _text(_js_or(nv.get("correlation_id"), nv.get("message_id"), "")),
        )
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Telemetry writes
# ---------------------------------------------------------------------------


async def log_agent_audit_entry(entry: dict) -> dict:
    """Source node: Log Agent Audit Entry (extracted/sql/Log_Agent_Audit_Entry.json).

    ``entry`` mirrors the Build Audit Entry output item; ``channel_type`` comes
    from the Normalize & Validate item (merged onto the entry, or under
    ``entry["normalized"]``).
    # PORT-TODO(n8n): param mapping needs runtime validation — channel_type is
    # read from $('Normalize & Validate') in n8n and must be merged onto the
    # entry (or passed via entry["normalized"]) by the pipeline runner.
    """
    item = entry if isinstance(entry, dict) else {}
    nv = _nv(item)
    understanding_confidence = item.get("understanding_confidence")
    understanding_draft_live = item.get("understanding_draft_live")
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_LOG_AGENT_AUDIT_ENTRY,
            _text(item.get("conversation_id")),
            _text(item.get("clinic_id")),
            _text(item.get("patient_id")),
            item.get("message_text"),
            _text(item.get("intent")),
            _text(item.get("operation_status")),
            _text(item.get("escalate")),
            _text(item.get("appointment_id")),
            item.get("reply_text"),
            _text(item.get("model")),
            _jsonb(item.get("tool_calls")),
            _text(item.get("tool_call_count")),
            _text(int(round(item["total_time_ms"])) if isinstance(item.get("total_time_ms"), (int, float)) else item.get("total_time_ms")),
            _text(item.get("received_at")),
            _text(_js_or(nv.get("channel_type"), None)),
            _text(_js_or(item.get("understanding_model_intent"), None)),
            "null" if understanding_confidence is None else _js_str(understanding_confidence),
            _jsonb(_js_or(item.get("understanding_failure_types"), [])),
            "true" if understanding_draft_live is True else ("false" if understanding_draft_live is False else "null"),
            _text(_js_or(item.get("response_code"), None)),
        )
    return dict(row) if row else {}


async def insert_ai_request_usage(usage: dict) -> str:
    """Source node: Insert AI Request Usage (extracted/sql/Insert_AI_Request_Usage.json).

    ``usage`` mirrors the Compute AI Request Usage output item. Returns the
    asyncpg execute status tag (the n8n node has no RETURNING).
    """
    item = usage if isinstance(usage, dict) else {}
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        status = await conn.execute(
            queries.QUERY_INSERT_AI_REQUEST_USAGE,
            _text(item.get("clinic_id")),
            _text(item.get("conversation_id")),
            _text(item.get("provider")),
            _text(item.get("model")),
            _text(item.get("input_tokens")),
            _text(item.get("output_tokens")),
            _text(item.get("total_tokens")),
            _text(item.get("cost")),
            _text(item.get("response_received_at")),
            _jsonb(item.get("metadata")),
            _jsonb(item.get("request_payload")),
        )
    return status


async def log_outgoing_message(params: Any) -> dict:
    """Source node: Log Outgoing Message (extracted/sql/Log_Outgoing_Message.json).

    ``params`` is the ordered ``query_params`` list emitted by the Build Outgoing
    Message SQL Parameters stage (queryReplacement: ``$json.query_params``):
    [idempotency_key, conversation_id, clinic_id, patient_id, reply, sent_at,
    metadata_json, user_message_id, llm_model, ai_tokens].
    # PORT-TODO(n8n): param mapping needs runtime validation — the positional
    # binding (and the per-position type conversions below) depend on the
    # Build Outgoing Message SQL Parameters output order.
    """
    if isinstance(params, dict):
        params = params.get("query_params")
    if not isinstance(params, (list, tuple)) or len(params) != 10:
        raise ValueError(
            "log_outgoing_message expects the 10-element query_params list "
            "produced by Build Outgoing Message SQL Parameters"
        )
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await _fetchrow(
            conn,
            queries.QUERY_LOG_OUTGOING_MESSAGE,
            params[0],
            _uuid(params[1]),
            _uuid(params[2]),
            _uuid(params[3]),
            params[4],
            _ts(params[5]),
            _jsonb(params[6]),
            params[7],
            params[8],
            _text(params[9]),
        )
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Conversation state save with stale-retry (v18)
# ---------------------------------------------------------------------------


async def _call_save_state_rpc(
    conn: "asyncpg.Connection",
    p_conversation_id: Any,
    p_state_data: Any,
    p_previous_state_version: Any,
) -> dict:
    """Direct Postgres port of the n8n HTTP node Save Conversation State.

    Body (verbatim from Save_Conversation_State.json jsonBody):
    {p_conversation_id, p_state_data, p_previous_state_version}.
    # PORT-TODO(n8n): param mapping needs runtime validation — the k2 RPC return
    # shape (record with saved/rejected_reason columns vs scalar JSON document)
    # is resolved at runtime: a single jsonb column is unwrapped to its document
    # form, matching PostgREST's scalar-return behavior.
    """
    row = await _fetchrow(
        conn,
        queries.QUERY_SAVE_CONVERSATION_STATE_RPC,
        _uuid(p_conversation_id),
        _jsonb(p_state_data),
        _int_or(p_previous_state_version, 0),
    )
    envelope = dict(row) if row else {}
    if len(envelope) == 1:
        only = next(iter(envelope.values()))
        if isinstance(only, dict):
            return dict(only)
    return envelope


async def save_conversation_state_with_retry(normalized: dict, save_body: dict) -> dict:
    """Source nodes: Build Save State RPC Body + Stale Save? (v18) + Get Conversation State (retry) (v18)
    + Build Retry RPC Body (v18) + Save Conversation State (retry) (v18)
    (extracted/code/Build_Save_State_RPC_Body.js, extracted/code/Build_Retry_RPC_Body_v18.js,
    extracted/nodes/Stale_Save_v18.json, extracted/nodes/Save_Conversation_State.json,
    extracted/nodes/Save_Conversation_State_retry_v18.json).

    ``save_body`` is the Build Save State RPC Body output item
    ({"rpc_body": {p_conversation_id, p_state_data, p_previous_state_version}});
    a bare rpc_body dict is accepted too.

    Flow, exactly like the n8n v18 nodes:
    1. first attempt through the k2_save_conversation_state RPC;
    2. Stale Save? (v18) IF: ``saved === false`` AND
       ``rejected_reason === 'CONCURRENT_STATE_STALE'`` — anything else returns
       the first envelope with retry None;
    3. on stale: re-read fresh state (Get Conversation State (retry) (v18)),
       semantically merge the current-turn keys per Build Retry RPC Body (v18)
       (terminal operation state or a different winning operation_id suppresses
       the current-turn keys, booking_context, slot_state and facts),
       bump state_version, and retry the RPC exactly ONCE.

    Returns {"initial": <first attempt envelope>, "retry": <retry envelope or None>}.
    Every JS-emitted key is kept (saved, rejected_reason, ..., plus
    semantic_merge_conflict on the retry envelope).
    """
    nv = normalized or {}
    body = save_body if isinstance(save_body, dict) else {}
    rpc_body = _obj(body.get("rpc_body")) if "rpc_body" in body else body
    # Save Conversation State jsonBody: p_conversation_id from Normalize & Validate,
    # p_state_data / p_previous_state_version from the Build Save State RPC Body item.
    conversation_id = _js_str(_js_or(nv.get("conversation_id"), ""))
    state_data = _obj(rpc_body.get("p_state_data"))
    previous_state_version = _int_or(rpc_body.get("p_previous_state_version"), 0)

    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        initial = await _call_save_state_rpc(conn, conversation_id, state_data, previous_state_version)

        # Stale Save? (v18): saved === false AND rejected_reason === 'CONCURRENT_STATE_STALE'
        if not (initial.get("saved") is False and initial.get("rejected_reason") == "CONCURRENT_STATE_STALE"):
            return {"initial": initial, "retry": None}

        # Get Conversation State (retry) (v18) — re-read fresh, scrubbed state.
        fresh_row = await _fetchrow(
            conn,
            queries.QUERY_GET_CONVERSATION_STATE_RETRY_V18,
            _uuid(nv.get("conversation_id")),
            _uuid(nv.get("clinic_id")),
            _uuid(nv.get("patient_id")),
        )
        # JS: $('Get Conversation State (retry) (v18)').first().json.state_data || {}
        fresh = _obj(fresh_row.get("state_data")) if fresh_row else {}
        bp_state_data = state_data  # == Build Persistent Conversation State state_data
        # JS: const sv = Number(fresh.state_version || 0) || 0;
        sv = _int_or(fresh.get("state_version"), 0)

        merged = dict(fresh)
        # ORCH-SEC: semantic merge guard (verbatim from Build_Retry_RPC_Body_v18.js).
        fresh_op_state = _js_str(_js_or(fresh.get("operation_state"), "")).upper()
        fresh_op_id = _js_str(_js_or(fresh.get("operation_id"), ""))
        my_op_id = _js_str(_js_or(bp_state_data.get("operation_id"), ""))
        semantic_conflict = (
            fresh_op_state in TERMINAL_OP_STATES
            or (my_op_id != "" and fresh_op_id != "" and fresh_op_id != my_op_id)
        )
        current_turn_keys = CURRENT_TURN_KEYS
        if semantic_conflict:
            current_turn_keys = ("last_channel", "last_updated")
        for key in current_turn_keys:
            if key in bp_state_data:
                merged[key] = bp_state_data[key]
        if not semantic_conflict and isinstance(bp_state_data.get("booking_context"), dict):
            merged["booking_context"] = bp_state_data["booking_context"]
        if not semantic_conflict and isinstance(bp_state_data.get("slot_state"), dict):
            merged["slot_state"] = bp_state_data["slot_state"]
        if not semantic_conflict and isinstance(bp_state_data.get("facts"), dict):
            # JS spread of non-object values would degrade to index properties;
            # the deterministic equivalent keeps only dict-typed facts.
            fresh_facts = _obj(fresh.get("facts"))
            retry_facts = dict(fresh_facts)
            retry_facts.update(bp_state_data["facts"])
            if _truthy(fresh_facts.get("patient")) or _truthy(bp_state_data["facts"].get("patient")):
                retry_facts["patient"] = {
                    **_obj(fresh_facts.get("patient")),
                    **_obj(bp_state_data["facts"].get("patient")),
                }
            if _truthy(fresh_facts.get("clinic")) or _truthy(bp_state_data["facts"].get("clinic")):
                retry_facts["clinic"] = {
                    **_obj(fresh_facts.get("clinic")),
                    **_obj(bp_state_data["facts"].get("clinic")),
                }
            merged["facts"] = retry_facts

        retry = await _call_save_state_rpc(
            conn,
            _js_str(_js_or(nv.get("conversation_id"), "")),
            {**merged, "state_version": sv + 1},
            sv,
        )
        # Build Retry RPC Body (v18) emits semantic_merge_conflict next to rpc_body;
        # keep it on the retry envelope unless the RPC itself returned that column.
        retry.setdefault("semantic_merge_conflict", semantic_conflict)
    return {"initial": initial, "retry": retry}


async def get_clinic_info(ctx: dict) -> dict:
    """Clinic working hours + active branches (Get_Clinic_Info tool, added 2026-09-25).

    ctx keys: clinic_id (required). Returns {"hours": [...], "branches": [...]}.
    """
    clinic_id = _js_str(_js_or((ctx or {}).get("clinic_id"), ""))
    if not clinic_id:
        return {}
    from app.db.pool import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        hours = await conn.fetch(
            """SELECT day_of_week, open_time, close_time, is_off_day
               FROM clinic_business_hours
               WHERE clinic_id = $1::uuid AND deleted_at IS NULL
               ORDER BY day_of_week""",
            _uuid(clinic_id))
        branches = await conn.fetch(
            """SELECT name, address, phone
               FROM branches
               WHERE clinic_id = $1::uuid AND deleted_at IS NULL AND is_active = true
               ORDER BY name""",
            _uuid(clinic_id))
    return {
        "hours": [dict(r) for r in hours],
        "branches": [dict(r) for r in branches],
    }


async def get_patient_appointments(
    clinic_id: Optional[str],
    patient_id: Optional[str],
    booking_number: Optional[str] = None,
) -> list[dict]:
    """Retrieve active/upcoming appointments for a patient in this clinic."""
    if not clinic_id or not patient_id:
        return []
    from app.db.pool import get_pool

    pool = await get_pool()
    sql = """
        SELECT a.id::text, a.booking_number, a.scheduled_at::text, a.appointment_status,
               d.name as doctor_name, s.name as service_name
        FROM appointments a
        LEFT JOIN doctors d ON d.id = a.doctor_id
        LEFT JOIN services s ON s.id = a.service_id
        WHERE a.clinic_id = $1::uuid AND a.patient_id = $2::uuid AND a.deleted_at IS NULL
          AND a.appointment_status IN ('scheduled', 'confirmed')
          AND ($3::text IS NULL OR a.booking_number = $3::text OR a.id::text = $3::text)
        ORDER BY a.scheduled_at ASC
        LIMIT 5;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, _uuid(clinic_id), _uuid(patient_id), booking_number)
    return [dict(r) for r in rows] if rows else []
