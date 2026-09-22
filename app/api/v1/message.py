"""Pipeline runner — FastAPI replacement for the n8n `agent k2` workflow (99 nodes).

Stage order mirrors the n8n main-path semantics (topological levels in
n8n_reference/extracted/topological_levels.txt + the IF branch map). Every stage
function is a 1:1 port of its n8n node; DB nodes call app.db.repository.
Stage `inputs` dicts carry upstream node outputs keyed by snake_cased node names,
exactly the keys the ported functions document.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid as uuid_mod
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.core.security import verify_internal_token
from app.core.config import settings
from app.core import agent_output, contract_adapter, gates, llm_safety, orchestrator, reply_guard, response_context, response_policy
from app.db import repository
from app.pipeline import normalize as normalize_mod
from app.pipeline import patient_fields
from app.pipeline import conditions_post, conditions_pre, stages_post, stages_pre
from app.pipeline.respond import build_final_response
from app.services import dialogue
from app.services import handoff as handoff_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/core-engine", tags=["Core Engine"])


class K2JSONResponse(JSONResponse):
    """JSONResponse that survives Decimal values returned by Postgres numeric columns."""

    def render(self, content: Any) -> bytes:
        return json.dumps(content, ensure_ascii=False, default=str).encode("utf-8")


# The only static patient-facing string in the pipeline. It is not a reply: it is an
# infrastructure-failure notice, used only when every model-authored candidate is empty
# (all LLM calls failed). It deliberately makes no claim about the patient's request.


class _Exit(Exception):
    """Early webhook exit carrying the exact n8n respond-node payload + status."""

    def __init__(self, body: Dict[str, Any], status_code: int = 200):
        self.body = body
        self.status_code = status_code


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _policy_ctx(current=None, system_orchestrator=None, persona_builder=None, merge_completion=None,
                execute_create=None, execute_cancel=None, execute_reschedule=None, validate_repaired=None,
                normalize_agent_output=None, normalize_validate=None, clinic_context=None,
                conversation_state=None) -> Dict[str, Any]:
    """ctx schema for app.core.response_policy.build_response (ResponsePolicyCtx, 12 keys)."""
    return {
        "current": current or {}, "system_orchestrator": system_orchestrator or {},
        "persona_builder": persona_builder or {}, "merge_completion": merge_completion or {},
        "execute_create": execute_create or {}, "execute_cancel": execute_cancel or {},
        "execute_reschedule": execute_reschedule or {}, "validate_repaired": validate_repaired or {},
        "normalize_agent_output": normalize_agent_output or {}, "normalize_validate": normalize_validate or {},
        "clinic_context": clinic_context or {}, "conversation_state": conversation_state or {},
    }


def _handoff_kwargs(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Filter the handoff child input to its declared schema fields."""
    allowed = {"clinic_id", "conversation_id", "patient_id", "channel_type", "channel_id",
               "handoff_reason", "reason_code", "reason_note", "correlation_id",
               "source_message_id", "context_snapshot", "metadata", "priority"}
    return {k: v for k, v in (payload or {}).items() if k in allowed}


def _correlation_of(normalized: Dict[str, Any]) -> Any:
    return normalized.get("correlation_id") or normalized.get("idempotency_key")


def _contract_v3_of(normalized_agent: Dict[str, Any], adapted: Dict[str, Any]) -> Dict[str, Any]:
    v3 = (normalized_agent or {}).get("contract_v3")
    if isinstance(v3, dict) and v3:
        return v3
    return (adapted or {}).get("contract_v3") or {}


def _guard_slot_of(decision: Optional[Dict[str, Any]]) -> Any:
    """Slot id for the double-booking guard: the canonical envelope booking_context
    first, then the inner decision's booking_context / confirmation_target."""
    if not isinstance(decision, dict):
        return None
    bc = decision.get("booking_context")
    if isinstance(bc, dict) and bc.get("slot_id"):
        return bc.get("slot_id")
    sd = decision.get("system_decision")
    if isinstance(sd, dict):
        bc2 = sd.get("booking_context")
        if isinstance(bc2, dict) and bc2.get("slot_id"):
            return bc2.get("slot_id")
        ct = sd.get("confirmation_target")
        if isinstance(ct, dict) and ct.get("slot_id"):
            return ct.get("slot_id")
    return None


# Per-conversation turn serialization (added 2026-09-18): two rapid messages from the
# same patient used to run the pipeline concurrently — distinct idempotency keys mean
# dedupe/claim never serialized them, and both could execute mutations (double booking).
# An in-process lock queues the second message until the first turn finishes; it then
# runs against the FRESH state. Single-replica deployment (Railway); multi-replica
# deployments would need a shared lock (Redis/DB advisory lock).
#
# Concurrency notes (post adversarial review 2026-09-18):
#   - WeakValueDictionary: a parked acquire() frame keeps the lock alive, so a waiter
#     can never end up on an evicted lock while a newcomer mints a fresh one (the
#     eviction race that re-opened the double-turn window); idle locks are GC'd.
#   - The acquire waits on a TASK, not wait_for: on some CPython versions
#     wait_for(acquire()) can raise TimeoutError on the very call that granted the
#     lock, which would brick the conversation until restart. task.done() is decisive.
_CONVERSATION_LOCKS = __import__("weakref").WeakValueDictionary()
_CONVERSATION_LOCK_WAIT_SECONDS = 90.0


def _lock_key_of(body: Dict[str, Any]) -> str:
    """Mirror normalize's conversation-id chain (conversation_id|conversationId|thread_id|chat_id),
    lowercased — uuids are case-insensitive in the DB but the lock dict is not. Values are
    string-coerced like normalize's _js_string: a numeric thread_id used to skip the lock
    entirely while normalize accepted it."""
    for key in ("conversation_id", "conversationId", "thread_id", "chat_id"):
        value = body.get(key)
        if value is not None and str(value).strip():
            return str(value).strip().lower()
    return ""



async def _dispatch_failure_handoff(normalized: Dict[str, Any], reason_code: str, note: str) -> None:
    """Owner directive (no static replies): when nothing model-authored exists for a
    turn, the reply suppresses and a HIGH-priority handoff dispatches so a human
    follows up. Contained — a handoff failure never breaks the turn."""
    try:
        await handoff_service.create_or_reuse_handoff(handoff_service.HandoffChildInput(
            clinic_id=(normalized or {}).get("clinic_id"),
            conversation_id=(normalized or {}).get("conversation_id"),
            patient_id=(normalized or {}).get("patient_id"),
            channel_type=(normalized or {}).get("channel_type"),
            channel_id=(normalized or {}).get("channel_id"),
            handoff_reason=note, reason_code=reason_code, priority="high",
            source_message_id=(normalized or {}).get("source_event_id"),
            context_snapshot={"message_text": (normalized or {}).get("message_text")}))
    except Exception:
        logger.exception("failure handoff dispatch failed — staff visibility lost for this turn")



@router.get("/usage")
async def clinic_token_usage(clinic_id: str, days: int = 30,
                             authorized: bool = Depends(verify_internal_token)):
    """Per-clinic token/cost accounting (owner console read-only).

    Aggregates ai_requests (one row per LLM call): totals, per-model, per-day and the
    20 most recent calls. Token-protected like the rest of the engine."""
    if not clinic_id:
        return K2JSONResponse(status_code=400, content={"ok": False, "error_code": "CLINIC_ID_REQUIRED"})
    try:
        uuid_mod.UUID(str(clinic_id))
    except (ValueError, AttributeError, TypeError):
        return K2JSONResponse(status_code=400, content={"ok": False, "error_code": "CLINIC_ID_INVALID"})
    days = max(1, min(int(days), 365))
    summary = await repository.get_clinic_usage_summary({"clinic_id": clinic_id, "days": days})
    return K2JSONResponse(status_code=200, content={"ok": True, "usage": summary})


@router.post("/message")
async def process_patient_message(request: Request,
                                  authorized: bool = Depends(verify_internal_token)) -> K2JSONResponse:
    started = time.time()
    raw_headers = {k.lower(): v for k, v in request.headers.items()}
    raw_body = await request.body()
    try:
        body = json.loads(raw_body) if raw_body else {}
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {"raw_payload": body}
    correlation_id = str(body.get("idempotency_key") or body.get("conversation_id") or "")
    conversation_key = _lock_key_of(body)
    logger.info("k2.request.start", extra={"correlation_id": correlation_id,
                                           "conversation_id": conversation_key})

    turn_lock: Optional[asyncio.Lock] = None
    acquire_task: Optional[asyncio.Task] = None
    lock_acquired = False
    if conversation_key:
        turn_lock = _CONVERSATION_LOCKS.setdefault(conversation_key, asyncio.Lock())
        acquire_task = asyncio.ensure_future(turn_lock.acquire())
        done, _pending = await asyncio.wait({acquire_task}, timeout=_CONVERSATION_LOCK_WAIT_SECONDS)
        if acquire_task not in done:
            # Queue cap hit: never drop the message silently — persist it through the
            # SAME deferred-batch mechanism the burst gate uses, so the deferred worker
            # replays it later. The parked acquire is cancelled (modern asyncio releases
            # the lock if cancellation lands after a grant).
            acquire_task.cancel()
            logger.warning("k2.request.turn_queue_timeout — deferring behind the active turn",
                           extra={"correlation_id": correlation_id, "conversation_id": conversation_key})
            deferred_payload = await _defer_behind_active_turn(body, raw_headers, raw_body)
            return K2JSONResponse(status_code=200, content=deferred_payload)
        lock_acquired = True
    try:
        out = await _run(body, raw_headers, raw_body)
        logger.info("k2.request.done", extra={"correlation_id": correlation_id,
                                              "elapsed_ms": int((time.time() - started) * 1000),
                                              "response_code": out.get("response_code")})
        return K2JSONResponse(status_code=200, content=out)
    except _Exit as exit_exc:
        logger.info("k2.request.early_exit", extra={"correlation_id": correlation_id,
                                                    "status": exit_exc.status_code,
                                                    "body": exit_exc.body.get("response_code") or exit_exc.body.get("error_code")})
        return K2JSONResponse(status_code=exit_exc.status_code, content=exit_exc.body)
    except Exception as exc:
        # n8n routed failures to error workflow KmQZ9bXmmP1YEZht; the sender received a 5xx.
        logger.exception("k2.request.failed", extra={"correlation_id": correlation_id})
        from app.core import alerting
        alerting.report_exception("core.request_failed", exc,
                                  detail=f"conversation {correlation_id}")
        return K2JSONResponse(status_code=500, content={"ok": False, "error_code": "INTERNAL_ERROR",
                                                        "correlation_id": correlation_id})
    finally:
        if lock_acquired and turn_lock is not None:
            turn_lock.release()


async def _defer_behind_active_turn(body: Dict[str, Any], raw_headers: Dict[str, str],
                                    raw_body: bytes) -> Dict[str, Any]:
    """Persist a queue-timed-out message through the burst-deferred path (nothing is lost)."""
    try:
        normalized = normalize_mod.normalize_and_validate(body, raw_headers)
        incoming = await repository.log_incoming_message(normalized)
        deferred = await repository.mark_k2_burst_message_deferred({
            "normalized": normalized, "gate": {"priority_allow": False}, "log_incoming_message": incoming})
        await repository.log_k2_rate_decision({
            "normalized": normalized, "gate": {"allowed": False}, "deferred": deferred,
            "allowed": False, "log_incoming_message": incoming})
        return {"reply_text": None, "suppress_reply": True,
                "response_code": "QUEUED_BEHIND_TURN",
                "batch_id": (deferred or {}).get("batch_id") or None,
                "batch_message_count": (deferred or {}).get("batch_message_count") or 1,
                "window_seconds": 15,
                "conversation_id": body.get("conversation_id"),
                "clinic_id": body.get("clinic_id")}
    except Exception:
        logger.exception("defer-behind-active-turn persistence failed — returning suppression only")
        return {"reply_text": None, "suppress_reply": True, "response_code": "QUEUED_BEHIND_TURN",
                "conversation_id": body.get("conversation_id"), "clinic_id": body.get("clinic_id")}


async def _run(inbound: Dict[str, Any], raw_headers: Dict[str, str], raw_body: bytes = b"") -> Dict[str, Any]:
    # ── L01 Normalize & Validate ────────────────────────────────────────────────
    normalized = normalize_mod.normalize_and_validate(inbound, raw_headers)

    # ── L02 Extract K2 Signature Context ───────────────────────────────────────
    # The port mirrors the n8n webhook-node output shape ({headers, body, query}).
    webhook_output = {"headers": raw_headers, "body": inbound, "query": {}}
    signature_ctx = stages_pre.extract_k2_signature_context(normalized, {
        "normalize_validate": normalized, "webhook_incoming_message": webhook_output,
        "headers": raw_headers})
    # The router signs the exact bytes it sends (JS JSON.stringify of the envelope).
    # Verify HMAC over those raw received bytes — immune to any re-serialization drift.
    if raw_body:
        signature_ctx["k2_signed_payload"] = raw_body.decode("utf-8", errors="replace")

    # ── L04 IF Normalize Error → Respond Invalid Inbound Payload (400) ─────────
    if conditions_pre.if_normalize_error(normalized):
        handled = normalize_mod.handle_normalize_error(normalized)
        raise _Exit({
            "reply_text": handled.get("normalization_error_message") or "تعذر معالجة الطلب بسبب نقص بيانات الرسالة الأساسية",
            "response_code": handled.get("normalization_error_code") or "INVALID_INBOUND_PAYLOAD",
            "metadata": {"missing_fields": handled.get("normalization_missing_fields") or []},
        }, status_code=400)

    # ── L05 Verify K2 Inbound Signature (DB RPC) + L06 IF accepted ─────────────
    signature_result = await repository.verify_k2_inbound_signature(signature_ctx)
    if not conditions_pre.if_k2_signature_accepted(signature_result):
        raise _Exit({"ok": False, "error_code": "PATIENT_CONVERSATION_OWNERSHIP_MISMATCH",
                     "message": "تعذر التحقق من بيانات المحادثة"}, status_code=403)

    # ── L07 Log Incoming Message ────────────────────────────────────────────────
    incoming_message = await repository.log_incoming_message(normalized)

    # ── L08 IF Early Security Reject → Respond Unauthorized (403/404) ──────────
    if conditions_pre.if_early_security_reject(incoming_message):
        error_code = (incoming_message or {}).get("security_error") or "PATIENT_CONVERSATION_OWNERSHIP_MISMATCH"
        is_clinic_missing = error_code == "CLINIC_NOT_FOUND"
        raise _Exit({"ok": False, "error_code": error_code,
                     "message": "تعذر العثور على العيادة المطلوبة" if is_clinic_missing else "تعذر التحقق من بيانات المحادثة"},
                    status_code=404 if is_clinic_missing else 403)

    # ── L09 Check Duplicate Message → Respond Duplicate (200) ──────────────────
    # Divergence (2026-09-18): a duplicate is suppressed ONLY when the first attempt
    # actually delivered a non-empty reply — and the stored reply text travels back so
    # the sender can redeliver it. A duplicate with no delivered content means the
    # previous attempt died mid-turn; suppressing it left the patient permanently
    # unanswered — the turn is re-run instead (the claim ledger keeps mutations safe).
    if conditions_pre.if_check_duplicate_message(incoming_message):
        delivered_reply = await repository.get_outgoing_reply(normalized)
        if delivered_reply:
            raise _Exit({"ok": True, "duplicate": True,
                         "idempotency_key": normalized.get("idempotency_key"),
                         "reply_text": delivered_reply,
                         "message": "already_processed"}, status_code=200)
        logger.info("k2.duplicate_without_delivered_reply — re-running the interrupted turn",
                    extra={"correlation_id": _correlation_of(normalized)})

    # ── L10 Get Clinic Context ─────────────────────────────────────────────────
    clinic_context = await repository.get_clinic_context(normalized)

    # ── L11-12 Validate Patient Ownership + IF Check Patient Ownership ─────────
    ownership = stages_pre.validate_patient_ownership(clinic_context, {"normalize_validate": normalized})
    if not conditions_pre.if_check_patient_ownership(ownership):
        error_code = (ownership or {}).get("security_error") or "PATIENT_CONVERSATION_OWNERSHIP_MISMATCH"
        is_clinic_missing = error_code == "CLINIC_NOT_FOUND"
        raise _Exit({"ok": False, "error_code": error_code,
                     "message": "تعذر العثور على العيادة المطلوبة" if is_clinic_missing else "تعذر التحقق من بيانات المحادثة"},
                    status_code=404 if is_clinic_missing else 403)
    canonical_time_context = (ownership or {}).get("canonical_time_context") or {}

    # ── L13-16 K2 Inbound Burst Rate Gate (DB) + IF K2 Burst Allowed ───────────
    gate = await repository.k2_inbound_burst_rate_gate({"normalized": normalized, "ownership": ownership})
    if not conditions_pre.if_k2_burst_allowed(gate):
        deferred = await repository.mark_k2_burst_message_deferred({
            "normalized": normalized, "gate": gate, "log_incoming_message": incoming_message})
        await repository.log_k2_rate_decision({
            "normalized": normalized, "gate": gate, "deferred": deferred, "allowed": False,
            "log_incoming_message": incoming_message})
        raise _Exit({"reply_text": None, "suppress_reply": True, "response_code": "QUEUED_DEBOUNCED",
                     "rate_limited": True, "priority": (gate or {}).get("priority_allow") is True,
                     "batch_id": (deferred or {}).get("batch_id") or None,
                     "batch_message_count": (deferred or {}).get("batch_message_count") or 1,
                     "window_seconds": 15,
                     "conversation_id": normalized.get("conversation_id"),
                     "clinic_id": normalized.get("clinic_id")}, status_code=200)
    await repository.log_k2_rate_decision({
        "normalized": normalized, "gate": gate, "allowed": True, "log_incoming_message": incoming_message})

    # ── L15 Get Conversation State + L16 Get Recent Window 2h ──────────────────
    state_row = await repository.get_conversation_state(normalized)
    state_data = (state_row or {}).get("state_data") or {}
    recent_window = await repository.get_recent_window_2h({"normalized": normalized, "state_data": state_data})

    # ── L17-18 Get Active Handoff Request + IF Handoff Active ──────────────────
    handoff_row = await repository.get_active_handoff_request(normalized)
    if conditions_pre.if_handoff_active(handoff_row):
        raise _Exit({"reply_text": None, "suppress_reply": True, "handoff_active": True,
                     "response_code": "HANDOFF_ACTIVE",
                     "handoff_request_id": (handoff_row or {}).get("id"),
                     "handoff_status": (handoff_row or {}).get("status"),
                     "conversation_id": normalized.get("conversation_id"),
                     "clinic_id": normalized.get("clinic_id"),
                     "processed_at": _now_iso()}, status_code=200)

    # ── L19-20 Evaluate Completed Create Replay + IF Completed Create Replay? ──
    replay_eval = stages_pre.evaluate_completed_create_replay(state_row, {"normalize_validate": normalized})
    if conditions_pre.if_completed_create_replay(replay_eval):
        policy = response_policy.build_response(_policy_ctx(
            current=replay_eval, normalize_validate=normalized, clinic_context=clinic_context,
            conversation_state=state_row))
        guard = reply_guard.apply_reply_guard({"response_policy": policy})
        replay_context = response_context.build_reply_context(
            normalized=normalized,
            clinic_context=clinic_context,
            state_data=state_data,
            policy=policy,
            decision=replay_eval,
            normalized_agent_output={},
            repaired_result={},
            tool_events=[],
            execution_results={"replay": replay_eval},
            faq_result={},
            guard=guard,
        )
        replay_composer: Dict[str, Any] = {}
        try:
            if getattr(settings, "LLM_COMPOSER_ENABLED", True):
                replay_composer = await dialogue.compose_patient_reply(replay_context)
        except Exception:
            logger.exception("replay response composer failed — using deterministic safety fallback")
        replay_reply = str((replay_composer or {}).get("reply") or "").strip()
        replay_guard = guard  # the guard carries metadata only, never text
        outgoing_params = stages_pre.build_outgoing_message_sql_parameters({}, {
            "normalize_validate": normalized,
            "save_conversation_state": {"saved": True},
            "save_conversation_state_retry_v18": {},
            "extract_single_agent_reply": {
                "rendered_reply": replay_reply,
                "render_used": bool(replay_reply),
            },
            "response_policy_deterministic": policy,
            "reply_guard_deterministic": replay_guard,
            # Same dead-wiring the main tail had: without these the replay outgoing
            # row logs ai_tokens null even though the composer ran here.
            "deepseek_result_model": [{"usage": (replay_composer or {}).get("usage") or {}}]
            if (replay_composer or {}).get("usage") else [],
        })
        outgoing_row = await repository.log_outgoing_message(outgoing_params)
        # No-static directive: when the composer authored nothing for the replay, the
        # turn suppresses and dispatches a handoff — the deterministic-with-number
        # text is ledger/audit metadata, not a patient reply.
        if not replay_reply:
            await _dispatch_failure_handoff(normalized, "REPLAY_UNANSWERED",
                                            "إعادة تشغيل رسالة مكررة بدون رد — متابعة مطلوبة")
        response = build_final_response(
            normalized,
            (outgoing_row or {}).get("id"),
            {"saved": True},
            {"saved": True},
            replay_guard,
            replay_reply or None,
            policy,
            _now_iso(),)
        if not replay_reply:
            response["reply_text"] = None
            response["suppress_reply"] = True
        return response

    # ── L21-23 Deterministic resolvers ─────────────────────────────────────────
    branch_fact = await repository.resolve_branch_inquiry({"normalized": normalized, "clinic_context": clinic_context})
    doctor_fact = await repository.resolve_doctor_inquiry({"normalized": normalized, "clinic_context": clinic_context, "state_data": state_data})
    service_fact = await repository.resolve_service_fact({"normalized": normalized, "clinic_context": clinic_context, "state_data": state_data})

    # ── L24 Build Clinic Persona Context (Deterministic) ───────────────────────
    persona_context = stages_pre.build_clinic_persona_context_deterministic(service_fact, {
        "get_clinic_context": clinic_context, "get_conversation_state": state_row,
        "normalize_validate": normalized, "get_recent_window_2h": recent_window,
        "resolve_doctor_inquiry_deterministic": doctor_fact,
        "resolve_branch_inquiry_deterministic": branch_fact})

    # ── L25 FAQ is on-demand through Search_Clinic_FAQ ────────────────────────
    # Do not prefetch on every turn. The dialogue model decides whether the user is
    # asking for clinic facts and invokes the Supabase-backed tool when needed.
    faq_result: Optional[Dict[str, Any]] = None

    # ── L26 Booking Assistant Agent (LLM with reception tools) ────────────────
    user_message = dialogue.build_user_message(
        clinic_context=clinic_context,
        canonical_time_context=canonical_time_context,
        normalized=normalized,
        persona_context=persona_context,
        state_data=state_data,
        faq_result=faq_result,
    )
    tool_events: list[Dict[str, Any]] = []
    agent_usage: list[Dict[str, Any]] = []
    agent_ms = 0
    composer_ms = 0

    def _timing() -> Dict[str, Any]:
        """Runtime snapshot for _respond_tail; reads the live locals of _run.

        _respond_tail is a separate function, so anything it needs about the agent turn
        (timing and provider token usage) has to be handed over explicitly.
        """
        return {"agent_ms": agent_ms, "composer_ms": composer_ms, "agent_usage": agent_usage}

    _agent_started = time.time()
    try:
        agent_turn = await dialogue.call_primary_model_with_tool(user_message, context={
            "clinic_id": normalized.get("clinic_id"),
            "conversation_id": normalized.get("conversation_id"),
            "patient_id": normalized.get("patient_id"),
            "state_data": state_data,
            "clinic_context": clinic_context,
            "persona_context": persona_context,
        })
        raw_llm_output = str(agent_turn)
        tool_events = list(getattr(agent_turn, "tool_events", []) or [])
        agent_usage = list(getattr(agent_turn, "usage", []) or [])
    except Exception:
        # The deterministic core still completes safely. The final composer gets the
        # verified policy/DB facts and can state that information is unavailable.
        logger.exception("primary LLM failed — degrading via MODEL_CALL_FAILED path")
        raw_llm_output = ""
    agent_ms = int((time.time() - _agent_started) * 1000)

    # Recover entities the model carried in its tool-call arguments but dropped from the
    # final contract (live incident 2026-09-17: doctor name lost after a failed
    # Check_Doctor_Availability round → state forgot the doctor → next turn re-asked).
    if tool_events and raw_llm_output:
        raw_llm_output = dialogue.recover_entities_from_tool_events(raw_llm_output, tool_events)

    # ── L27-29 R3 LLM Response Safety → R2 Error Detection → R1 Reply Recovery ─
    safety = llm_safety.r3_llm_response_safety({"output": raw_llm_output})
    error_detected = llm_safety.r2_llm_error_detection(safety)
    recovery = llm_safety.r1_agent_reply_recovery(error_detected)

    # ── L30 K2 Contract Adapter (v4 to v3) ─────────────────────────────────────
    adapted = contract_adapter.contract_v4_to_v3(recovery or {})

    # ── L33 Normalize Agent Output (Deterministic) ─────────────────────────────
    # The n8n graph branched here on Route Single Agent Phase -> IF Single Agent Result
    # Phase, whose [true] arm called Extract Single Agent Reply (a per-response-code
    # Arabic reply table). That branch was UNREACHABLE: route_single_agent_phase was
    # called with empty inputs, so upstream_phase was always None, has_execution_evidence
    # was always False and loop_count always 1 — phase was always "understand".
    # Both ports and the branch were removed 2026-09-17. Replies are authored by the
    # model from the fact catalog (app/core/response_context.py).
    normalized_agent_output = agent_output.normalize_agent_output({
        "current": adapted, "normalize_validate": normalized, "conversation_state": state_row,
        "clinic_context": clinic_context, "patient_ownership": ownership,
        "persona_builder": persona_context})
    normalized_agent = normalized_agent_output

    # ── L34 IF Contract Needs Repair → Build Repair Prompt → Repair Chain ──────
    repaired_result: Dict[str, Any] = {}
    if conditions_post.if_contract_needs_repair(normalized_agent):
        repair_prompt_item = llm_safety.build_repair_prompt_deterministic({
            "normalize_agent_output": normalized_agent_output, "normalize_validate": normalized,
            "clinic_persona_context": persona_context, "clinic_context": clinic_context,
            "patient_ownership": ownership})
        try:
            repair_raw = await dialogue.call_repair_model(repair_prompt_item.get("prompt") or "")
            # The repair model rewrites the whole contract; without this back-fill a
            # rewrite that drops entities erases patient data the tool calls carried.
            repair_raw = dialogue.recover_entities_from_tool_events(repair_raw, tool_events)
            # Reviewer finding: the repair validator skips day-word absorption — a
            # repaired 'بكرة' nulled entities.date and _keep resurrected the STALE
            # prior date. Absorb here, exactly like the primary path does.
            try:
                _repair_doc = json.loads(repair_raw)
                _repair_ent = _repair_doc.get("entities")
                _raw_date = str((_repair_ent or {}).get("date") or "")
                if _repair_ent is not None and _raw_date and not re.fullmatch(
                        r"[0-9]{4}-[0-9]{2}-[0-9]{2}", _raw_date):
                    _now_local = str((canonical_time_context or {}).get("now_local_date")
                                     or datetime.now(timezone.utc).date().isoformat())
                    _absorbed = agent_output._absorb_day_word_to_iso(_raw_date, _now_local)
                    if _absorbed:
                        _repair_ent["date"] = _absorbed
                        repair_raw = json.dumps(_repair_doc, ensure_ascii=False)
            except Exception:
                pass
            repaired_result = llm_safety.validate_repaired_contract_deterministic(
                {"text": repair_raw, "output": repair_raw},
                {"normalize_agent_output": normalized_agent_output, "normalize_validate": normalized,
                 "conversation_state": state_row, "clinic_context": clinic_context,
                 "patient_ownership": ownership, "persona_builder": persona_context,
                 "clinic_persona_context": persona_context,
                 "repair_prompt": repair_prompt_item})
            normalized_agent = repaired_result
            normalized_agent_output = repaired_result
        except Exception:
            logger.exception("repair LLM failed — continuing with the degraded contract")

    # ── L38-40 Resolve Booking IDs → Apply Resolved Booking IDs → P1.7 ─────────
    booking_ids_result = await repository.resolve_booking_ids({
        "normalized": normalized, "state_data": state_data,
        "agent_output": normalized_agent_output, "repaired_contract": repaired_result})
    apply_ids_result = stages_post.apply_resolved_booking_ids_deterministic(
        booking_ids_result,
        {"normalize_agent_output": normalized_agent_output, "validate_repaired_contract": repaired_result,
         "normalize_validate": normalized, "system_orchestrator": {},
         # P-SLOT-GUARD liveness input (reviewer-verified): the guard reads the live
         # offer/target from conversation_state — omitting it failed the guard closed
         # and dropped a still-live offered slot.
         "conversation_state": state_row},
        now_ms=time.time() * 1000)
    p17_result = patient_fields.normalize_patient_fields({**apply_ids_result, "normalize_validate": normalized})
    normalized_agent = p17_result

    # ── L41 System Orchestrator (Policy) — Decision Core v3 ────────────────────
    decision = orchestrator.decide(
        contract_v3=_contract_v3_of(normalized_agent, adapted),
        state_data=state_data,
        clinic_context=clinic_context,
        now_ts=time.time(),
        normalize_agent_output=normalized_agent_output,
        validate_repaired_contract=repaired_result,
        apply_resolved_booking_ids=apply_ids_result,
        normalize_validate=normalized,
        get_clinic_context=clinic_context,
        validate_patient_ownership=ownership,
        p1_7_patient_field_normalization=p17_result,
        current_item=normalized,
    )

    # ── L42 IF Non-Scheduling Turn (v19) ───────────────────────────────────────
    merge_completion_result: Dict[str, Any] = {}
    exec_create_result: Dict[str, Any] = {}
    exec_cancel_result: Dict[str, Any] = {}
    exec_reschedule_result: Dict[str, Any] = {}
    claim_result: Dict[str, Any] = {}
    claim_applied: Dict[str, Any] = {}
    claim_input: Dict[str, Any] = {}
    if not conditions_pre.if_non_scheduling_turn_v19(decision):
        # ── L43 Execution Transition Guard (Deterministic) ─────────────────────
        guard_result = gates.execution_transition_guard_deterministic(decision, state_row)
        decision = guard_result

        # ── L44-46 Business Time Context → Gate → IF Business Time Allowed ─────
        time_ctx_row = await repository.lookup_business_time_context({
            "normalized": normalized, "clinic_context": clinic_context, "guard": guard_result})
        gate_decision = gates.business_time_gate_deterministic(guard_result, time_ctx_row)
        if not conditions_post.if_business_time_allowed(gate_decision):
            decision = gate_decision
            policy = response_policy.build_response(_policy_ctx(
                current=gate_decision, system_orchestrator=decision, persona_builder=persona_context,
                validate_repaired=repaired_result, normalize_agent_output=normalized_agent_output,
                normalize_validate=normalized, clinic_context=clinic_context, conversation_state=state_row))
            return await _respond_tail(normalized, state_row, state_data, policy, decision,
                                       persona_context, repaired_result, normalized_agent_output,
                                       normalized_agent, clinic_context, raw_llm_output,
                                       tool_events=tool_events, timing=_timing())

        # ── L47-51 P1.6 operation claim ledger ─────────────────────────────────
        claim_input = stages_post.prepare_operation_claim_input(gate_decision, {"normalize_validate": normalized})
        if conditions_post.if_operation_claim_required(claim_input):
            claim_result = await repository.claim_operation(claim_input)
            claim_applied = stages_post.apply_operation_claim_deterministic(claim_result, claim_input)
            if not conditions_post.if_claim_allows_child(claim_applied):
                policy = response_policy.build_response(_policy_ctx(
                    current=claim_applied, system_orchestrator=decision, persona_builder=persona_context,
                    validate_repaired=repaired_result, normalize_agent_output=normalized_agent_output,
                    normalize_validate=normalized, clinic_context=clinic_context, conversation_state=state_row))
                return await _respond_tail(normalized, state_row, state_data, policy, claim_applied,
                                           persona_context, repaired_result, normalized_agent_output,
                                           normalized_agent, clinic_context, raw_llm_output,
                                           tool_events=tool_events, timing=_timing())
            decision = claim_applied

        # ── L52-55 executors ────────────────────────────────────────────────────
        # try/except: a granted claim with a crashed executor must not strand the
        # ledger IN_PROGRESS — close it FAILED_FINAL before propagating.
        exec_result: Optional[Dict[str, Any]] = None
        try:
            if conditions_post.if_approved_create_action(decision):
                # DOUBLE-BOOKING GUARD (2026-09-19): the claim ledger is keyed per
                # operation_id, and a re-affirm after a lost state save mints a NEW one —
                # only a slot-scoped existence check refuses the second booking for the
                # same patient. When one already exists, its row becomes the executor
                # result (the same direct-create envelope shape) and no new booking runs.
                preexisting = await repository.find_active_appointment_for_slot({
                    "clinic_id": normalized.get("clinic_id"),
                    "patient_id": normalized.get("patient_id"),
                    "slot_id": _guard_slot_of(decision)})
                if preexisting:
                    exec_create_result = {
                        "success": True, "response_code": "APPOINTMENT_CREATED",
                        "appointment_id": preexisting.get("id"),
                        "booking_number": preexisting.get("booking_number"),
                        "public_id": preexisting.get("public_id") or preexisting.get("booking_number"),
                        "preexisting_slot": True,
                    }
                    exec_result = exec_create_result
                else:
                    exec_input = stages_post.prepare_execute_input({
                        "conversation_state": state_row, "resolve_booking_ids": booking_ids_result,
                        "apply_resolved_booking_ids": apply_ids_result, "system_orchestrator": decision,
                        "normalize_validate": normalized})
                    exec_ctx = stages_post.prepare_execute_context(exec_input, {
                        "normalize_validate": normalized, "system_orchestrator": decision,
                        "conversation_state": state_row,
                        "resolve_booking_ids": booking_ids_result,
                        "apply_resolved_booking_ids": apply_ids_result,
                        "clinic_context": clinic_context})
                    exec_create_result = await repository.execute_approved_create_appointment({
                        "normalized": normalized, "claim": claim_result, "exec": exec_ctx, **(exec_ctx or {}),
                        # Reviewer-verified: prepare_execute_input computes `notes` but its
                        # output was discarded at the call site — $5 notes always bound "".
                        "notes": (exec_input or {}).get("notes")})
                    exec_result = exec_create_result
            elif conditions_post.if_approved_cancel_action(decision):
                # Wiring (fixed 2026-09-18, reviewer-verified): the executor reads
                # system_decision/operation_id from the TOP level of its item — the old
                # {"decision": ...} key bound appointment_id="" and operation_id=the raw
                # idempotency key, so approved cancels never executed.
                exec_cancel_result = await repository.execute_approved_cancel_appointment({
                    **(decision or {}), "normalized": normalized, "claim": claim_result,
                    "operation_id": claim_applied.get("operation_id")})
                exec_result = exec_cancel_result
            elif conditions_post.if_approved_reschedule_action(decision):
                exec_reschedule_result = await repository.execute_approved_reschedule_appointment({
                    **(decision or {}), "normalized": normalized, "claim": claim_result,
                    "operation_id": claim_applied.get("operation_id")})
                exec_result = exec_reschedule_result
        except BaseException:
            # BaseException (not Exception): asyncio.CancelledError from a deploy or a
            # client disconnect during the executor RPC must STILL close the claim —
            # otherwise the ledger stays IN_PROGRESS and every retry is told
            # "قيد التنفيذ" forever. The outcome is UNKNOWN here (the RPC may have
            # committed), so INCONCLUSIVE — never FAILED_FINAL.
            if claim_applied.get("operation_id"):
                try:
                    await repository.finalize_operation({
                        "finalize_clinic_id": normalized.get("clinic_id"),
                        "finalize_operation_id": claim_applied.get("operation_id"),
                        "finalize_status": "INCONCLUSIVE",
                        "finalize_mutation_status": "UNKNOWN",
                    })
                except Exception:
                    logger.exception("claim-failure finalize also failed — ledger row left for reconcile")
            raise

        # ── L56-59 Validate Child Envelope → Finalize → Merge Completion ───────
        # Wiring (fixed 2026-09-17): the nodes read system_orchestrator /
        # normalize_validate / apply_operation_claim — the previous call passed
        # "normalized"/"decision", so every successful mutation was finalized
        # CHILD_CONTRACT_INVALID → INCONCLUSIVE and the patient was told the
        # operation failed. merge_operation_completion consumes the FINALIZE row.
        # INSIDE the guarded region (reviewer-verified): a crash here after a
        # successful mutation would otherwise strand the ledger IN_PROGRESS forever.
        if exec_result is not None:
            execution_id = normalized.get("source_event_id") or _correlation_of(normalized)
            envelope = stages_post.validate_child_envelope(exec_result, {
                "system_orchestrator": decision, "normalize_validate": normalized,
                "apply_operation_claim": claim_applied}, execution_id=execution_id)
            finalize_input = stages_post.prepare_operation_finalize_input(envelope, {
                "normalize_validate": normalized}, execution_id=execution_id)
            finalized = await repository.finalize_operation(finalize_input)
            merge_completion_result = stages_post.merge_operation_completion(envelope, finalized or {})
            # n8n parity: downstream nodes read system_decision/booking_context/
            # confirmation_target from the System Orchestrator item — carry them
            # onto the merged completion item.
            for _carry in ("system_decision", "booking_context", "confirmation_target", "response_code"):
                if merge_completion_result.get(_carry) is None and (decision or {}).get(_carry) is not None:
                    merge_completion_result[_carry] = decision.get(_carry)
            decision = merge_completion_result

    # ── L60 Response Policy (Deterministic) ────────────────────────────────────
    policy = response_policy.build_response(_policy_ctx(
        current=decision, system_orchestrator=decision, persona_builder=persona_context,
        merge_completion=merge_completion_result, execute_create=exec_create_result,
        execute_cancel=exec_cancel_result, execute_reschedule=exec_reschedule_result,
        validate_repaired=repaired_result, normalize_agent_output=normalized_agent_output,
        normalize_validate=normalized, clinic_context=clinic_context, conversation_state=state_row))

    return await _respond_tail(normalized, state_row, state_data, policy, decision,
                               persona_context, repaired_result, normalized_agent_output,
                               normalized_agent, clinic_context, raw_llm_output,
                               faq_result=faq_result, tool_events=tool_events, timing=_timing(),
                               execution_results={
                                   "create": exec_create_result,
                                   "cancel": exec_cancel_result,
                                   "reschedule": exec_reschedule_result,
                                   "completion": merge_completion_result,
                               })


async def _respond_tail(normalized: Dict[str, Any], state_row: Dict[str, Any], state_data: Dict[str, Any],
                        policy: Dict[str, Any], decision: Dict[str, Any], persona_context: Dict[str, Any],
                        repaired_result: Dict[str, Any], normalized_agent_output: Dict[str, Any],
                        normalized_agent: Dict[str, Any], clinic_context: Optional[Dict[str, Any]] = None,
                        raw_llm_output: Optional[str] = None,
                        pre_extracted: Optional[Dict[str, Any]] = None,
                        faq_result: Optional[Dict[str, Any]] = None,
                        tool_events: Optional[list[Dict[str, Any]]] = None,
                        execution_results: Optional[Dict[str, Any]] = None,
                        timing: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Shared tail: persist confirmation → reply guard → reply extraction → audit/usage →
    handoff → state save with stale retry → log outgoing → Respond To Patient."""
    # ── L61 Persist Pending Confirmation (Deterministic) ───────────────────────
    # Wiring (fixed 2026-09-17): the SQL node gates on a top-level response_code —
    # the previous {"normalized", "decision"} envelope never provided it, so
    # confirmations were never persisted (cancel flow downstream depends on this).
    await repository.persist_pending_confirmation({"normalized": normalized, **(decision or {})})

    # ── L62 Reply Guard (Deterministic) — always runs, even when the caller
    # pre-extracted the reply (single-agent result phase): safety is not optional.
    guard = reply_guard.apply_reply_guard({"response_policy": policy})

    # ── Dynamic final reply composer ───────────────────────────────────────────
    # Normal patient-facing prose is authored by the model from an authoritative
    # fact catalog. No regex, response-code phrase table, or fixed response template
    # participates in this path. Deterministic policy still owns actions and facts.
    base_extracted = dict(pre_extracted or normalized_agent_output or normalized_agent or {})
    reply_context = response_context.build_reply_context(
        normalized=normalized,
        clinic_context=clinic_context or {},
        state_data=state_data,
        policy=policy,
        decision=decision,
        normalized_agent_output=normalized_agent_output,
        repaired_result=repaired_result,
        tool_events=tool_events or [],
        execution_results=execution_results or {},
        faq_result=faq_result or {},
        guard=guard,
    )

    # ── Cost gate: ground the PRIMARY model's own draft first ──────────────────
    # The dialogue agent already saw the same facts; validating its reply is free,
    # while the composer costs a full LLM round trip. When the draft passes the same
    # evidence contract the composer is held to, it ships directly and the composer
    # call is skipped (most conversational turns). Any failure falls through to the
    # composer unchanged — the safety floor never drops.
    primary_grounded = None
    primary_reply = str(
        (policy or {}).get("agent_reply")
        or (normalized_agent_output or {}).get("agent_reply")
        or "").strip()
    if primary_reply:
        primary_grounded = response_context.try_ground_primary_reply(primary_reply, reply_context)

    composer_result: Dict[str, Any] = {}
    composer_error: Optional[str] = None
    composer_skipped = False
    composer_ms = 0
    if primary_grounded is not None:
        composer_result = primary_grounded
        composer_skipped = True
    elif getattr(settings, "LLM_COMPOSER_ENABLED", True):
        _composer_started = time.time()
        try:
            composer_result = await dialogue.compose_patient_reply(reply_context)
        except Exception as exc:
            composer_error = str(exc)
            logger.exception("final response composer failed — using the safest available model/guard reply")
        composer_ms = int((time.time() - _composer_started) * 1000)

    rendered_reply = str((composer_result or {}).get("reply") or "").strip()
    if not rendered_reply:
        # Model-first, always. Every model-authored candidate is tried; a prewritten
        # sentence must never stand in for the agent's own words. Deterministic status
        # notices (claim-blocked final_reply) are metadata for the ledger/audit — they
        # do NOT reach the patient as replies (owner directive: no static text ever).
        # When nothing model-authored exists, the turn SUPPRESSES and dispatches a
        # handoff — a human follows up instead of a canned line.
        rendered_reply = str(
            policy.get("agent_reply")
            or normalized_agent_output.get("agent_reply")
            or repaired_result.get("agent_reply")
            or base_extracted.get("agent_reply")
            or ""
        ).strip()
        if rendered_reply and primary_grounded is None:
            # Invariant-3 gate (2026-09-19): this fallback candidate is the same draft
            # family the primary grounding check may have just rejected — with the
            # composer down it would otherwise ship ungrounded (fabricated dates/times
            # reachable during LLM degradation). It ships only through the same
            # value-evidence check; otherwise the turn suppresses and the handoff
            # below follows up.
            if response_context.try_ground_primary_reply(rendered_reply, reply_context) is None:
                rendered_reply = ""

    extracted = {
        **base_extracted,
        "agent_reply": rendered_reply,
        "rendered_reply": rendered_reply,
        "final_reply": rendered_reply,
        "canonical_reply": rendered_reply,
        "render_error": composer_error,
        "render_used": True,
        "reply_origin": ("primary_grounded" if composer_skipped
                         else ("model_composer" if composer_result else "safe_fallback")),
        "composer_evidence_ids": (composer_result or {}).get("evidence_ids") or [],
        "composer_missing_information": (composer_result or {}).get("missing_information") or [],
    }

    # apply_reply_guard no longer produces patient-facing text at all (override is always
    # None), so nothing has to be cleared here. The metadata still travels for the audit.
    guard_for_delivery = guard

    # ── L65-66 Derive Actions → Handoff → audit + AI usage ─────────────────────
    # Handoff runs BEFORE the audit write so a contained handoff failure is visible
    # in the audit row (the flag used to be set after the entry was already logged).
    actions = contract_adapter.derive_actions({
        "current": extracted or normalized_agent or {}, "system_orchestrator": decision,
        "normalize_validate": normalized})
    if conditions_pre.if_handoff_required(actions):
        handoff_input = stages_post.prepare_handoff_input(extracted or {}, {
            "normalized": normalized, "system_orchestrator": decision, "actions": actions,
            "normalize_validate": normalized,
            # Reviewer-verified: the node reads Get Conversation State for
            # conversation_summary/recent_turns/booking_context — without it staff
            # received a bare handoff with no history.
            "conversation_state": state_row})
        handoff_payload = (handoff_input or {}).get("handoff_input") or handoff_input or {}
        # Divergence (2026-09-17): the ported Restore Handoff Context raises when the
        # handoff RPC fails, which aborted the whole turn — the patient asking for a
        # human got a 500 with no reply and no state save. The handoff failure is now
        # contained: the reply and state save still happen, flagged in the audit.
        try:
            handoff_result = await handoff_service.create_or_reuse_handoff(
                handoff_service.HandoffChildInput(**_handoff_kwargs(handoff_payload)))
            stages_post.restore_handoff_context(handoff_result, handoff_input)
        except Exception:
            logger.exception("handoff failed — delivering the reply without the handoff link")
            extracted["handoff_failed"] = True
    audit_entry = stages_pre.build_audit_entry(extracted or {}, {
        "validate_repaired_contract_deterministic": repaired_result,
        "normalize_agent_output_deterministic": normalized_agent_output,
        "response_policy_deterministic": policy, "normalize_validate": normalized,
        "system_orchestrator_policy": decision})
    audit_entry["normalized"] = normalized
    # The composer path no longer spreads the agent item into the audit input, so the
    # tool trace must be attached explicitly or tool_call_count silently reports 0.
    audit_entry["tool_calls"] = [
        {"name": event.get("name"), "arguments": event.get("arguments")}
        for event in (tool_events or [])
    ]
    audit_entry["tool_call_count"] = len(tool_events or [])
    audit_entry["reply_composer"] = {
        "origin": extracted.get("reply_origin"),
        "composer_skipped": composer_skipped,
        "evidence_ids": extracted.get("composer_evidence_ids") or [],
        "missing_information": extracted.get("composer_missing_information") or [],
        "error": composer_error,
        "tool_event_count": len(tool_events or []),
        "agent_ms": (timing or {}).get("agent_ms", 0),
        "composer_ms": composer_ms,
        "agent_llm_calls": len((timing or {}).get("agent_usage") or []),
    }
    if extracted.get("handoff_failed"):
        audit_entry["handoff_failed"] = True
    await repository.log_agent_audit_entry(audit_entry)
    composer_usage = (composer_result or {}).get("usage") or {}
    usage_rows = stages_pre.compute_ai_request_usage_deterministic({}, {
        "normalize_validate": normalized,
        "build_clinic_persona_context_deterministic": persona_context,
        "booking_assistant_agent": {"output": raw_llm_output},
        "result_reply_composer": {"output": (composer_result or {}).get("raw_output") or ""},
        "prepare_single_agent_result_context": reply_context,
        # Provider-reported usage (preferred by _read_model_tokens over the char estimate)
        "deepseek_model": [{"usage": u} for u in ((timing or {}).get("agent_usage") or [])],
        "deepseek_result_model": [{"usage": composer_usage}] if composer_usage else [],
        "response_policy_deterministic": policy})
    for usage_row in (usage_rows or []):
        await repository.insert_ai_request_usage(usage_row)

    fresh_offer = await repository.read_fresh_offer_midturn({"normalized": normalized})

    # ── L71-77 Build Persistent Conversation State → save with stale retry ─────
    # Wiring (fixed 2026-09-18, reviewer-verified): the node reads $json.booking_number /
    # $json.output / $json.system_decision from the DOWNSTREAM merged item — the previous
    # call passed the 2-column fresh-offer row as item, so a successful create saved
    # booking_number=None and operation_state=EXECUTING (the reply promised a number the
    # next turn's state did not have, and the replay gate could never match).
    state_item = {**(decision or {}), **(policy or {}), "output": extracted or {}}
    persistent_state = stages_pre.build_persistent_conversation_state(state_item, {
        "normalize_validate": normalized, "get_clinic_context": clinic_context,
        "system_orchestrator_policy": decision, "get_conversation_state": state_row,
        "read_fresh_offer_midturn": fresh_offer or {}})
    save_body = stages_pre.build_save_state_rpc_body(persistent_state, {
        "build_persistent_conversation_state": persistent_state, "normalize_validate": normalized})
    save_result = await repository.save_conversation_state_with_retry(normalized, save_body)

    # ── L78-79 Build Outgoing Message SQL Parameters + Log Outgoing Message ────
    outgoing_params = stages_pre.build_outgoing_message_sql_parameters({}, {
        "normalize_validate": normalized,
        "save_conversation_state": (save_result or {}).get("initial") or {},
        "save_conversation_state_retry_v18": (save_result or {}).get("retry") or {},
        "extract_single_agent_reply": extracted,
        "response_policy_deterministic": policy, "reply_guard_deterministic": guard_for_delivery,
        # Reviewer-verified dead-wiring: without these the outgoing row's ai_tokens and
        # agent1/agent2 token metadata are always null while the data sits 30 lines up.
        "deepseek_model": [{"usage": u} for u in ((timing or {}).get("agent_usage") or [])],
        "deepseek_result_model": [{"usage": composer_usage}] if composer_usage else []})
    outgoing_row = await repository.log_outgoing_message(outgoing_params)

    # ── L80 Respond To Patient ─────────────────────────────────────────────────
    response = build_final_response(
        normalized=normalized,
        outgoing_message_id=(outgoing_row or {}).get("id"),
        save_initial=(save_result or {}).get("initial") or {},
        save_retry=(save_result or {}).get("retry"),
        reply_guard_result=guard_for_delivery,
        rendered_reply=rendered_reply,
        response_policy_output=policy,
        processed_at_iso=_now_iso(),
    )
    if not rendered_reply:
        # Owner directive (no static replies, ever): nothing model-authored exists for
        # this turn — suppress the reply and dispatch a HIGH-priority handoff so a
        # human follows up. The patient is never fed a canned line.
        response["reply_text"] = None
        response["suppress_reply"] = True
        response["response_code"] = response.get("response_code") or "MODEL_UNAVAILABLE"
        await _dispatch_failure_handoff(normalized, "MODEL_UNAVAILABLE",
                                        "النموذج لم يكتب ردًا لهذا الدور — تدخل بشري مطلوب")
    return response
