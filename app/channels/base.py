"""Unified channel adapter layer — provider-agnostic webhook → core-engine → reply.

Each provider lives in its own module (telegram.py, gupshup.py, superchat.py,
thikaa.py, meta_whatsapp.py) and implements the small ChannelAdapter contract:
parse / verify_signature / channel_lookup / send / typing. Everything else —
idempotency, clinic/patient/conversation resolution, K2 signing, the core-engine
call, reply extraction with the fallback chain, webhook_logs bookkeeping — is
shared here, ported 1:1 from the n8n routers that ran in production.

Honesty note: every code path below mirrors a working n8n node (export of
2026-09-11, live-verified 2026-09-22). The n8n instance stays deployed as the
rollback path: cutover = pointing the provider webhook at /channels/.../webhook,
rollback = pointing it back.
"""
from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import httpx

from app.api.v1 import message as core_runner
from app.core.config import settings

logger = logging.getLogger(__name__)

# The only static patient-facing string in the layer — the same infrastructure-
# failure notice the n8n routers used (Prepare * Reply fallback), kept verbatim
# for behavior parity. It is not a reply authored by us: it signals that the
# deterministic core could not be reached.
CORE_UNREACHABLE_FALLBACK = "عذراً، حصلت مشكلة في معالجة طلبك. حاول مرة أخرى."

REPLY_FALLBACK_CHAIN = ("reply_text", "final_reply", "canonical_reply",
                        "response", "message", "answer", "body", "output", "result")


@dataclass
class InboundMessage:
    """Provider-neutral view of one inbound webhook event."""
    provider: str
    channel_code: str                 # whatsapp / instagram / messenger / telegram
    channel_patient_id: str           # sender external id (phone / platform user id)
    patient_name: Optional[str]
    message_text: str
    message_type: str
    message_id: Optional[str]
    idempotency_key: Optional[str]
    raw_body: Dict[str, Any]
    extra: Dict[str, Any] = field(default_factory=dict)   # provider-specific (chat ids, instance ids...)
    reply_to: Dict[str, Any] = field(default_factory=dict)  # what send() needs later

    @property
    def is_status_update(self) -> bool:
        return not (self.channel_patient_id and self.message_text)


@dataclass
class ChannelResolution:
    clinic_id: str
    channel_id: str
    config: Dict[str, Any] = field(default_factory=dict)


class ChannelAdapter(ABC):
    """One provider = one file = one class. The registry resolves by name."""

    provider: str = ""
    workflow_version: str = ""
    supports_typing: bool = False

    # ── provider-specific contract ────────────────────────────────────────────
    @abstractmethod
    def parse(self, headers: Dict[str, str], body: Dict[str, Any]) -> InboundMessage:
        """Translate the provider JSON into InboundMessage. Status events must
        come back with empty channel_patient_id/message_text (is_status_update)."""

    def verify_signature(self, headers: Dict[str, str], raw_body: bytes,
                         secret_row: Dict[str, Any]) -> bool:
        """Provider webhook authenticity check. Default: accept (providers like
        Gupshup/SuperChat send no HMAC — the secret URL is the gate, as in n8n)."""
        return True

    @abstractmethod
    def channel_lookup(self, msg: InboundMessage) -> Tuple[str, list]:
        """(SQL, params) resolving ONE clinic/channel row for this event."""

    @abstractmethod
    async def send(self, msg: InboundMessage, secret_row: Dict[str, Any],
                   channel_cfg: Dict[str, Any], reply_text: str) -> Dict[str, Any]:
        """Deliver reply_text via the provider API. Returns provider response JSON."""

    async def send_typing(self, msg: InboundMessage, secret_row: Dict[str, Any],
                          channel_cfg: Dict[str, Any]) -> bool:
        return False

    def core_payload(self, msg: InboundMessage, resolution: ChannelResolution,
                     patient_id: str, conversation_id: str) -> Dict[str, Any]:
        """The K2 envelope — same fields the n8n Build K2 Signed Core Envelope nodes emitted.
        Key insertion order matters: the HMAC is computed over json.dumps of this dict."""
        return {
            "clinic_id": resolution.clinic_id,
            "channel_type": msg.channel_code,
            "channel_provider": self.provider,
            "channel_id": resolution.channel_id,
            "patient_id": patient_id,
            "conversation_id": conversation_id,
            "message_text": msg.message_text,
            "message_id": msg.message_id,
            "source_event_id": msg.idempotency_key or msg.message_id,
            "metadata": {"provider": self.provider, **msg.extra},
        }


# ── shared pipeline (the n8n router logic, once) ──────────────────────────────

async def _db_row(sql: str, *params) -> Optional[Dict[str, Any]]:
    from app.db.pool import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, *params)
    return dict(row) if row else None


async def _db_execute(sql: str, *params) -> Optional[Dict[str, Any]]:
    from app.db.pool import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, *params)
    return dict(row) if row else None


async def _supabase_rpc(fn: str, payload: Dict[str, Any]) -> Any:
    """Same Supabase REST RPCs the n8n routers called (find_or_create_patient /
    get_or_create_channel_conversation) — same param names, same PostgREST shapes."""
    url = f"{settings.SUPABASE_REST_URL.rstrip('/')}/rpc/{fn}"
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.post(url, json=payload, headers={
            "Content-Type": "application/json",
            "apikey": settings.SUPABASE_SECRET_KEY,
            "Authorization": f"Bearer {settings.SUPABASE_SECRET_KEY}",
        })
    if r.status_code >= 400:
        raise RuntimeError(f"supabase rpc {fn} failed: {r.status_code} {r.text[:200]}")
    return r.json()


def _extract_patient_id(result: Any) -> str:
    """Port of every 'Merge Patient Data' n8n code node — PostgREST shape tolerance."""
    if isinstance(result, dict):
        data = result.get("data")
        if isinstance(data, list) and data and isinstance(data[0], dict) and data[0].get("patient_id"):
            return data[0]["patient_id"]
        if result.get("patient_id"):
            return result["patient_id"]
    if isinstance(result, list) and result and isinstance(result[0], dict) and result[0].get("patient_id"):
        return result[0]["patient_id"]
    raise RuntimeError("Failed to get patient_id from Supabase RPC response")


def _extract_conversation(result: Any) -> Tuple[str, Optional[str]]:
    """Port of every 'Merge Conversation Data' n8n code node."""
    conversation_id = channel_id = None
    candidates: list = []
    if isinstance(result, dict):
        candidates = [result, result.get("result")]
        data = result.get("data")
        if isinstance(data, list):
            candidates.extend(data)
    elif isinstance(result, list):
        candidates = list(result)
    for c in candidates:
        if not isinstance(c, dict):
            continue
        nested = c.get("get_or_create_channel_conversation") or c.get("result") or c
        if isinstance(nested, dict) and nested.get("conversation_id"):
            conversation_id = nested["conversation_id"]
            channel_id = nested.get("channel_id")
            break
    if not conversation_id:
        raise RuntimeError("Failed to get conversation_id from Supabase RPC response")
    return conversation_id, channel_id


def _extract_reply(core_result: Dict[str, Any]) -> Optional[str]:
    """Port of every 'Prepare * Reply' n8n node — same precedence chain, same fallback."""
    for key in REPLY_FALLBACK_CHAIN:
        v = core_result.get(key)
        if v:
            text = v.get("text", v) if isinstance(v, dict) else v
            if text and str(text).strip():
                return str(text)
    return None


def _handoff_suppressed(core_result: Dict[str, Any]) -> bool:
    body = core_result.get("body") if isinstance(core_result.get("body"), dict) else {}
    return bool(core_result.get("suppress_reply") is True or body.get("suppress_reply") is True
                or core_result.get("duplicate") is True
                or core_result.get("message") == "already_processed"
                or body.get("message") == "already_processed")


def _json_compact(obj: Dict[str, Any]) -> str:
    """JS JSON.stringify equivalent — compact separators, insertion order preserved.
    The K2 HMAC is computed over exactly this serialization."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


async def _mark_webhook(log_id: int, status: str, clinic_id: str = "",
                        channel_id: str = "", error: str = "") -> None:
    if error:
        await _db_execute("UPDATE webhook_logs SET status = $1, error_message = $2 WHERE id = $3",
                          status, error, log_id)
        return
    await _db_execute(
        """UPDATE webhook_logs
           SET status = $1, clinic_id = $2, channel_id = $3,
               processing_time_ms = EXTRACT(EPOCH FROM (now() - received_at)) * 1000
           WHERE id = $4 AND status = 'pending' AND clinic_id IS NULL AND channel_id IS NULL""",
        status, clinic_id, channel_id, log_id)


async def handle_webhook(adapter: ChannelAdapter, headers: Dict[str, str],
                         body: Dict[str, Any], raw_body: bytes) -> Tuple[int, Dict[str, Any]]:
    """The whole n8n router, once, for every provider."""
    started = time.monotonic()

    # ── parse (provider JSON → neutral envelope) ──────────────────────────────
    msg = adapter.parse(headers, body)
    if msg.is_status_update:
        return 200, {"status": "ok", "reason": "event_received"}

    # ── idempotency claim (webhook_logs, same ON CONFLICT trick) ──────────────
    row = await _db_execute(
        """INSERT INTO webhook_logs (provider, idempotency_key, payload, status, received_at, workflow_version)
           VALUES ($1, $2, $3::jsonb, 'pending', now(), $4)
           ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
           DO UPDATE SET id = webhook_logs.id
           RETURNING id, (xmax = 0) AS is_new""",
        adapter.provider, msg.idempotency_key, _json_compact(msg.raw_body), adapter.workflow_version)
    log_id = row["id"] if row else None   # webhook_logs.id is uuid — keep native type
    is_new = bool(row["is_new"]) if row else False
    if not is_new:
        return 200, {"status": "ok", "reason": "duplicate_ignored"}

    # ── resolve clinic/channel (adapter SQL) ──────────────────────────────────
    sql, params = adapter.channel_lookup(msg)
    ch_row = await _db_row(sql, *params)
    if not ch_row or not ch_row.get("clinic_id"):
        if log_id:
            await _mark_webhook(log_id, "ignored", error="unknown_or_disabled_channel")
        return 200, {"status": "error", "reason": "unknown_or_disabled_channel"}
    resolution = ChannelResolution(str(ch_row["clinic_id"]), str(ch_row["channel_id"]),
                                   {k: ch_row[k] for k in ch_row.keys()
                                    if k not in ("clinic_id", "channel_id")})

    # ── provider signature/authenticity ───────────────────────────────────────
    secret_row = await _db_row(
        """SELECT COALESCE(cs.provider, c.provider) AS provider,
                  cs.api_token AS api_token, cs.signing_secret AS signing_secret,
                  cs.tenant_identifier AS tenant_identifier
           FROM channels c LEFT JOIN get_channel_secret_db(c.id) cs ON true
           WHERE c.id = $1""", resolution.channel_id) or {}
    if not adapter.verify_signature(headers, raw_body, secret_row):
        if log_id:
            await _mark_webhook(log_id, "rejected", error="invalid_signature")
        return 401, {"status": "error", "reason": "invalid_signature"}

    try:
        # ── patient + conversation (same Supabase RPCs, same param names) ─────
        patient_id = _extract_patient_id(await _supabase_rpc("find_or_create_patient", {
            "p_clinic_id": resolution.clinic_id,
            "p_phone": msg.channel_patient_id,
            "p_channel_type": msg.channel_code,
            "p_channel_patient_id": msg.channel_patient_id,
            "p_full_name": msg.patient_name,
        }))
        conv_raw = await _supabase_rpc("get_or_create_channel_conversation", {
            "p_clinic_id": resolution.clinic_id,
            "p_patient_id": patient_id,
            "p_channel_id": resolution.channel_id,
            "p_channel_conversation_id": msg.channel_patient_id,
        })
        conversation_id, conv_channel_id = _extract_conversation(conv_raw)
        if conv_channel_id:
            resolution.channel_id = str(conv_channel_id)

        # ── K2 envelope + HMAC (same SQL, same serialization) ─────────────────
        core_payload = adapter.core_payload(msg, resolution, patient_id, conversation_id)
        payload_str = _json_compact(core_payload)
        sig_row = await _db_row(
            """SELECT encode(extensions.hmac($1::text, secret.signing_secret, 'sha256'), 'hex') AS signature
               FROM public.get_channel_secret_db($2::uuid) AS secret
               WHERE secret.signing_secret IS NOT NULL LIMIT 1""",
            payload_str, resolution.channel_id)
        signature = (sig_row or {}).get("signature") or ""

        # ── call the deterministic core (in-process, same signed contract) ────
        core_headers = {
            "content-type": "application/json",
            "x-k2-internal-token": settings.K2_INTERNAL_TOKEN,
            "x-k2-signature": f"sha256={signature}",
        }
        core_result = await core_runner._run(core_payload, core_headers, payload_str.encode("utf-8"))
    except Exception:
        logger.exception("channels.pipeline_failed", extra={"provider": adapter.provider})
        if log_id:
            await _mark_webhook(log_id, "error", error="pipeline_exception")
        return 500, {"ok": False, "error_code": "INTERNAL_ERROR"}

    # ── suppressed handoff / duplicate → acknowledge, stay silent ─────────────
    if _handoff_suppressed(core_result):
        if log_id:
            await _mark_webhook(log_id, "processed", resolution.clinic_id, resolution.channel_id)
        return 200, {"status": "ok", "reason": "suppressed"}

    reply_text = _extract_reply(core_result if isinstance(core_result, dict) else {}) \
        or CORE_UNREACHABLE_FALLBACK
    reply_text = reply_text[:4000]

    # ── deliver via the provider ──────────────────────────────────────────────
    send_error = None
    try:
        await adapter.send(msg, secret_row, resolution.config, reply_text)
    except Exception:
        logger.exception("channels.send_failed", extra={"provider": adapter.provider})
        send_error = "send_failed"

    if log_id:
        await _mark_webhook(log_id, "error" if send_error else "processed",
                            resolution.clinic_id, resolution.channel_id,
                            error=send_error or "")
    logger.info("channels.turn_done", extra={
        "provider": adapter.provider, "clinic_id": resolution.clinic_id,
        "response_code": core_result.get("response_code") if isinstance(core_result, dict) else None,
        "elapsed_ms": int((time.monotonic() - started) * 1000)})
    return 200, {"status": "ok"}


def supports_typing(adapter: ChannelAdapter) -> bool:
    return adapter.supports_typing
