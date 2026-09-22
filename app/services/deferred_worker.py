"""In-process deferred-batch worker — replaces the n8n 'K2 Deferred Message Worker'.

The n8n worker polled every 5s: claim a deferred batch (5s polling lease) →
rebuild one merged payload → call the core → hand the outgoing message to the
n8n Outbound Reply Dispatcher → complete/release/log. That worker predates the
K2 auth era (it posted to an unauthenticated n8n webhook) and was disabled in
production. This port keeps the exact DB contract (claim/complete/release/log
SQL verbatim) but runs the core IN-PROCESS, where normalize reads
metadata.k2_deferred_replay and the signature check relaxes accordingly.
Delivery still goes through the n8n Outbound Reply Dispatcher (still the live
staff/handoff exit) — same payload, same auth header as the n8n worker used.
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.core import alerting
from app.core.config import settings

logger = logging.getLogger(__name__)

WORKER_NAME = "k2-deferred-worker-fastapi"
LEASE_SECONDS = 120
POLL_SECONDS = 5.0
DELIVERY_OK_RE = re.compile(r"^ok:\s*[0-9a-f-]{36}$", re.IGNORECASE)

_task: Optional[asyncio.Task] = None
_stop = asyncio.Event()


async def _claim_batch() -> Optional[Dict[str, Any]]:
    from app.channels.base import _db_row
    row = await _db_row("SELECT * FROM public.k2_claim_deferred_batch_v2(%s, %s) LIMIT 1",
                        WORKER_NAME, LEASE_SECONDS)
    return row


async def _complete_batch(batch_id: str, claim_token: str, success: bool,
                          delivery_response: str) -> Optional[Dict[str, Any]]:
    from app.channels.base import _db_row
    return await _db_row(
        "SELECT * FROM public.k2_complete_deferred_batch_v2($1::uuid, $2::uuid, ($3::text = 'success'), $4::text) LIMIT 2",
        batch_id, claim_token, "success" if success else "failed", delivery_response)


async def _release_lease(conversation_id: str, lease_token: str) -> None:
    from app.channels.base import _db_row
    await _db_row("SELECT public.k2_release_deferred_conversation_lease($1::uuid, $2::uuid) AS released LIMIT 1",
                  conversation_id, lease_token)


async def _log_outcome(final_status: str, batch: Dict[str, Any], batch_id: str,
                       attempts: int, item_count: int) -> None:
    from app.channels.base import _db_row
    await _db_row(
        """SELECT * FROM public.k2_log_operational_event(
               CASE WHEN $1::text = 'completed' THEN 'deferred_completed' ELSE 'deferred_failed' END,
               $2::uuid, $3::uuid, $4::uuid, $1::text,
               CASE WHEN $1::text = 'completed' THEN 'processed' ELSE 'requeued_or_dead' END,
               jsonb_build_object('batch_id', $5::text, 'attempts', $6::integer,
                                  'item_count', $7::integer, 'worker_status', $8::text)) LIMIT 1""",
        final_status, batch.get("clinic_id"), batch.get("patient_id"),
        batch.get("conversation_id"), batch_id, attempts, item_count,
        WORKER_NAME)


def _build_core_payload(batch: Dict[str, Any]) -> Dict[str, Any]:
    """Port of 'Build Deferred Core Payload' — same fields, same id scheme."""
    b = batch or {}
    batch_id = str(b.get("batch_id") or "")
    items = b.get("message_items") if isinstance(b.get("message_items"), list) else []
    last = items[-1] if items else {}
    source_event_id = f"k2-deferred-{batch_id}"
    return {
        "clinic_id": b.get("clinic_id"),
        "patient_id": b.get("patient_id"),
        "conversation_id": b.get("conversation_id"),
        "channel_type": b.get("channel_type"),
        "channel_id": b.get("channel_id"),
        "message_id": source_event_id,
        "source_event_id": source_event_id,
        "operation_id": source_event_id,
        "message_text": str(b.get("merged_message_text") or "")[:6000],
        "received_at": last.get("received_at")
                       or datetime.now(timezone.utc).isoformat(),
        "metadata": {
            "k2_deferred_replay": True,
            "k2_deferred_batch_id": batch_id,
            "k2_deferred_item_count": len(items),
            "k2_deferred_message_ids": [x.get("message_id") for x in items
                                        if isinstance(x, dict) and x.get("message_id")],
        },
        "_worker": {
            "batch_id": batch_id,
            "claim_token": b.get("claim_token"),
            "attempts": b.get("attempts"),
            "item_count": len(items),
            "priority": b.get("priority") is True,
            "conversation_lease_token": b.get("conversation_lease_token"),
        },
    }


def _validate_delivery(raw: Any, already_processed: bool) -> bool:
    """Port of 'Validate Deferred Delivery'."""
    if already_processed:
        return True
    text = raw if isinstance(raw, str) else str(
        (raw.get("data") if isinstance(raw, dict) else None)
        or (raw.get("responseText") if isinstance(raw, dict) else None)
        or (raw.get("body") if isinstance(raw, dict) else None)
        or (raw.get("response") if isinstance(raw, dict) else None)
        or "")
    trimmed = text.strip()
    return bool(DELIVERY_OK_RE.match(trimmed)) or trimmed == "skipped: already_delivered"


async def _deliver_via_dispatcher(payload: Optional[Dict[str, Any]]) -> Any:
    """Port of 'Call K2 Outbound Dispatcher' — the n8n exit stays the live delivery path."""
    url = settings.OUTBOUND_DISPATCHER_URL
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(url, json=payload or {}, headers={
            "Authorization": f"Bearer {settings.OUTBOUND_DISPATCHER_TOKEN}"})
    if r.status_code >= 400:
        raise RuntimeError(f"dispatcher failed: {r.status_code} {r.text[:200]}")
    try:
        return r.json()
    except Exception:
        return r.text


async def process_one_batch() -> bool:
    """Claim and process one deferred batch. Returns True when a batch was claimed."""
    batch = await _claim_batch()
    if not batch or not batch.get("batch_id"):
        return False

    batch_id = str(batch["batch_id"])
    payload = _build_core_payload(batch)
    worker = payload["_worker"]
    logger.info("deferred.claimed", extra={"batch_id": batch_id, "items": worker["item_count"]})

    core_result: Dict[str, Any] = {}
    core_error: Optional[str] = None
    try:
        from app.api.v1 import message as core_runner
        from app.core.config import settings as s
        core_result = await core_runner._run(payload, {
            "content-type": "application/json",
            "x-k2-internal-token": s.K2_INTERNAL_TOKEN,
        }, json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    except Exception as exc:
        core_error = f"{type(exc).__name__}: {exc}"
        alerting.report_exception("deferred.core_failed", exc,
                                  detail=f"batch {batch_id}")

    already_processed = bool(core_result.get("duplicate") is True
                             and str(core_result.get("message") or "") == "already_processed")
    outgoing_message_id = str(core_result.get("outgoing_message_id") or "").strip()

    delivery_success = False
    delivery_response = core_error or ""
    if core_error is None:
        if already_processed or not outgoing_message_id:
            delivery_success = already_processed
            delivery_response = "already_processed" if already_processed else "no_outgoing_message_id"
        else:
            try:
                raw = await _deliver_via_dispatcher({
                    "conversation_id": payload["conversation_id"],
                    "message_id": outgoing_message_id,
                })
                delivery_success = _validate_delivery(raw, already_processed)
                delivery_response = raw if isinstance(raw, str) else json.dumps(raw)[:500]
            except Exception as exc:
                delivery_response = f"{type(exc).__name__}: {exc}"
                alerting.report_exception("deferred.delivery_failed", exc,
                                          detail=f"batch {batch_id}")

    final_status = "completed" if (core_error is None and delivery_success) else "failed"
    try:
        await _complete_batch(batch_id, worker["claim_token"], final_status == "completed",
                              delivery_response[:500])
        await _release_lease(payload["conversation_id"], worker["conversation_lease_token"])
        await _log_outcome(final_status, batch, batch_id,
                           int(worker.get("attempts") or 0), int(worker["item_count"]))
    except Exception as exc:
        alerting.report_exception("deferred.complete_failed", exc, detail=f"batch {batch_id}")

    logger.info("deferred.batch_done", extra={"batch_id": batch_id, "final_status": final_status})
    return True


async def _loop() -> None:
    logger.info("deferred.worker_started")
    while not _stop.is_set():
        try:
            claimed = await process_one_batch()
            if not claimed:
                await asyncio.sleep(POLL_SECONDS)
        except Exception:
            logger.exception("deferred.worker_loop_error")
            alerting.report_exception("deferred.worker_loop", Exception("loop error"))
            await asyncio.sleep(POLL_SECONDS)


def start() -> None:
    global _task
    if _task is None or _task.done():
        _stop.clear()
        _task = asyncio.create_task(_loop())


async def stop() -> None:
    _stop.set()
    if _task is not None:
        _task.cancel()
        try:
            await _task
        except (asyncio.CancelledError, Exception):
            pass
        _task = None
